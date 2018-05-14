from queue import Queue
from threading import Thread
from pymysql import connections
from datetime import datetime
from time import sleep
import logging


# 日志配置
logging.basicConfig(level=logging.INFO, filename='spmpool.log', filemode='w',
        format='%(asctime)s %(pathname)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s ',
        datefmt='%a, %d %b %Y %H:%M:%S')


class Connection(object):
    """ 可回收connection """
    def __init__(self, pool, **kwargs):
        self._closed = False
        self._pool = pool
        self.result = None
        self.proto = connections.Connection(**kwargs)
        self._restore()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _restore(self):
        self._closed = False
        self.proto.autocommit(True)
        self.result = None
        logging.info('restore a connection.')

    def _check(self):
        if self._closed:
            logging.exception('this connection is closed.')
            raise Exception('this connection is closed.')

    def begin(self):
        self._check()
        self.proto.autocommit(False)

    def commit(self):
        self._check()
        if not self.proto.autocommit_mode:
            self.proto.commit()
            self.proto.autocommit(True)

    def rollback(self):
        self._check()
        self.proto.rollback()

    def execute(self, sql):
        self._check()
        cursor = self.proto.cursor()
        cursor.execute(sql)
        self.result = ResultProxy(cursor)
        return self.result

    def close(self):
        if not self._closed:
            self._pool.put(self)
            self._closed = True
            logging.info('a connection closed.')

    def shutdown(self):
        self.proto.close()


class ConnectionPool(object):
    """ 连接池实现 """
    _configs = {
        'dev':{
            'user': 'root',
            'password': '123456',
            'database': 'test',
            'host': '127.0.0.1',
            'port': 3306,
            'charset': 'utf8',
            'init_size': 10
        }
    }
    _instances = {}  # 所有数据源
    _resources = []  # 可以被关闭的资源，connection，cursor
    _res_queue = Queue(20000 * 2)  # 待关闭队列，两种资源，所以是20000的2倍

    def __init__(self, init_size, **kwargs):
        self._init_size = init_size
        self._queue = Queue(20000) # the queue of connection wrapper
        self.stopped = False
        self.kwargs = kwargs
        self.startup()

    def put(self, conn):
        self._queue.put(conn)

    def get(self):
        if self.stopped:
            logging.exception("this connection pool is stopped. run this connection pool's startup() is useful!")
            return
        try:
            conn = self._queue.get_nowait()
            logging.info('get a connection.')
        except Exception as e:
            print('the init_size of ConnectionPool is so small ! waiting...')
            conn = self._queue.get()
        ConnectionPool._resources.append({'pdate': datetime.now(), 'value': conn})
        conn._restore()
        return conn

    def size(self):
        return self._queue.qsize()

    def useable(self):
        return True if self._queue.qsize() > 0 else False

    def extend(self, poolorsize=10):
        if type(poolorsize) == ConnectionPool:
            while poolorsize.readycount() > 0:
                self._queue.put(poolorsize.get())
        elif type(poolorsize) == int:
            for i in range(poolorsize):
                self._queue.put(Connection(self, **self.kwargs))
        else:
            return

    def startup(self):
        try:
            for i in range(self._init_size):
                self._queue.put(Connection(self, **self.kwargs))
        except Exception as e:
            logging.exception('Warning:%s' % e)
            logging.exception('%d connections create successful !' % self._queue.qsize())
        self.stopped = False

    def shutdown(self):
        while self._queue.qsize() > 0:
            self._queue.get().shutdown()
        self.stopped = True

    def gc(self):
        while self._queue.qsize() > 0:
            self._queue.get().close()


class ResultProxy(object):
    """ 查询结果代理对象 """
    def __init__(self, cursor):
        self.cursor = cursor
        self.rowcount = cursor.rowcount

    def first(self):
        data = self.cursor.fetchone()
        row = None
        if data:
            row = self._2row(data, self.cursor.description)
            self.cursor.scroll(-1)
        ConnectionPool._resources.append({'pdate':datetime.now(), 'value': self.cursor})
        return row

    def fetchall(self):
        data = self.cursor.fetchall()
        result = []
        desc = self.cursor.description
        for item in data : result.append(self._2row(item, desc))
        if self.rowcount > 0 : self.cursor.scroll(-self.rowcount)
        ConnectionPool._resources.append({'pdate': datetime.now(), 'value': self.cursor})
        return result

    def _2row(self, proto_row, desc):
        row = _Row()
        for i in range(len(desc)):
            col_name = desc[i][0]
            if col_name.isdigit():
                col_name = 'col%d' % i
            row.index.append(col_name)
            setattr(row, col_name, proto_row[i])
        return row

    def __iter__(self):
        result = self.fetchall()
        self.allresult = result
        return result.__iter__()

    def __next__(self):
        self.allresult.__next__()


class _Row(object):
    """ 数据表行对象 """
    def __init__(self) : self.index = []

    def __getitem__(self, key):
        if type(key) == int:
            return getattr(self, self.index[key])
        else:
            return getattr(self, key)


def add_config(
        pool_name,
        user,
        password,
        database,
        host='127.0.0.1',
        port=3306,
        charset='utf8',
        init_pool_size=10):
    """ 添加数据源配置 """
    ConnectionPool._configs.update({
            pool_name:{
                'user':user,
                'password':password,
                'database':database,
                'host':host,
                'port':port,
                'charset':charset,
                'init_size':init_pool_size
            }
        }
    )


def spmpool(pool_name='dev'):
    """ 获取某个数据源的连接池 """
    if pool_name not in ConnectionPool._instances.keys():
        if pool_name not in ConnectionPool._configs.keys():
            print('none this config.')
            return
        else:
            config = ConnectionPool._configs[pool_name]
            ConnectionPool._instances.update(
                {
                    pool_name:ConnectionPool(
                        config['init_size'],
                        host=config['host'],
                        user=config['user'],
                        password=config['password'],
                        database=config['database'],
                        port=config['port'],
                        charset=config['charset']
                    )
                }
            )           
    return ConnectionPool._instances[pool_name]


def _resource_monitor(condition):
    """ 每隔300秒检测一次未手动关闭的资源，将其加入待关闭队列 """
    while condition:
        logging.info('%d resources need to close.' % len(ConnectionPool._resources))
        for r in ConnectionPool._resources:
            if (datetime.now() - r['pdate']).seconds > 150:
                ConnectionPool._resources.remove(r)
                ConnectionPool._res_queue.put(r['value'])
        sleep(300)
    logging.info('Monitor stopped.')


def _resource_cleaner(condition):
    """ 执行关闭 """
    while condition:
        r = ConnectionPool._res_queue.get()
        if r:
            r.close()
            logging.info('a connection auto closed.')
    logging.info('Cleaner stopped.')


def _spmpool_startup():
    """ 启动连接池 """
    monitor = Thread(target=_resource_monitor, args=(True,))
    cleaner = Thread(target=_resource_cleaner, args=(True,))
    monitor.setDaemon(True)
    cleaner.setDaemon(True)
    monitor.start()
    cleaner.start()


def shutdown():
    """ 关闭连接池 """
    for instance in ConnectionPool._instances.values():
        instance.shutdown()


_spmpool_startup()
