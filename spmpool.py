from queue import Queue, Empty
from pymysql import connections
from datetime import datetime
from threading import Thread

import time
import logging


class Connection(object):
    """ 可回收connection """
    def __init__(self, pool, **kwargs):
        self._closed = False
        self._pool = pool
        self.result = None
        self.proto = connections.Connection(**kwargs)
        self.restore()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def restore(self):
        self._closed = False
        self.proto.autocommit(True)
        self.result = None

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
        self.result = ResultProxy(cursor, self._pool)
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
    configs = {
        'dev': {
            'user': 'root',
            'password': '123456',
            'database': 'test',
            'host': '127.0.0.1',
            'port': 3306,
            'charset': 'utf8',
            'init_size': 10
        }
    }
    instances = {}  # 所有数据源

    def __init__(self, init_size, **kwargs):
        self._init_size = init_size
        self._get_count = 0
        self._queue = Queue(20000)  # the queue of connection wrapper
        self.resources = []
        self._ready_closed_resources = Queue(20000 * 2)
        self._stopped = False
        self.kwargs = kwargs
        self.name = self.__class__.__name__
        self.startup()

    def __keep_alive_runner(self):
        _flag = 0
        _sleep_time = 0
        _cur_day = datetime.now().day
        while True:
            if _flag == 0:
                _flag += 1
                time.sleep(3600)
                if self._get_count < self._init_size / 5:
                    _sleep_time = 7 * 3600 / self._init_size - 10
                    logging.debug('hit 1.')
                elif self._get_count < self._init_size / 3:
                    _sleep_time = 7 * 3600 / (self._init_size - self._init_size / 5) - 5
                    logging.debug('hit 2')
                elif self._get_count < self._init_size / 2:
                    _sleep_time = 7 * 3600 / (self._init_size - self._init_size / 3) - 2
                    logging.debug('hit 3.')
                elif self._get_count < self._init_size:
                    _sleep_time = 7 * 3600 / (self._init_size - self._init_size / 2) - 2
                    logging.debug('hit 4')
                else:
                    _sleep_time = 7 * 3600
                    _flag = 0
                    self._get_count = 0
                    logging.debug('hit final.')
            conn = self.get()
            assert isinstance(conn, Connection)
            conn.execute('select 1')
            conn.close()
            time.sleep(_sleep_time)
            if datetime.now().day != _cur_day:  # 1天后重新选择keep_alive策略
                self._get_count = 0
                _cur_day = datetime.now().day
                _flag = 0

    def __monitor_runner(self):
        """ 每隔300秒检测一次未手动关闭的资源，将其加入待关闭队列 """
        while True:
            logging.info('%s has %d resources need to check to close.' % (self.name, len(self.resources)))
            for r in self.resources:
                if (datetime.now() - r['put_time']).seconds > 150:
                    self.resources.remove(r)
                    self._ready_closed_resources.put(r['value'])
            time.sleep(300)

    def __cleaner_runner(self):
        """ 执行关闭 """
        while True:
            resource = self._ready_closed_resources.get()
            if resource:
                resource.close()
                logging.info('a connection auto closed.')

    def put(self, conn):
        self._queue.put(conn)

    def get(self):
        if self._stopped:
            logging.exception("this connection pool is stopped. run this connection pool's startup() is useful!")
            return
        try:
            conn = self._queue.get_nowait()
            self._get_count += 1
            logging.info('get a connection.')
        except Empty:
            print('The init_size of ConnectionPool is empty ! waiting...')
            conn = self._queue.get()
        self.resources.append({'put_time': datetime.now(), 'value': conn})
        conn.restore()
        return conn

    def size(self):
        return self._queue.qsize()

    def extend(self, pool_or_size=10):
        """ 扩展连接池 """
        if type(pool_or_size) == ConnectionPool:
            assert isinstance(pool_or_size, ConnectionPool)
            while pool_or_size.size() > 0:
                self._queue.put(pool_or_size.get())
        elif type(pool_or_size) == int:
            for i in range(pool_or_size):
                self._queue.put(Connection(self, **self.kwargs))
        else:
            return

    def usable(self):
        """ 连接池是否可用 """
        return True if self._queue.qsize() > 0 else False

    def startup(self):
        try:
            for i in range(self._init_size):
                self._queue.put(Connection(self, **self.kwargs))
        except Exception as e:
            logging.exception('Warning:%s' % e)
            logging.exception('%d connections create successful !' % self._queue.qsize())
        self._stopped = False

    def shutdown(self):
        while self._queue.qsize() > 0:
            self._queue.get().shutdown()
        self._stopped = True

    def gc(self):
        while self._queue.qsize() > 0:
            self._queue.get().close()

    def keep_alive(self):
        kal_t = Thread(target=self.__keep_alive_runner)  # 一定的策略保持connection一直存活
        monitor_t = Thread(target=self.__monitor_runner)
        cleaner_t = Thread(target=self.__cleaner_runner)
        kal_t.setDaemon(True)
        monitor_t.setDaemon(True)
        cleaner_t.setDaemon(True)
        kal_t.start()
        monitor_t.start()
        cleaner_t.start()


class ResultProxy(object):
    """ 查询结果代理对象 """
    def __init__(self, cursor, pool):
        self._pool = pool
        self._all_results = None
        self.cursor = cursor
        self.rowcount = cursor.rowcount

    def first(self):
        data = self.cursor.fetchone()
        row = None
        if data:
            row = ResultProxy._2row(data, self.cursor.description)
            self.cursor.scroll(-1)
        self._pool.resources.append({'put_time': datetime.now(), 'value': self.cursor})
        return row

    def fetchall(self):
        data = self.cursor.fetchall()
        result = []
        desc = self.cursor.description
        for item in data:
            result.append(ResultProxy._2row(item, desc))
        if self.rowcount > 0:
            self.cursor.scroll(-self.rowcount)
        self._pool.resources.append({'put_time': datetime.now(), 'value': self.cursor})
        return result

    @classmethod
    def _2row(cls, proto_row, desc):
        row = Row()
        for i in range(len(desc)):
            col_name = desc[i][0]
            if col_name.isdigit():
                col_name = 'col%d' % i
            row.index.append(col_name)
            setattr(row, col_name, proto_row[i])
        return row

    def __iter__(self):
        result = self.fetchall()
        self._all_results = result
        return result.__iter__()

    def __next__(self):
        self._all_results.__next__()


class Row(object):
    """ 数据表行对象 """
    def __init__(self):
        self.index = []

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
    ConnectionPool.configs.update({
            pool_name: {
                'user': user,
                'password': password,
                'database': database,
                'host': host,
                'port': port,
                'charset': charset,
                'init_size': init_pool_size
            }
        }
    )


def spmpool(pool_name='dev'):
    """ 获取某个数据源的连接池 """
    if pool_name not in ConnectionPool.instances.keys():
        if pool_name not in ConnectionPool.configs.keys():
            print('none this config.')
            return
        else:
            config = ConnectionPool.configs[pool_name]
            ConnectionPool.instances.update(
                {
                    pool_name: ConnectionPool(
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
    pool = ConnectionPool.instances[pool_name]
    assert isinstance(pool, ConnectionPool)
    pool.keep_alive()  # 开启一些后台线程保持连接池高可用性
    return pool


def shutdown():
    """ 关闭连接池 """
    for instance in ConnectionPool.instances.values():
        instance.shutdown()
