python3依赖包：pymysql   
spmpool.py 放到lib目录，使用方法：   
1. python3项目中直接   
```python
import spmpool
```
2. 添加连接池配置   
```python
spmpool.add_config('local', 'root', '123456', 'testdb', init_pool_size=20)
spmpool.add_config('remote', 'ujued', '123456', 'db', host='135.23.45.1', init_pool_size=120)
```
3.获取连接池   
```python
local_pool = spmpool.spmpool('local')
remote_pool = spmpool.spmpool('remote')
```
4.获取连接并使用
```python
conn = local_pool.get()
result = conn.execute('select id, name, password from user')
rows = result.fetchall()
for row in rows:
    print(row.id, row.password, row.name)
conn.close()
```
