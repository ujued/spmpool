# Python3的MySQL数据连接池spmpool

Python优点很多，适合快速开发，常用数据结构都有，且提供了很方便的api。我比较倾向于用新版的`Python3`，而它在操作MySQL数据库时，会用到`PyMySQL`驱动，我就针对这个驱动，为Python3编写了一个小巧的数据库连接池`spmpool`，使用非常方便。

```python
python3依赖包：pymysql

使用之前记得把 spmpool.py 放到lib目录中
```

### 使用方法
```
import spmpool 

# 添加连接池配置
spmpool.add_config('local', 'root', '123456', 'testdb', init_pool_size=20)
spmpool.add_config('remote', 'ujued', '123456', 'db', init_pool_size=120)

# 获取连接池  
local_pool = spmpool.spmpool('local')
remote_pool = spmpool.spmpool('remote')

# 获取连接并使用
conn = local_pool.get()
result = conn.execute('select id, name, password from user')
rows = result.fetchall()
for row in rows:
    print(row.id, row.password, row.name)
conn.close()
```
