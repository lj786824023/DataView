import pymysql

# 数据库配置
config = {
    'host': 'localhost',  # 数据库服务器地址
    'user': 'root',  # 数据库用户名
    'password': 'rootroot',  # 数据库密码
    'database': 'test',  # 数据库名
    'charset': 'utf8mb4',  # 字符编码
    # 'cursorclass': pymysql.cursors.DictCursor  # 使用字典模式获取查询结果
}

# 连接数据库
connection = pymysql.connect(**config)

try:
    # 创建游标对象
    with connection.cursor() as cursor:
        # 执行SQL语句
        sql = "SELECT 1+1 as a union all select %(par)s as a"
        cursor.execute(sql, {'par': 'ppp'})

        # 获取查询结果
        results = cursor.fetchall()
        for row in results:
            print(row)
finally:
    connection.close()  # 关闭数据库连接
