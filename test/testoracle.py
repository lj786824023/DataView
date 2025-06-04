import os

from loguru import logger

try:
    import cx_Oracle
    import sys

    if sys.platform.startswith("darwin"):
        lib_dir = os.path.join(os.environ.get("HOME"), "Downloads",
                               "instantclient_19_8")
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    elif sys.platform.startswith("win32"):
        # lib_dir = r"C:\Users\lojn\PycharmProjects\tool\drivers\oracle\instantclient_11_2"
        lib_dir = r"C:\Users\lojn\PycharmProjects\DataView\drivers\oracle\instantclient"
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
except Exception as err:
    logger.error("Whoops!")
    logger.error(err)

# 建立数据库连接
conn = cx_Oracle.connect('cgrzzl/cgrzzl@10.8.7.60:1521/RZZL')
conn = cx_Oracle.connect('ods/ods@10.18.106.77:1521/orcl')

# 创建游标
cursor = conn.cursor()

# 执行SQL查询
cursor.execute("select count(1) from user_procedures")

# 获取结果
for row in cursor:
    print(row)

# 关闭游标和连接
cursor.close()
conn.close()

