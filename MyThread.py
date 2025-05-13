import os
import re
from datetime import datetime
from time import sleep
import pymysql
from PySide6.QtCore import QThread, Signal
from dbutils.pooled_db import PooledDB
from loguru import logger


class PoolThread(QThread):
    sign_end = Signal(bool)
    sign_err = Signal(str)

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.kwargs = kwargs

        self.pool = None  # 线程池

    def run(self):
        try:
            self.pool = {'mysql': self.mysql_pool,
                         'oracle': self.oracle_pool,
                         'gbase': self.gbase_pool,
                         'dameng': self.dameng_pool}[self.kwargs["type"]]()
            self.sign_end.emit(True)
        except Exception as e:
            self.sign_err.emit(str(e))
        finally:
            pass

    def get_connection(self):
        return self.pool.connection()

    def get_pool(self, creator):
        return {'mysql': self.mysql_pool,
                'oracle': self.oracle_pool,
                'gbase': self.gbase_pool,
                'dameng': self.dameng_pool}.get(creator)()

    def mysql_pool(self):
        return PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxconnections=10,  # 连接池允许的最大连接数
            mincached=2,  # 初始化时，链接池中至少创建的空闲的链接
            maxcached=2,  # 链接池中最多闲置的链接
            maxshared=3,  # 链接池中最多共享的链接数量
            blocking=False,  # 连接池中如果没有可用连接后，是否阻塞等待
            host=self.kwargs['host'],  # 主机号
            port=self.kwargs['port'],  # 端口号
            user=self.kwargs['user'],  # 用户名
            password=self.kwargs['password'],  # 密码
            database=self.kwargs['schema'],  # schema
            charset='utf8',  # 数据库编码
            cursorclass=pymysql.cursors.SSCursor
        )

    def oracle_pool(self):
        """ init"""
        # 设置oracle客户端
        try:
            import cx_Oracle
            import sys
            if sys.platform.startswith("darwin"):
                lib_dir = os.path.join(os.environ.get("HOME"), "Downloads",
                                       "instantclient_19_8")
                cx_Oracle.init_oracle_client(lib_dir=lib_dir)
            elif sys.platform.startswith("win32"):
                # lib_dir = r"C:\Users\lojn\PycharmProjects\tool\drivers\oracle\instantclient_11_2"
                lib_dir = os.path.dirname(sys.argv[0]) + r"/drivers/oracle/instantclient"
                cx_Oracle.init_oracle_client(lib_dir=lib_dir)
        except Exception as err:
            logger.error("Whoops!")
            logger.error(err)
            # sys.exit(1)

        oracle_dsn = cx_Oracle.makedsn(self.kwargs['host'], self.kwargs['port'], service_name=self.kwargs['schema'])
        # import oracledb
        # oracle_dsn = oracledb.makedsn(self.kwargs['host'], self.kwargs['port'], service_name=self.kwargs['schema'])
        return PooledDB(
            creator=cx_Oracle,  # 使用链接数据库的模块
            # creator=oracledb,  # 使用链接数据库的模块
            maxconnections=10,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=2,  # 链接池中最多闲置的链接，0和None不限制
            maxshared=3,  # 链接池中最多共享的链接数量，0和None表示全部共享      blocking=True, # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待
            # host=self.kwargs['host'],  # 主机号
            # port=self.kwargs['port'],  # 端口号
            dsn=oracle_dsn,  # oracle_dsn
            user=self.kwargs['user'],  # 用户名
            password=self.kwargs['password'],  # 密码
            # schema=self.kwargs['schema'],  # schema
        )

    def gbase_pool(self):
        return PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxconnections=10,  # 连接池允许的最大连接数
            mincached=2,  # 初始化时，链接池中至少创建的空闲的链接
            maxcached=2,  # 链接池中最多闲置的链接
            maxshared=3,  # 链接池中最多共享的链接数量
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待
            host=self.kwargs['host'],  # 主机号
            port=self.kwargs['port'],  # 端口号
            user=self.kwargs['user'],  # 用户名
            password=self.kwargs['password'],  # 密码
            database=self.kwargs['schema'],  # schema
            charset='utf8',  # 数据库编码
            cursorclass=pymysql.cursors.SSCursor
        )

    def dameng_pool(self):
        import dmPython
        return PooledDB(
            creator=dmPython,  # 使用链接数据库的模块
            maxconnections=10,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=2,  # 链接池中最多闲置的链接，0和None不限制
            maxshared=3,  # 链接池中最多共享的链接数量，0和None表示全部共享      blocking=True, # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            ping=0,
            # pingDM服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 7 = always
            host=self.kwargs['host'],  # 主机号
            port=self.kwargs['port'],  # 端口号
            user=self.kwargs['user'],  # 用户名
            password=self.kwargs['password'],  # 密码
            schema=self.kwargs['schema'],  # schema
            cursorclass=dmPython.TupleCursor
        )


class DBQueryThread(QThread):
    sign_end = Signal(str, dict)  # 传出数据： 影响行，头，表数据
    sign_err = Signal(str)  # 异常报错信息

    def __init__(self, connection=None, sql=None, sql_parameter=None, row_max: int = 100, obj=None):  # 连接，sql，参数
        super().__init__()
        self.connection = connection
        self.sql = sql
        self.sql_parameter = sql_parameter
        self.row_max = row_max
        self.obj = obj

        self.result = {}
        self.result["obj"] = obj

    def run(self):
        self.result["begin_time"] = datetime.now()  # 记录开始时间
        cursor = self.connection.cursor()
        sql = self.sql
        try:
            if self.sql_parameter:
                logger.info(f"执行sql：\n{self.sql}")
                cursor.execute(self.sql, self.sql_parameter)
            else:
                for sql_sub in self.sql.split(";"):
                    sql_sub = sql_sub.strip()  # 去掉两段空字符串
                    if re.findall(r"\S+", sql_sub):
                        logger.info(f"执行sql：\n{self.sql}")
                        cursor.execute(sql_sub)
                        sql = sql_sub
            self.result["end_time"] = datetime.now()  # 记录结束时间
            self.result["head"] = [line[0] for line in cursor.description] if cursor.description else []
            self.result["data"] = []
            row_max = self.row_max  # 页面传过来的最大条数
            row_fetch = 1000  # 每次取的条数
            row_get = 0  # 已经取到的条数
            # while True:
            while self.is_select_query(sql):
                if row_max - row_get < row_fetch:  # 剩余待条数小于每次取的条数时，获取剩余条数
                    row_fetch = row_max - row_get
                data = list(cursor.fetchmany(row_fetch))

                # 把clob转成普通数据
                new_data = []
                for line in data:
                    # new_line = [column if type(column) != cx_Oracle.LOB else str(column) for column in line]
                    new_line = [str(column) if column else column for column in line]
                    new_data.append(new_line)
                data = new_data

                if not data:  # 游标到底了就退出循环
                    break
                self.result["data"] += data
                row_get += row_fetch
                if row_get >= row_max:  # 如果取到的条数大于等于最大条数
                    break
            # print(f"MyThread result data 条数 ： {len(self.result['data'])}")
            l = cursor.rowcount if len(str(cursor.rowcount)) < 10 else 0
            # print(len(self.result["data"]),cursor.rowcount)
            self.result["affected_rows"] = len(self.result["data"]) or l  # pymysql limit 0时，报错，数字非常大
            self.connection.commit()
            self.sign_end.emit(self.obj, self.result)
        except Exception as e:
            logger.warning(f"sql异常：{str(e)}")
            self.sign_err.emit(str(e))
        finally:
            pass
            # self.connection.close()

    def is_select_query(self, sql):
        """ 判断是否以...开头 """
        return re.match("^(select)|(show)|(call)|(with)", sql, re.I)


class SilenceThread(QThread):
    sign_end = Signal()

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        self.func(*self.args, **self.kwargs)
        self.sign_end.emit()
