


cfg = mysql_cfg = {'type': 'dameng',
                       'host': '172.20.10.3',# 172.20.10.3
                       'port': '5236',
                       'user': 'SYSDBA',
                       'password': 'SYSDBA123',
                       'schema': 'SYS'}
import dmPython
conn=dmPython.connect(user='SYSDBA',password='SYSDBA123',server= '172.20.10.3',port=5236)
cursor = conn.cursor()
cursor.execute('select username from dba_users')
values = cursor.fetchall()
print(values)
cursor.close()
conn.close()
