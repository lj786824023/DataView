# 这里存放的是打包用到的DLL库

# 关于打包方法
# 1.打包成文件
# pyinstaller -F App.py -n app -w
#   -n 指定文件名
#   -w 执行的时候不显示cmd（测试时使用）
#
# 2.打包成文件夹
# pyinstaller -D App.py -n app -w
#   -D 打包成文件夹
#
# 3.推荐打包方式
# 以pyqt5+cx_Oracle+pyinstaller，界面连接数据库举例
#   1.开发时依赖库DLL文件放在python命令目录site-packages下
#   2.打包时先正常执行上述1或2方法
#   3.修改编译文件app.spec,将a.binaries改为
#     a.binaries+[('oraociei11.dll','C:\\Users\\lojn\\AppData\\Local\\Programs\\Python\\Python38\\Lib\\site-packages\\oraociei11.dll','BINARY'),('oci.dll','C:\\Users\\lojn\\AppData\\Local\\Programs\\Python\\Python38\\Lib\\site-packages\\oci.dll','BINARY')],
#   4.删除dist下的app.exe
#   5.执行pyinstaller app.spec
#   这样就把连同的依赖库DLL一块打包进去了
#
# 4.其他参数
#   -i 指定图片，生成应用程序图标
#     pyinstaller -F -i C:\Users\lojn\PycharmProjects\tool\img\app.ico App.py -n app
#     pyinstaller -D -w -i C:\Users\lojn\PycharmProjects\tool\img\App.ico App.py -n app
#   --add-data abc/*:def  将abc文件内的所有内容放到打包文件里的def文件夹内(_internal/def)
#
# 5.日常打包
#   pyinstaller -D -i C:\Users\lojn\PycharmProjects\DataView\img\weixinshoucang.ico --add-data drivers/dameng/dpi/*:. --add-data drivers/oracle/instantclient/*.dll:. --add-data _internal/aaa_book/*:aaa_book --add-data _internal/aaa_etc/*:aaa_etc --add-data _internal/aaa_sql/*:aaa_sql -n DataView App.py -w
#   pyinstaller -D -i C:\Users\lojn\PycharmProjects\DataView\img\weixinshoucang.ico --add-data drivers/dameng/dpi/*:. --add-data drivers/oracle/instantclient/*.dll:. --add-data _internal/aaa_book/*:aaa_book --add-data _internal/aaa_etc/*:aaa_etc --add-data _internal/aaa_sql/*:aaa_sql -n DataView App.py
#   修改app.spec
#   删除app
#   pyinstaller app.spec
#
#   pyinstaller -D -i C:\Users\lojn\PycharmProjects\tool\img\App.ico App.py -n app_test
#   修改app_test.spec
#   删除app
#   pyinstaller app_test.spec
#
#   打包后需要放入的文件夹
#     book  存放书签
#     etc   存放数据库配置文件
#     sql   存放系统sql
#
#
#


if __name__ == '__main__':
    app = QApplication(sys.argv) # 创建应用程序
    window = QMainWindow() # 创建窗口
    ui = Ui_Form() # 创建UI界面对象
    ui.setupUi(window) # 使用UI对象初始化window窗口
    window.show() # 显示窗口
    sys.exit(app.exec_()) # 主事件循环