import shutil
import pymysql
from openpyxl import load_workbook


def get_datas(sql) -> list:
    config = {
        'host': "win.nickyhome.top",  # 数据库服务器地址
        'port': 33306,
        'database': "etl",  # 数据库名
        'user': "etl",  # 数据库用户名
        'password': "!23QweAsd",  # 数据库密码
        # 'charset': 'utf8mb4',  # 字符编码
        # 'cursorclass': pymysql.cursors.DictCursor  # 使用字典模式获取查询结果
    }
    try:
        connect = pymysql.connect(**config)
        cursor = connect.cursor()
        cursor.execute(sql)
        data = list(cursor.fetchall())
    except Exception as e:
        print("异常：", e)
    finally:
        # 释放连接
        if cursor:
            cursor.close()
            print("释放cursor")
        if connect:
            connect.close()
            print("释放connect")
    return data


if __name__ == "__main__":
    datas = get_datas("select * from etl.report_main order by report_dt")
    for line in datas:
        pass
        # 复制excel
        source_file = "项目周报.xlsx"
        destination_file = f"report/项目周报-{line[0]}.xlsx"
        # 打开新的Excel
        wb = load_workbook(source_file)
        sheet = wb.active
        # 写入主表数据
        sheet["C6"] = line[1]
        sheet["C8"] = line[2]
        sheet["I8"] = line[3]
        sheet["N8"] = line[4]
        sheet["C9"] = line[5]
        sheet["C10"] = line[6]
        sheet["I9"] = line[7]
        sheet["I10"] = line[8]
        sheet["N9"] = line[9]
        sheet["N10"] = line[10]
        sheet["C11"] = line[11]
        sheet["C12"] = line[12]
        sheet["B52"] = line[13]
        # 写入子表1数据
        sub1_datas = get_datas(f"select * from etl.report_sub_1 where report_dt={line[0]} order by id")
        i = 15
        for sub1_line in sub1_datas:  # 15~24
            sheet[f"B{i}"] = sub1_line[2]
            sheet[f"J{i}"] = sub1_line[3]
            sheet[f"K{i}"] = sub1_line[4]
            sheet[f"L{i}"] = sub1_line[5]
            sheet[f"N{i}"] = sub1_line[6]
            i += 1
        # 写入子表2数据
        sub2_datas = get_datas(f"select * from etl.report_sub_2 where report_dt={line[0]} order by id")
        i = 27
        for sub1_line in sub2_datas:  # 27~36
            sheet[f"B{i}"] = sub1_line[2]
            sheet[f"F{i}"] = sub1_line[3]
            sheet[f"H{i}"] = sub1_line[4]
            sheet[f"K{i}"] = sub1_line[5]
            sheet[f"L{i}"] = sub1_line[6]
            sheet[f"N{i}"] = sub1_line[7]
            i += 1
        # 写入子表3数据
        sub3_datas = get_datas(f"select * from etl.report_sub_3 where report_dt={line[0]} order by id")
        i = 39
        for sub1_line in sub3_datas:  # 39~41
            sheet[f"B{i}"] = sub1_line[2]
            sheet[f"F{i}"] = sub1_line[3]
            sheet[f"H{i}"] = sub1_line[4]
            sheet[f"K{i}"] = sub1_line[5]
            sheet[f"L{i}"] = sub1_line[6]
            sheet[f"N{i}"] = sub1_line[7]
            i += 1
        # 写入子表4数据
        sub4_datas = get_datas(f"select * from etl.report_sub_4 where report_dt={line[0]} order by id")
        i = 44
        for sub4_line in sub4_datas:  # 44~46
            sheet[f"B{i}"] = sub1_line[2]
            sheet[f"F{i}"] = sub1_line[3]
            sheet[f"H{i}"] = sub1_line[4]
            sheet[f"K{i}"] = sub1_line[5]
            sheet[f"L{i}"] = sub1_line[6]
            i += 1
        # 写入子表5数据
        sub5_datas = get_datas(f"select * from etl.report_sub_5 where report_dt={line[0]} order by id")
        i = 49
        for sub5_line in sub5_datas:  # 49~51
            sheet[f"B{i}"] = sub1_line[2]
            sheet[f"H{i}"] = sub1_line[3]
            sheet[f"K{i}"] = sub1_line[4]
            sheet[f"L{i}"] = sub1_line[5]
            i += 1
        # 执行保存
        wb.save(destination_file)
