import os
import re

def is_text_file(file_path: str = "") -> bool:
    """检查文件是否是文本文件"""
    # 判断文件
    if os.path.isdir(file_path):
        print("选中到是一个目录！")
        return False
    # 检查文件扩展名是否常见的文本文件扩展名
    text_extensions = {'txt', 'css', 'html', 'js', 'json', 'py', 'md', 'csv', 'xml', 'sql', 'proc'}
    file_extension = os.path.splitext(file_path)[1].strip('.').lower()
    if file_extension in text_extensions:
        return True
    # 尝试检测文件编码
    with open(file_path, 'rb') as f:
        data = f.read()
    # 使用chardet检测编码
    import chardet
    result = chardet.detect(data)
    if result['confidence'] > 0.8:  # 置信度高于80%的情况下，认为是文本文件
        return True
    return False


def pars_text(text: str = "") -> list:
    """解析存储过程文本，提出血缘表，返回列表"""
    """
    1.过滤注释内容
    2.转成单行文本
    3.按分号转成多行文本
    4.提取有用的sql
    5.逐句处理
      处理规则：
        遇到from、join就换行
        from后面匹配表名，join on之间匹配表名
    """
    tab_list = []
    text = re.sub(r"--.*|/\*[\s\S]*?\*/|['|]+", '', text)  # 去掉--和/* */注释的内容
    text = re.sub(r"\n", '', text)  # 转成单行文本
    text = re.sub(r"[\t ]+", ' ', text)  # 精简内容，去掉不必要的空白内容
    text = re.sub(r";", ';\n', text)  # 按;转成多行文本
    sql_lines = re.findall(r"select .*? from .*?;|insert .*?;|delete .*?;|update .*?;", text, re.I)  # 提取含有增删改查的行
    # 逐句处理
    for sql_line in sql_lines:
        lines = re.sub(r"(from|join)", "\n\\1", sql_line, 0, re.I).splitlines()  # 遇到from join就换行，返回列表
        # 逐行处理
        for line in lines:
            # 注意理解(?<=from\s) (?:[\w_${}]+\.)的含义
            tabs = re.findall(r"(?<=from\s)(?:[\w_${}]+\.)?[\w_]+|(?<=join\s)(?:[\w_${}]+\.)?[\w_]+", line, re.I)
            tab_list += tabs
    return sorted(list(set(tab_list)))
