import os
import re
import sys

from PySide6.QtGui import QSyntaxHighlighter, QTextCharFormat, QColor


class Highlighter(QSyntaxHighlighter):
    """继承高亮类QSyntaxHighlighter，注：文档类型传入之后，QSyntaxHighlighter是逐行执行高亮方法的"""

    def __init__(self, parent=None):
        super(Highlighter, self).__init__(parent)

        self.comment_flag = False  # 多行注释标志
        self.str_flag = False  # 多行字符串标志
        # 设置关键字颜色
        self.text_blue = QTextCharFormat()
        self.text_blue.setForeground(QColor("blue"))
        self.text_green = QTextCharFormat()
        self.text_green.setForeground(QColor("green"))
        self.text_gray = QTextCharFormat()
        self.text_gray.setForeground(QColor("gray"))
        self.text_red = QTextCharFormat()
        self.text_red.setForeground(QColor("red"))
        # 从配置文件读取关键字
        with open(os.path.dirname(sys.argv[0]) + "/_internal/aaa_etc/keywords_config.txt") as f:
            text = f.readlines()
        keywords = [line.replace("\n", "") for line in text]
        # 高亮规则 (正则表达式,文本格式类)
        self.highlighting_rules = []
        for keyword in keywords:  # 蓝色
            pattern = r'\b{}\b'.format(keyword)
            self.highlighting_rules.append((pattern, self.text_blue))
        # self.highlighting_rules.append((r"[\\']?'.*?[\\']?'", self.text_green))  # 绿色
        self.highlighting_rules.append((r"'.*?'", self.text_green))  # 绿色
        self.highlighting_rules.append((r"\\'.*?\\'", self.text_green))  # 绿色
        self.highlighting_rules.append((r"''.*?''", self.text_green))  # 绿色
        self.highlighting_rules.append((r"--.*", self.text_gray))  # 灰色
        self.highlighting_rules.append((r"/\*.*?\*/", self.text_gray))  # 灰色
        # self.highlighting_rules.append((r"\${.*?}", self.text_red))  # 红色

    def highlightBlock(self, text):
        """重写高亮语法，逐行执行该方法"""

        # 多行注释
        start_pattern = re.compile(r"/\*")
        end_pattern = re.compile(r"\*/")
        # 如果这行有匹配到/*没有匹配到*/，则/*之后设置未灰色，开启多行标志
        if start_pattern.search(text) and (not end_pattern.search(text)):
            self.comment_flag = True
            matchs = list(start_pattern.finditer(text))
            self.setFormat(matchs[0].start(), 1000, self.text_gray)
        # 如果这行没有匹配到/*有匹配到*/，则*/之前设置未灰色，关闭多行标志
        if (not start_pattern.search(text)) and end_pattern.search(text):
            self.comment_flag = False
            matchs = list(end_pattern.finditer(text))
            self.setFormat(0, matchs[0].end(), self.text_gray)
        # 如果开启多行标志，则整行设置为灰色，并退出函数
        if self.comment_flag:
            if not start_pattern.search(text):
                self.setFormat(0, 1000, self.text_gray)
            return
        # 多行注释

        # 多行字符串
        # """
        # 1.当识别到'为奇数到行时，开启多行模式
        # 2.当识别到'为奇数到行诗，关闭多行模式
        # """
        # result = re.findall(r"'", re.sub(r"--.*|[\\']'", "", text))  # 先把\' ''和--后面的文本排除，然后搜索'
        # if len(result) % 2 != 0:  # 如果\和'的数量是奇数，则开启多行标志
        #     self.str_flag = not self.str_flag  # 改变多行标志
        #     matchs = list(re.finditer(r"'", text))
        #     if self.str_flag:  # 如果是多行结尾，设置'前面的为绿色
        #         self.setFormat(matchs[0].start(), len(text), self.text_green)
        #     else:  # 如果是多行开始，设置'后面的为绿色
        #         self.setFormat(0, matchs[-1].end(), self.text_green)
        # if self.str_flag:  # 如果开启多行，设置整行为绿色
        #     self.setFormat(0, len(text), self.text_green)
        # 多行字符串

        # 正常高亮规则
        for rule in self.highlighting_rules:
            matchs = re.finditer(rule[0], text, re.I)  # 获取匹配到到文本
            for match in matchs:
                start = match.start()  # 索引开始索引
                length = len(match.group())  # 匹配内容长度
                self.setFormat(start, length, rule[1])  # 设置颜色
