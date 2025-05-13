import os
import sys

import chardet
from PySide6 import QtWidgets
from PySide6.QtWidgets import QMainWindow, QListWidgetItem, QFileDialog
from Func import pars_text, is_text_file
from Highlighter import Highlighter
from ui.FileBloodUI import Ui_MainWindow


class MyMainWindow(QMainWindow):
    def __init__(self):
        """需要修改原始tbw_1、tbw_2、tbw_3的类名MyTableWidget.CleverTableWidget()"""
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.highlighter_file_content = Highlighter(self.ui.edt_file_content.document())  # 文本文件内容高亮
        self.ui.lsw_blood.itemClicked.connect(self.show_file_content)  # 血缘文件清单项目点击
        self.ui.btn_dir_blood.clicked.connect(self.choose_dir_blood)  # 血缘选择目录按钮点击
        self.ui.edt_file_content.textChanged.connect(self.show_file_blood)  # 血缘文件内容文本改变

        # 设置血缘页左右比例
        self.ui.splitter.setStretchFactor(0, 3)
        self.ui.splitter.setStretchFactor(1, 5)
        self.ui.splitter.setStretchFactor(2, 2)

    def choose_dir_blood(self):
        """选择并设置工作目录"""
        directory = QFileDialog.getExistingDirectory(None, "选择目录", os.getcwd())  # 获取目录
        if not directory:  # 如果没有选择目录
            return
        self.ui.edt_dir_blood.setText(directory)
        file_list = os.listdir(directory)  # 获取文件列表
        file_list.sort()  # 升序
        # 添加lsw
        self.ui.lsw_blood.clear()
        self.ui.lsw_blood.addItems(file_list)

    def show_file_content(self, item: QListWidgetItem = None) -> None:
        """显示文件内容"""
        file_name = item.text()
        file_path = self.ui.edt_dir_blood.text() + "/" + file_name
        if not is_text_file(file_path):
            print(f"{file_path}不是一个文本文件！")
            return
        # 打开文件
        with open(file_path, 'rb') as f:  # 获取编码
            data = f.read()
            file_encoding = chardet.detect(data)["encoding"]
        with open(file_path, encoding=file_encoding) as f:
            file_content = f.read()
        # 显示
        self.ui.lab_file_name.setText(file_name)
        self.ui.edt_file_content.setText(file_content)

    def show_file_blood(self):
        """显示文件血缘"""
        text = self.ui.edt_file_content.toPlainText()
        tab_list = pars_text(text)
        bloods = "".join(tab + "\n" for tab in tab_list)
        self.ui.edt_file_blood.setText(bloods)


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    ui = MyMainWindow()
    ui.show()
    sys.exit(app.exec())
