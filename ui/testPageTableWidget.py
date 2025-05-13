import sys

from PySide6 import QtWidgets
from PySide6.QtWidgets import QWidget, QTableWidgetItem

from ui.PageTableWidget import Ui_Form

class PageTableWidget(QWidget,Ui_Form):

    def __init__(self,parent=None):
        # super().__init__()
        super().__init__(parent)
        self.setupUi(self)
        self.initUI()

    def initUI(self):

        self.head = []  # 表头
        self.data = []  # 数据
        # for i in range(1, 2101):  # 测试数据
        #     self.data.append([f"第{i}行，第{j}列" for j in range(1, 5)])

        self.cnt = 100  # 默认每页显示100条数
        self.cur_page = 1  # 默认当前显示页
        self.cnt_page = 1  # 默认总页数

        self.cbb_cnt.currentIndexChanged.connect(self.setCnt)
        self.btn_first.clicked.connect(lambda: self.setCur_page(1))
        self.btn_last.clicked.connect(lambda: self.setCur_page(int(self.lab_cnt_page.text())))
        self.btn_pre.clicked.connect(lambda: self.setCur_page(int(self.edt_cur_page.text()) - 1))
        self.btn_next.clicked.connect(lambda: self.setCur_page(int(self.edt_cur_page.text()) + 1))
        self.btn_jump.clicked.connect(lambda: self.setCur_page(int(self.edt_cur_page.text())))

        self.showdata()  # 显示

    def setCnt(self, index):
        self.cnt = {0: 100, 1: 1000, 2: 10000, 3: 100000}[index]
        self.setCur_page(1)
        # self.showdata()

    def setCur_page(self, index):
        if index < 1:
            index = 1
        if index > int(self.lab_cnt_page.text()):
            index = int(self.lab_cnt_page.text())
        self.cur_page = index
        self.edt_cur_page.setText(str(index))
        self.showdata()

    def showdata(self):
        """显示表格数据"""

        # 数据索引开始、结束位置
        index_begin = self.cnt * (self.cur_page - 1) if self.data else 0
        index_end = self.cnt * self.cur_page if len(self.data) > self.cnt * self.cur_page else len(self.data)

        # 写入表格
        self.tbw_table.setRowCount(index_end - index_begin)
        self.tbw_table.setColumnCount(len(self.head))
        index_cur = index_begin
        for i in range(self.tbw_table.rowCount()):
            for j in range(self.tbw_table.columnCount()):
                item = QTableWidgetItem(self.data[index_cur][j])
                self.tbw_table.setItem(i, j, item)
            index_cur += 1

        # 设置表头
        self.tbw_table.setHorizontalHeaderLabels(self.head)
        # 设置行号
        self.tbw_table.setVerticalHeaderLabels(
            [str(self.cnt * (self.cur_page - 1) + 1 + i) for i in range(self.tbw_table.rowCount())])
        self.tbw_table.resizeColumnToContents_new() # 200
        # 设置显示
        self.edt_cur_page.setText(str(self.cur_page))
        self.lab_cnt_page.setText(str(-(-len(self.data) // self.cnt) or 1))  # 向上取整


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    # w = QWidget()
    # w = QMainWindow()
    ui = PageTableWidget()
    ui.show()
    # ui.show()
    # ui.initUI()
    # w.setCentralWidget(ui)
    # w.show()
    # w.setCentralWidget(ui)
    # ui.setupUi(w)
    # ui.initUI()
    # w.show()



    sys.exit(app.exec())  # pyside6
