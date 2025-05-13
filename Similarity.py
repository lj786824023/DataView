import sys
from operator import itemgetter

from PySide6 import QtWidgets
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMainWindow

from ui.SimilarityUI import Ui_MainWindow


class MyMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.pushButton.clicked.connect(self.similarity)

        # 表头居左
        self.ui.tableWidget.horizontalHeader().setDefaultAlignment(Qt.AlignLeft)

    def similarity(self):
        # 清空3，4，5列
        for row in range(self.ui.tableWidget.rowCount()):
            self.ui.tableWidget.setItem(row, 2, QtWidgets.QTableWidgetItem(""))
            self.ui.tableWidget.setItem(row, 3, QtWidgets.QTableWidgetItem(""))
            self.ui.tableWidget.setItem(row, 4, QtWidgets.QTableWidgetItem(""))
        # 获取数据
        sim_value = float(self.ui.lineEdit_1.text())
        source_cnt = int(self.ui.lineEdit_2.text())
        source_data = []  # 源字符串列表
        target_data = []  # 目标字符串列表
        for row in range(self.ui.tableWidget.rowCount()):
            source_item = self.ui.tableWidget.item(row, 0)
            target_item = self.ui.tableWidget.item(row, 1)
            if not source_item and not target_item:
                break
            if source_item and target_item:
                if source_item.text() == "" and target_item.text() == "":
                    break
            if source_item:
                source_str = source_item.text()
                source_data.append(source_str)
            if target_item:
                target_str = target_item.text()
                target_data.append(target_str)
        target_sim = []  # 结果
        for target_str in target_data:
            source_sim = []
            for source_str in source_data:
                if self.ui.cbx_case.isChecked():
                    similarity_score = round(similarity(target_str, source_str), 2)  # 相似度保留2位小数
                else:
                    similarity_score = round(similarity(target_str.upper(), source_str.upper()), 2)  # 相似度保留2位小数
                if similarity_score >= sim_value:
                    source_sim.append((source_str, similarity_score))
            source_sim.sort(key=itemgetter(1), reverse=True)  # 结果集排序
            if not source_sim and self.ui.cbx_one.isChecked():
                source_sim = [("", 0)]
            for source in source_sim[:source_cnt]:
                target_sim.append((target_str, *source))
        # 写入表格
        if len(target_sim) > self.ui.tableWidget.rowCount():
            self.ui.tableWidget.setRowCount(len(target_sim))
        for row in range(len(target_sim)):
            item1 = QtWidgets.QTableWidgetItem(target_sim[row][0])
            item2 = QtWidgets.QTableWidgetItem(target_sim[row][1])
            item3 = QtWidgets.QTableWidgetItem(str(target_sim[row][2]))
            self.ui.tableWidget.setItem(row, 2, item1)
            self.ui.tableWidget.setItem(row, 3, item2)
            self.ui.tableWidget.setItem(row, 4, item3)


def levenshtein_distance(s1, s2):
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def similarity(s1, s2):
    max_length = max(len(s1), len(s2))
    distance = levenshtein_distance(s1, s2)
    sim = 1 - distance / max_length
    return sim


# test
# s1 = "贷款合同表"
# s2 = "贷申请"
# similarity_score = similarity(s1, s2)
# print(f"The similarity score between '{s1}' and '{s2}' is {similarity_score}")

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    ui = MyMainWindow()
    ui.show()
    sys.exit(app.exec())
