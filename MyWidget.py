import re
import sys

from PySide6 import QtWidgets
from PySide6.QtCore import Qt, Signal, Slot, QObject
from PySide6.QtGui import QAction, QColor, QShortcut, QKeySequence
from PySide6.QtWidgets import QApplication, QTableWidgetItem, QDialog, QGroupBox, QRadioButton, QVBoxLayout, \
    QPushButton, QHBoxLayout, QTableWidget, QWidget
from qfluentwidgets import RoundMenu, TableWidget, RoundMenu, SmoothScrollDelegate, TableItemDelegate, PushButton, \
    RadioButton


class TableWidget(QTableWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        # self.setColumnCount(30)
        # self.setRowCount(30)
        self.copy_action = QAction("复制", self)
        self.copy_head_action = QAction("复制表头", self)  # 复制表头
        self.paste_action = QAction("粘贴", self)
        self.clear_action = QAction("清空", self)
        self.delete_action = QAction("删除", self)
        self.align_left_action = QAction("左对齐", self)
        self.align_right_action = QAction("右对齐", self)
        self.align_center_action = QAction("居中对齐", self)
        self.highlight_action = QAction("高亮", self)
        self.cancle_highlight_action = QAction("取消高亮", self)
        self.bold_action = QAction("加粗", self)
        self.cancle_bold_action = QAction("取消加粗", self)

        self.setSelectionBehavior(QTableWidget.SelectItems)  # 选择项目
        self.horizontalHeader().setDefaultAlignment(Qt.AlignLeft)  # 表头居左
        self.horizontalHeader().setDefaultAlignment(Qt.AlignVCenter)  # 表头居左
        # self.setShowGrid(True)  # 显示网格线
        self.scrollDelagate = SmoothScrollDelegate(self)  # 优化滚动条

        self.delegate = TableItemDelegate(self)  # 优化item
        super().setItemDelegate(self.delegate)

        self.sort_flag = True  # 升序
        self.horizontalHeader().sectionDoubleClicked.connect(self.sort_result)

        self.delegate = TableItemDelegate(self)
        super().setItemDelegate(self.delegate)

        # 如下代码设置横向表格头的间隔线，有四个方向的间隔线,不需要间隔线的可以设置为0px
        self.horizontalHeader().setStyleSheet("QHeaderView::section{"
                                              "border-top:0px solid #E5E5E5;"
                                              "border-left:0px solid #E5E5E5;"
                                              "border-right:0.5px solid #E5E5E5;"
                                              "border-bottom: 0.5px solid #E5E5E5;"
                                              "background-color:white;"
                                              "padding:4px;"
                                              "}")
        # 如下代码设置纵向表格头的间隔线，有四个方向的间隔线, 不需要间隔线的可以设置为0px
        self.verticalHeader().setStyleSheet("QHeaderView::section{"
                                            "border-top:0px solid #E5E5E5;"
                                            "border-left:0px solid #E5E5E5;"
                                            "border-right:0.5px solid #E5E5E5;"
                                            "border-bottom: 0.5px solid #E5E5E5;"
                                            "background-color:white;"
                                            "padding:4px;"
                                            "}")
        # 如下代码设置列表左上角那个格子的边框线
        self.setStyleSheet(
            "QTableCornerButton::section{"
            "border-top:0px solid #E5E5E5;"
            "border-left:0px solid #E5E5E5;"
            "border-right:0.5px solid #E5E5E5;"
            "border-bottom: 0.5px solid #E5E5E5;"
            "background-color:white;"
            "}"
        )

        self.copy_action.triggered.connect(self.copy_selection)
        self.copy_head_action.triggered.connect(self.copy_head_selection)
        self.paste_action.triggered.connect(self.paste_selection)
        self.clear_action.triggered.connect(self.clear_selection)
        self.delete_action.triggered.connect(self.delete_selection)
        self.align_left_action.triggered.connect(self.align_left)
        self.align_right_action.triggered.connect(self.align_right)
        self.align_center_action.triggered.connect(self.align_center)
        self.highlight_action.triggered.connect(self.highlight_background(choice='Y'))
        self.cancle_highlight_action.triggered.connect(self.highlight_background(choice='T'))
        self.bold_action.triggered.connect(self.bold_text(choice='Y'))
        self.cancle_bold_action.triggered.connect(self.bold_text(choice='N'))

        # 标题行增加筛选按钮

        # 设置快捷键
        self.copy_action.setShortcut('Ctrl+C')  # 为action设置快捷键
        self.paste_action.setShortcut('Ctrl+V')
        self.clear_action.setShortcut('Ctrl+0')
        self.delete_action.setShortcut('Del')
        self.align_left_action.setShortcut('Ctrl+L')
        self.align_right_action.setShortcut('Ctrl+R')
        self.align_center_action.setShortcut('Ctrl+E')
        # # 按键直接绑定到函数上
        # QShortcut(QKeySequence("Ctrl+C"), self).activated.connect(self.copy_selection)
        # QShortcut(QKeySequence("Ctrl+V"), self).activated.connect(self.paste_selection)
        # QShortcut(QKeySequence("Ctrl+0"), self).activated.connect(self.clear_selection)
        # QShortcut(QKeySequence("Del"), self).activated.connect(self.delete_selection)
        # QShortcut(QKeySequence("Ctrl+L"), self).activated.connect(self.align_left)
        # QShortcut(QKeySequence("Ctrl+R"), self).activated.connect(self.align_right)
        # QShortcut(QKeySequence("Ctrl+E"), self).activated.connect(self.align_center)
        self.addAction(self.copy_action)
        self.addAction(self.copy_head_action)
        self.addAction(self.paste_action)
        self.addAction(self.clear_action)
        self.addAction(self.delete_action)
        self.addAction(self.align_left_action)
        self.addAction(self.copy_action)
        self.addAction(self.align_right_action)
        self.addAction(self.align_center_action)

        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.customContextMenuRequested.connect(self.showContextMenu)

    def showContextMenu(self, pos):
        menu = RoundMenu()
        align_menu = RoundMenu('对齐方式', self)

        insert_action = QAction("插入", self)
        insert_action_common = QAction("插入", self)
        insert_action_common.triggered.connect(self.insert_base_on_selection)

        menu.addAction(self.copy_action)
        menu.addAction(self.copy_head_action)
        menu.addAction(self.paste_action)
        menu.addAction(self.clear_action)
        menu.addAction(self.delete_action)
        align_menu.addAction(self.align_center_action)
        align_menu.addAction(self.align_left_action)
        align_menu.addAction(self.align_right_action)

        # 插入操作
        selected = self.selectedRanges()
        if len(selected) == 0:
            return
        elif len(selected) > 1:  # 不支持多重选择场景
            return
        else:
            s = selected[0]
            srn = s.bottomRow() - s.topRow() + 1
            scn = s.rightColumn() - s.leftColumn() + 1
            if srn == self.rowCount():  # 选中区域行数等于表格最大行数, 选中整列, 向左侧插入新列
                insert_action.triggered.connect(self.insert_whole_base_on_selection(insert_type='C'))
                menu.addAction(insert_action)
            elif scn == self.columnCount():  # 选中区域列数等于表格最大列数, 选中整行, 向上方插入新行
                insert_action.triggered.connect(self.insert_whole_base_on_selection(insert_type='R'))
                menu.addAction(insert_action)
            else:
                menu.addAction(insert_action_common)

        menu.addMenu(align_menu)
        menu.addAction(self.highlight_action)
        menu.addAction(self.bold_action)
        menu.addAction(self.cancle_highlight_action)
        menu.addAction(self.cancle_bold_action)
        menu.exec(self.viewport().mapToGlobal(pos))

    def get_first_empty_row_id(self):
        first_empty_row_id = 0
        for row in range(self.rowCount()):
            exist_item = False
            for column in range(self.columnCount()):
                if self.item(row, column) is not None and self.item(row, column).text() not in ['', ' ']:
                    exist_item = True
                    break
            if not exist_item:
                return first_empty_row_id
            first_empty_row_id += 1
        return first_empty_row_id

    def read_table_context(self):
        context = [[self.item(row, col).text() if self.item(row, col) is not None else ''
                    for col in range(self.columnCount())] for row in range(self.rowCount())]
        return context

    def _insert_add_row_helper(self, selected_top, selected_bottom):
        finaly_unempty_row_id = self.rowCount()
        end = False
        for row in sorted(range(self.rowCount()), reverse=True):
            finaly_unempty_row_id -= 1
            for column in range(selected_top, selected_bottom + 1):
                if self.item(row, column) is not None and self.item(row, column).text() not in ['', ' ']:
                    end = True
                    break
            if end:
                break
        return finaly_unempty_row_id

    def _insert_add_col_helper(self, selected_left, selected_right):
        finaly_unempty_col_id = self.columnCount()
        end = False
        for column in sorted(range(self.columnCount()), reverse=True):
            finaly_unempty_col_id -= 1
            for row in range(selected_left, selected_right + 1):
                if self.item(row, column) is not None and self.item(row, column).text() not in ['', ' ']:
                    end = True
                    break
            if end:
                break
        return finaly_unempty_col_id

    def _judge_rectangular_selected(self):
        selection = self.selectedRanges()
        if len(selection) != 1:
            return False
        return True

    def copy_selection(self):
        if not self._judge_rectangular_selected():
            return
        selected = self.selectedRanges()[0]
        text = "\n".join(['\t'.join([self.item(row, col).text() if self.item(row, col) is not None else ''
                                     for col in range(selected.leftColumn(), selected.rightColumn() + 1)])
                          for row in range(selected.topRow(), selected.bottomRow() + 1)])
        text += "\n" # add by ljz
        QApplication.clipboard().setText(text)

    def copy_head_selection(self):
        if not self._judge_rectangular_selected():
            return
        selected = self.selectedRanges()[0]
        text = ",".join(
            [self.horizontalHeaderItem(i).text() for i in range(selected.leftColumn(), selected.rightColumn() + 1)])
        QApplication.clipboard().setText(text)

    def paste_selection(self):
        if not self._judge_rectangular_selected():
            return
        selected = self.selectedRanges()[0]
        text = QApplication.clipboard().text()
        rows = text.split('\n')
        # if '' in rows:
        #     rows.remove('')
        if not rows[-1]: # add by ljz
            rows = rows[:-1]

        for r, row in enumerate(rows):
            if selected.topRow() + r >= self.rowCount():
                self.insertRow(selected.topRow() + r)
            cols = row.split('\t')
            for c, text in enumerate(cols):
                if selected.leftColumn() + c >= self.columnCount():
                    self.insertColumn(selected.leftColumn() + c)
                self.setItem(selected.topRow() + r, selected.leftColumn() + c, QTableWidgetItem(text))

    def clear_selection(self):
        for item in self.selectedItems():
            self.setItem(item.row(), item.column(), QTableWidgetItem(""))

    def delete_selection(self):
        if not self._judge_rectangular_selected():
            return
        delete_dialog = DeleteInsertDialog(dialog_type='delete')
        delete_dialog.DelSignal.connect(self._delete_operations)
        delete_dialog.setWindowModality(Qt.WindowModality.ApplicationModal)
        delete_dialog.exec()

    @Slot(str)
    def _delete_operations(self, message):
        selected = self.selectedRanges()[0]
        if message == 'Move Left':
            selected_cols_num = selected.rightColumn() - selected.leftColumn() + 1
            start_col_index = selected.leftColumn() + selected_cols_num
            for col in range(start_col_index, self.columnCount() + selected_cols_num):
                ori_col = col - selected_cols_num
                for row in range(selected.topRow(), selected.bottomRow() + 1):
                    if col < self.columnCount():
                        text = self.item(row, col).text() if self.item(row, col) is not None \
                            else ''
                        self.setItem(row, ori_col, QTableWidgetItem(text))
                    else:
                        self.setItem(row, ori_col, QTableWidgetItem(''))
        elif message == 'Move Up':
            selected_rows_num = selected.bottomRow() - selected.topRow() + 1
            start_row_index = selected.topRow() + selected_rows_num
            for row in range(start_row_index, self.rowCount() + selected_rows_num):
                ori_row = row - selected_rows_num
                for col in range(selected.leftColumn(), selected.rightColumn() + 1):
                    if row < self.rowCount():
                        text = self.item(row, col).text() if self.item(row, col) is not None \
                            else ''
                        self.setItem(ori_row, col, QTableWidgetItem(text))
                    else:
                        self.setItem(ori_row, col, QTableWidgetItem(''))
        elif message == 'Delete Selected Rows':
            # 从最后一列开始删除，避免删除后索引变化
            for row in sorted(range(selected.topRow(), selected.bottomRow() + 1), reverse=True):
                self.removeRow(row)
        elif message == 'Delete Selected Cols':
            # 从最后一列开始删除，避免删除后索引变化
            for column in sorted(range(selected.leftColumn(), selected.rightColumn() + 1), reverse=True):
                self.removeColumn(column)
        else:
            print('Empty Message')

    def insert_whole_base_on_selection(self, insert_type):
        def insert():
            selected = self.selectedRanges()[0]
            if insert_type == 'R':
                row_id = selected.topRow()
                for i in range(selected.bottomRow() - selected.topRow() + 1):
                    self.insertRow(row_id)
            elif insert_type == 'C':
                col_id = selected.leftColumn()
                for i in range(selected.rightColumn() - selected.leftColumn() + 1):
                    self.insertColumn(col_id)

        return insert

    def insert_base_on_selection(self):
        if not self._judge_rectangular_selected():
            return
        insert_dialog = DeleteInsertDialog(dialog_type='insert')
        insert_dialog.InsertSignal.connect(self._insert_operations)
        insert_dialog.setWindowModality(Qt.WindowModality.ApplicationModal)
        insert_dialog.exec()

    @Slot(str)
    def _insert_operations(self, message):
        selected = self.selectedRanges()[0]
        if message == 'Move Right':
            selected_cols_num = selected.rightColumn() - selected.leftColumn() + 1
            final_col = self._insert_add_col_helper(selected.topRow(), selected.bottomRow()) + 1 + selected_cols_num
            while self.columnCount() < final_col:
                self.insertColumn(self.columnCount())
            for col in sorted(range(selected.leftColumn(), final_col), reverse=True):
                ori_col = col - selected_cols_num
                for row in range(selected.topRow(), selected.bottomRow() + 1):
                    if col >= selected.leftColumn() + selected_cols_num:
                        text = self.item(row, ori_col).text() if self.item(row, ori_col) is not None else ''
                        self.setItem(row, col, QTableWidgetItem(text))
                    else:
                        self.setItem(row, col, QTableWidgetItem(''))
            print('OK')
        elif message == 'Move Down':
            selected_rows_num = selected.bottomRow() - selected.topRow() + 1
            final_row = self._insert_add_row_helper(selected.leftColumn(),
                                                    selected.rightColumn()) + 1 + selected_rows_num
            while self.rowCount() < final_row:
                self.insertRow(self.rowCount())
            for row in sorted(range(selected.topRow(), final_row), reverse=True):
                ori_row = row - selected_rows_num
                for col in range(selected.leftColumn(), selected.rightColumn() + 1):
                    if row >= selected.topRow() + selected_rows_num:
                        text = self.item(ori_row, col).text() if self.item(ori_row, col) is not None else ''
                        self.setItem(row, col, QTableWidgetItem(text))
                    else:
                        self.setItem(row, col, QTableWidgetItem(''))
        elif message == 'Insert Rows Above':
            self.insert_whole_base_on_selection('R')()
        elif message == 'Insert Cols Left':
            self.insert_whole_base_on_selection('C')()
        else:
            print('Empty Message')

    def _set_null_item(self):
        if not self._judge_rectangular_selected():
            return
        s = self.selectedRanges()[0]
        for row in range(s.topRow(), s.bottomRow() + 1):
            for col in range(s.leftColumn(), s.rightColumn() + 1):
                item = self.item(row, col)
                if item is None:
                    self.setItem(row, col, QTableWidgetItem(''))

    def align_right(self):
        self._set_null_item()
        for item in self.selectedItems():
            item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

    def align_left(self):
        self._set_null_item()
        for item in self.selectedItems():
            item.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)

    def align_center(self):
        self._set_null_item()
        for item in self.selectedItems():
            item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

    def highlight_background(self, choice):
        def highlight_operation():
            self._set_null_item()
            yellow_bg = QColor(255, 255, 0)
            transparent_bg = QColor(255, 255, 255, 0)
            for item in self.selectedItems():
                if choice == 'Y':
                    item.setBackground(yellow_bg)
                elif choice == 'T':
                    item.setBackground(transparent_bg)
                else:
                    print('highlight background error input')

        return highlight_operation

    def bold_text(self, choice):
        def bold_operation():
            self._set_null_item()
            for item in self.selectedItems():
                item_font = item.font()
                if choice == 'Y':
                    item_font.setBold(True)
                    item.setFont(item_font)
                elif choice == 'N':
                    item_font.setBold(False)
                    item.setFont(item_font)
                else:
                    print('bold operation error input')

        return bold_operation

    @staticmethod
    def write_dim2_list_to_table(dim2_list, tablewidget_object):
        for row, dim1_list in enumerate(dim2_list):
            for col, text in enumerate(dim1_list):
                item = QTableWidgetItem(str(text))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                tablewidget_object.setItem(row, col, item)

    def filter_operation(self, col, filter_list):
        for row in range(self.rowCount()):
            item = self.item(row, col)
            if item is not None and item.text() in filter_list:
                continue
            else:
                self.setRowHidden(row, True)

    def resizeColumnToContents_new(self):
        """设置每列最大宽度300"""
        super().resizeColumnsToContents()
        # 设置每列最大宽度300
        for i in range(self.columnCount()):
            width = self.columnWidth(i)
            if width > 300:
                self.setColumnWidth(i, 300)

    def sort_result(self, index=0):
        """点击表头，排序功能"""
        # tbw: QTableWidget = self.sender().parent()  # 获取点击表头的父类
        # # 获取结果集数据
        row_data = []
        for i in range(self.rowCount()):
            col_data = []
            for j in range(self.columnCount()):
                col_data.append(self.item(i, j).text())
            row_data.append(col_data)

        # 判断选中列是否全是数字
        decimal_flag = True  # 默认全数字
        for row in row_data:
            if not re.match("^[+-]?[0-9]+[.]?[0-9]*$", row[index]):
                decimal_flag = False  # 不是全数字

        if decimal_flag:
            row_data.sort(key=lambda x: float(x[index]), reverse=self.sort_flag)  # decimal排序 , reverse=True降序
        else:
            row_data.sort(key=lambda x: x[index], reverse=self.sort_flag)  # 字符排序
        self.sort_flag = not self.sort_flag

        # 显示到表单
        for row in range(self.rowCount()):
            for col in range(self.columnCount()):
                self.item(row, col).setText(row_data[row][col])


class DeleteInsertDialog(QDialog):
    DelSignal = Signal(str)
    InsertSignal = Signal(str)

    def __init__(self, dialog_type):
        super().__init__()
        self.dialog_type = dialog_type
        self.setWindowFlags(Qt.WindowType.Drawer | Qt.WindowType.WindowCloseButtonHint)
        self.dialog_typ_ch = '删除' if dialog_type.lower() == 'delete' else '插入'
        self.setWindowTitle(self.dialog_typ_ch)
        self.resize(250, 150)

        # 1. 创建GroupBox
        self.groupBox = QGroupBox(self.dialog_typ_ch)
        self.groupBox.setFlat(True)

        # 1.1 创建RadioButton
        self.radioButtonA = RadioButton()
        self.radioButtonB = RadioButton()
        self.radioButtonC = RadioButton()
        self.radioButtonD = RadioButton()

        # 1.2 将RadioButton添加到GroupBox中
        gb_vbox = QVBoxLayout()
        gb_vbox.addWidget(self.radioButtonA)
        gb_vbox.addWidget(self.radioButtonB)
        gb_vbox.addWidget(self.radioButtonC)
        gb_vbox.addWidget(self.radioButtonD)
        self.groupBox.setLayout(gb_vbox)

        # 2. 创建确定和取消按钮
        self.buttonOK = PushButton("确定")
        self.buttonCancel = PushButton("取消")

        # 2.1 将按钮添加到水平布局中
        hbox = QHBoxLayout()
        hbox.addWidget(self.buttonOK)
        hbox.addWidget(self.buttonCancel)

        # 3. 将GroupBox和按钮添加到垂直布局中
        vbox = QVBoxLayout()
        vbox.addWidget(self.groupBox)
        vbox.addLayout(hbox)

        # 4. 设置对话框的布局
        self.setLayout(vbox)
        self.buttonCancel.clicked.connect(self.close)
        self._preperation()

    def _preperation(self):
        if self.dialog_type.lower() == 'delete':
            # 1. 设置文本
            self.radioButtonA.setText('右侧单元格左移(L)')
            self.radioButtonB.setText('下方单元格上移(U)')
            self.radioButtonC.setText('整行(R)')
            self.radioButtonD.setText('整列(C)')

            # 2 设置热键
            QShortcut(QKeySequence("L"), self).activated.connect(self.radioButtonA.toggle)
            QShortcut(QKeySequence("U"), self).activated.connect(self.radioButtonB.toggle)
            QShortcut(QKeySequence("R"), self).activated.connect(self.radioButtonC.toggle)
            QShortcut(QKeySequence("C"), self).activated.connect(self.radioButtonD.toggle)

            self.buttonOK.clicked.connect(self.delete_button_ok_clicked)
        else:
            # 1. 设置文本
            self.radioButtonA.setText('活动单元格右移(R)')
            self.radioButtonB.setText('活动单元格下移(D)')
            self.radioButtonC.setText('整行(R)')
            self.radioButtonD.setText('整列(C)')

            # 2 设置热键
            QShortcut(QKeySequence("R"), self).activated.connect(self.radioButtonA.toggle)
            QShortcut(QKeySequence("D"), self).activated.connect(self.radioButtonB.toggle)
            QShortcut(QKeySequence("R"), self).activated.connect(self.radioButtonC.toggle)
            QShortcut(QKeySequence("C"), self).activated.connect(self.radioButtonD.toggle)

            self.buttonOK.clicked.connect(self.insert_button_ok_clicked)

    def delete_button_ok_clicked(self):
        if self.radioButtonA.isChecked():
            self.DelSignal.emit('Move Left')
        elif self.radioButtonB.isChecked():
            self.DelSignal.emit('Move Up')
        elif self.radioButtonC.isChecked():
            self.DelSignal.emit('Delete Selected Rows')
        elif self.radioButtonD.isChecked():
            self.DelSignal.emit('Delete Selected Cols')
        else:
            self.DelSignal.emit('')
        self.close()

    def insert_button_ok_clicked(self):
        if self.radioButtonA.isChecked():
            self.InsertSignal.emit('Move Right')
        elif self.radioButtonB.isChecked():
            self.InsertSignal.emit('Move Down')
        elif self.radioButtonC.isChecked():
            self.InsertSignal.emit('Insert Rows Above')
        elif self.radioButtonD.isChecked():
            self.InsertSignal.emit('Insert Cols Left')
        else:
            self.InsertSignal.emit('')
        self.close()


class UsingCleverTW(QDialog):  # 测试类
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("CleverTableWidget")
        self.setWindowFlags(
            Qt.WindowType.WindowMinimizeButtonHint | Qt.WindowType.WindowMaximizeButtonHint | Qt.WindowType.WindowCloseButtonHint)
        self.tableWidget = TableWidget()
        self.tableWidget.setRowCount(10)
        self.tableWidget.setColumnCount(5)

        layout = QVBoxLayout()
        layout.addWidget(self.tableWidget)
        self.setLayout(layout)
        self.add_table_content()
        self.resize(700, 400)
        self.show()

    def add_table_content(self):
        # 设置表头
        self.tableWidget.setHorizontalHeaderLabels(("1", "2", "3", "4", "5"))
        # 填充默认数据
        for i in range(10):
            for j in range(5):
                item = QTableWidgetItem('{}{}'.format(i + 1, j + 1))
                self.tableWidget.setItem(i, j, item)


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    w = UsingCleverTW()
    w.show()
    sys.exit(app.exec())  # pyside6
