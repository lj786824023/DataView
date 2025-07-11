# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'PageTableWidget.ui'
##
## Created by: Qt User Interface Compiler version 6.6.3
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
    QFont, QFontDatabase, QGradient, QIcon,
    QImage, QKeySequence, QLinearGradient, QPainter,
    QPalette, QPixmap, QRadialGradient, QTransform)
from PySide6.QtWidgets import (QApplication, QHBoxLayout, QHeaderView, QSizePolicy,
    QSpacerItem, QTableWidgetItem, QVBoxLayout, QWidget)

from MyWidget import TableWidget
from qfluentwidgets import (BodyLabel, ComboBox, LineEdit, PushButton,
    StrongBodyLabel)

class Ui_Form(object):
    def setupUi(self, Form):
        if not Form.objectName():
            Form.setObjectName(u"Form")
        Form.resize(860, 449)
        self.verticalLayout = QVBoxLayout(Form)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.verticalLayout.setContentsMargins(9, 9, 9, 9)
        self.tbw_table = TableWidget(Form)
        if (self.tbw_table.columnCount() < 3):
            self.tbw_table.setColumnCount(3)
        __qtablewidgetitem = QTableWidgetItem()
        __qtablewidgetitem.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tbw_table.setHorizontalHeaderItem(0, __qtablewidgetitem)
        __qtablewidgetitem1 = QTableWidgetItem()
        self.tbw_table.setHorizontalHeaderItem(1, __qtablewidgetitem1)
        __qtablewidgetitem2 = QTableWidgetItem()
        self.tbw_table.setHorizontalHeaderItem(2, __qtablewidgetitem2)
        if (self.tbw_table.rowCount() < 9):
            self.tbw_table.setRowCount(9)
        __qtablewidgetitem3 = QTableWidgetItem()
        self.tbw_table.setVerticalHeaderItem(0, __qtablewidgetitem3)
        __qtablewidgetitem4 = QTableWidgetItem()
        self.tbw_table.setVerticalHeaderItem(1, __qtablewidgetitem4)
        __qtablewidgetitem5 = QTableWidgetItem()
        self.tbw_table.setVerticalHeaderItem(2, __qtablewidgetitem5)
        __qtablewidgetitem6 = QTableWidgetItem()
        self.tbw_table.setVerticalHeaderItem(3, __qtablewidgetitem6)
        __qtablewidgetitem7 = QTableWidgetItem()
        self.tbw_table.setVerticalHeaderItem(4, __qtablewidgetitem7)
        __qtablewidgetitem8 = QTableWidgetItem()
        self.tbw_table.setVerticalHeaderItem(5, __qtablewidgetitem8)
        __qtablewidgetitem9 = QTableWidgetItem()
        self.tbw_table.setVerticalHeaderItem(6, __qtablewidgetitem9)
        __qtablewidgetitem10 = QTableWidgetItem()
        self.tbw_table.setVerticalHeaderItem(7, __qtablewidgetitem10)
        __qtablewidgetitem11 = QTableWidgetItem()
        self.tbw_table.setVerticalHeaderItem(8, __qtablewidgetitem11)
        __qtablewidgetitem12 = QTableWidgetItem()
        self.tbw_table.setItem(0, 0, __qtablewidgetitem12)
        __qtablewidgetitem13 = QTableWidgetItem()
        self.tbw_table.setItem(0, 1, __qtablewidgetitem13)
        __qtablewidgetitem14 = QTableWidgetItem()
        self.tbw_table.setItem(0, 2, __qtablewidgetitem14)
        __qtablewidgetitem15 = QTableWidgetItem()
        self.tbw_table.setItem(1, 0, __qtablewidgetitem15)
        __qtablewidgetitem16 = QTableWidgetItem()
        self.tbw_table.setItem(2, 0, __qtablewidgetitem16)
        __qtablewidgetitem17 = QTableWidgetItem()
        self.tbw_table.setItem(3, 0, __qtablewidgetitem17)
        __qtablewidgetitem18 = QTableWidgetItem()
        self.tbw_table.setItem(4, 0, __qtablewidgetitem18)
        __qtablewidgetitem19 = QTableWidgetItem()
        self.tbw_table.setItem(5, 0, __qtablewidgetitem19)
        __qtablewidgetitem20 = QTableWidgetItem()
        self.tbw_table.setItem(6, 0, __qtablewidgetitem20)
        __qtablewidgetitem21 = QTableWidgetItem()
        self.tbw_table.setItem(7, 0, __qtablewidgetitem21)
        __qtablewidgetitem22 = QTableWidgetItem()
        self.tbw_table.setItem(8, 0, __qtablewidgetitem22)
        __qtablewidgetitem23 = QTableWidgetItem()
        self.tbw_table.setItem(8, 1, __qtablewidgetitem23)
        __qtablewidgetitem24 = QTableWidgetItem()
        self.tbw_table.setItem(8, 2, __qtablewidgetitem24)
        self.tbw_table.setObjectName(u"tbw_table")
        self.tbw_table.setFocusPolicy(Qt.NoFocus)
        self.tbw_table.setStyleSheet(u"\n"
"/* \u5982\u4e0b\u4ee3\u7801\u8bbe\u7f6e\u5217\u8868\u5de6\u4e0a\u89d2\u90a3\u4e2a\u683c\u5b50\u7684\u8fb9\u6846\u7ebf */\n"
"QTableCornerButton::section{ \n"
"  border-top:0px solid #E5E5E5;\n"
"  border-left:0px solid #E5E5E5;\n"
"  border-right:0.5px solid #E5E5E5;\n"
"  border-bottom: 0.5px solid #E5E5E5;\n"
"  background-color:white;\n"
"}\n"
"/* \u5982\u4e0b\u4ee3\u7801\u8bbe\u7f6e\u6a2a\u5411\u8868\u683c\u5934\u7684\u95f4\u9694\u7ebf\uff0c\u6709\u56db\u4e2a\u65b9\u5411\u7684\u95f4\u9694\u7ebf,\u4e0d\u9700\u8981\u95f4\u9694\u7ebf\u7684\u53ef\u4ee5\u8bbe\u7f6e\u4e3a0px */\n"
"QHeaderView::section{\n"
"  border-top:0px solid #E5E5E5;\n"
"  border-left:0px solid #E5E5E5;\n"
"  border-right:0.5px solid #E5E5E5;\n"
"  border-bottom: 0.5px solid #E5E5E5;\n"
"  background-color:white;\n"
"  padding:4px;\n"
"}\n"
"/* \u5982\u4e0b\u4ee3\u7801\u8bbe\u7f6e\u7eb5\u5411\u8868\u683c\u5934\u7684\u95f4\u9694\u7ebf\uff0c\u6709\u56db\u4e2a\u65b9\u5411\u7684\u95f4\u9694\u7ebf, \u4e0d\u9700\u8981\u95f4\u9694\u7ebf\u7684"
                        "\u53ef\u4ee5\u8bbe\u7f6e\u4e3a0px */\n"
"QHeaderView::section{\n"
"  border-top:0px solid #E5E5E5;\n"
"  border-left:0px solid #E5E5E5;\n"
"  border-right:0.5px solid #E5E5E5;\n"
"  border-bottom: 0.5px solid #E5E5E5;\n"
"  background-color:white;\n"
"  padding:4px;\n"
"}\n"
"/* \u9009\u4e2d\u9879\u76ee */\n"
"QTableWidget::item:selected {\n"
"  background-color: rgb(225, 227, 230);  /*rgb(0, 159, 170) (225, 227, 230)*/\n"
"  border-radius: 3px; /*\u5706\u89d2\u5ea6*/\n"
"}")

        self.verticalLayout.addWidget(self.tbw_table)

        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.cbb_cnt = ComboBox(Form)
        self.cbb_cnt.addItem("")
        self.cbb_cnt.addItem("")
        self.cbb_cnt.addItem("")
        self.cbb_cnt.addItem("")
        self.cbb_cnt.setObjectName(u"cbb_cnt")
        sizePolicy = QSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.cbb_cnt.sizePolicy().hasHeightForWidth())
        self.cbb_cnt.setSizePolicy(sizePolicy)
        self.cbb_cnt.setMaximumSize(QSize(100, 16777215))

        self.horizontalLayout.addWidget(self.cbb_cnt)

        self.btn_first = PushButton(Form)
        self.btn_first.setObjectName(u"btn_first")
        sizePolicy.setHeightForWidth(self.btn_first.sizePolicy().hasHeightForWidth())
        self.btn_first.setSizePolicy(sizePolicy)
        self.btn_first.setMaximumSize(QSize(100, 16777215))

        self.horizontalLayout.addWidget(self.btn_first)

        self.btn_pre = PushButton(Form)
        self.btn_pre.setObjectName(u"btn_pre")
        sizePolicy.setHeightForWidth(self.btn_pre.sizePolicy().hasHeightForWidth())
        self.btn_pre.setSizePolicy(sizePolicy)
        self.btn_pre.setMaximumSize(QSize(100, 16777215))

        self.horizontalLayout.addWidget(self.btn_pre)

        self.edt_cur_page = LineEdit(Form)
        self.edt_cur_page.setObjectName(u"edt_cur_page")
        sizePolicy.setHeightForWidth(self.edt_cur_page.sizePolicy().hasHeightForWidth())
        self.edt_cur_page.setSizePolicy(sizePolicy)
        self.edt_cur_page.setMaximumSize(QSize(50, 16777215))
        self.edt_cur_page.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.horizontalLayout.addWidget(self.edt_cur_page)

        self.label = StrongBodyLabel(Form)
        self.label.setObjectName(u"label")

        self.horizontalLayout.addWidget(self.label)

        self.lab_cnt_page = BodyLabel(Form)
        self.lab_cnt_page.setObjectName(u"lab_cnt_page")

        self.horizontalLayout.addWidget(self.lab_cnt_page)

        self.btn_next = PushButton(Form)
        self.btn_next.setObjectName(u"btn_next")
        sizePolicy.setHeightForWidth(self.btn_next.sizePolicy().hasHeightForWidth())
        self.btn_next.setSizePolicy(sizePolicy)
        self.btn_next.setMaximumSize(QSize(100, 16777215))

        self.horizontalLayout.addWidget(self.btn_next)

        self.btn_last = PushButton(Form)
        self.btn_last.setObjectName(u"btn_last")
        sizePolicy.setHeightForWidth(self.btn_last.sizePolicy().hasHeightForWidth())
        self.btn_last.setSizePolicy(sizePolicy)
        self.btn_last.setMaximumSize(QSize(100, 16777215))

        self.horizontalLayout.addWidget(self.btn_last)

        self.btn_jump = PushButton(Form)
        self.btn_jump.setObjectName(u"btn_jump")
        sizePolicy.setHeightForWidth(self.btn_jump.sizePolicy().hasHeightForWidth())
        self.btn_jump.setSizePolicy(sizePolicy)
        self.btn_jump.setMaximumSize(QSize(100, 16777215))

        self.horizontalLayout.addWidget(self.btn_jump)

        self.horizontalSpacer = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout.addItem(self.horizontalSpacer)

        self.btn_export = PushButton(Form)
        self.btn_export.setObjectName(u"btn_export")
        sizePolicy.setHeightForWidth(self.btn_export.sizePolicy().hasHeightForWidth())
        self.btn_export.setSizePolicy(sizePolicy)
        self.btn_export.setMaximumSize(QSize(100, 16777215))

        self.horizontalLayout.addWidget(self.btn_export)

        self.horizontalLayout.setStretch(0, 1)
        self.horizontalLayout.setStretch(1, 1)
        self.horizontalLayout.setStretch(2, 1)
        self.horizontalLayout.setStretch(3, 1)
        self.horizontalLayout.setStretch(6, 1)
        self.horizontalLayout.setStretch(7, 1)
        self.horizontalLayout.setStretch(8, 1)
        self.horizontalLayout.setStretch(10, 1)

        self.verticalLayout.addLayout(self.horizontalLayout)


        self.retranslateUi(Form)

        QMetaObject.connectSlotsByName(Form)
    # setupUi

    def retranslateUi(self, Form):
        Form.setWindowTitle(QCoreApplication.translate("Form", u"tablewidget", None))
        ___qtablewidgetitem = self.tbw_table.horizontalHeaderItem(0)
        ___qtablewidgetitem.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u5217", None));
        ___qtablewidgetitem1 = self.tbw_table.horizontalHeaderItem(1)
        ___qtablewidgetitem1.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u5217", None));
        ___qtablewidgetitem2 = self.tbw_table.horizontalHeaderItem(2)
        ___qtablewidgetitem2.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u5217", None));
        ___qtablewidgetitem3 = self.tbw_table.verticalHeaderItem(0)
        ___qtablewidgetitem3.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u884c", None));
        ___qtablewidgetitem4 = self.tbw_table.verticalHeaderItem(1)
        ___qtablewidgetitem4.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u884c", None));
        ___qtablewidgetitem5 = self.tbw_table.verticalHeaderItem(2)
        ___qtablewidgetitem5.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u884c", None));
        ___qtablewidgetitem6 = self.tbw_table.verticalHeaderItem(3)
        ___qtablewidgetitem6.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u884c", None));
        ___qtablewidgetitem7 = self.tbw_table.verticalHeaderItem(4)
        ___qtablewidgetitem7.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u884c", None));
        ___qtablewidgetitem8 = self.tbw_table.verticalHeaderItem(5)
        ___qtablewidgetitem8.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u884c", None));
        ___qtablewidgetitem9 = self.tbw_table.verticalHeaderItem(6)
        ___qtablewidgetitem9.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u884c", None));
        ___qtablewidgetitem10 = self.tbw_table.verticalHeaderItem(7)
        ___qtablewidgetitem10.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u884c", None));
        ___qtablewidgetitem11 = self.tbw_table.verticalHeaderItem(8)
        ___qtablewidgetitem11.setText(QCoreApplication.translate("Form", u"\u65b0\u5efa\u884c", None));

        __sortingEnabled = self.tbw_table.isSortingEnabled()
        self.tbw_table.setSortingEnabled(False)
        ___qtablewidgetitem12 = self.tbw_table.item(0, 0)
        ___qtablewidgetitem12.setText(QCoreApplication.translate("Form", u"1", None));
        ___qtablewidgetitem13 = self.tbw_table.item(0, 1)
        ___qtablewidgetitem13.setText(QCoreApplication.translate("Form", u"2", None));
        ___qtablewidgetitem14 = self.tbw_table.item(0, 2)
        ___qtablewidgetitem14.setText(QCoreApplication.translate("Form", u"a", None));
        ___qtablewidgetitem15 = self.tbw_table.item(1, 0)
        ___qtablewidgetitem15.setText(QCoreApplication.translate("Form", u"2", None));
        ___qtablewidgetitem16 = self.tbw_table.item(2, 0)
        ___qtablewidgetitem16.setText(QCoreApplication.translate("Form", u"322", None));
        ___qtablewidgetitem17 = self.tbw_table.item(3, 0)
        ___qtablewidgetitem17.setText(QCoreApplication.translate("Form", u"4", None));
        ___qtablewidgetitem18 = self.tbw_table.item(4, 0)
        ___qtablewidgetitem18.setText(QCoreApplication.translate("Form", u"5", None));
        ___qtablewidgetitem19 = self.tbw_table.item(5, 0)
        ___qtablewidgetitem19.setText(QCoreApplication.translate("Form", u"11", None));
        ___qtablewidgetitem20 = self.tbw_table.item(6, 0)
        ___qtablewidgetitem20.setText(QCoreApplication.translate("Form", u"12", None));
        ___qtablewidgetitem21 = self.tbw_table.item(7, 0)
        ___qtablewidgetitem21.setText(QCoreApplication.translate("Form", u"13", None));
        ___qtablewidgetitem22 = self.tbw_table.item(8, 0)
        ___qtablewidgetitem22.setText(QCoreApplication.translate("Form", u"123", None));
        ___qtablewidgetitem23 = self.tbw_table.item(8, 1)
        ___qtablewidgetitem23.setText(QCoreApplication.translate("Form", u"1", None));
        ___qtablewidgetitem24 = self.tbw_table.item(8, 2)
        ___qtablewidgetitem24.setText(QCoreApplication.translate("Form", u"b", None));
        self.tbw_table.setSortingEnabled(__sortingEnabled)

        self.cbb_cnt.setItemText(0, QCoreApplication.translate("Form", u"100\u6761/\u9875", None))
        self.cbb_cnt.setItemText(1, QCoreApplication.translate("Form", u"1000\u6761/\u9875", None))
        self.cbb_cnt.setItemText(2, QCoreApplication.translate("Form", u"10000\u6761/\u9875", None))
        self.cbb_cnt.setItemText(3, QCoreApplication.translate("Form", u"100000\u6761/\u9875", None))

        self.btn_first.setText(QCoreApplication.translate("Form", u"\u9996\u9875", None))
        self.btn_pre.setText(QCoreApplication.translate("Form", u"\u4e0a\u4e00\u9875", None))
        self.edt_cur_page.setText(QCoreApplication.translate("Form", u"0", None))
        self.label.setText(QCoreApplication.translate("Form", u"/", None))
        self.lab_cnt_page.setText(QCoreApplication.translate("Form", u"0", None))
        self.btn_next.setText(QCoreApplication.translate("Form", u"\u4e0b\u4e00\u9875", None))
        self.btn_last.setText(QCoreApplication.translate("Form", u"\u5c3e\u9875", None))
        self.btn_jump.setText(QCoreApplication.translate("Form", u"\u8df3\u8f6c", None))
        self.btn_export.setText(QCoreApplication.translate("Form", u"\u5bfc\u51fa", None))
    # retranslateUi

