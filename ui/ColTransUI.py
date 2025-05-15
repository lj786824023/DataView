# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'ColTransUI.ui'
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
from PySide6.QtWidgets import (QApplication, QHBoxLayout, QHeaderView, QMainWindow,
    QSizePolicy, QSpacerItem, QTabWidget, QTableWidgetItem,
    QVBoxLayout, QWidget)

from MyWidget import TableWidget
from qfluentwidgets import (BodyLabel, PushButton)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(800, 600)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.verticalLayout = QVBoxLayout(self.centralwidget)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.label = BodyLabel(self.centralwidget)
        self.label.setObjectName(u"label")

        self.horizontalLayout.addWidget(self.label)

        self.horizontalSpacer = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout.addItem(self.horizontalSpacer)

        self.btn_trans = PushButton(self.centralwidget)
        self.btn_trans.setObjectName(u"btn_trans")
        sizePolicy = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.btn_trans.sizePolicy().hasHeightForWidth())
        self.btn_trans.setSizePolicy(sizePolicy)
        self.btn_trans.setMinimumSize(QSize(100, 0))

        self.horizontalLayout.addWidget(self.btn_trans)

        self.btn_save = PushButton(self.centralwidget)
        self.btn_save.setObjectName(u"btn_save")
        sizePolicy.setHeightForWidth(self.btn_save.sizePolicy().hasHeightForWidth())
        self.btn_save.setSizePolicy(sizePolicy)
        self.btn_save.setMinimumSize(QSize(100, 0))

        self.horizontalLayout.addWidget(self.btn_save)


        self.verticalLayout.addLayout(self.horizontalLayout)

        self.tabWidget = QTabWidget(self.centralwidget)
        self.tabWidget.setObjectName(u"tabWidget")
        self.tabWidget.setTabPosition(QTabWidget.South)
        self.tabWidget.setTabShape(QTabWidget.Triangular)
        self.tab_1 = QWidget()
        self.tab_1.setObjectName(u"tab_1")
        self.horizontalLayout_2 = QHBoxLayout(self.tab_1)
        self.horizontalLayout_2.setObjectName(u"horizontalLayout_2")
        self.tbw_1 = TableWidget(self.tab_1)
        if (self.tbw_1.columnCount() < 4):
            self.tbw_1.setColumnCount(4)
        __qtablewidgetitem = QTableWidgetItem()
        self.tbw_1.setHorizontalHeaderItem(0, __qtablewidgetitem)
        __qtablewidgetitem1 = QTableWidgetItem()
        self.tbw_1.setHorizontalHeaderItem(1, __qtablewidgetitem1)
        __qtablewidgetitem2 = QTableWidgetItem()
        self.tbw_1.setHorizontalHeaderItem(2, __qtablewidgetitem2)
        __qtablewidgetitem3 = QTableWidgetItem()
        self.tbw_1.setHorizontalHeaderItem(3, __qtablewidgetitem3)
        if (self.tbw_1.rowCount() < 50):
            self.tbw_1.setRowCount(50)
        self.tbw_1.setObjectName(u"tbw_1")
        self.tbw_1.setRowCount(50)

        self.horizontalLayout_2.addWidget(self.tbw_1)

        self.tabWidget.addTab(self.tab_1, "")
        self.tab_2 = QWidget()
        self.tab_2.setObjectName(u"tab_2")
        self.horizontalLayout_3 = QHBoxLayout(self.tab_2)
        self.horizontalLayout_3.setObjectName(u"horizontalLayout_3")
        self.tbw_2 = TableWidget(self.tab_2)
        if (self.tbw_2.columnCount() < 5):
            self.tbw_2.setColumnCount(5)
        __qtablewidgetitem4 = QTableWidgetItem()
        self.tbw_2.setHorizontalHeaderItem(0, __qtablewidgetitem4)
        __qtablewidgetitem5 = QTableWidgetItem()
        self.tbw_2.setHorizontalHeaderItem(1, __qtablewidgetitem5)
        __qtablewidgetitem6 = QTableWidgetItem()
        self.tbw_2.setHorizontalHeaderItem(2, __qtablewidgetitem6)
        __qtablewidgetitem7 = QTableWidgetItem()
        self.tbw_2.setHorizontalHeaderItem(3, __qtablewidgetitem7)
        __qtablewidgetitem8 = QTableWidgetItem()
        self.tbw_2.setHorizontalHeaderItem(4, __qtablewidgetitem8)
        if (self.tbw_2.rowCount() < 10):
            self.tbw_2.setRowCount(10)
        self.tbw_2.setObjectName(u"tbw_2")
        self.tbw_2.setRowCount(10)

        self.horizontalLayout_3.addWidget(self.tbw_2)

        self.tabWidget.addTab(self.tab_2, "")
        self.tab_3 = QWidget()
        self.tab_3.setObjectName(u"tab_3")
        self.horizontalLayout_4 = QHBoxLayout(self.tab_3)
        self.horizontalLayout_4.setObjectName(u"horizontalLayout_4")
        self.tbw_3 = TableWidget(self.tab_3)
        if (self.tbw_3.columnCount() < 3):
            self.tbw_3.setColumnCount(3)
        __qtablewidgetitem9 = QTableWidgetItem()
        self.tbw_3.setHorizontalHeaderItem(0, __qtablewidgetitem9)
        __qtablewidgetitem10 = QTableWidgetItem()
        self.tbw_3.setHorizontalHeaderItem(1, __qtablewidgetitem10)
        __qtablewidgetitem11 = QTableWidgetItem()
        self.tbw_3.setHorizontalHeaderItem(2, __qtablewidgetitem11)
        self.tbw_3.setObjectName(u"tbw_3")

        self.horizontalLayout_4.addWidget(self.tbw_3)

        self.tabWidget.addTab(self.tab_3, "")

        self.verticalLayout.addWidget(self.tabWidget)

        MainWindow.setCentralWidget(self.centralwidget)

        self.retranslateUi(MainWindow)

        self.tabWidget.setCurrentIndex(0)


        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"\u5b57\u6bb5\u7ffb\u8bd1", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"/home/lojn/abc.xlsx", None))
        self.btn_trans.setText(QCoreApplication.translate("MainWindow", u"\u7ffb\u8bd1", None))
        self.btn_save.setText(QCoreApplication.translate("MainWindow", u"\u4fdd\u5b58", None))
        ___qtablewidgetitem = self.tbw_1.horizontalHeaderItem(0)
        ___qtablewidgetitem.setText(QCoreApplication.translate("MainWindow", u"\u5b57\u6bb5\u4e2d\u6587\u540d", None));
        ___qtablewidgetitem1 = self.tbw_1.horizontalHeaderItem(1)
        ___qtablewidgetitem1.setText(QCoreApplication.translate("MainWindow", u"\u751f\u6210\u4e2d\u6587\u540d", None));
        ___qtablewidgetitem2 = self.tbw_1.horizontalHeaderItem(2)
        ___qtablewidgetitem2.setText(QCoreApplication.translate("MainWindow", u"\u751f\u6210\u82f1\u6587\u540d", None));
        ___qtablewidgetitem3 = self.tbw_1.horizontalHeaderItem(3)
        ___qtablewidgetitem3.setText(QCoreApplication.translate("MainWindow", u"\u8c03\u6574\u540e", None));
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_1), QCoreApplication.translate("MainWindow", u"\u5b57\u6bb5\u5217\u8868", None))
        ___qtablewidgetitem4 = self.tbw_2.horizontalHeaderItem(0)
        ___qtablewidgetitem4.setText(QCoreApplication.translate("MainWindow", u"\u4fee\u6539\u65e5\u671f", None));
        ___qtablewidgetitem5 = self.tbw_2.horizontalHeaderItem(1)
        ___qtablewidgetitem5.setText(QCoreApplication.translate("MainWindow", u"\u4fee\u6539\u4eba", None));
        ___qtablewidgetitem6 = self.tbw_2.horizontalHeaderItem(2)
        ___qtablewidgetitem6.setText(QCoreApplication.translate("MainWindow", u"\u4fee\u6539\u8bf4\u660e", None));
        ___qtablewidgetitem7 = self.tbw_2.horizontalHeaderItem(3)
        ___qtablewidgetitem7.setText(QCoreApplication.translate("MainWindow", u"\u4fee\u6539\u539f\u56e0", None));
        ___qtablewidgetitem8 = self.tbw_2.horizontalHeaderItem(4)
        ___qtablewidgetitem8.setText(QCoreApplication.translate("MainWindow", u"\u5907\u6ce8", None));
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), QCoreApplication.translate("MainWindow", u"\u4fee\u6539\u8bb0\u5f55", None))
        ___qtablewidgetitem9 = self.tbw_3.horizontalHeaderItem(0)
        ___qtablewidgetitem9.setText(QCoreApplication.translate("MainWindow", u"\u540d\u6b21\u4e2d\u6587", None));
        ___qtablewidgetitem10 = self.tbw_3.horizontalHeaderItem(1)
        ___qtablewidgetitem10.setText(QCoreApplication.translate("MainWindow", u"\u540d\u6b21\u82f1\u6587", None));
        ___qtablewidgetitem11 = self.tbw_3.horizontalHeaderItem(2)
        ___qtablewidgetitem11.setText(QCoreApplication.translate("MainWindow", u"\u4f7f\u7528\u6b21\u6570", None));
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_3), QCoreApplication.translate("MainWindow", u"\u672f\u8bed\u6c47\u603b", None))
    # retranslateUi

