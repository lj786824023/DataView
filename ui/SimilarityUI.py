# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'SimilarityUI.ui'
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
from PySide6.QtWidgets import (QApplication, QHBoxLayout, QHeaderView, QLabel,
    QMainWindow, QSizePolicy, QSpacerItem, QTableWidgetItem,
    QVBoxLayout, QWidget)

from qfluentwidgets import (BodyLabel, CheckBox, LineEdit, PushButton)

from MyWidget import TableWidget


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
        sizePolicy = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.label.sizePolicy().hasHeightForWidth())
        self.label.setSizePolicy(sizePolicy)

        self.horizontalLayout.addWidget(self.label)

        self.lineEdit_1 = LineEdit(self.centralwidget)
        self.lineEdit_1.setObjectName(u"lineEdit_1")
        sizePolicy1 = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        sizePolicy1.setHorizontalStretch(0)
        sizePolicy1.setVerticalStretch(0)
        sizePolicy1.setHeightForWidth(self.lineEdit_1.sizePolicy().hasHeightForWidth())
        self.lineEdit_1.setSizePolicy(sizePolicy1)
        self.lineEdit_1.setMaximumSize(QSize(50, 16777215))

        self.horizontalLayout.addWidget(self.lineEdit_1)

        self.label_3 = BodyLabel(self.centralwidget)
        self.label_3.setObjectName(u"label_3")
        sizePolicy.setHeightForWidth(self.label_3.sizePolicy().hasHeightForWidth())
        self.label_3.setSizePolicy(sizePolicy)

        self.horizontalLayout.addWidget(self.label_3)

        self.lineEdit_2 = LineEdit(self.centralwidget)
        self.lineEdit_2.setObjectName(u"lineEdit_2")
        sizePolicy1.setHeightForWidth(self.lineEdit_2.sizePolicy().hasHeightForWidth())
        self.lineEdit_2.setSizePolicy(sizePolicy1)
        self.lineEdit_2.setMinimumSize(QSize(10, 0))
        self.lineEdit_2.setMaximumSize(QSize(50, 16777215))

        self.horizontalLayout.addWidget(self.lineEdit_2)

        self.label_2 = QLabel(self.centralwidget)
        self.label_2.setObjectName(u"label_2")
        sizePolicy.setHeightForWidth(self.label_2.sizePolicy().hasHeightForWidth())
        self.label_2.setSizePolicy(sizePolicy)

        self.horizontalLayout.addWidget(self.label_2)

        self.cbx_one = CheckBox(self.centralwidget)
        self.cbx_one.setObjectName(u"cbx_one")

        self.horizontalLayout.addWidget(self.cbx_one)

        self.cbx_case = CheckBox(self.centralwidget)
        self.cbx_case.setObjectName(u"cbx_case")

        self.horizontalLayout.addWidget(self.cbx_case)

        self.horizontalSpacer = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout.addItem(self.horizontalSpacer)

        self.pushButton = PushButton(self.centralwidget)
        self.pushButton.setObjectName(u"pushButton")
        sizePolicy1.setHeightForWidth(self.pushButton.sizePolicy().hasHeightForWidth())
        self.pushButton.setSizePolicy(sizePolicy1)
        self.pushButton.setMinimumSize(QSize(100, 0))

        self.horizontalLayout.addWidget(self.pushButton)


        self.verticalLayout.addLayout(self.horizontalLayout)

        self.tableWidget = TableWidget(self.centralwidget)
        if (self.tableWidget.columnCount() < 5):
            self.tableWidget.setColumnCount(5)
        __qtablewidgetitem = QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(0, __qtablewidgetitem)
        __qtablewidgetitem1 = QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(1, __qtablewidgetitem1)
        __qtablewidgetitem2 = QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(2, __qtablewidgetitem2)
        __qtablewidgetitem3 = QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(3, __qtablewidgetitem3)
        __qtablewidgetitem4 = QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(4, __qtablewidgetitem4)
        if (self.tableWidget.rowCount() < 10):
            self.tableWidget.setRowCount(10)
        __qtablewidgetitem5 = QTableWidgetItem()
        self.tableWidget.setItem(0, 0, __qtablewidgetitem5)
        __qtablewidgetitem6 = QTableWidgetItem()
        self.tableWidget.setItem(0, 1, __qtablewidgetitem6)
        __qtablewidgetitem7 = QTableWidgetItem()
        self.tableWidget.setItem(1, 0, __qtablewidgetitem7)
        __qtablewidgetitem8 = QTableWidgetItem()
        self.tableWidget.setItem(1, 1, __qtablewidgetitem8)
        __qtablewidgetitem9 = QTableWidgetItem()
        self.tableWidget.setItem(2, 0, __qtablewidgetitem9)
        __qtablewidgetitem10 = QTableWidgetItem()
        self.tableWidget.setItem(2, 1, __qtablewidgetitem10)
        __qtablewidgetitem11 = QTableWidgetItem()
        self.tableWidget.setItem(3, 0, __qtablewidgetitem11)
        __qtablewidgetitem12 = QTableWidgetItem()
        self.tableWidget.setItem(3, 1, __qtablewidgetitem12)
        __qtablewidgetitem13 = QTableWidgetItem()
        self.tableWidget.setItem(4, 0, __qtablewidgetitem13)
        __qtablewidgetitem14 = QTableWidgetItem()
        self.tableWidget.setItem(4, 1, __qtablewidgetitem14)
        __qtablewidgetitem15 = QTableWidgetItem()
        self.tableWidget.setItem(5, 0, __qtablewidgetitem15)
        __qtablewidgetitem16 = QTableWidgetItem()
        self.tableWidget.setItem(5, 1, __qtablewidgetitem16)
        __qtablewidgetitem17 = QTableWidgetItem()
        self.tableWidget.setItem(6, 0, __qtablewidgetitem17)
        __qtablewidgetitem18 = QTableWidgetItem()
        self.tableWidget.setItem(6, 1, __qtablewidgetitem18)
        __qtablewidgetitem19 = QTableWidgetItem()
        self.tableWidget.setItem(7, 0, __qtablewidgetitem19)
        __qtablewidgetitem20 = QTableWidgetItem()
        self.tableWidget.setItem(7, 1, __qtablewidgetitem20)
        __qtablewidgetitem21 = QTableWidgetItem()
        self.tableWidget.setItem(8, 0, __qtablewidgetitem21)
        __qtablewidgetitem22 = QTableWidgetItem()
        self.tableWidget.setItem(8, 1, __qtablewidgetitem22)
        __qtablewidgetitem23 = QTableWidgetItem()
        self.tableWidget.setItem(9, 0, __qtablewidgetitem23)
        __qtablewidgetitem24 = QTableWidgetItem()
        self.tableWidget.setItem(9, 1, __qtablewidgetitem24)
        self.tableWidget.setObjectName(u"tableWidget")
        self.tableWidget.setRowCount(10)

        self.verticalLayout.addWidget(self.tableWidget)

        MainWindow.setCentralWidget(self.centralwidget)

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"\u76f8\u4f3c\u5ea6", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"\u76f8\u4f3c\u5ea6\u5927\u4e8e\uff1a", None))
        self.lineEdit_1.setText(QCoreApplication.translate("MainWindow", u"0.5", None))
        self.label_3.setText(QCoreApplication.translate("MainWindow", u"    \u6bcf\u4e2a\u76ee\u6807\u5b57\u7b26\u4e32\u63d0\u53d6", None))
        self.lineEdit_2.setText(QCoreApplication.translate("MainWindow", u"2", None))
        self.label_2.setText(QCoreApplication.translate("MainWindow", u"\u6761    ", None))
        self.cbx_one.setText(QCoreApplication.translate("MainWindow", u"\u6bcf\u4e2a\u76ee\u6807\u5b57\u7b26\u4e32\u81f3\u5c11\u51fa\u73b01\u6761", None))
        self.cbx_case.setText(QCoreApplication.translate("MainWindow", u"\u533a\u5206\u5927\u5c0f\u5199", None))
        self.pushButton.setText(QCoreApplication.translate("MainWindow", u"\u5f00\u59cb\u89e3\u6790", None))
        ___qtablewidgetitem = self.tableWidget.horizontalHeaderItem(0)
        ___qtablewidgetitem.setText(QCoreApplication.translate("MainWindow", u"[\u6570\u636e]\u6e90\u5b57\u7b26\u4e32", None));
        ___qtablewidgetitem1 = self.tableWidget.horizontalHeaderItem(1)
        ___qtablewidgetitem1.setText(QCoreApplication.translate("MainWindow", u"[\u6570\u636e]\u76ee\u6807\u5b57\u7b26\u4e32", None));
        ___qtablewidgetitem2 = self.tableWidget.horizontalHeaderItem(2)
        ___qtablewidgetitem2.setText(QCoreApplication.translate("MainWindow", u"[\u7ed3\u679c]\u76ee\u6807\u5b57\u7b26\u4e32", None));
        ___qtablewidgetitem3 = self.tableWidget.horizontalHeaderItem(3)
        ___qtablewidgetitem3.setText(QCoreApplication.translate("MainWindow", u"[\u7ed3\u679c]\u6e90\u5b57\u7b26\u4e32", None));
        ___qtablewidgetitem4 = self.tableWidget.horizontalHeaderItem(4)
        ___qtablewidgetitem4.setText(QCoreApplication.translate("MainWindow", u"[\u7ed3\u679c]\u76f8\u4f3c\u5ea6", None));

        __sortingEnabled = self.tableWidget.isSortingEnabled()
        self.tableWidget.setSortingEnabled(False)
        ___qtablewidgetitem5 = self.tableWidget.item(0, 0)
        ___qtablewidgetitem5.setText(QCoreApplication.translate("MainWindow", u"\u59d3\u540d", None));
        ___qtablewidgetitem6 = self.tableWidget.item(0, 1)
        ___qtablewidgetitem6.setText(QCoreApplication.translate("MainWindow", u"\u8d37\u6b3e\u4eba\u59d3\u540d", None));
        ___qtablewidgetitem7 = self.tableWidget.item(1, 0)
        ___qtablewidgetitem7.setText(QCoreApplication.translate("MainWindow", u"\u8eab\u4efd\u8bc1\u53f7", None));
        ___qtablewidgetitem8 = self.tableWidget.item(1, 1)
        ___qtablewidgetitem8.setText(QCoreApplication.translate("MainWindow", u"\u8eab\u4efd\u8bc1\u53f7\u7801", None));
        ___qtablewidgetitem9 = self.tableWidget.item(2, 0)
        ___qtablewidgetitem9.setText(QCoreApplication.translate("MainWindow", u"\u5730\u5740", None));
        ___qtablewidgetitem10 = self.tableWidget.item(2, 1)
        ___qtablewidgetitem10.setText(QCoreApplication.translate("MainWindow", u"\u5730\u5740\u540d", None));
        ___qtablewidgetitem11 = self.tableWidget.item(3, 0)
        ___qtablewidgetitem11.setText(QCoreApplication.translate("MainWindow", u"\u8054\u7cfb\u7535\u8bdd", None));
        ___qtablewidgetitem12 = self.tableWidget.item(3, 1)
        ___qtablewidgetitem12.setText(QCoreApplication.translate("MainWindow", u"\u8054\u7cfb\u7535\u8bdd", None));
        ___qtablewidgetitem13 = self.tableWidget.item(4, 0)
        ___qtablewidgetitem13.setText(QCoreApplication.translate("MainWindow", u"\u8d37\u6b3e\u989d", None));
        ___qtablewidgetitem14 = self.tableWidget.item(4, 1)
        ___qtablewidgetitem14.setText(QCoreApplication.translate("MainWindow", u"\u8d37\u6b3e\u989d\u5ea6", None));
        ___qtablewidgetitem15 = self.tableWidget.item(5, 0)
        ___qtablewidgetitem15.setText(QCoreApplication.translate("MainWindow", u"\u8d37\u6b3e\u5229\u7387", None));
        ___qtablewidgetitem16 = self.tableWidget.item(5, 1)
        ___qtablewidgetitem16.setText(QCoreApplication.translate("MainWindow", u"\u8d37\u6b3e", None));
        ___qtablewidgetitem17 = self.tableWidget.item(6, 0)
        ___qtablewidgetitem17.setText(QCoreApplication.translate("MainWindow", u"\u8fd8\u6b3e\u65b9\u5f0f", None));
        ___qtablewidgetitem18 = self.tableWidget.item(6, 1)
        ___qtablewidgetitem18.setText(QCoreApplication.translate("MainWindow", u"\u8fd8\u6b3e\u65b9\u5f0f", None));
        ___qtablewidgetitem19 = self.tableWidget.item(7, 0)
        ___qtablewidgetitem19.setText(QCoreApplication.translate("MainWindow", u"\u8d37\u6b3e\u5e74\u6708\u4efd", None));
        ___qtablewidgetitem20 = self.tableWidget.item(7, 1)
        ___qtablewidgetitem20.setText(QCoreApplication.translate("MainWindow", u"\u8d37\u6b3e\u5e74\u4efd", None));
        ___qtablewidgetitem21 = self.tableWidget.item(8, 0)
        ___qtablewidgetitem21.setText(QCoreApplication.translate("MainWindow", u"\u8d37\u6b3e\u65e5\u671f", None));
        ___qtablewidgetitem22 = self.tableWidget.item(8, 1)
        ___qtablewidgetitem22.setText(QCoreApplication.translate("MainWindow", u"\u8d37\u6b3e\u65e5\u671f\u5929", None));
        ___qtablewidgetitem23 = self.tableWidget.item(9, 0)
        ___qtablewidgetitem23.setText(QCoreApplication.translate("MainWindow", u"\u62c5\u4fdd\u4eba", None));
        ___qtablewidgetitem24 = self.tableWidget.item(9, 1)
        ___qtablewidgetitem24.setText(QCoreApplication.translate("MainWindow", u"\u62c5\u4fdd\u4eba\u7f16\u53f7", None));
        self.tableWidget.setSortingEnabled(__sortingEnabled)

    # retranslateUi

