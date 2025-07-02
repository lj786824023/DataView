# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'FileBloodUI.ui'
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
from PySide6.QtWidgets import (QApplication, QHBoxLayout, QListWidgetItem, QMainWindow,
    QSizePolicy, QSplitter, QTextEdit, QVBoxLayout,
    QWidget)

from qfluentwidgets import (BodyLabel, LineEdit, ListWidget, PushButton,
    TextEdit)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(1003, 600)
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.horizontalLayout = QHBoxLayout(self.centralwidget)
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.splitter = QSplitter(self.centralwidget)
        self.splitter.setObjectName(u"splitter")
        self.splitter.setOrientation(Qt.Horizontal)
        self.layoutWidget = QWidget(self.splitter)
        self.layoutWidget.setObjectName(u"layoutWidget")
        self.verticalLayout_11 = QVBoxLayout(self.layoutWidget)
        self.verticalLayout_11.setObjectName(u"verticalLayout_11")
        self.verticalLayout_11.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout_19 = QHBoxLayout()
        self.horizontalLayout_19.setObjectName(u"horizontalLayout_19")
        self.label_10 = BodyLabel(self.layoutWidget)
        self.label_10.setObjectName(u"label_10")
        sizePolicy = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.label_10.sizePolicy().hasHeightForWidth())
        self.label_10.setSizePolicy(sizePolicy)

        self.horizontalLayout_19.addWidget(self.label_10, 0, Qt.AlignLeft)

        self.btn_dir_blood = PushButton(self.layoutWidget)
        self.btn_dir_blood.setObjectName(u"btn_dir_blood")
        sizePolicy1 = QSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        sizePolicy1.setHorizontalStretch(0)
        sizePolicy1.setVerticalStretch(0)
        sizePolicy1.setHeightForWidth(self.btn_dir_blood.sizePolicy().hasHeightForWidth())
        self.btn_dir_blood.setSizePolicy(sizePolicy1)

        self.horizontalLayout_19.addWidget(self.btn_dir_blood, 0, Qt.AlignRight)


        self.verticalLayout_11.addLayout(self.horizontalLayout_19)

        self.edt_dir_blood = LineEdit(self.layoutWidget)
        self.edt_dir_blood.setObjectName(u"edt_dir_blood")
        sizePolicy1.setHeightForWidth(self.edt_dir_blood.sizePolicy().hasHeightForWidth())
        self.edt_dir_blood.setSizePolicy(sizePolicy1)
        self.edt_dir_blood.setReadOnly(True)

        self.verticalLayout_11.addWidget(self.edt_dir_blood)

        self.lsw_blood = ListWidget(self.layoutWidget)
        self.lsw_blood.setObjectName(u"lsw_blood")
        sizePolicy2 = QSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Expanding)
        sizePolicy2.setHorizontalStretch(0)
        sizePolicy2.setVerticalStretch(0)
        sizePolicy2.setHeightForWidth(self.lsw_blood.sizePolicy().hasHeightForWidth())
        self.lsw_blood.setSizePolicy(sizePolicy2)
        self.lsw_blood.setMinimumSize(QSize(100, 0))

        self.verticalLayout_11.addWidget(self.lsw_blood)

        self.splitter.addWidget(self.layoutWidget)
        self.layoutWidget_2 = QWidget(self.splitter)
        self.layoutWidget_2.setObjectName(u"layoutWidget_2")
        self.verticalLayout_12 = QVBoxLayout(self.layoutWidget_2)
        self.verticalLayout_12.setObjectName(u"verticalLayout_12")
        self.verticalLayout_12.setContentsMargins(0, 0, 0, 0)
        self.lab_file_name = BodyLabel(self.layoutWidget_2)
        self.lab_file_name.setObjectName(u"lab_file_name")

        self.verticalLayout_12.addWidget(self.lab_file_name)

        self.edt_file_content = TextEdit(self.layoutWidget_2)
        self.edt_file_content.setObjectName(u"edt_file_content")
        self.edt_file_content.setLineWrapMode(QTextEdit.NoWrap)

        self.verticalLayout_12.addWidget(self.edt_file_content)

        self.splitter.addWidget(self.layoutWidget_2)
        self.layoutWidget_3 = QWidget(self.splitter)
        self.layoutWidget_3.setObjectName(u"layoutWidget_3")
        self.verticalLayout_13 = QVBoxLayout(self.layoutWidget_3)
        self.verticalLayout_13.setObjectName(u"verticalLayout_13")
        self.verticalLayout_13.setContentsMargins(0, 0, 0, 0)
        self.label_9 = BodyLabel(self.layoutWidget_3)
        self.label_9.setObjectName(u"label_9")
        sizePolicy.setHeightForWidth(self.label_9.sizePolicy().hasHeightForWidth())
        self.label_9.setSizePolicy(sizePolicy)

        self.verticalLayout_13.addWidget(self.label_9)

        self.edt_file_blood = TextEdit(self.layoutWidget_3)
        self.edt_file_blood.setObjectName(u"edt_file_blood")
        sizePolicy2.setHeightForWidth(self.edt_file_blood.sizePolicy().hasHeightForWidth())
        self.edt_file_blood.setSizePolicy(sizePolicy2)
        self.edt_file_blood.setMinimumSize(QSize(100, 0))
        self.edt_file_blood.setLineWrapMode(QTextEdit.NoWrap)
        self.edt_file_blood.setReadOnly(True)

        self.verticalLayout_13.addWidget(self.edt_file_blood)

        self.splitter.addWidget(self.layoutWidget_3)

        self.horizontalLayout.addWidget(self.splitter)

        MainWindow.setCentralWidget(self.centralwidget)

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"\u4f9d\u8d56\u89e3\u6790", None))
        self.label_10.setText(QCoreApplication.translate("MainWindow", u"\u5de5\u4f5c\u76ee\u5f55\uff1a", None))
        self.btn_dir_blood.setText(QCoreApplication.translate("MainWindow", u"\u9009\u62e9", None))
        self.lab_file_name.setText(QCoreApplication.translate("MainWindow", u"\u6587\u4ef6\u540d", None))
        self.label_9.setText(QCoreApplication.translate("MainWindow", u"\u8840\u7f18\u4f9d\u8d56\uff1a", None))
    # retranslateUi

