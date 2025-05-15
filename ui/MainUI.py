# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'MainUI.ui'
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
from PySide6.QtWidgets import (QAbstractItemView, QApplication, QFrame, QHBoxLayout,
    QHeaderView, QLabel, QLayout, QLineEdit,
    QListWidgetItem, QMainWindow, QPushButton, QSizePolicy,
    QSpacerItem, QSplitter, QStackedWidget, QTableWidget,
    QTableWidgetItem, QTextEdit, QVBoxLayout, QWidget)

from MyWidget import TableWidget
from qfluentwidgets import (BodyLabel, CaptionLabel, CheckBox, ComboBox,
    DropDownPushButton, IconWidget, LineEdit, ListWidget,
    PasswordLineEdit, Pivot, PrimaryPushButton, PushButton,
    SegmentedWidget, Slider, SmoothScrollArea, StrongBodyLabel,
    SubtitleLabel, SwitchButton, TextEdit)
from ui.testPageTableWidget import PageTableWidget

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(1178, 696)
        MainWindow.setStyleSheet(u"QMainWindow {\n"
"     background-color: rgb(237, 246, 248); /* rgb(240, 244, 249)(237, 246, 248) */\n"
" }")
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.verticalLayout_13 = QVBoxLayout(self.centralwidget)
        self.verticalLayout_13.setObjectName(u"verticalLayout_13")
        self.horizontalLayout_39 = QHBoxLayout()
        self.horizontalLayout_39.setObjectName(u"horizontalLayout_39")
        self.segw_1 = Pivot(self.centralwidget)
        self.segw_1.setObjectName(u"segw_1")
        self.horizontalLayout_19 = QHBoxLayout(self.segw_1)
        self.horizontalLayout_19.setObjectName(u"horizontalLayout_19")
        self.pushButton_1 = QPushButton(self.segw_1)
        self.pushButton_1.setObjectName(u"pushButton_1")

        self.horizontalLayout_19.addWidget(self.pushButton_1)

        self.pushButton_2 = QPushButton(self.segw_1)
        self.pushButton_2.setObjectName(u"pushButton_2")

        self.horizontalLayout_19.addWidget(self.pushButton_2)

        self.pushButton_3 = QPushButton(self.segw_1)
        self.pushButton_3.setObjectName(u"pushButton_3")

        self.horizontalLayout_19.addWidget(self.pushButton_3)

        self.pushButton_4 = QPushButton(self.segw_1)
        self.pushButton_4.setObjectName(u"pushButton_4")

        self.horizontalLayout_19.addWidget(self.pushButton_4)

        self.pushButton_5 = QPushButton(self.segw_1)
        self.pushButton_5.setObjectName(u"pushButton_5")

        self.horizontalLayout_19.addWidget(self.pushButton_5)


        self.horizontalLayout_39.addWidget(self.segw_1)

        self.horizontalSpacer = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_39.addItem(self.horizontalSpacer)

        self.horizontalLayout_21 = QHBoxLayout()
        self.horizontalLayout_21.setObjectName(u"horizontalLayout_21")
        self.cbb_choose_db = ComboBox(self.centralwidget)
        self.cbb_choose_db.addItem("")
        self.cbb_choose_db.addItem("")
        self.cbb_choose_db.addItem("")
        self.cbb_choose_db.setObjectName(u"cbb_choose_db")
        sizePolicy = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.cbb_choose_db.sizePolicy().hasHeightForWidth())
        self.cbb_choose_db.setSizePolicy(sizePolicy)
        self.cbb_choose_db.setMinimumSize(QSize(220, 0))
        self.cbb_choose_db.setMaxVisibleItems(20)

        self.horizontalLayout_21.addWidget(self.cbb_choose_db)

        self.btn_connect = PushButton(self.centralwidget)
        self.btn_connect.setObjectName(u"btn_connect")
        sizePolicy.setHeightForWidth(self.btn_connect.sizePolicy().hasHeightForWidth())
        self.btn_connect.setSizePolicy(sizePolicy)
        self.btn_connect.setMinimumSize(QSize(120, 0))

        self.horizontalLayout_21.addWidget(self.btn_connect)


        self.horizontalLayout_39.addLayout(self.horizontalLayout_21)


        self.verticalLayout_13.addLayout(self.horizontalLayout_39)

        self.stackedWidget = QStackedWidget(self.centralwidget)
        self.stackedWidget.setObjectName(u"stackedWidget")
        self.stackedWidget.setStyleSheet(u"#tab_table,#tab_proc,#tab_sql,#tab_db,#tab_settings {\n"
"  background-color: rgb(247, 249, 252); /* rgb(237, 246, 248) */\n"
"  border: 1px solid rgb(220, 220, 220); /* \u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
" }\n"
"")
        self.stackedWidget.setFrameShape(QFrame.NoFrame)
        self.stackedWidget.setMidLineWidth(0)
        self.tab_table = QWidget()
        self.tab_table.setObjectName(u"tab_table")
        self.tab_table.setStyleSheet(u"#widget_1_1,#widget_1_2,#widget_1_3 {\n"
"  background-color: rgb(241, 243, 246); /* rgb(237, 246, 248) */\n"
"  border: 1px solid rgb(220, 220, 220); /* \u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
" }")
        self.horizontalLayout_7 = QHBoxLayout(self.tab_table)
        self.horizontalLayout_7.setObjectName(u"horizontalLayout_7")
        self.splitter_table = QSplitter(self.tab_table)
        self.splitter_table.setObjectName(u"splitter_table")
        self.splitter_table.setContextMenuPolicy(Qt.NoContextMenu)
        self.splitter_table.setOrientation(Qt.Horizontal)
        self.widget_1_1 = QWidget(self.splitter_table)
        self.widget_1_1.setObjectName(u"widget_1_1")
        self.widget_1_1.setStyleSheet(u"")
        self.verticalLayout = QVBoxLayout(self.widget_1_1)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.verticalLayout.setContentsMargins(9, 9, 9, 9)
        self.cbb_database = ComboBox(self.widget_1_1)
        self.cbb_database.addItem("")
        self.cbb_database.addItem("")
        self.cbb_database.setObjectName(u"cbb_database")
        self.cbb_database.setMaxVisibleItems(20)

        self.verticalLayout.addWidget(self.cbb_database)

        self.edt_table = LineEdit(self.widget_1_1)
        self.edt_table.setObjectName(u"edt_table")

        self.verticalLayout.addWidget(self.edt_table)

        self.tbw_table = TableWidget(self.widget_1_1)
        if (self.tbw_table.columnCount() < 3):
            self.tbw_table.setColumnCount(3)
        __qtablewidgetitem = QTableWidgetItem()
        __qtablewidgetitem.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tbw_table.setHorizontalHeaderItem(0, __qtablewidgetitem)
        __qtablewidgetitem1 = QTableWidgetItem()
        __qtablewidgetitem1.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tbw_table.setHorizontalHeaderItem(1, __qtablewidgetitem1)
        __qtablewidgetitem2 = QTableWidgetItem()
        __qtablewidgetitem2.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tbw_table.setHorizontalHeaderItem(2, __qtablewidgetitem2)
        self.tbw_table.setObjectName(u"tbw_table")
        self.tbw_table.setFocusPolicy(Qt.NoFocus)
        self.tbw_table.setStyleSheet(u"QTableWidget {\n"
"  border: 1px solid rgb(220, 220, 220); /* \u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
" }\n"
"\n"
"QTableWidget::item:selected {\n"
"  background-color: rgb(225, 227, 230);  /*rgb(0, 159, 170) (225, 227, 230)*/\n"
"  border-radius: 3px; /*\u5706\u89d2\u5ea6*/\n"
"}")
        self.tbw_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbw_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.tbw_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbw_table.verticalHeader().setVisible(False)

        self.verticalLayout.addWidget(self.tbw_table)

        self.splitter_table.addWidget(self.widget_1_1)
        self.widget_1_2 = QWidget(self.splitter_table)
        self.widget_1_2.setObjectName(u"widget_1_2")
        self.verticalLayout_6 = QVBoxLayout(self.widget_1_2)
        self.verticalLayout_6.setObjectName(u"verticalLayout_6")
        self.verticalLayout_6.setContentsMargins(9, 9, 9, 9)
        self.horizontalLayout_13 = QHBoxLayout()
        self.horizontalLayout_13.setObjectName(u"horizontalLayout_13")
        self.cbb_find_col_1 = ComboBox(self.widget_1_2)
        self.cbb_find_col_1.addItem("")
        self.cbb_find_col_1.setObjectName(u"cbb_find_col_1")
        sizePolicy.setHeightForWidth(self.cbb_find_col_1.sizePolicy().hasHeightForWidth())
        self.cbb_find_col_1.setSizePolicy(sizePolicy)
        self.cbb_find_col_1.setMinimumSize(QSize(80, 0))

        self.horizontalLayout_13.addWidget(self.cbb_find_col_1)

        self.edt_find_str_1 = LineEdit(self.widget_1_2)
        self.edt_find_str_1.setObjectName(u"edt_find_str_1")
        sizePolicy1 = QSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        sizePolicy1.setHorizontalStretch(0)
        sizePolicy1.setVerticalStretch(0)
        sizePolicy1.setHeightForWidth(self.edt_find_str_1.sizePolicy().hasHeightForWidth())
        self.edt_find_str_1.setSizePolicy(sizePolicy1)

        self.horizontalLayout_13.addWidget(self.edt_find_str_1)

        self.cbb_find_col_2 = ComboBox(self.widget_1_2)
        self.cbb_find_col_2.addItem("")
        self.cbb_find_col_2.setObjectName(u"cbb_find_col_2")
        sizePolicy.setHeightForWidth(self.cbb_find_col_2.sizePolicy().hasHeightForWidth())
        self.cbb_find_col_2.setSizePolicy(sizePolicy)
        self.cbb_find_col_2.setMinimumSize(QSize(80, 0))

        self.horizontalLayout_13.addWidget(self.cbb_find_col_2)

        self.edt_find_str_2 = LineEdit(self.widget_1_2)
        self.edt_find_str_2.setObjectName(u"edt_find_str_2")
        sizePolicy1.setHeightForWidth(self.edt_find_str_2.sizePolicy().hasHeightForWidth())
        self.edt_find_str_2.setSizePolicy(sizePolicy1)

        self.horizontalLayout_13.addWidget(self.edt_find_str_2)

        self.cbb_find_col_3 = ComboBox(self.widget_1_2)
        self.cbb_find_col_3.addItem("")
        self.cbb_find_col_3.setObjectName(u"cbb_find_col_3")
        sizePolicy.setHeightForWidth(self.cbb_find_col_3.sizePolicy().hasHeightForWidth())
        self.cbb_find_col_3.setSizePolicy(sizePolicy)
        self.cbb_find_col_3.setMinimumSize(QSize(80, 0))

        self.horizontalLayout_13.addWidget(self.cbb_find_col_3)

        self.edt_find_str_3 = LineEdit(self.widget_1_2)
        self.edt_find_str_3.setObjectName(u"edt_find_str_3")
        sizePolicy1.setHeightForWidth(self.edt_find_str_3.sizePolicy().hasHeightForWidth())
        self.edt_find_str_3.setSizePolicy(sizePolicy1)

        self.horizontalLayout_13.addWidget(self.edt_find_str_3)


        self.verticalLayout_6.addLayout(self.horizontalLayout_13)

        self.horizontalLayout_15 = QHBoxLayout()
        self.horizontalLayout_15.setObjectName(u"horizontalLayout_15")
        self.edt_result_row = LineEdit(self.widget_1_2)
        self.edt_result_row.setObjectName(u"edt_result_row")
        sizePolicy.setHeightForWidth(self.edt_result_row.sizePolicy().hasHeightForWidth())
        self.edt_result_row.setSizePolicy(sizePolicy)
        self.edt_result_row.setInputMethodHints(Qt.ImhNone)

        self.horizontalLayout_15.addWidget(self.edt_result_row)

        self.btn_get_data = PushButton(self.widget_1_2)
        self.btn_get_data.setObjectName(u"btn_get_data")
        sizePolicy.setHeightForWidth(self.btn_get_data.sizePolicy().hasHeightForWidth())
        self.btn_get_data.setSizePolicy(sizePolicy)
        self.btn_get_data.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_15.addWidget(self.btn_get_data)

        self.btn_reset = PushButton(self.widget_1_2)
        self.btn_reset.setObjectName(u"btn_reset")
        sizePolicy.setHeightForWidth(self.btn_reset.sizePolicy().hasHeightForWidth())
        self.btn_reset.setSizePolicy(sizePolicy)
        self.btn_reset.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_15.addWidget(self.btn_reset)

        self.cbx_is_sensitive = CheckBox(self.widget_1_2)
        self.cbx_is_sensitive.setObjectName(u"cbx_is_sensitive")

        self.horizontalLayout_15.addWidget(self.cbx_is_sensitive)

        self.horizontalSpacer_7 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_15.addItem(self.horizontalSpacer_7)

        self.btn_mapping = PushButton(self.widget_1_2)
        self.btn_mapping.setObjectName(u"btn_mapping")
        sizePolicy.setHeightForWidth(self.btn_mapping.sizePolicy().hasHeightForWidth())
        self.btn_mapping.setSizePolicy(sizePolicy)
        self.btn_mapping.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_15.addWidget(self.btn_mapping)

        self.btn_trans = DropDownPushButton(self.widget_1_2)
        self.btn_trans.setObjectName(u"btn_trans")
        sizePolicy.setHeightForWidth(self.btn_trans.sizePolicy().hasHeightForWidth())
        self.btn_trans.setSizePolicy(sizePolicy)
        self.btn_trans.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_15.addWidget(self.btn_trans)

        self.btn_trans_setting = PushButton(self.widget_1_2)
        self.btn_trans_setting.setObjectName(u"btn_trans_setting")
        sizePolicy.setHeightForWidth(self.btn_trans_setting.sizePolicy().hasHeightForWidth())
        self.btn_trans_setting.setSizePolicy(sizePolicy)
        self.btn_trans_setting.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_15.addWidget(self.btn_trans_setting)


        self.verticalLayout_6.addLayout(self.horizontalLayout_15)

        self.horizontalLayout_12 = QHBoxLayout()
        self.horizontalLayout_12.setObjectName(u"horizontalLayout_12")
        self.lab_table_comment = BodyLabel(self.widget_1_2)
        self.lab_table_comment.setObjectName(u"lab_table_comment")
        self.lab_table_comment.setEnabled(True)
        sizePolicy2 = QSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        sizePolicy2.setHorizontalStretch(0)
        sizePolicy2.setVerticalStretch(0)
        sizePolicy2.setHeightForWidth(self.lab_table_comment.sizePolicy().hasHeightForWidth())
        self.lab_table_comment.setSizePolicy(sizePolicy2)

        self.horizontalLayout_12.addWidget(self.lab_table_comment)


        self.verticalLayout_6.addLayout(self.horizontalLayout_12)

        self.tbw_column = PageTableWidget(self.widget_1_2)
        self.tbw_column.setObjectName(u"tbw_column")
        self.horizontalLayout_26 = QHBoxLayout(self.tbw_column)
        self.horizontalLayout_26.setObjectName(u"horizontalLayout_26")
        self.tableWidget = TableWidget(self.tbw_column)
        if (self.tableWidget.columnCount() < 6):
            self.tableWidget.setColumnCount(6)
        __qtablewidgetitem3 = QTableWidgetItem()
        __qtablewidgetitem3.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tableWidget.setHorizontalHeaderItem(0, __qtablewidgetitem3)
        __qtablewidgetitem4 = QTableWidgetItem()
        __qtablewidgetitem4.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tableWidget.setHorizontalHeaderItem(1, __qtablewidgetitem4)
        __qtablewidgetitem5 = QTableWidgetItem()
        __qtablewidgetitem5.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tableWidget.setHorizontalHeaderItem(2, __qtablewidgetitem5)
        __qtablewidgetitem6 = QTableWidgetItem()
        __qtablewidgetitem6.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tableWidget.setHorizontalHeaderItem(3, __qtablewidgetitem6)
        __qtablewidgetitem7 = QTableWidgetItem()
        __qtablewidgetitem7.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tableWidget.setHorizontalHeaderItem(4, __qtablewidgetitem7)
        __qtablewidgetitem8 = QTableWidgetItem()
        __qtablewidgetitem8.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tableWidget.setHorizontalHeaderItem(5, __qtablewidgetitem8)
        self.tableWidget.setObjectName(u"tableWidget")

        self.horizontalLayout_26.addWidget(self.tableWidget)


        self.verticalLayout_6.addWidget(self.tbw_column)

        self.splitter_table.addWidget(self.widget_1_2)
        self.widget_1_3 = QWidget(self.splitter_table)
        self.widget_1_3.setObjectName(u"widget_1_3")
        self.verticalLayout_5 = QVBoxLayout(self.widget_1_3)
        self.verticalLayout_5.setObjectName(u"verticalLayout_5")
        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.edt_yf = LineEdit(self.widget_1_3)
        self.edt_yf.setObjectName(u"edt_yf")
        sizePolicy1.setHeightForWidth(self.edt_yf.sizePolicy().hasHeightForWidth())
        self.edt_yf.setSizePolicy(sizePolicy1)

        self.horizontalLayout.addWidget(self.edt_yf)

        self.btn_yf = PushButton(self.widget_1_3)
        self.btn_yf.setObjectName(u"btn_yf")
        sizePolicy1.setHeightForWidth(self.btn_yf.sizePolicy().hasHeightForWidth())
        self.btn_yf.setSizePolicy(sizePolicy1)

        self.horizontalLayout.addWidget(self.btn_yf)


        self.verticalLayout_5.addLayout(self.horizontalLayout)

        self.horizontalLayout_8 = QHBoxLayout()
        self.horizontalLayout_8.setObjectName(u"horizontalLayout_8")
        self.verticalLayout_10 = QVBoxLayout()
        self.verticalLayout_10.setObjectName(u"verticalLayout_10")
        self.lab_sta = BodyLabel(self.widget_1_3)
        self.lab_sta.setObjectName(u"lab_sta")

        self.verticalLayout_10.addWidget(self.lab_sta)

        self.edt_sta = TextEdit(self.widget_1_3)
        self.edt_sta.setObjectName(u"edt_sta")
        sizePolicy3 = QSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        sizePolicy3.setHorizontalStretch(0)
        sizePolicy3.setVerticalStretch(0)
        sizePolicy3.setHeightForWidth(self.edt_sta.sizePolicy().hasHeightForWidth())
        self.edt_sta.setSizePolicy(sizePolicy3)
        self.edt_sta.setReadOnly(False)

        self.verticalLayout_10.addWidget(self.edt_sta)


        self.horizontalLayout_8.addLayout(self.verticalLayout_10)

        self.verticalLayout_23 = QVBoxLayout()
        self.verticalLayout_23.setObjectName(u"verticalLayout_23")
        self.lab_ods = BodyLabel(self.widget_1_3)
        self.lab_ods.setObjectName(u"lab_ods")
        sizePolicy4 = QSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        sizePolicy4.setHorizontalStretch(0)
        sizePolicy4.setVerticalStretch(0)
        sizePolicy4.setHeightForWidth(self.lab_ods.sizePolicy().hasHeightForWidth())
        self.lab_ods.setSizePolicy(sizePolicy4)

        self.verticalLayout_23.addWidget(self.lab_ods)

        self.edt_ods = TextEdit(self.widget_1_3)
        self.edt_ods.setObjectName(u"edt_ods")
        sizePolicy3.setHeightForWidth(self.edt_ods.sizePolicy().hasHeightForWidth())
        self.edt_ods.setSizePolicy(sizePolicy3)
        self.edt_ods.setReadOnly(False)

        self.verticalLayout_23.addWidget(self.edt_ods)


        self.horizontalLayout_8.addLayout(self.verticalLayout_23)


        self.verticalLayout_5.addLayout(self.horizontalLayout_8)

        self.splitter_table.addWidget(self.widget_1_3)

        self.horizontalLayout_7.addWidget(self.splitter_table)

        self.stackedWidget.addWidget(self.tab_table)
        self.tab_proc = QWidget()
        self.tab_proc.setObjectName(u"tab_proc")
        self.tab_proc.setStyleSheet(u"#widget_2_1,#widget_2_2,#widget_2_3 {\n"
"  background-color: rgb(241, 243, 246); /* rgb(237, 246, 248) */\n"
"  border: 1px solid rgb(220, 220, 220); /* \u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
" }")
        self.horizontalLayout_10 = QHBoxLayout(self.tab_proc)
        self.horizontalLayout_10.setObjectName(u"horizontalLayout_10")
        self.splitter_procedure = QSplitter(self.tab_proc)
        self.splitter_procedure.setObjectName(u"splitter_procedure")
        self.splitter_procedure.setOrientation(Qt.Horizontal)
        self.widget_2_1 = QWidget(self.splitter_procedure)
        self.widget_2_1.setObjectName(u"widget_2_1")
        self.verticalLayout_7 = QVBoxLayout(self.widget_2_1)
        self.verticalLayout_7.setObjectName(u"verticalLayout_7")
        self.cbb_find_procedure = ComboBox(self.widget_2_1)
        self.cbb_find_procedure.addItem("")
        self.cbb_find_procedure.addItem("")
        self.cbb_find_procedure.setObjectName(u"cbb_find_procedure")
        self.cbb_find_procedure.setMaxVisibleItems(20)

        self.verticalLayout_7.addWidget(self.cbb_find_procedure)

        self.edt_find_procedure = LineEdit(self.widget_2_1)
        self.edt_find_procedure.setObjectName(u"edt_find_procedure")

        self.verticalLayout_7.addWidget(self.edt_find_procedure)

        self.tbw_procedure = TableWidget(self.widget_2_1)
        if (self.tbw_procedure.columnCount() < 3):
            self.tbw_procedure.setColumnCount(3)
        __qtablewidgetitem9 = QTableWidgetItem()
        __qtablewidgetitem9.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tbw_procedure.setHorizontalHeaderItem(0, __qtablewidgetitem9)
        __qtablewidgetitem10 = QTableWidgetItem()
        __qtablewidgetitem10.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tbw_procedure.setHorizontalHeaderItem(1, __qtablewidgetitem10)
        __qtablewidgetitem11 = QTableWidgetItem()
        __qtablewidgetitem11.setTextAlignment(Qt.AlignLeading|Qt.AlignVCenter);
        self.tbw_procedure.setHorizontalHeaderItem(2, __qtablewidgetitem11)
        self.tbw_procedure.setObjectName(u"tbw_procedure")
        self.tbw_procedure.setFocusPolicy(Qt.NoFocus)
        self.tbw_procedure.setStyleSheet(u"QTableWidget {\n"
"  border: 1px solid rgb(220, 220, 220); /* \u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
" }\n"
"\n"
"QTableWidget::item:selected {\n"
"  background-color: rgb(225, 227, 230);  /*rgb(0, 159, 170) (225, 227, 230)*/\n"
"  border-radius: 3px; /*\u5706\u89d2\u5ea6*/\n"
"}")
        self.tbw_procedure.setAutoScroll(False)
        self.tbw_procedure.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbw_procedure.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.tbw_procedure.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbw_procedure.horizontalHeader().setVisible(False)
        self.tbw_procedure.verticalHeader().setVisible(False)

        self.verticalLayout_7.addWidget(self.tbw_procedure)

        self.splitter_procedure.addWidget(self.widget_2_1)
        self.widget_2_2 = QWidget(self.splitter_procedure)
        self.widget_2_2.setObjectName(u"widget_2_2")
        self.verticalLayout_8 = QVBoxLayout(self.widget_2_2)
        self.verticalLayout_8.setObjectName(u"verticalLayout_8")
        self.lab_procedure_name = BodyLabel(self.widget_2_2)
        self.lab_procedure_name.setObjectName(u"lab_procedure_name")

        self.verticalLayout_8.addWidget(self.lab_procedure_name)

        self.edt_procedure_body = TextEdit(self.widget_2_2)
        self.edt_procedure_body.setObjectName(u"edt_procedure_body")
        self.edt_procedure_body.setLineWrapMode(QTextEdit.NoWrap)
        self.edt_procedure_body.setReadOnly(True)

        self.verticalLayout_8.addWidget(self.edt_procedure_body)

        self.splitter_procedure.addWidget(self.widget_2_2)
        self.widget_2_3 = QWidget(self.splitter_procedure)
        self.widget_2_3.setObjectName(u"widget_2_3")
        self.verticalLayout_9 = QVBoxLayout(self.widget_2_3)
        self.verticalLayout_9.setObjectName(u"verticalLayout_9")
        self.lab_procedure_blood = BodyLabel(self.widget_2_3)
        self.lab_procedure_blood.setObjectName(u"lab_procedure_blood")

        self.verticalLayout_9.addWidget(self.lab_procedure_blood)

        self.edt_procedure_blood = TextEdit(self.widget_2_3)
        self.edt_procedure_blood.setObjectName(u"edt_procedure_blood")
        self.edt_procedure_blood.setLineWrapMode(QTextEdit.NoWrap)
        self.edt_procedure_blood.setReadOnly(True)

        self.verticalLayout_9.addWidget(self.edt_procedure_blood)

        self.splitter_procedure.addWidget(self.widget_2_3)

        self.horizontalLayout_10.addWidget(self.splitter_procedure)

        self.stackedWidget.addWidget(self.tab_proc)
        self.tab_sql = QWidget()
        self.tab_sql.setObjectName(u"tab_sql")
        self.tab_sql.setStyleSheet(u"#widget_3_1{\n"
"  background-color: rgb(241, 243, 246); /* rgb(237, 246, 248) */\n"
"  border: 1px solid rgb(220, 220, 220); /* \u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
" }")
        self.horizontalLayout_33 = QHBoxLayout(self.tab_sql)
        self.horizontalLayout_33.setObjectName(u"horizontalLayout_33")
        self.verticalLayout_18 = QVBoxLayout()
        self.verticalLayout_18.setObjectName(u"verticalLayout_18")
        self.segw_sql = SegmentedWidget(self.tab_sql)
        self.segw_sql.setObjectName(u"segw_sql")
        self.horizontalLayout_11 = QHBoxLayout(self.segw_sql)
        self.horizontalLayout_11.setObjectName(u"horizontalLayout_11")
        self.horizontalLayout_11.setContentsMargins(-1, 0, 0, 0)
        self.pushButton = QPushButton(self.segw_sql)
        self.pushButton.setObjectName(u"pushButton")

        self.horizontalLayout_11.addWidget(self.pushButton)

        self.pushButton_9 = QPushButton(self.segw_sql)
        self.pushButton_9.setObjectName(u"pushButton_9")

        self.horizontalLayout_11.addWidget(self.pushButton_9)

        self.pushButton_10 = QPushButton(self.segw_sql)
        self.pushButton_10.setObjectName(u"pushButton_10")

        self.horizontalLayout_11.addWidget(self.pushButton_10)


        self.verticalLayout_18.addWidget(self.segw_sql, 0, Qt.AlignLeft)

        self.staw_sql = QStackedWidget(self.tab_sql)
        self.staw_sql.setObjectName(u"staw_sql")
        self.staw_sql.setStyleSheet(u"#tab_sql_1,#tab_sql_2,#tab_sql_3 {\n"
"  background-color: rgb(241, 243, 246); /* rgb(237, 246, 248) */\n"
"  border: 1px solid rgb(220, 220, 220); /* \u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
" }")
        self.tab_sql_1 = QWidget()
        self.tab_sql_1.setObjectName(u"tab_sql_1")
        self.tab_sql_1.setStyleSheet(u"")
        self.horizontalLayout_14 = QHBoxLayout(self.tab_sql_1)
        self.horizontalLayout_14.setObjectName(u"horizontalLayout_14")
        self.horizontalLayout_14.setContentsMargins(9, 9, 9, 9)
        self.splitter_3 = QSplitter(self.tab_sql_1)
        self.splitter_3.setObjectName(u"splitter_3")
        self.splitter_3.setOrientation(Qt.Vertical)
        self.layoutWidget_9 = QWidget(self.splitter_3)
        self.layoutWidget_9.setObjectName(u"layoutWidget_9")
        self.verticalLayout_14 = QVBoxLayout(self.layoutWidget_9)
        self.verticalLayout_14.setObjectName(u"verticalLayout_14")
        self.verticalLayout_14.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout_16 = QHBoxLayout()
        self.horizontalLayout_16.setObjectName(u"horizontalLayout_16")
        self.label_3 = BodyLabel(self.layoutWidget_9)
        self.label_3.setObjectName(u"label_3")
        sizePolicy.setHeightForWidth(self.label_3.sizePolicy().hasHeightForWidth())
        self.label_3.setSizePolicy(sizePolicy)

        self.horizontalLayout_16.addWidget(self.label_3)

        self.edt_sql_row_1 = LineEdit(self.layoutWidget_9)
        self.edt_sql_row_1.setObjectName(u"edt_sql_row_1")
        sizePolicy.setHeightForWidth(self.edt_sql_row_1.sizePolicy().hasHeightForWidth())
        self.edt_sql_row_1.setSizePolicy(sizePolicy)

        self.horizontalLayout_16.addWidget(self.edt_sql_row_1)

        self.btn_sql_execute_1 = PushButton(self.layoutWidget_9)
        self.btn_sql_execute_1.setObjectName(u"btn_sql_execute_1")
        sizePolicy.setHeightForWidth(self.btn_sql_execute_1.sizePolicy().hasHeightForWidth())
        self.btn_sql_execute_1.setSizePolicy(sizePolicy)
        self.btn_sql_execute_1.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_16.addWidget(self.btn_sql_execute_1)

        self.horizontalSpacer_8 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_16.addItem(self.horizontalSpacer_8)

        self.edt_sql_book_1 = LineEdit(self.layoutWidget_9)
        self.edt_sql_book_1.setObjectName(u"edt_sql_book_1")
        sizePolicy.setHeightForWidth(self.edt_sql_book_1.sizePolicy().hasHeightForWidth())
        self.edt_sql_book_1.setSizePolicy(sizePolicy)
        self.edt_sql_book_1.setMinimumSize(QSize(200, 0))
        self.edt_sql_book_1.setFocusPolicy(Qt.ClickFocus)

        self.horizontalLayout_16.addWidget(self.edt_sql_book_1)

        self.btn_sql_save_1 = PushButton(self.layoutWidget_9)
        self.btn_sql_save_1.setObjectName(u"btn_sql_save_1")
        self.btn_sql_save_1.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_16.addWidget(self.btn_sql_save_1)

        self.btn_sql_del_1 = PushButton(self.layoutWidget_9)
        self.btn_sql_del_1.setObjectName(u"btn_sql_del_1")
        self.btn_sql_del_1.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_16.addWidget(self.btn_sql_del_1)


        self.verticalLayout_14.addLayout(self.horizontalLayout_16)

        self.edt_sql_body_1 = TextEdit(self.layoutWidget_9)
        self.edt_sql_body_1.setObjectName(u"edt_sql_body_1")

        self.verticalLayout_14.addWidget(self.edt_sql_body_1)

        self.splitter_3.addWidget(self.layoutWidget_9)
        self.layoutWidget_10 = QWidget(self.splitter_3)
        self.layoutWidget_10.setObjectName(u"layoutWidget_10")
        self.verticalLayout_11 = QVBoxLayout(self.layoutWidget_10)
        self.verticalLayout_11.setSpacing(0)
        self.verticalLayout_11.setObjectName(u"verticalLayout_11")
        self.verticalLayout_11.setContentsMargins(0, 0, 0, 0)
        self.segw_sql_1 = SegmentedWidget(self.layoutWidget_10)
        self.segw_sql_1.setObjectName(u"segw_sql_1")
        self.horizontalLayout_17 = QHBoxLayout(self.segw_sql_1)
        self.horizontalLayout_17.setObjectName(u"horizontalLayout_17")
        self.horizontalLayout_17.setContentsMargins(0, 0, 0, 0)
        self.pushButton_11 = QPushButton(self.segw_sql_1)
        self.pushButton_11.setObjectName(u"pushButton_11")

        self.horizontalLayout_17.addWidget(self.pushButton_11)

        self.pushButton_12 = QPushButton(self.segw_sql_1)
        self.pushButton_12.setObjectName(u"pushButton_12")

        self.horizontalLayout_17.addWidget(self.pushButton_12)


        self.verticalLayout_11.addWidget(self.segw_sql_1, 0, Qt.AlignLeft)

        self.staw_sql_1 = QStackedWidget(self.layoutWidget_10)
        self.staw_sql_1.setObjectName(u"staw_sql_1")
        self.tab_sql_1_result = QWidget()
        self.tab_sql_1_result.setObjectName(u"tab_sql_1_result")
        self.verticalLayout_12 = QVBoxLayout(self.tab_sql_1_result)
        self.verticalLayout_12.setSpacing(0)
        self.verticalLayout_12.setObjectName(u"verticalLayout_12")
        self.verticalLayout_12.setContentsMargins(0, 0, 0, 0)
        self.tbw_sql_result_1 = PageTableWidget(self.tab_sql_1_result)
        self.tbw_sql_result_1.setObjectName(u"tbw_sql_result_1")
        self.horizontalLayout_18 = QHBoxLayout(self.tbw_sql_result_1)
        self.horizontalLayout_18.setObjectName(u"horizontalLayout_18")
        self.horizontalLayout_18.setContentsMargins(0, 0, 0, 0)
        self.tableWidget_2 = QTableWidget(self.tbw_sql_result_1)
        self.tableWidget_2.setObjectName(u"tableWidget_2")

        self.horizontalLayout_18.addWidget(self.tableWidget_2)


        self.verticalLayout_12.addWidget(self.tbw_sql_result_1)

        self.staw_sql_1.addWidget(self.tab_sql_1_result)
        self.tab_sql_1_log = QWidget()
        self.tab_sql_1_log.setObjectName(u"tab_sql_1_log")
        self.horizontalLayout_20 = QHBoxLayout(self.tab_sql_1_log)
        self.horizontalLayout_20.setObjectName(u"horizontalLayout_20")
        self.horizontalLayout_20.setContentsMargins(0, 0, 0, 0)
        self.edt_sql_log_1 = QTextEdit(self.tab_sql_1_log)
        self.edt_sql_log_1.setObjectName(u"edt_sql_log_1")
        self.edt_sql_log_1.setReadOnly(True)

        self.horizontalLayout_20.addWidget(self.edt_sql_log_1)

        self.staw_sql_1.addWidget(self.tab_sql_1_log)

        self.verticalLayout_11.addWidget(self.staw_sql_1)

        self.splitter_3.addWidget(self.layoutWidget_10)

        self.horizontalLayout_14.addWidget(self.splitter_3)

        self.staw_sql.addWidget(self.tab_sql_1)
        self.tab_sql_2 = QWidget()
        self.tab_sql_2.setObjectName(u"tab_sql_2")
        self.horizontalLayout_22 = QHBoxLayout(self.tab_sql_2)
        self.horizontalLayout_22.setObjectName(u"horizontalLayout_22")
        self.horizontalLayout_22.setContentsMargins(9, 9, 9, 9)
        self.splitter_5 = QSplitter(self.tab_sql_2)
        self.splitter_5.setObjectName(u"splitter_5")
        self.splitter_5.setOrientation(Qt.Vertical)
        self.layoutWidget_20 = QWidget(self.splitter_5)
        self.layoutWidget_20.setObjectName(u"layoutWidget_20")
        self.verticalLayout_15 = QVBoxLayout(self.layoutWidget_20)
        self.verticalLayout_15.setObjectName(u"verticalLayout_15")
        self.verticalLayout_15.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout_27 = QHBoxLayout()
        self.horizontalLayout_27.setObjectName(u"horizontalLayout_27")
        self.label_9 = BodyLabel(self.layoutWidget_20)
        self.label_9.setObjectName(u"label_9")
        sizePolicy.setHeightForWidth(self.label_9.sizePolicy().hasHeightForWidth())
        self.label_9.setSizePolicy(sizePolicy)

        self.horizontalLayout_27.addWidget(self.label_9)

        self.edt_sql_row_2 = LineEdit(self.layoutWidget_20)
        self.edt_sql_row_2.setObjectName(u"edt_sql_row_2")
        sizePolicy.setHeightForWidth(self.edt_sql_row_2.sizePolicy().hasHeightForWidth())
        self.edt_sql_row_2.setSizePolicy(sizePolicy)

        self.horizontalLayout_27.addWidget(self.edt_sql_row_2)

        self.btn_sql_execute_2 = PushButton(self.layoutWidget_20)
        self.btn_sql_execute_2.setObjectName(u"btn_sql_execute_2")
        sizePolicy.setHeightForWidth(self.btn_sql_execute_2.sizePolicy().hasHeightForWidth())
        self.btn_sql_execute_2.setSizePolicy(sizePolicy)
        self.btn_sql_execute_2.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_27.addWidget(self.btn_sql_execute_2)

        self.horizontalSpacer_15 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_27.addItem(self.horizontalSpacer_15)

        self.edt_sql_book_2 = LineEdit(self.layoutWidget_20)
        self.edt_sql_book_2.setObjectName(u"edt_sql_book_2")
        sizePolicy.setHeightForWidth(self.edt_sql_book_2.sizePolicy().hasHeightForWidth())
        self.edt_sql_book_2.setSizePolicy(sizePolicy)
        self.edt_sql_book_2.setMinimumSize(QSize(200, 0))
        self.edt_sql_book_2.setFocusPolicy(Qt.ClickFocus)

        self.horizontalLayout_27.addWidget(self.edt_sql_book_2)

        self.btn_sql_save_2 = PushButton(self.layoutWidget_20)
        self.btn_sql_save_2.setObjectName(u"btn_sql_save_2")
        self.btn_sql_save_2.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_27.addWidget(self.btn_sql_save_2)

        self.btn_sql_del_2 = PushButton(self.layoutWidget_20)
        self.btn_sql_del_2.setObjectName(u"btn_sql_del_2")
        self.btn_sql_del_2.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_27.addWidget(self.btn_sql_del_2)


        self.verticalLayout_15.addLayout(self.horizontalLayout_27)

        self.edt_sql_body_2 = TextEdit(self.layoutWidget_20)
        self.edt_sql_body_2.setObjectName(u"edt_sql_body_2")

        self.verticalLayout_15.addWidget(self.edt_sql_body_2)

        self.splitter_5.addWidget(self.layoutWidget_20)
        self.layoutWidget_21 = QWidget(self.splitter_5)
        self.layoutWidget_21.setObjectName(u"layoutWidget_21")
        self.verticalLayout_16 = QVBoxLayout(self.layoutWidget_21)
        self.verticalLayout_16.setSpacing(0)
        self.verticalLayout_16.setObjectName(u"verticalLayout_16")
        self.verticalLayout_16.setContentsMargins(0, 0, 0, 0)
        self.segw_sql_2 = SegmentedWidget(self.layoutWidget_21)
        self.segw_sql_2.setObjectName(u"segw_sql_2")
        self.horizontalLayout_28 = QHBoxLayout(self.segw_sql_2)
        self.horizontalLayout_28.setObjectName(u"horizontalLayout_28")
        self.horizontalLayout_28.setContentsMargins(0, 0, 0, 0)
        self.pushButton_16 = QPushButton(self.segw_sql_2)
        self.pushButton_16.setObjectName(u"pushButton_16")

        self.horizontalLayout_28.addWidget(self.pushButton_16)

        self.pushButton_17 = QPushButton(self.segw_sql_2)
        self.pushButton_17.setObjectName(u"pushButton_17")

        self.horizontalLayout_28.addWidget(self.pushButton_17)


        self.verticalLayout_16.addWidget(self.segw_sql_2, 0, Qt.AlignLeft)

        self.staw_sql_2 = QStackedWidget(self.layoutWidget_21)
        self.staw_sql_2.setObjectName(u"staw_sql_2")
        self.tab_sql_2_result = QWidget()
        self.tab_sql_2_result.setObjectName(u"tab_sql_2_result")
        self.verticalLayout_19 = QVBoxLayout(self.tab_sql_2_result)
        self.verticalLayout_19.setSpacing(0)
        self.verticalLayout_19.setObjectName(u"verticalLayout_19")
        self.verticalLayout_19.setContentsMargins(0, 0, 0, 0)
        self.tbw_sql_result_2 = PageTableWidget(self.tab_sql_2_result)
        self.tbw_sql_result_2.setObjectName(u"tbw_sql_result_2")
        self.horizontalLayout_29 = QHBoxLayout(self.tbw_sql_result_2)
        self.horizontalLayout_29.setObjectName(u"horizontalLayout_29")
        self.horizontalLayout_29.setContentsMargins(0, 0, 0, 0)
        self.tableWidget_3 = QTableWidget(self.tbw_sql_result_2)
        self.tableWidget_3.setObjectName(u"tableWidget_3")

        self.horizontalLayout_29.addWidget(self.tableWidget_3)


        self.verticalLayout_19.addWidget(self.tbw_sql_result_2)

        self.staw_sql_2.addWidget(self.tab_sql_2_result)
        self.tab_sql_2_log = QWidget()
        self.tab_sql_2_log.setObjectName(u"tab_sql_2_log")
        self.horizontalLayout_30 = QHBoxLayout(self.tab_sql_2_log)
        self.horizontalLayout_30.setSpacing(0)
        self.horizontalLayout_30.setObjectName(u"horizontalLayout_30")
        self.horizontalLayout_30.setContentsMargins(0, 0, 0, 0)
        self.edt_sql_log_2 = QTextEdit(self.tab_sql_2_log)
        self.edt_sql_log_2.setObjectName(u"edt_sql_log_2")
        self.edt_sql_log_2.setReadOnly(True)

        self.horizontalLayout_30.addWidget(self.edt_sql_log_2)

        self.staw_sql_2.addWidget(self.tab_sql_2_log)

        self.verticalLayout_16.addWidget(self.staw_sql_2)

        self.splitter_5.addWidget(self.layoutWidget_21)

        self.horizontalLayout_22.addWidget(self.splitter_5)

        self.staw_sql.addWidget(self.tab_sql_2)
        self.tab_sql_3 = QWidget()
        self.tab_sql_3.setObjectName(u"tab_sql_3")
        self.horizontalLayout_38 = QHBoxLayout(self.tab_sql_3)
        self.horizontalLayout_38.setObjectName(u"horizontalLayout_38")
        self.horizontalLayout_38.setContentsMargins(9, 9, 9, 9)
        self.splitter_6 = QSplitter(self.tab_sql_3)
        self.splitter_6.setObjectName(u"splitter_6")
        self.splitter_6.setOrientation(Qt.Vertical)
        self.layoutWidget_22 = QWidget(self.splitter_6)
        self.layoutWidget_22.setObjectName(u"layoutWidget_22")
        self.verticalLayout_20 = QVBoxLayout(self.layoutWidget_22)
        self.verticalLayout_20.setObjectName(u"verticalLayout_20")
        self.verticalLayout_20.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout_35 = QHBoxLayout()
        self.horizontalLayout_35.setObjectName(u"horizontalLayout_35")
        self.label_10 = BodyLabel(self.layoutWidget_22)
        self.label_10.setObjectName(u"label_10")
        sizePolicy.setHeightForWidth(self.label_10.sizePolicy().hasHeightForWidth())
        self.label_10.setSizePolicy(sizePolicy)

        self.horizontalLayout_35.addWidget(self.label_10)

        self.edt_sql_row_3 = LineEdit(self.layoutWidget_22)
        self.edt_sql_row_3.setObjectName(u"edt_sql_row_3")
        sizePolicy.setHeightForWidth(self.edt_sql_row_3.sizePolicy().hasHeightForWidth())
        self.edt_sql_row_3.setSizePolicy(sizePolicy)

        self.horizontalLayout_35.addWidget(self.edt_sql_row_3)

        self.btn_sql_execute_3 = PushButton(self.layoutWidget_22)
        self.btn_sql_execute_3.setObjectName(u"btn_sql_execute_3")
        sizePolicy.setHeightForWidth(self.btn_sql_execute_3.sizePolicy().hasHeightForWidth())
        self.btn_sql_execute_3.setSizePolicy(sizePolicy)
        self.btn_sql_execute_3.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_35.addWidget(self.btn_sql_execute_3)

        self.horizontalSpacer_16 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_35.addItem(self.horizontalSpacer_16)

        self.edt_sql_book_3 = LineEdit(self.layoutWidget_22)
        self.edt_sql_book_3.setObjectName(u"edt_sql_book_3")
        sizePolicy.setHeightForWidth(self.edt_sql_book_3.sizePolicy().hasHeightForWidth())
        self.edt_sql_book_3.setSizePolicy(sizePolicy)
        self.edt_sql_book_3.setMinimumSize(QSize(200, 0))
        self.edt_sql_book_3.setFocusPolicy(Qt.ClickFocus)

        self.horizontalLayout_35.addWidget(self.edt_sql_book_3)

        self.btn_sql_save_3 = PushButton(self.layoutWidget_22)
        self.btn_sql_save_3.setObjectName(u"btn_sql_save_3")
        self.btn_sql_save_3.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_35.addWidget(self.btn_sql_save_3)

        self.btn_sql_del_3 = PushButton(self.layoutWidget_22)
        self.btn_sql_del_3.setObjectName(u"btn_sql_del_3")
        self.btn_sql_del_3.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_35.addWidget(self.btn_sql_del_3)


        self.verticalLayout_20.addLayout(self.horizontalLayout_35)

        self.edt_sql_body_3 = TextEdit(self.layoutWidget_22)
        self.edt_sql_body_3.setObjectName(u"edt_sql_body_3")

        self.verticalLayout_20.addWidget(self.edt_sql_body_3)

        self.splitter_6.addWidget(self.layoutWidget_22)
        self.layoutWidget_23 = QWidget(self.splitter_6)
        self.layoutWidget_23.setObjectName(u"layoutWidget_23")
        self.verticalLayout_21 = QVBoxLayout(self.layoutWidget_23)
        self.verticalLayout_21.setSpacing(0)
        self.verticalLayout_21.setObjectName(u"verticalLayout_21")
        self.verticalLayout_21.setContentsMargins(0, 0, 0, 0)
        self.segw_sql_3 = SegmentedWidget(self.layoutWidget_23)
        self.segw_sql_3.setObjectName(u"segw_sql_3")
        self.horizontalLayout_31 = QHBoxLayout(self.segw_sql_3)
        self.horizontalLayout_31.setObjectName(u"horizontalLayout_31")
        self.horizontalLayout_31.setContentsMargins(0, 0, 0, 0)
        self.pushButton_19 = QPushButton(self.segw_sql_3)
        self.pushButton_19.setObjectName(u"pushButton_19")

        self.horizontalLayout_31.addWidget(self.pushButton_19)

        self.pushButton_20 = QPushButton(self.segw_sql_3)
        self.pushButton_20.setObjectName(u"pushButton_20")

        self.horizontalLayout_31.addWidget(self.pushButton_20)


        self.verticalLayout_21.addWidget(self.segw_sql_3, 0, Qt.AlignLeft)

        self.staw_sql_3 = QStackedWidget(self.layoutWidget_23)
        self.staw_sql_3.setObjectName(u"staw_sql_3")
        self.tab_sql_3_result = QWidget()
        self.tab_sql_3_result.setObjectName(u"tab_sql_3_result")
        self.verticalLayout_22 = QVBoxLayout(self.tab_sql_3_result)
        self.verticalLayout_22.setObjectName(u"verticalLayout_22")
        self.verticalLayout_22.setContentsMargins(0, 0, 0, 0)
        self.tbw_sql_result_3 = PageTableWidget(self.tab_sql_3_result)
        self.tbw_sql_result_3.setObjectName(u"tbw_sql_result_3")
        self.horizontalLayout_36 = QHBoxLayout(self.tbw_sql_result_3)
        self.horizontalLayout_36.setObjectName(u"horizontalLayout_36")
        self.horizontalLayout_36.setContentsMargins(0, 0, 0, 0)
        self.tableWidget_4 = QTableWidget(self.tbw_sql_result_3)
        self.tableWidget_4.setObjectName(u"tableWidget_4")

        self.horizontalLayout_36.addWidget(self.tableWidget_4)


        self.verticalLayout_22.addWidget(self.tbw_sql_result_3)

        self.staw_sql_3.addWidget(self.tab_sql_3_result)
        self.tab_sql_3_log = QWidget()
        self.tab_sql_3_log.setObjectName(u"tab_sql_3_log")
        self.horizontalLayout_32 = QHBoxLayout(self.tab_sql_3_log)
        self.horizontalLayout_32.setObjectName(u"horizontalLayout_32")
        self.horizontalLayout_32.setContentsMargins(0, 0, 0, 0)
        self.edt_sql_log_3 = QTextEdit(self.tab_sql_3_log)
        self.edt_sql_log_3.setObjectName(u"edt_sql_log_3")
        self.edt_sql_log_3.setReadOnly(True)

        self.horizontalLayout_32.addWidget(self.edt_sql_log_3)

        self.staw_sql_3.addWidget(self.tab_sql_3_log)

        self.verticalLayout_21.addWidget(self.staw_sql_3)

        self.splitter_6.addWidget(self.layoutWidget_23)

        self.horizontalLayout_38.addWidget(self.splitter_6)

        self.staw_sql.addWidget(self.tab_sql_3)

        self.verticalLayout_18.addWidget(self.staw_sql)


        self.horizontalLayout_33.addLayout(self.verticalLayout_18)

        self.widget_3_1 = QWidget(self.tab_sql)
        self.widget_3_1.setObjectName(u"widget_3_1")
        self.verticalLayout_17 = QVBoxLayout(self.widget_3_1)
        self.verticalLayout_17.setObjectName(u"verticalLayout_17")
        self.label_13 = QLabel(self.widget_3_1)
        self.label_13.setObjectName(u"label_13")

        self.verticalLayout_17.addWidget(self.label_13)

        self.lsw_book = ListWidget(self.widget_3_1)
        self.lsw_book.setObjectName(u"lsw_book")
        self.lsw_book.setMaximumSize(QSize(200, 16777215))
        self.lsw_book.setStyleSheet(u"ListView,\n"
"ListWidget {\n"
"    background: transparent;\n"
"    background-color: rgb(255, 255, 255);\n"
"    outline: none;\n"
"    border: none;\n"
"    /* font: 13px 'Segoe UI', 'Microsoft YaHei'; */\n"
"    selection-background-color: transparent;\n"
"    alternate-background-color: transparent;\n"
"    padding-left: 4px;\n"
"    padding-right: 4px;\n"
"}\n"
"\n"
"ListView::item,\n"
"ListWidget::item {\n"
"    background: transparent;\n"
"    border: 1px;\n"
"    padding-left: 11px;\n"
"    padding-right: 11px;\n"
"    height: 35px;\n"
"}\n"
"\n"
"\n"
"ListView::indicator,\n"
"ListWidget::indicator {\n"
"    width: 18px;\n"
"    height: 18px;\n"
"    border-radius: 5px;\n"
"    border: none;\n"
"    background-color: transparent;\n"
"    margin-right: 4px;\n"
"}\n"
"\n"
"")

        self.verticalLayout_17.addWidget(self.lsw_book)


        self.horizontalLayout_33.addWidget(self.widget_3_1)

        self.horizontalLayout_33.setStretch(0, 8)
        self.stackedWidget.addWidget(self.tab_sql)
        self.tab_db = QWidget()
        self.tab_db.setObjectName(u"tab_db")
        self.tab_db.setStyleSheet(u"\n"
"#widget_db_left,#widget_db_right {\n"
"  background-color: rgb(241, 243, 246); /* rgb(237, 246, 248) */\n"
"  border: 1px solid rgb(220, 220, 220); /* \u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
" }")
        self.horizontalLayout_5 = QHBoxLayout(self.tab_db)
        self.horizontalLayout_5.setObjectName(u"horizontalLayout_5")
        self.widget_db_left = QWidget(self.tab_db)
        self.widget_db_left.setObjectName(u"widget_db_left")
        sizePolicy5 = QSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        sizePolicy5.setHorizontalStretch(0)
        sizePolicy5.setVerticalStretch(0)
        sizePolicy5.setHeightForWidth(self.widget_db_left.sizePolicy().hasHeightForWidth())
        self.widget_db_left.setSizePolicy(sizePolicy5)
        self.verticalLayout_3 = QVBoxLayout(self.widget_db_left)
        self.verticalLayout_3.setObjectName(u"verticalLayout_3")
        self.horizontalLayout_4 = QHBoxLayout()
        self.horizontalLayout_4.setObjectName(u"horizontalLayout_4")
        self.label_5 = BodyLabel(self.widget_db_left)
        self.label_5.setObjectName(u"label_5")
        self.label_5.setTextFormat(Qt.AutoText)

        self.horizontalLayout_4.addWidget(self.label_5)

        self.btn_dbinfo_add = PushButton(self.widget_db_left)
        self.btn_dbinfo_add.setObjectName(u"btn_dbinfo_add")
        sizePolicy.setHeightForWidth(self.btn_dbinfo_add.sizePolicy().hasHeightForWidth())
        self.btn_dbinfo_add.setSizePolicy(sizePolicy)
        self.btn_dbinfo_add.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_4.addWidget(self.btn_dbinfo_add)

        self.btn_dbinfo_del = PushButton(self.widget_db_left)
        self.btn_dbinfo_del.setObjectName(u"btn_dbinfo_del")
        sizePolicy.setHeightForWidth(self.btn_dbinfo_del.sizePolicy().hasHeightForWidth())
        self.btn_dbinfo_del.setSizePolicy(sizePolicy)
        self.btn_dbinfo_del.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_4.addWidget(self.btn_dbinfo_del)


        self.verticalLayout_3.addLayout(self.horizontalLayout_4)

        self.lswDB = ListWidget(self.widget_db_left)
        self.lswDB.setObjectName(u"lswDB")
        self.lswDB.setStyleSheet(u"ListView,\n"
"ListWidget {\n"
"    background: transparent;\n"
"    background-color: rgb(255, 255, 255);\n"
"    outline: none;\n"
"    border: none;\n"
"    /* font: 13px 'Segoe UI', 'Microsoft YaHei'; */\n"
"    selection-background-color: transparent;\n"
"    alternate-background-color: transparent;\n"
"    padding-left: 4px;\n"
"    padding-right: 4px;\n"
"}\n"
"\n"
"ListView::item,\n"
"ListWidget::item {\n"
"    background: transparent;\n"
"    border: 1px;\n"
"    padding-left: 11px;\n"
"    padding-right: 11px;\n"
"    height: 35px;\n"
"}\n"
"\n"
"\n"
"ListView::indicator,\n"
"ListWidget::indicator {\n"
"    width: 18px;\n"
"    height: 18px;\n"
"    border-radius: 5px;\n"
"    border: none;\n"
"    background-color: transparent;\n"
"    margin-right: 4px;\n"
"}\n"
"\n"
"")

        self.verticalLayout_3.addWidget(self.lswDB)


        self.horizontalLayout_5.addWidget(self.widget_db_left)

        self.widget_db_right = QWidget(self.tab_db)
        self.widget_db_right.setObjectName(u"widget_db_right")
        self.verticalLayout_4 = QVBoxLayout(self.widget_db_right)
        self.verticalLayout_4.setObjectName(u"verticalLayout_4")
        self.horizontalLayout_3 = QHBoxLayout()
        self.horizontalLayout_3.setObjectName(u"horizontalLayout_3")
        self.label_4 = BodyLabel(self.widget_db_right)
        self.label_4.setObjectName(u"label_4")

        self.horizontalLayout_3.addWidget(self.label_4, 0, Qt.AlignRight)

        self.edt_name = LineEdit(self.widget_db_right)
        self.edt_name.setObjectName(u"edt_name")

        self.horizontalLayout_3.addWidget(self.edt_name)

        self.horizontalLayout_3.setStretch(0, 1)
        self.horizontalLayout_3.setStretch(1, 9)

        self.verticalLayout_4.addLayout(self.horizontalLayout_3)

        self.horizontalLayout_6 = QHBoxLayout()
        self.horizontalLayout_6.setObjectName(u"horizontalLayout_6")
        self.label_6 = BodyLabel(self.widget_db_right)
        self.label_6.setObjectName(u"label_6")

        self.horizontalLayout_6.addWidget(self.label_6, 0, Qt.AlignRight)

        self.edt_desc = LineEdit(self.widget_db_right)
        self.edt_desc.setObjectName(u"edt_desc")

        self.horizontalLayout_6.addWidget(self.edt_desc)

        self.horizontalLayout_6.setStretch(0, 1)
        self.horizontalLayout_6.setStretch(1, 9)

        self.verticalLayout_4.addLayout(self.horizontalLayout_6)

        self.HL1 = QHBoxLayout()
        self.HL1.setObjectName(u"HL1")
        self.lab_type = BodyLabel(self.widget_db_right)
        self.lab_type.setObjectName(u"lab_type")
        self.lab_type.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.HL1.addWidget(self.lab_type)

        self.cbb_type = ComboBox(self.widget_db_right)
        self.cbb_type.addItem("")
        self.cbb_type.addItem("")
        self.cbb_type.addItem("")
        self.cbb_type.addItem("")
        self.cbb_type.addItem("")
        self.cbb_type.setObjectName(u"cbb_type")

        self.HL1.addWidget(self.cbb_type)

        self.horizontalSpacer_2 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.HL1.addItem(self.horizontalSpacer_2)

        self.HL1.setStretch(0, 1)
        self.HL1.setStretch(1, 3)
        self.HL1.setStretch(2, 6)

        self.verticalLayout_4.addLayout(self.HL1)

        self.HL4 = QHBoxLayout()
        self.HL4.setObjectName(u"HL4")
        self.HL4.setSizeConstraint(QLayout.SetDefaultConstraint)
        self.lab_host = BodyLabel(self.widget_db_right)
        self.lab_host.setObjectName(u"lab_host")
        self.lab_host.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.HL4.addWidget(self.lab_host)

        self.edt_host = LineEdit(self.widget_db_right)
        self.edt_host.setObjectName(u"edt_host")

        self.HL4.addWidget(self.edt_host)

        self.HL4.setStretch(0, 1)
        self.HL4.setStretch(1, 9)

        self.verticalLayout_4.addLayout(self.HL4)

        self.HL5 = QHBoxLayout()
        self.HL5.setObjectName(u"HL5")
        self.lab_port = BodyLabel(self.widget_db_right)
        self.lab_port.setObjectName(u"lab_port")
        self.lab_port.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.HL5.addWidget(self.lab_port)

        self.edt_port = LineEdit(self.widget_db_right)
        self.edt_port.setObjectName(u"edt_port")

        self.HL5.addWidget(self.edt_port)

        self.HL5.setStretch(0, 1)
        self.HL5.setStretch(1, 9)

        self.verticalLayout_4.addLayout(self.HL5)

        self.HL6 = QHBoxLayout()
        self.HL6.setObjectName(u"HL6")
        self.lab_database = BodyLabel(self.widget_db_right)
        self.lab_database.setObjectName(u"lab_database")
        self.lab_database.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.HL6.addWidget(self.lab_database)

        self.edt_database = LineEdit(self.widget_db_right)
        self.edt_database.setObjectName(u"edt_database")

        self.HL6.addWidget(self.edt_database)

        self.HL6.setStretch(0, 1)
        self.HL6.setStretch(1, 9)

        self.verticalLayout_4.addLayout(self.HL6)

        self.HL2 = QHBoxLayout()
        self.HL2.setObjectName(u"HL2")
        self.lab_username = BodyLabel(self.widget_db_right)
        self.lab_username.setObjectName(u"lab_username")
        self.lab_username.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.HL2.addWidget(self.lab_username)

        self.edt_username = LineEdit(self.widget_db_right)
        self.edt_username.setObjectName(u"edt_username")

        self.HL2.addWidget(self.edt_username)

        self.HL2.setStretch(0, 1)
        self.HL2.setStretch(1, 9)

        self.verticalLayout_4.addLayout(self.HL2)

        self.HL3 = QHBoxLayout()
        self.HL3.setObjectName(u"HL3")
        self.lab_password = BodyLabel(self.widget_db_right)
        self.lab_password.setObjectName(u"lab_password")
        self.lab_password.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.HL3.addWidget(self.lab_password)

        self.edt_password = PasswordLineEdit(self.widget_db_right)
        self.edt_password.setObjectName(u"edt_password")
        self.edt_password.setEchoMode(QLineEdit.PasswordEchoOnEdit)

        self.HL3.addWidget(self.edt_password)

        self.HL3.setStretch(0, 1)
        self.HL3.setStretch(1, 9)

        self.verticalLayout_4.addLayout(self.HL3)

        self.HL7 = QHBoxLayout()
        self.HL7.setObjectName(u"HL7")
        self.lab_charset = BodyLabel(self.widget_db_right)
        self.lab_charset.setObjectName(u"lab_charset")
        self.lab_charset.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.HL7.addWidget(self.lab_charset)

        self.edt_charset = LineEdit(self.widget_db_right)
        self.edt_charset.setObjectName(u"edt_charset")

        self.HL7.addWidget(self.edt_charset)

        self.horizontalSpacer_3 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.HL7.addItem(self.horizontalSpacer_3)

        self.HL7.setStretch(0, 1)
        self.HL7.setStretch(1, 3)
        self.HL7.setStretch(2, 6)

        self.verticalLayout_4.addLayout(self.HL7)

        self.HL8 = QHBoxLayout()
        self.HL8.setObjectName(u"HL8")
        self.btn_test_db = PushButton(self.widget_db_right)
        self.btn_test_db.setObjectName(u"btn_test_db")
        sizePolicy.setHeightForWidth(self.btn_test_db.sizePolicy().hasHeightForWidth())
        self.btn_test_db.setSizePolicy(sizePolicy)
        self.btn_test_db.setMinimumSize(QSize(100, 0))

        self.HL8.addWidget(self.btn_test_db)

        self.btn_save_db = PushButton(self.widget_db_right)
        self.btn_save_db.setObjectName(u"btn_save_db")
        sizePolicy.setHeightForWidth(self.btn_save_db.sizePolicy().hasHeightForWidth())
        self.btn_save_db.setSizePolicy(sizePolicy)
        self.btn_save_db.setMinimumSize(QSize(100, 0))

        self.HL8.addWidget(self.btn_save_db)


        self.verticalLayout_4.addLayout(self.HL8)


        self.horizontalLayout_5.addWidget(self.widget_db_right)

        self.horizontalLayout_5.setStretch(0, 1)
        self.horizontalLayout_5.setStretch(1, 3)
        self.stackedWidget.addWidget(self.tab_db)
        self.tab_settings = QWidget()
        self.tab_settings.setObjectName(u"tab_settings")
        self.tab_settings.setStyleSheet(u"QWidget {\n"
"     background-color: rgb(247, 249, 252);\n"
"}\n"
"\n"
"#widget_5_1,#widget_5_2,#widget_5_3,#widget_5_4,#widget_5_5,#widget_5_6,#widget_5_7,#widget_5_8,#widget_5_9,#widget_5_10,#widget_5_11,#widget_5_12 {\n"
"  background-color: rgb(252, 253, 254);\n"
"  border: 1px solid rgb(220, 220, 220); /* \u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
"}\n"
"\n"
"QLabel {\n"
"  background-color: rgb(247, 249, 252);\n"
"}\n"
"")
        self.horizontalLayout_9 = QHBoxLayout(self.tab_settings)
        self.horizontalLayout_9.setObjectName(u"horizontalLayout_9")
        self.scrollArea = SmoothScrollArea(self.tab_settings)
        self.scrollArea.setObjectName(u"scrollArea")
        self.scrollArea.setStyleSheet(u"#scrollArea{\n"
"border : 0px;\n"
"}\n"
"\n"
"#widget_5 {\n"
"  background-color: rgb(247, 249, 252); /* rgb(237, 246, 248) */\n"
"  /*  border: 1px solid rgb(220, 220, 220);\u8fb9\u6846\u5bbd\u5ea6\u3001\u6837\u5f0f\u548c\u989c\u8272 */\n"
"  border-radius: 8px; /*\u5706\u89d2\u5ea6*/\n"
" }\n"
"\n"
"\n"
"")
        self.scrollArea.setWidgetResizable(True)
        self.widget_5 = QWidget()
        self.widget_5.setObjectName(u"widget_5")
        self.widget_5.setGeometry(QRect(0, 0, 504, 804))
        self.verticalLayout_2 = QVBoxLayout(self.widget_5)
        self.verticalLayout_2.setObjectName(u"verticalLayout_2")
        self.label_18 = SubtitleLabel(self.widget_5)
        self.label_18.setObjectName(u"label_18")

        self.verticalLayout_2.addWidget(self.label_18)

        self.widget_5_1 = QWidget(self.widget_5)
        self.widget_5_1.setObjectName(u"widget_5_1")
        self.widget_5_1.setStyleSheet(u"")
        self.horizontalLayout_37 = QHBoxLayout(self.widget_5_1)
        self.horizontalLayout_37.setObjectName(u"horizontalLayout_37")
        self.icon_dpi = IconWidget(self.widget_5_1)
        self.icon_dpi.setObjectName(u"icon_dpi")
        sizePolicy.setHeightForWidth(self.icon_dpi.sizePolicy().hasHeightForWidth())
        self.icon_dpi.setSizePolicy(sizePolicy)
        self.icon_dpi.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_37.addWidget(self.icon_dpi)

        self.label_8 = StrongBodyLabel(self.widget_5_1)
        self.label_8.setObjectName(u"label_8")
        sizePolicy6 = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
        sizePolicy6.setHorizontalStretch(0)
        sizePolicy6.setVerticalStretch(0)
        sizePolicy6.setHeightForWidth(self.label_8.sizePolicy().hasHeightForWidth())
        self.label_8.setSizePolicy(sizePolicy6)

        self.horizontalLayout_37.addWidget(self.label_8)

        self.label_27 = CaptionLabel(self.widget_5_1)
        self.label_27.setObjectName(u"label_27")
        sizePolicy6.setHeightForWidth(self.label_27.sizePolicy().hasHeightForWidth())
        self.label_27.setSizePolicy(sizePolicy6)

        self.horizontalLayout_37.addWidget(self.label_27)

        self.horizontalSpacer_4 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_37.addItem(self.horizontalSpacer_4)

        self.cbb_dpi = ComboBox(self.widget_5_1)
        self.cbb_dpi.addItem("")
        self.cbb_dpi.addItem("")
        self.cbb_dpi.addItem("")
        self.cbb_dpi.addItem("")
        self.cbb_dpi.addItem("")
        self.cbb_dpi.addItem("")
        self.cbb_dpi.setObjectName(u"cbb_dpi")
        self.cbb_dpi.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_37.addWidget(self.cbb_dpi)


        self.verticalLayout_2.addWidget(self.widget_5_1)

        self.widget_5_9 = QWidget(self.widget_5)
        self.widget_5_9.setObjectName(u"widget_5_9")
        self.widget_5_9.setStyleSheet(u"")
        self.horizontalLayout_40 = QHBoxLayout(self.widget_5_9)
        self.horizontalLayout_40.setObjectName(u"horizontalLayout_40")
        self.icon_yunmu = IconWidget(self.widget_5_9)
        self.icon_yunmu.setObjectName(u"icon_yunmu")
        sizePolicy.setHeightForWidth(self.icon_yunmu.sizePolicy().hasHeightForWidth())
        self.icon_yunmu.setSizePolicy(sizePolicy)
        self.icon_yunmu.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_40.addWidget(self.icon_yunmu)

        self.label_14 = StrongBodyLabel(self.widget_5_9)
        self.label_14.setObjectName(u"label_14")
        sizePolicy6.setHeightForWidth(self.label_14.sizePolicy().hasHeightForWidth())
        self.label_14.setSizePolicy(sizePolicy6)

        self.horizontalLayout_40.addWidget(self.label_14)

        self.label_30 = CaptionLabel(self.widget_5_9)
        self.label_30.setObjectName(u"label_30")
        sizePolicy6.setHeightForWidth(self.label_30.sizePolicy().hasHeightForWidth())
        self.label_30.setSizePolicy(sizePolicy6)

        self.horizontalLayout_40.addWidget(self.label_30)

        self.horizontalSpacer_6 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_40.addItem(self.horizontalSpacer_6)

        self.sbtn_1 = SwitchButton(self.widget_5_9)
        self.sbtn_1.setObjectName(u"sbtn_1")

        self.horizontalLayout_40.addWidget(self.sbtn_1)


        self.verticalLayout_2.addWidget(self.widget_5_9)

        self.widget_5_10 = QWidget(self.widget_5)
        self.widget_5_10.setObjectName(u"widget_5_10")
        self.widget_5_10.setStyleSheet(u"")
        self.horizontalLayout_46 = QHBoxLayout(self.widget_5_10)
        self.horizontalLayout_46.setObjectName(u"horizontalLayout_46")
        self.icon_theme = IconWidget(self.widget_5_10)
        self.icon_theme.setObjectName(u"icon_theme")
        sizePolicy.setHeightForWidth(self.icon_theme.sizePolicy().hasHeightForWidth())
        self.icon_theme.setSizePolicy(sizePolicy)
        self.icon_theme.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_46.addWidget(self.icon_theme)

        self.label_17 = StrongBodyLabel(self.widget_5_10)
        self.label_17.setObjectName(u"label_17")
        sizePolicy6.setHeightForWidth(self.label_17.sizePolicy().hasHeightForWidth())
        self.label_17.setSizePolicy(sizePolicy6)

        self.horizontalLayout_46.addWidget(self.label_17)

        self.label_31 = CaptionLabel(self.widget_5_10)
        self.label_31.setObjectName(u"label_31")
        sizePolicy6.setHeightForWidth(self.label_31.sizePolicy().hasHeightForWidth())
        self.label_31.setSizePolicy(sizePolicy6)

        self.horizontalLayout_46.addWidget(self.label_31)

        self.horizontalSpacer_14 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_46.addItem(self.horizontalSpacer_14)

        self.cbb_dpi_2 = ComboBox(self.widget_5_10)
        self.cbb_dpi_2.addItem("")
        self.cbb_dpi_2.addItem("")
        self.cbb_dpi_2.addItem("")
        self.cbb_dpi_2.setObjectName(u"cbb_dpi_2")
        self.cbb_dpi_2.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_46.addWidget(self.cbb_dpi_2)


        self.verticalLayout_2.addWidget(self.widget_5_10)

        self.widget_5_11 = QWidget(self.widget_5)
        self.widget_5_11.setObjectName(u"widget_5_11")
        self.widget_5_11.setStyleSheet(u"")
        self.horizontalLayout_47 = QHBoxLayout(self.widget_5_11)
        self.horizontalLayout_47.setObjectName(u"horizontalLayout_47")
        self.icon_theme_color = IconWidget(self.widget_5_11)
        self.icon_theme_color.setObjectName(u"icon_theme_color")
        sizePolicy.setHeightForWidth(self.icon_theme_color.sizePolicy().hasHeightForWidth())
        self.icon_theme_color.setSizePolicy(sizePolicy)
        self.icon_theme_color.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_47.addWidget(self.icon_theme_color)

        self.label_20 = StrongBodyLabel(self.widget_5_11)
        self.label_20.setObjectName(u"label_20")
        sizePolicy6.setHeightForWidth(self.label_20.sizePolicy().hasHeightForWidth())
        self.label_20.setSizePolicy(sizePolicy6)

        self.horizontalLayout_47.addWidget(self.label_20)

        self.label_32 = CaptionLabel(self.widget_5_11)
        self.label_32.setObjectName(u"label_32")
        sizePolicy6.setHeightForWidth(self.label_32.sizePolicy().hasHeightForWidth())
        self.label_32.setSizePolicy(sizePolicy6)

        self.horizontalLayout_47.addWidget(self.label_32)

        self.horizontalSpacer_18 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_47.addItem(self.horizontalSpacer_18)

        self.btn_choose_color = PushButton(self.widget_5_11)
        self.btn_choose_color.setObjectName(u"btn_choose_color")
        sizePolicy.setHeightForWidth(self.btn_choose_color.sizePolicy().hasHeightForWidth())
        self.btn_choose_color.setSizePolicy(sizePolicy)
        self.btn_choose_color.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_47.addWidget(self.btn_choose_color)


        self.verticalLayout_2.addWidget(self.widget_5_11)

        self.verticalSpacer_3 = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)

        self.verticalLayout_2.addItem(self.verticalSpacer_3)

        self.label_33 = SubtitleLabel(self.widget_5)
        self.label_33.setObjectName(u"label_33")

        self.verticalLayout_2.addWidget(self.label_33)

        self.widget_5_12 = QWidget(self.widget_5)
        self.widget_5_12.setObjectName(u"widget_5_12")
        self.widget_5_12.setStyleSheet(u"")
        self.horizontalLayout_48 = QHBoxLayout(self.widget_5_12)
        self.horizontalLayout_48.setObjectName(u"horizontalLayout_48")
        self.icon_ykl = IconWidget(self.widget_5_12)
        self.icon_ykl.setObjectName(u"icon_ykl")
        sizePolicy.setHeightForWidth(self.icon_ykl.sizePolicy().hasHeightForWidth())
        self.icon_ykl.setSizePolicy(sizePolicy)
        self.icon_ykl.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_48.addWidget(self.icon_ykl)

        self.label_34 = StrongBodyLabel(self.widget_5_12)
        self.label_34.setObjectName(u"label_34")
        sizePolicy6.setHeightForWidth(self.label_34.sizePolicy().hasHeightForWidth())
        self.label_34.setSizePolicy(sizePolicy6)

        self.horizontalLayout_48.addWidget(self.label_34)

        self.label_35 = CaptionLabel(self.widget_5_12)
        self.label_35.setObjectName(u"label_35")
        sizePolicy6.setHeightForWidth(self.label_35.sizePolicy().hasHeightForWidth())
        self.label_35.setSizePolicy(sizePolicy6)

        self.horizontalLayout_48.addWidget(self.label_35)

        self.horizontalSpacer_19 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_48.addItem(self.horizontalSpacer_19)

        self.horizontalSlider = Slider(self.widget_5_12)
        self.horizontalSlider.setObjectName(u"horizontalSlider")
        sizePolicy7 = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Expanding)
        sizePolicy7.setHorizontalStretch(0)
        sizePolicy7.setVerticalStretch(0)
        sizePolicy7.setHeightForWidth(self.horizontalSlider.sizePolicy().hasHeightForWidth())
        self.horizontalSlider.setSizePolicy(sizePolicy7)
        self.horizontalSlider.setMinimumSize(QSize(200, 0))
        self.horizontalSlider.setOrientation(Qt.Horizontal)

        self.horizontalLayout_48.addWidget(self.horizontalSlider)


        self.verticalLayout_2.addWidget(self.widget_5_12)

        self.verticalSpacer = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)

        self.verticalLayout_2.addItem(self.verticalSpacer)

        self.label_19 = SubtitleLabel(self.widget_5)
        self.label_19.setObjectName(u"label_19")

        self.verticalLayout_2.addWidget(self.label_19)

        self.widget_5_2 = QWidget(self.widget_5)
        self.widget_5_2.setObjectName(u"widget_5_2")
        self.widget_5_2.setStyleSheet(u"")
        self.horizontalLayout_42 = QHBoxLayout(self.widget_5_2)
        self.horizontalLayout_42.setObjectName(u"horizontalLayout_42")
        self.icon_help = IconWidget(self.widget_5_2)
        self.icon_help.setObjectName(u"icon_help")
        sizePolicy.setHeightForWidth(self.icon_help.sizePolicy().hasHeightForWidth())
        self.icon_help.setSizePolicy(sizePolicy)
        self.icon_help.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_42.addWidget(self.icon_help)

        self.label_21 = StrongBodyLabel(self.widget_5_2)
        self.label_21.setObjectName(u"label_21")
        sizePolicy6.setHeightForWidth(self.label_21.sizePolicy().hasHeightForWidth())
        self.label_21.setSizePolicy(sizePolicy6)

        self.horizontalLayout_42.addWidget(self.label_21)

        self.label_26 = CaptionLabel(self.widget_5_2)
        self.label_26.setObjectName(u"label_26")
        sizePolicy6.setHeightForWidth(self.label_26.sizePolicy().hasHeightForWidth())
        self.label_26.setSizePolicy(sizePolicy6)

        self.horizontalLayout_42.addWidget(self.label_26)

        self.horizontalSpacer_10 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_42.addItem(self.horizontalSpacer_10)

        self.btn_file_blood_2 = PushButton(self.widget_5_2)
        self.btn_file_blood_2.setObjectName(u"btn_file_blood_2")
        sizePolicy.setHeightForWidth(self.btn_file_blood_2.sizePolicy().hasHeightForWidth())
        self.btn_file_blood_2.setSizePolicy(sizePolicy)
        self.btn_file_blood_2.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_42.addWidget(self.btn_file_blood_2)


        self.verticalLayout_2.addWidget(self.widget_5_2)

        self.widget_5_3 = QWidget(self.widget_5)
        self.widget_5_3.setObjectName(u"widget_5_3")
        self.widget_5_3.setStyleSheet(u"")
        self.horizontalLayout_43 = QHBoxLayout(self.widget_5_3)
        self.horizontalLayout_43.setObjectName(u"horizontalLayout_43")
        self.icon_res = IconWidget(self.widget_5_3)
        self.icon_res.setObjectName(u"icon_res")
        sizePolicy.setHeightForWidth(self.icon_res.sizePolicy().hasHeightForWidth())
        self.icon_res.setSizePolicy(sizePolicy)
        self.icon_res.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_43.addWidget(self.icon_res)

        self.label_22 = StrongBodyLabel(self.widget_5_3)
        self.label_22.setObjectName(u"label_22")
        sizePolicy6.setHeightForWidth(self.label_22.sizePolicy().hasHeightForWidth())
        self.label_22.setSizePolicy(sizePolicy6)

        self.horizontalLayout_43.addWidget(self.label_22)

        self.label_25 = CaptionLabel(self.widget_5_3)
        self.label_25.setObjectName(u"label_25")
        sizePolicy6.setHeightForWidth(self.label_25.sizePolicy().hasHeightForWidth())
        self.label_25.setSizePolicy(sizePolicy6)

        self.horizontalLayout_43.addWidget(self.label_25)

        self.horizontalSpacer_11 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_43.addItem(self.horizontalSpacer_11)

        self.btn_file_blood_3 = PushButton(self.widget_5_3)
        self.btn_file_blood_3.setObjectName(u"btn_file_blood_3")
        sizePolicy.setHeightForWidth(self.btn_file_blood_3.sizePolicy().hasHeightForWidth())
        self.btn_file_blood_3.setSizePolicy(sizePolicy)
        self.btn_file_blood_3.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_43.addWidget(self.btn_file_blood_3)


        self.verticalLayout_2.addWidget(self.widget_5_3)

        self.widget_5_4 = QWidget(self.widget_5)
        self.widget_5_4.setObjectName(u"widget_5_4")
        self.widget_5_4.setStyleSheet(u"")
        self.horizontalLayout_45 = QHBoxLayout(self.widget_5_4)
        self.horizontalLayout_45.setObjectName(u"horizontalLayout_45")
        self.icon_update = IconWidget(self.widget_5_4)
        self.icon_update.setObjectName(u"icon_update")
        sizePolicy.setHeightForWidth(self.icon_update.sizePolicy().hasHeightForWidth())
        self.icon_update.setSizePolicy(sizePolicy)
        self.icon_update.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_45.addWidget(self.icon_update)

        self.label_28 = StrongBodyLabel(self.widget_5_4)
        self.label_28.setObjectName(u"label_28")
        sizePolicy6.setHeightForWidth(self.label_28.sizePolicy().hasHeightForWidth())
        self.label_28.setSizePolicy(sizePolicy6)

        self.horizontalLayout_45.addWidget(self.label_28)

        self.label_29 = CaptionLabel(self.widget_5_4)
        self.label_29.setObjectName(u"label_29")
        sizePolicy6.setHeightForWidth(self.label_29.sizePolicy().hasHeightForWidth())
        self.label_29.setSizePolicy(sizePolicy6)

        self.horizontalLayout_45.addWidget(self.label_29)

        self.horizontalSpacer_17 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_45.addItem(self.horizontalSpacer_17)

        self.btn_update_log = PushButton(self.widget_5_4)
        self.btn_update_log.setObjectName(u"btn_update_log")
        sizePolicy.setHeightForWidth(self.btn_update_log.sizePolicy().hasHeightForWidth())
        self.btn_update_log.setSizePolicy(sizePolicy)
        self.btn_update_log.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_45.addWidget(self.btn_update_log)


        self.verticalLayout_2.addWidget(self.widget_5_4)

        self.widget_5_5 = QWidget(self.widget_5)
        self.widget_5_5.setObjectName(u"widget_5_5")
        self.widget_5_5.setStyleSheet(u"")
        self.horizontalLayout_44 = QHBoxLayout(self.widget_5_5)
        self.horizontalLayout_44.setObjectName(u"horizontalLayout_44")
        self.icon_about = IconWidget(self.widget_5_5)
        self.icon_about.setObjectName(u"icon_about")
        sizePolicy.setHeightForWidth(self.icon_about.sizePolicy().hasHeightForWidth())
        self.icon_about.setSizePolicy(sizePolicy)
        self.icon_about.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_44.addWidget(self.icon_about)

        self.label_23 = StrongBodyLabel(self.widget_5_5)
        self.label_23.setObjectName(u"label_23")
        sizePolicy6.setHeightForWidth(self.label_23.sizePolicy().hasHeightForWidth())
        self.label_23.setSizePolicy(sizePolicy6)

        self.horizontalLayout_44.addWidget(self.label_23)

        self.label_24 = CaptionLabel(self.widget_5_5)
        self.label_24.setObjectName(u"label_24")
        sizePolicy6.setHeightForWidth(self.label_24.sizePolicy().hasHeightForWidth())
        self.label_24.setSizePolicy(sizePolicy6)

        self.horizontalLayout_44.addWidget(self.label_24)

        self.horizontalSpacer_13 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_44.addItem(self.horizontalSpacer_13)

        self.btn_file_blood_5 = PushButton(self.widget_5_5)
        self.btn_file_blood_5.setObjectName(u"btn_file_blood_5")
        sizePolicy.setHeightForWidth(self.btn_file_blood_5.sizePolicy().hasHeightForWidth())
        self.btn_file_blood_5.setSizePolicy(sizePolicy)
        self.btn_file_blood_5.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_44.addWidget(self.btn_file_blood_5)


        self.verticalLayout_2.addWidget(self.widget_5_5)

        self.verticalSpacer_4 = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)

        self.verticalLayout_2.addItem(self.verticalSpacer_4)

        self.verticalSpacer_2 = QSpacerItem(20, 40, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)

        self.verticalLayout_2.addItem(self.verticalSpacer_2)

        self.label_11 = SubtitleLabel(self.widget_5)
        self.label_11.setObjectName(u"label_11")

        self.verticalLayout_2.addWidget(self.label_11)

        self.widget_5_6 = QWidget(self.widget_5)
        self.widget_5_6.setObjectName(u"widget_5_6")
        self.widget_5_6.setStyleSheet(u"")
        self.horizontalLayout_24 = QHBoxLayout(self.widget_5_6)
        self.horizontalLayout_24.setObjectName(u"horizontalLayout_24")
        self.icon_file_blood = IconWidget(self.widget_5_6)
        self.icon_file_blood.setObjectName(u"icon_file_blood")
        sizePolicy.setHeightForWidth(self.icon_file_blood.sizePolicy().hasHeightForWidth())
        self.icon_file_blood.setSizePolicy(sizePolicy)
        self.icon_file_blood.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_24.addWidget(self.icon_file_blood)

        self.label_12 = StrongBodyLabel(self.widget_5_6)
        self.label_12.setObjectName(u"label_12")
        sizePolicy6.setHeightForWidth(self.label_12.sizePolicy().hasHeightForWidth())
        self.label_12.setSizePolicy(sizePolicy6)

        self.horizontalLayout_24.addWidget(self.label_12)

        self.label_2 = CaptionLabel(self.widget_5_6)
        self.label_2.setObjectName(u"label_2")
        sizePolicy6.setHeightForWidth(self.label_2.sizePolicy().hasHeightForWidth())
        self.label_2.setSizePolicy(sizePolicy6)

        self.horizontalLayout_24.addWidget(self.label_2)

        self.horizontalSpacer_5 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_24.addItem(self.horizontalSpacer_5)

        self.btn_file_blood = PrimaryPushButton(self.widget_5_6)
        self.btn_file_blood.setObjectName(u"btn_file_blood")
        sizePolicy.setHeightForWidth(self.btn_file_blood.sizePolicy().hasHeightForWidth())
        self.btn_file_blood.setSizePolicy(sizePolicy)
        self.btn_file_blood.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_24.addWidget(self.btn_file_blood)


        self.verticalLayout_2.addWidget(self.widget_5_6)

        self.widget_5_7 = QWidget(self.widget_5)
        self.widget_5_7.setObjectName(u"widget_5_7")
        self.horizontalLayout_23 = QHBoxLayout(self.widget_5_7)
        self.horizontalLayout_23.setObjectName(u"horizontalLayout_23")
        self.horizontalLayout_23.setContentsMargins(9, 9, 9, 9)
        self.icon_col_trans = IconWidget(self.widget_5_7)
        self.icon_col_trans.setObjectName(u"icon_col_trans")
        sizePolicy.setHeightForWidth(self.icon_col_trans.sizePolicy().hasHeightForWidth())
        self.icon_col_trans.setSizePolicy(sizePolicy)
        self.icon_col_trans.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_23.addWidget(self.icon_col_trans)

        self.label_15 = StrongBodyLabel(self.widget_5_7)
        self.label_15.setObjectName(u"label_15")
        sizePolicy6.setHeightForWidth(self.label_15.sizePolicy().hasHeightForWidth())
        self.label_15.setSizePolicy(sizePolicy6)

        self.horizontalLayout_23.addWidget(self.label_15)

        self.label = CaptionLabel(self.widget_5_7)
        self.label.setObjectName(u"label")

        self.horizontalLayout_23.addWidget(self.label)

        self.horizontalSpacer_9 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_23.addItem(self.horizontalSpacer_9)

        self.btn_col_trans = PrimaryPushButton(self.widget_5_7)
        self.btn_col_trans.setObjectName(u"btn_col_trans")
        sizePolicy.setHeightForWidth(self.btn_col_trans.sizePolicy().hasHeightForWidth())
        self.btn_col_trans.setSizePolicy(sizePolicy)
        self.btn_col_trans.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_23.addWidget(self.btn_col_trans)


        self.verticalLayout_2.addWidget(self.widget_5_7)

        self.widget_5_8 = QWidget(self.widget_5)
        self.widget_5_8.setObjectName(u"widget_5_8")
        self.horizontalLayout_41 = QHBoxLayout(self.widget_5_8)
        self.horizontalLayout_41.setObjectName(u"horizontalLayout_41")
        self.horizontalLayout_41.setContentsMargins(9, 9, 9, 9)
        self.icon_similarity = IconWidget(self.widget_5_8)
        self.icon_similarity.setObjectName(u"icon_similarity")
        sizePolicy.setHeightForWidth(self.icon_similarity.sizePolicy().hasHeightForWidth())
        self.icon_similarity.setSizePolicy(sizePolicy)
        self.icon_similarity.setMinimumSize(QSize(16, 16))

        self.horizontalLayout_41.addWidget(self.icon_similarity)

        self.label_16 = StrongBodyLabel(self.widget_5_8)
        self.label_16.setObjectName(u"label_16")

        self.horizontalLayout_41.addWidget(self.label_16)

        self.label_7 = CaptionLabel(self.widget_5_8)
        self.label_7.setObjectName(u"label_7")

        self.horizontalLayout_41.addWidget(self.label_7)

        self.horizontalSpacer_12 = QSpacerItem(40, 20, QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)

        self.horizontalLayout_41.addItem(self.horizontalSpacer_12)

        self.btn_similarity = PrimaryPushButton(self.widget_5_8)
        self.btn_similarity.setObjectName(u"btn_similarity")
        sizePolicy.setHeightForWidth(self.btn_similarity.sizePolicy().hasHeightForWidth())
        self.btn_similarity.setSizePolicy(sizePolicy)
        self.btn_similarity.setMinimumSize(QSize(100, 0))

        self.horizontalLayout_41.addWidget(self.btn_similarity)


        self.verticalLayout_2.addWidget(self.widget_5_8)

        self.scrollArea.setWidget(self.widget_5)

        self.horizontalLayout_9.addWidget(self.scrollArea)

        self.stackedWidget.addWidget(self.tab_settings)

        self.verticalLayout_13.addWidget(self.stackedWidget)

        MainWindow.setCentralWidget(self.centralwidget)

        self.retranslateUi(MainWindow)

        self.stackedWidget.setCurrentIndex(0)
        self.staw_sql.setCurrentIndex(0)
        self.staw_sql_1.setCurrentIndex(0)
        self.staw_sql_2.setCurrentIndex(0)
        self.staw_sql_3.setCurrentIndex(0)


        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"DataTools", None))
        self.pushButton_1.setText(QCoreApplication.translate("MainWindow", u"\u8868\u9884\u89c8", None))
        self.pushButton_2.setText(QCoreApplication.translate("MainWindow", u"\u8fc7\u7a0b\u9884\u89c8", None))
        self.pushButton_3.setText(QCoreApplication.translate("MainWindow", u"SQ\u6267\u884c\u7a97\u53e3", None))
        self.pushButton_4.setText(QCoreApplication.translate("MainWindow", u"\u6570\u636e\u5e93\u914d\u7f6e", None))
        self.pushButton_5.setText(QCoreApplication.translate("MainWindow", u"\u7cfb\u7edf\u8bbe\u7f6e", None))
        self.cbb_choose_db.setItemText(0, QCoreApplication.translate("MainWindow", u"\u4fe1\u8d37sit", None))
        self.cbb_choose_db.setItemText(1, QCoreApplication.translate("MainWindow", u"\u6838\u5fc3uit", None))
        self.cbb_choose_db.setItemText(2, QCoreApplication.translate("MainWindow", u"\u5f00\u53d1\u5e93", None))

        self.cbb_choose_db.setCurrentText("")
        self.cbb_choose_db.setPlaceholderText(QCoreApplication.translate("MainWindow", u"--\u9009\u62e9\u6570\u636e\u5e93\u8fde\u63a5--", None))
        self.btn_connect.setText(QCoreApplication.translate("MainWindow", u"\u8fde\u63a5", None))
        self.cbb_database.setItemText(0, QCoreApplication.translate("MainWindow", u"\u6240\u6709\u5e93", None))
        self.cbb_database.setItemText(1, QCoreApplication.translate("MainWindow", u"\u5f53\u524d\u5e93", None))

        self.edt_table.setText("")
        self.edt_table.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u6a21\u7cca\u5339\u914d\uff0c\u4e0d\u533a\u5206\u5927\u5c0f\u5199...", None))
        ___qtablewidgetitem = self.tbw_table.horizontalHeaderItem(0)
        ___qtablewidgetitem.setText(QCoreApplication.translate("MainWindow", u"\u5e93", None));
        ___qtablewidgetitem1 = self.tbw_table.horizontalHeaderItem(1)
        ___qtablewidgetitem1.setText(QCoreApplication.translate("MainWindow", u"\u8868\u540d", None));
        ___qtablewidgetitem2 = self.tbw_table.horizontalHeaderItem(2)
        ___qtablewidgetitem2.setText(QCoreApplication.translate("MainWindow", u"\u6ce8\u91ca", None));
        self.cbb_find_col_1.setItemText(0, QCoreApplication.translate("MainWindow", u"--\u6761\u4ef61--", None))

        self.cbb_find_col_2.setItemText(0, QCoreApplication.translate("MainWindow", u"--\u6761\u4ef62--", None))

        self.cbb_find_col_3.setItemText(0, QCoreApplication.translate("MainWindow", u"--\u6761\u4ef63--", None))

        self.edt_result_row.setText("")
        self.edt_result_row.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u9ed8\u8ba41000\u884c", None))
        self.btn_get_data.setText(QCoreApplication.translate("MainWindow", u"\u67e5\u8be2\u6570\u636e", None))
        self.btn_reset.setText(QCoreApplication.translate("MainWindow", u"\u91cd\u7f6e\u6761\u4ef6", None))
        self.cbx_is_sensitive.setText(QCoreApplication.translate("MainWindow", u"\u533a\u5206\u5927\u5c0f\u5199", None))
        self.btn_mapping.setText(QCoreApplication.translate("MainWindow", u"\u5bfc\u51faMAPPING", None))
        self.btn_trans.setText(QCoreApplication.translate("MainWindow", u"\u7ed3\u6784\u8f6c\u6362", None))
        self.btn_trans_setting.setText(QCoreApplication.translate("MainWindow", u"\u8f6c\u6362\u914d\u7f6e", None))
        self.lab_table_comment.setText(QCoreApplication.translate("MainWindow", u"\u8868\u540d\uff08\u6ce8\u91ca\uff09", None))
        ___qtablewidgetitem3 = self.tableWidget.horizontalHeaderItem(0)
        ___qtablewidgetitem3.setText(QCoreApplication.translate("MainWindow", u"\u8868\u540d", None));
        ___qtablewidgetitem4 = self.tableWidget.horizontalHeaderItem(1)
        ___qtablewidgetitem4.setText(QCoreApplication.translate("MainWindow", u"\u8868\u6ce8\u91ca", None));
        ___qtablewidgetitem5 = self.tableWidget.horizontalHeaderItem(2)
        ___qtablewidgetitem5.setText(QCoreApplication.translate("MainWindow", u"\u5e8f\u53f7", None));
        ___qtablewidgetitem6 = self.tableWidget.horizontalHeaderItem(3)
        ___qtablewidgetitem6.setText(QCoreApplication.translate("MainWindow", u"\u5b57\u6bb5\u540d", None));
        ___qtablewidgetitem7 = self.tableWidget.horizontalHeaderItem(4)
        ___qtablewidgetitem7.setText(QCoreApplication.translate("MainWindow", u"\u5b57\u6bb5\u7c7b\u578b", None));
        ___qtablewidgetitem8 = self.tableWidget.horizontalHeaderItem(5)
        ___qtablewidgetitem8.setText(QCoreApplication.translate("MainWindow", u"\u5b57\u6bb5\u6ce8\u91ca", None));
        self.edt_yf.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u8bf7\u8f93\u5165\u7cfb\u7edf\u540d...", None))
        self.btn_yf.setText(QCoreApplication.translate("MainWindow", u"\u7ffb\u8bd1GBase\u8bed\u53e5", None))
        self.lab_sta.setText(QCoreApplication.translate("MainWindow", u"STA\u5c42DDL\u8bed\u53e5", None))
        self.lab_ods.setText(QCoreApplication.translate("MainWindow", u"ODS\u5c42DDL\u8bed\u53e5", None))
        self.cbb_find_procedure.setItemText(0, QCoreApplication.translate("MainWindow", u"\u6240\u6709\u5e93", None))
        self.cbb_find_procedure.setItemText(1, QCoreApplication.translate("MainWindow", u"\u5f53\u524d\u5e93", None))

        self.edt_find_procedure.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u6a21\u7cca\u5339\u914d\uff0c\u4e0d\u533a\u5206\u5927\u5c0f\u5199...", None))
        ___qtablewidgetitem9 = self.tbw_procedure.horizontalHeaderItem(0)
        ___qtablewidgetitem9.setText(QCoreApplication.translate("MainWindow", u"\u5e93", None));
        ___qtablewidgetitem10 = self.tbw_procedure.horizontalHeaderItem(1)
        ___qtablewidgetitem10.setText(QCoreApplication.translate("MainWindow", u"\u7c7b\u578b", None));
        ___qtablewidgetitem11 = self.tbw_procedure.horizontalHeaderItem(2)
        ___qtablewidgetitem11.setText(QCoreApplication.translate("MainWindow", u"\u8fc7\u7a0b\u540d", None));
        self.lab_procedure_name.setText(QCoreApplication.translate("MainWindow", u"\u8fc7\u7a0b\u540d", None))
        self.lab_procedure_blood.setText(QCoreApplication.translate("MainWindow", u"\u8840\u7f18\u4f9d\u8d56\uff1a", None))
        self.pushButton.setText(QCoreApplication.translate("MainWindow", u"SQL\u6267\u884c\u7a97\u53e3\u4e00", None))
        self.pushButton_9.setText(QCoreApplication.translate("MainWindow", u"SQL\u6267\u884c\u7a97\u53e3\u4e8c", None))
        self.pushButton_10.setText(QCoreApplication.translate("MainWindow", u"SQL\u6267\u884c\u7a97\u53e3\u4e09", None))
        self.label_3.setText(QCoreApplication.translate("MainWindow", u"\u67e5\u8be2\u6570\u636e\u884c\u6570\uff1a", None))
        self.edt_sql_row_1.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u9ed8\u8ba41000\u884c", None))
        self.btn_sql_execute_1.setText(QCoreApplication.translate("MainWindow", u"\u6267\u884c", None))
        self.edt_sql_book_1.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u4e66\u7b7e\u540d", None))
        self.btn_sql_save_1.setText(QCoreApplication.translate("MainWindow", u"\u4fdd\u5b58\u4e66\u7b7e", None))
        self.btn_sql_del_1.setText(QCoreApplication.translate("MainWindow", u"\u5220\u9664\u4e66\u7b7e", None))
        self.pushButton_11.setText(QCoreApplication.translate("MainWindow", u"\u6267\u884c\u7ed3\u679c", None))
        self.pushButton_12.setText(QCoreApplication.translate("MainWindow", u"\u6267\u884c\u65e5\u5fd7", None))
        self.label_9.setText(QCoreApplication.translate("MainWindow", u"\u67e5\u8be2\u6570\u636e\u884c\u6570\uff1a", None))
        self.edt_sql_row_2.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u9ed8\u8ba41000\u884c", None))
        self.btn_sql_execute_2.setText(QCoreApplication.translate("MainWindow", u"\u6267\u884c", None))
        self.edt_sql_book_2.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u4e66\u7b7e\u540d", None))
        self.btn_sql_save_2.setText(QCoreApplication.translate("MainWindow", u"\u4fdd\u5b58\u4e66\u7b7e", None))
        self.btn_sql_del_2.setText(QCoreApplication.translate("MainWindow", u"\u5220\u9664\u4e66\u7b7e", None))
        self.pushButton_16.setText(QCoreApplication.translate("MainWindow", u"\u6267\u884c\u7ed3\u679c", None))
        self.pushButton_17.setText(QCoreApplication.translate("MainWindow", u"\u6267\u884c\u65e5\u5fd7", None))
        self.label_10.setText(QCoreApplication.translate("MainWindow", u"\u67e5\u8be2\u6570\u636e\u884c\u6570\uff1a", None))
        self.edt_sql_row_3.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u9ed8\u8ba41000\u884c", None))
        self.btn_sql_execute_3.setText(QCoreApplication.translate("MainWindow", u"\u6267\u884c", None))
        self.edt_sql_book_3.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u4e66\u7b7e\u540d", None))
        self.btn_sql_save_3.setText(QCoreApplication.translate("MainWindow", u"\u4fdd\u5b58\u4e66\u7b7e", None))
        self.btn_sql_del_3.setText(QCoreApplication.translate("MainWindow", u"\u5220\u9664\u4e66\u7b7e", None))
        self.pushButton_19.setText(QCoreApplication.translate("MainWindow", u"\u6267\u884c\u7ed3\u679c", None))
        self.pushButton_20.setText(QCoreApplication.translate("MainWindow", u"\u6267\u884c\u65e5\u5fd7", None))
        self.label_13.setText(QCoreApplication.translate("MainWindow", u"SQL\u4e66\u7b7e\uff1a", None))
        self.label_5.setText(QCoreApplication.translate("MainWindow", u"\u6570\u636e\u5e93\u8fde\u63a5", None))
        self.btn_dbinfo_add.setText(QCoreApplication.translate("MainWindow", u"\u65b0\u589e", None))
        self.btn_dbinfo_del.setText(QCoreApplication.translate("MainWindow", u"\u5220\u9664", None))
        self.label_4.setText(QCoreApplication.translate("MainWindow", u"\u8fde\u63a5\u540d\uff1a", None))
        self.edt_name.setPlaceholderText("")
        self.label_6.setText(QCoreApplication.translate("MainWindow", u"  \u63cf\u8ff0\uff1a", None))
        self.edt_desc.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u53ef\u4e0d\u586b...", None))
        self.lab_type.setText(QCoreApplication.translate("MainWindow", u"\u6570\u636e\u5e93\uff1a", None))
        self.cbb_type.setItemText(0, QCoreApplication.translate("MainWindow", u"--\u8bf7\u9009\u62e9--", None))
        self.cbb_type.setItemText(1, QCoreApplication.translate("MainWindow", u"Oracle", None))
        self.cbb_type.setItemText(2, QCoreApplication.translate("MainWindow", u"Mysql", None))
        self.cbb_type.setItemText(3, QCoreApplication.translate("MainWindow", u"GBase", None))
        self.cbb_type.setItemText(4, QCoreApplication.translate("MainWindow", u"Dameng", None))

        self.lab_host.setText(QCoreApplication.translate("MainWindow", u"\u4e3b\u673aIP\uff1a", None))
        self.edt_host.setPlaceholderText("")
        self.lab_port.setText(QCoreApplication.translate("MainWindow", u"\u7aef\u53e3\u53f7\uff1a", None))
        self.edt_port.setPlaceholderText("")
        self.lab_database.setText(QCoreApplication.translate("MainWindow", u"  \u5e93\u540d\uff1a", None))
        self.edt_database.setPlaceholderText("")
        self.lab_username.setText(QCoreApplication.translate("MainWindow", u"\u7528\u6237\u540d\uff1a", None))
        self.edt_username.setPlaceholderText("")
        self.lab_password.setText(QCoreApplication.translate("MainWindow", u"  \u5bc6\u7801\uff1a", None))
        self.edt_password.setPlaceholderText("")
        self.lab_charset.setText(QCoreApplication.translate("MainWindow", u"\u5b57\u7b26\u96c6\uff1a", None))
        self.edt_charset.setPlaceholderText(QCoreApplication.translate("MainWindow", u"\u53ef\u4e0d\u586b...", None))
        self.btn_test_db.setText(QCoreApplication.translate("MainWindow", u"\u6d4b\u8bd5\u8fde\u63a5", None))
        self.btn_save_db.setText(QCoreApplication.translate("MainWindow", u"\u4fdd\u5b58", None))
        self.label_18.setText(QCoreApplication.translate("MainWindow", u"\u4e2a\u6027\u5316", None))
        self.label_8.setText(QCoreApplication.translate("MainWindow", u"\u754c\u9762\u7f29\u653e", None))
        self.label_27.setText(QCoreApplication.translate("MainWindow", u"\u8c03\u6574\u5c0f\u90e8\u4ef6\u548c\u5b57\u4f53\u7684\u5927\u5c0f", None))
        self.cbb_dpi.setItemText(0, QCoreApplication.translate("MainWindow", u"AUTO", None))
        self.cbb_dpi.setItemText(1, QCoreApplication.translate("MainWindow", u"75%", None))
        self.cbb_dpi.setItemText(2, QCoreApplication.translate("MainWindow", u"100%", None))
        self.cbb_dpi.setItemText(3, QCoreApplication.translate("MainWindow", u"150%", None))
        self.cbb_dpi.setItemText(4, QCoreApplication.translate("MainWindow", u"200%", None))
        self.cbb_dpi.setItemText(5, QCoreApplication.translate("MainWindow", u"300%", None))

        self.label_14.setText(QCoreApplication.translate("MainWindow", u"\u4e91\u6bcd\u6548\u679c", None))
        self.label_30.setText(QCoreApplication.translate("MainWindow", u"\u7a97\u53e3\u548c\u8868\u9762\u663e\u793a\u534a\u900f\u660e", None))
        self.label_17.setText(QCoreApplication.translate("MainWindow", u"\u5e94\u7528\u4e3b\u9898", None))
        self.label_31.setText(QCoreApplication.translate("MainWindow", u"\u8c03\u6574\u5e94\u7528\u7684\u4e3b\u9898\u5916\u89c2", None))
        self.cbb_dpi_2.setItemText(0, QCoreApplication.translate("MainWindow", u"\u8ddf\u968f\u7cfb\u7edf", None))
        self.cbb_dpi_2.setItemText(1, QCoreApplication.translate("MainWindow", u"\u6d45\u8272", None))
        self.cbb_dpi_2.setItemText(2, QCoreApplication.translate("MainWindow", u"\u6df1\u8272", None))

        self.label_20.setText(QCoreApplication.translate("MainWindow", u"\u4e3b\u9898\u989c\u8272", None))
        self.label_32.setText(QCoreApplication.translate("MainWindow", u"\u8c03\u6574\u5e94\u7528\u7684\u7684\u4e3b\u9898\u989c\u8272", None))
        self.btn_choose_color.setText(QCoreApplication.translate("MainWindow", u"\u9009\u62e9\u989c\u8272", None))
        self.label_33.setText(QCoreApplication.translate("MainWindow", u"\u6750\u6599", None))
        self.label_34.setText(QCoreApplication.translate("MainWindow", u"\u4e9a\u514b\u529b\u78e8\u7802\u534a\u5f84", None))
        self.label_35.setText(QCoreApplication.translate("MainWindow", u"\u78e8\u7802\u534a\u5f84\u8d8a\u5927\uff0c\u56fe\u8c61\u8d8a\u6a21\u7cca", None))
        self.label_19.setText(QCoreApplication.translate("MainWindow", u"\u5173\u4e8e", None))
        self.label_21.setText(QCoreApplication.translate("MainWindow", u"\u4f7f\u7528\u5e2e\u52a9", None))
        self.label_26.setText(QCoreApplication.translate("MainWindow", u"\u53d1\u73b0\u65b0\u529f\u80fd\u5e76\u4e86\u89e3\u6709\u5173DataTools\u7684\u4f7f\u7528\u6280\u5de7", None))
        self.btn_file_blood_2.setText(QCoreApplication.translate("MainWindow", u"\u5e2e\u52a9\u9875\u9762", None))
        self.label_22.setText(QCoreApplication.translate("MainWindow", u"\u63d0\u4f9b\u53cd\u9988", None))
        self.label_25.setText(QCoreApplication.translate("MainWindow", u"\u901a\u8fc7\u63d0\u4f9b\u53cd\u9988\u5e2e\u52a9\u6211\u6539\u8fdbDataTools", None))
        self.btn_file_blood_3.setText(QCoreApplication.translate("MainWindow", u"\u63d0\u4f9b\u53cd\u9988", None))
        self.label_28.setText(QCoreApplication.translate("MainWindow", u"\u66f4\u65b0\u65e5\u5fd7", None))
        self.label_29.setText(QCoreApplication.translate("MainWindow", u"\u5386\u53f2\u66f4\u65b0\u65e5\u5fd7", None))
        self.btn_update_log.setText(QCoreApplication.translate("MainWindow", u"\u66f4\u65b0\u65e5\u5fd7", None))
        self.label_23.setText(QCoreApplication.translate("MainWindow", u"\u5173\u4e8eAPP", None))
        self.label_24.setText(QCoreApplication.translate("MainWindow", u"\u7248\u6743\u6240\u6709@2024, LJZ. \u5f53\u524d\u7248\u672c2.1.5", None))
        self.btn_file_blood_5.setText(QCoreApplication.translate("MainWindow", u"\u68c0\u67e5\u66f4\u65b0", None))
        self.label_11.setText(QCoreApplication.translate("MainWindow", u"\u5176\u4ed6\u529f\u80fd\u7a97\u53e3", None))
        self.label_12.setText(QCoreApplication.translate("MainWindow", u"\u4f9d\u8d56\u89e3\u6790", None))
        self.label_2.setText(QCoreApplication.translate("MainWindow", u"\u89e3\u6790\u811a\u672c\u6587\u4ef6\u4e2d\u7684\u8868\u7ea7\u4f9d\u8d56", None))
        self.btn_file_blood.setText(QCoreApplication.translate("MainWindow", u"\u6253\u5f00", None))
        self.label_15.setText(QCoreApplication.translate("MainWindow", u"\u5b57\u6bb5\u7ffb\u8bd1", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"\u5c06\u4e2d\u6587\u5b57\u6bb5\u7ffb\u8bd1\u6210\u6807\u51c6\u8bdd\u7684\u82f1\u6587\u5b57\u6bb5\u540d", None))
        self.btn_col_trans.setText(QCoreApplication.translate("MainWindow", u"\u6253\u5f00", None))
        self.label_16.setText(QCoreApplication.translate("MainWindow", u"\u76f8\u4f3c\u5ea6", None))
        self.label_7.setText(QCoreApplication.translate("MainWindow", u"\u63d0\u53d6\u4e00\u7ec4\u6587\u672c\u5728\u53e6\u4e00\u7ec4\u6587\u672c\u4e2d\u76f8\u4f3c\u5ea6\u8f83\u5927\u7684\u6587\u672c", None))
        self.btn_similarity.setText(QCoreApplication.translate("MainWindow", u"\u6253\u5f00", None))
    # retranslateUi

