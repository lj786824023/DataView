from PySide6.QtCore import Signal, QObject
from PySide6.QtWidgets import QWidget, QPushButton


class MyWidget(QWidget,QObject):
    my_signal = Signal()

    def __init__(self):
        super().__init__()
        self.button = QPushButton('Press me', self)

        # 正确连接信号和槽
        self.my_signal.connect(self.handle_signal)

        self.button.clicked.connect(self.emit_signal)

    def emit_signal(self):
        self.my_signal.emit()

    def handle_signal(self):
        print('Signal received')


# 创建并使用 MyWidget 实例
app = QWidget()
app.show()
