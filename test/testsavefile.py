from PyQt5.QtWidgets import QFileDialog, QApplication


def save_file_dialog():
    app = QApplication([])

    filename, _ = QFileDialog.getSaveFileName(None,
                                              "Save File",
                                              ".txt",
                                              "All Files (*);;Text Files (*.txt)")

    if filename:
        # 使用filename来保存文件
        with open(filename, 'w') as file:
            file.write("Hello, world!")

    app.exit()


if __name__ == '__main__':
    save_file_dialog()
