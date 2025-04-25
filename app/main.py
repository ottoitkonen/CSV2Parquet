"""
CSV2Parquet

A simple GUI application to convert CSV files to Parquet format and vice versa.

TODO: Customizable conversion options (e.g., separator, compression).
"""

from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from app.convert import Convert


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CSV2Parquet")
        self.setFixedHeight(164)
        self.setMinimumWidth(480)
        layout = QVBoxLayout()
        io_layout = QHBoxLayout()
        btn_layout = QVBoxLayout()
        txt_layout = QVBoxLayout()

        self.input_file = None
        self.output_file = None
        self.file_type = None

        self.convert_label = "Convert"
        self.convert_button = QPushButton(self.convert_label)
        self.convert_button.clicked.connect(self.convert_file)
        self.convert_button.setEnabled(False)
        self.file_button = QPushButton("Open File")
        self.file_button.clicked.connect(self.open_file)
        io_layout.addWidget(self.file_button)
        io_layout.addWidget(self.convert_button)

        select_layout = QHBoxLayout()
        select_label = QLabel("Selected:")
        self.select_line = QLineEdit()
        self.select_line.setReadOnly(True)
        self.select_line.setPlaceholderText("No file selected")
        select_layout.addWidget(select_label)
        select_layout.addWidget(self.select_line)

        output_layout = QHBoxLayout()
        output_label = QLabel("Output:")
        self.output_line = QLineEdit()
        self.output_line.setReadOnly(True)
        self.output_line.setPlaceholderText("No output file")
        output_layout.addWidget(output_label)
        output_layout.addWidget(self.output_line)

        txt_layout.addLayout(select_layout)
        txt_layout.addLayout(output_layout)

        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setStyleSheet("background-color: red; color: white;")
        self.cancel_button.clicked.connect(self.close)
        btn_layout.addLayout(io_layout)
        btn_layout.addWidget(self.cancel_button)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("...")
        self.progress_bar.setTextVisible(True)

        layout.addLayout(txt_layout)
        layout.addStretch()
        layout.addWidget(self.progress_bar)
        layout.addLayout(btn_layout)

        widget = QWidget()
        widget.setLayout(layout)
        self.setCentralWidget(widget)

    def open_file(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self,
            "Open File",
            "",
            "CSV Files (*.csv *.gz *.bz2 *.zip *.xz *.zst *.tar *.tar.gz *.tar.xz *.tar.bz2);;Parquet Files (*.parquet)",
        )
        c2p_dict = {
            "csv": "parquet",
            "parquet": "csv",
        }

        if file_name:
            self.input_file = file_name
            file_tuple = self.input_file.rsplit(".", 1)
            if file_tuple[1] != "parquet":
                self.file_type = c2p_dict.get("parquet")
                output_name = file_tuple[0] + ".parquet"
                self.convert_label = "CSV to Parquet"
            else:
                self.file_type = c2p_dict.get("csv")
                output_name = file_tuple[0] + ".csv"
                self.convert_label = "Parquet to CSV"

            self.output_file, _ = QFileDialog.getSaveFileName(
                self,
                "Save File",
                output_name,
                f"All Files (*.{c2p_dict.get(self.file_type)})",
            )

            self.convert_button.setText(f"Convert {self.convert_label}")
            self.convert_button.setStyleSheet("background-color: green; color: white;")
            self.convert_button.setEnabled(True)
            self.select_line.setText(self.input_file)
            self.output_line.setText(self.output_file)
            self.progress_bar.setFormat("%p%")
            self.progress_bar.setValue(0)
            print(f"Selected file: {self.input_file}")

    def convert_file(self):
        self.progress_bar.setFormat("%p%")
        self.progress_bar.setValue(20)
        try:
            convert = Convert(
                input_file=self.input_file,
                output_file=self.output_file,
                file_type=self.file_type,
            )
            convert.convert()
            dialog_msg = f"File converted successfully:\n{self.output_file}"
            success = True
            self.progress_bar.setValue(100)
        except Exception as e:
            dialog_msg = f"Error converting file:\n{e}"
            success = False
            self.progress_bar.setValue(0)
        finally:
            if success:
                QMessageBox.information(self, "Success", dialog_msg)
            else:
                QMessageBox.critical(self, "Conversion failed", dialog_msg)
        print(f"File converted successfully: {self.output_file}")
        return True


app = QApplication([])

window = MainWindow()
window.show()

app.exec()
