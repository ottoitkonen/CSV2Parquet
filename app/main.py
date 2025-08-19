"""
CSV2Parquet

A simple GUI application to:
- display the contents of a CSV or a Parquet file
- convert CSV files to Parquet format and vice versa
- cut CSV/Parquet files into smaller parts if preferred
"""

from os.path import splitext

from pandas import DataFrame, isna
from PySide6.QtCore import QAbstractTableModel, Qt
from PySide6.QtGui import QDragEnterEvent, QDropEvent
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QButtonGroup,
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QSpinBox,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from app.convert import Convert


class PandasModel(QAbstractTableModel):
    """A model to display a Pandas DataFrame in a QTableView."""

    def __init__(self, df=DataFrame(), parent=None):
        super().__init__(parent)
        self._df = df

    def rowCount(self, parent=None):
        return self._df.shape[0]

    def columnCount(self, parent=None):
        return self._df.shape[1]

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.ItemDataRole.DisplayRole:
            value = self._df.iloc[index.row(), index.column()]
            # Handle NaN, None, and other null values
            if isna(value) or value is None:
                return ""
            return str(value)
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            return (
                str(self._df.columns[section])
                if self._df.columns.size > section
                else ""
            )
        else:
            return str(self._df.index[section]) if self._df.index.size > section else ""

    def setDataFrame(self, df):
        self.beginResetModel()
        self._df = df
        self.endResetModel()


class MainWindow(QMainWindow):
    """Main application window for CSV-Parquet Handler."""

    def __init__(self):
        """Initialize the main window with widgets and layout."""

        super().__init__()
        self.setWindowTitle("CSV-Parquet Handler")
        self.setMinimumSize(540, 320)
        base_layout = QVBoxLayout()
        submit_layout = QHBoxLayout()
        io_layout = QVBoxLayout()

        self.input_file = ""
        self.output_file = ""
        self.input_type = None
        self.output_type = None
        self.input_settings = {"dtype": str}
        self.output_settings = {}

        self.convert_label = "Confirm"
        self.convert_button = QPushButton(self.convert_label)
        self.convert_button.clicked.connect(self.convert_file)
        self.convert_button.setEnabled(False)
        self.convert_button.setMinimumWidth(124)
        self.convert_button.setStyleSheet(
            "QPushButton { background-color: #cccccc; color: #666666; border-radius: 6px; padding: 6px 12px; font-weight: bold; } "
            "QPushButton:disabled { background-color: #cccccc; color: #666666; }"
        )
        self.cancel_button = QPushButton("Close")
        self.cancel_button.setStyleSheet(
            "QPushButton { background-color: #dc3545; color: white; border-radius: 6px; padding: 6px 12px; font-weight: bold; } "
            "QPushButton:hover { background-color: #c82333; } "
            "QPushButton:pressed { background-color: #bd2130; }"
        )
        self.cancel_button.clicked.connect(self.close)
        self.cancel_button.setMinimumWidth(124)
        submit_layout.addStretch(1)
        submit_layout.addWidget(self.convert_button)
        submit_layout.addWidget(self.cancel_button)

        select_layout = QHBoxLayout()
        self.input_button = QPushButton("Open File")
        self.input_button.clicked.connect(self.select_input_file)
        self.select_line = QLineEdit()
        self.select_line.setReadOnly(True)
        self.select_line.setPlaceholderText("No input file selected")
        self.select_line.setStyleSheet("background-color: white; color: black;")
        select_layout.addWidget(self.input_button)
        select_layout.addWidget(self.select_line)

        output_layout = QHBoxLayout()
        self.output_button = QPushButton("Convert To")
        self.output_button.clicked.connect(self.select_output_file)
        self.output_line = QLineEdit()
        self.output_line.setReadOnly(True)
        self.output_line.setPlaceholderText("No output file selected")
        self.output_line.setStyleSheet("background-color: white; color: black;")
        output_layout.addWidget(self.output_button)
        output_layout.addWidget(self.output_line)

        # Input settings radio menu (infer dtypes or all str)
        input_settings_layout = QHBoxLayout()
        input_settings_label = QLabel("Input data as:")
        self.radio_infer_types = QRadioButton("Infer from data")
        self.radio_all_str = QRadioButton("Text (recommended)")
        self.radio_all_str.setChecked(True)
        self.input_settings_group = QButtonGroup()
        self.input_settings_group.addButton(self.radio_all_str)
        self.input_settings_group.addButton(self.radio_infer_types)
        input_settings_layout.addWidget(input_settings_label)
        input_settings_layout.addWidget(self.radio_all_str)
        input_settings_layout.addWidget(self.radio_infer_types)
        input_settings_layout.addStretch(1)
        self.radio_all_str.toggled.connect(self.handle_input_settings_change)
        self.radio_infer_types.toggled.connect(self.handle_input_settings_change)

        # Split options radio menu
        split_layout = QHBoxLayout()
        split_label = QLabel("Split data by:")
        self.radio_no_split = QRadioButton("No split")
        self.radio_split_date = QRadioButton("Date")
        self.radio_split_length = QRadioButton("Length:")
        self.radio_no_split.setChecked(True)
        self.split_group = QButtonGroup()
        self.split_group.addButton(self.radio_no_split)
        self.split_group.addButton(self.radio_split_date)
        self.split_group.addButton(self.radio_split_length)
        split_layout.addWidget(split_label)
        split_layout.addWidget(self.radio_no_split)
        split_layout.addWidget(self.radio_split_date)
        split_layout.addWidget(self.radio_split_length)

        # Add part spinbox on the same row as 'parts'
        parts_label = QLabel("parts")
        self.parts_spinbox = QSpinBox()
        self.parts_spinbox.setRange(1, 100)
        self.parts_spinbox.setValue(4)
        self.parts_spinbox.setEnabled(False)
        split_layout.addWidget(self.parts_spinbox)
        split_layout.addWidget(parts_label)
        split_layout.addStretch(1)
        self.radio_no_split.toggled.connect(self.handle_split_change)
        self.radio_split_date.toggled.connect(self.handle_split_change)
        self.radio_split_length.toggled.connect(self.handle_split_change)
        self.parts_spinbox.valueChanged.connect(self.handle_parts_change)

        # Date column and time selection (enabled when split by date is selected)
        date_layout = QHBoxLayout()
        date_col_label = QLabel("Date column:")
        self.date_col_combo = QComboBox()
        self.date_col_combo.setEnabled(False)
        self.date_col_combo.setFixedWidth(132)
        timeframe_label = QLabel("Time:")
        self.timeframe_combo = QComboBox()
        self.timeframe_combo.addItems(["Last X Months", "Monthly", "Yearly"])
        self.timeframe_combo.setCurrentText("Last X Months")
        self.timeframe_combo.setFixedWidth(132)
        self.timeframe_combo.setEnabled(False)

        # Add months spinbox for "Last X Months" option
        self.months_spinbox = QSpinBox()
        self.months_spinbox.setRange(1, 120)
        self.months_spinbox.setValue(1)
        self.months_spinbox.setVisible(False)

        date_layout.addWidget(date_col_label)
        date_layout.addWidget(self.date_col_combo)
        date_layout.addWidget(timeframe_label)
        date_layout.addWidget(self.timeframe_combo)
        date_layout.addWidget(self.months_spinbox)
        date_layout.addStretch(1)
        self.date_col_combo.currentTextChanged.connect(self.handle_date_col_change)
        self.timeframe_combo.currentTextChanged.connect(self.handle_timeframe_change)
        self.months_spinbox.valueChanged.connect(self.handle_months_change)

        # Output file type switch (radio buttons)
        filetype_layout = QHBoxLayout()
        filetype_label = QLabel("Output file as:")
        self.radio_parquet = QRadioButton("Parquet")
        self.radio_csv = QRadioButton("CSV")
        self.radio_parquet.setChecked(True)
        self.filetype_group = QButtonGroup()
        self.filetype_group.addButton(self.radio_parquet)
        self.filetype_group.addButton(self.radio_csv)
        filetype_layout.addWidget(filetype_label)
        filetype_layout.addWidget(self.radio_parquet)
        filetype_layout.addWidget(self.radio_csv)
        filetype_layout.addStretch(1)
        self.radio_parquet.toggled.connect(self.handle_filetype_change)
        self.radio_csv.toggled.connect(self.handle_filetype_change)

        io_layout.addLayout(select_layout)
        io_layout.addLayout(output_layout)
        io_layout.addLayout(input_settings_layout)
        io_layout.addLayout(split_layout)
        io_layout.addLayout(date_layout)
        io_layout.addLayout(filetype_layout)

        self.preview_table = QTableView()
        self.preview_table.setMinimumHeight(120)
        self.preview_table.setStyleSheet(
            "background-color: white; color: black; border: 1px solid #888; border-radius: 4px;"
        )
        self.preview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.pandas_model = PandasModel()
        self.preview_table.setModel(self.pandas_model)

        # Display mode selection with connected buttons
        full_table_layout = QHBoxLayout()
        self.full_df_label = QLabel("Display")
        self.display_mode = 1  # 0=OFF, 1=SHORT, 2=ON

        # Create three buttons for display modes
        self.display_off_btn = QPushButton("OFF")
        self.display_short_btn = QPushButton("SHORT")
        self.display_on_btn = QPushButton("ON")

        # Set up button properties with simple styling
        for btn in [
            self.display_off_btn,
            self.display_short_btn,
            self.display_on_btn,
        ]:
            btn.setCheckable(True)
            btn.setFixedHeight(24)
            if btn == self.display_short_btn:
                btn.setFixedWidth(60)
            else:
                btn.setFixedWidth(40)
            btn.setStyleSheet(
                "QPushButton { background-color: transparent; color: #222; border: none; padding: 4px 6px; font-weight: bold; } "
                "QPushButton:checked { background-color: #4caf50; color: white; font-weight: bold; } "
                "QPushButton:hover { background-color: #bbb; } "
                "QPushButton:checked:hover { background-color: #45a049; }"
            )

        # Set initial state (SHORT selected)
        self.display_short_btn.setChecked(True)
        self.display_last = 1  # Save last display mode

        # Connect button signals
        self.display_off_btn.clicked.connect(lambda: self.set_display_mode(0))
        self.display_short_btn.clicked.connect(lambda: self.set_display_mode(1))
        self.display_on_btn.clicked.connect(lambda: self.set_display_mode(2))

        # Create a container widget with gray background and rounded corners
        button_container = QWidget()
        button_container.setFixedSize(140, 24)
        button_container.setStyleSheet(
            "QWidget { background-color: #ccc; border: 1px solid #999; border-radius: 6px; }"
        )

        # Layout for the buttons inside the container
        button_layout = QHBoxLayout(button_container)
        button_layout.setSpacing(0)
        button_layout.setContentsMargins(0, 0, 0, 0)  # Small padding inside container
        button_layout.addWidget(self.display_off_btn)
        button_layout.addWidget(self.display_short_btn)
        button_layout.addWidget(self.display_on_btn)

        full_table_layout.addWidget(self.full_df_label)
        full_table_layout.addWidget(button_container)
        full_table_layout.addStretch(1)

        # DataFrame size info label
        self.df_size_label = QLabel("No data loaded")
        self.df_size_label.setStyleSheet(
            "font-size: 11px; vertical-align: top; padding-bottom: 10px;"
        )
        full_table_layout.addWidget(self.df_size_label)

        base_layout.addLayout(io_layout)
        base_layout.addWidget(self.preview_table)
        base_layout.addLayout(full_table_layout)
        base_layout.addLayout(submit_layout)

        widget = QWidget()
        widget.setLayout(base_layout)
        self.setCentralWidget(widget)

        # Enable drag and drop
        self.setAcceptDrops(True)
        # Initialize preview table after widgets are created
        self.show_empty_preview()
        self._cached_df = None
        self._cached_df_file = None
        self._cached_df_settings = None

    def show_empty_preview(self):
        """Show an empty preview table with a placeholder message."""

        # Show an empty DataFrame and gray out the table
        df = DataFrame().head(0)
        self.pandas_model.setDataFrame(df)
        self.preview_table.setDisabled(True)
        self.preview_table.setStyleSheet(
            "background-color: #eee; color: #aaa; border: 1px solid #888; border-radius: 4px;"
        )
        self.df_size_label.setText("No data loaded")

    def select_input_file(self):
        """Open a file dialog to select an input CSV or Parquet file."""

        # Show loading state immediately when dialog opens
        self.select_line.setText("Opening file...")
        self.select_line.setStyleSheet("background-color: #fff3cd; color: #856404;")
        QApplication.processEvents()  # Force UI update
        self._cached_df = None
        self._cached_df_file = None
        self._cached_df_settings = None

        file_name, _ = QFileDialog.getOpenFileName(
            self,
            "Select Input File",
            "",
            "All Files (*.csv *.parquet *.gz *.bz2 *.zip *.xz *.zst *.tar *.tar.gz *.tar.xz *.tar.bz2);;CSV Files (*.csv *.gz *.bz2 *.zip *.xz *.zst *.tar *.tar.gz *.tar.xz *.tar.bz2);;Parquet Files (*.parquet)",
        )
        if file_name:
            self.input_file = file_name
            file_tuple = self.input_file.rsplit(".", 1)
            self.input_type = "parquet" if file_tuple[-1] == "parquet" else "csv"
            # Reset output so we don't accidentally overwrite a previous selection
            self.output_file = ""
            self.output_line.setText("")
            self.update_convert_button_state()
            self.update_preview_table()
            # Show actual file path
            self.select_line.setText(self.input_file)
            self.select_line.setStyleSheet("background-color: white; color: black;")
        else:
            self.input_file = ""
            self.select_line.setText("")
            self.select_line.setStyleSheet("background-color: white; color: black;")
            self.show_empty_preview()
        self.update_convert_button_state()

    def select_output_file(self):
        """Open a file dialog to select an output file (path) for conversion."""

        if not self.input_file:
            QMessageBox.warning(
                self, "No Input File", "Please select an input file first."
            )
            return
        file_tuple = self.input_file.rsplit(".", 1)
        if self.radio_parquet.isChecked():
            output_ext = ".parquet"
            file_filter = "Parquet Files (*.parquet)"
        else:
            output_ext = ".csv"
            file_filter = "CSV Files (*.csv)"
        output_name = file_tuple[0] + output_ext
        output_file, _ = QFileDialog.getSaveFileName(
            self,
            "Select Output File",
            output_name,
            file_filter,
        )
        if output_file:
            self.output_file = output_file
            self.output_line.setText(self.output_file)
        else:
            self.output_file = ""
            self.output_line.setText("")
        self.update_convert_button_state()

    def handle_filetype_change(self):
        """Update the output file type based on the selected radio button."""

        # Update output file extension if output file is already set
        if not self.input_file:
            return
        if self.radio_parquet.isChecked():
            output_ext = ".parquet"
        else:
            output_ext = ".csv"
        # Only update if output_line is not empty
        if self.output_line.text():
            base, _ = splitext(self.output_line.text())
            new_output = base + output_ext
            self.output_line.setText(new_output)
            self.output_file = new_output
        # Also update self.output_type for downstream logic
        self.output_type = "parquet" if self.radio_parquet.isChecked() else "csv"

    def update_convert_button_state(self):
        """Update the state of the convert button ("Confirm") based on input and output files."""

        is_ready = bool(self.input_file and self.output_file)
        self.convert_button.setEnabled(is_ready)

        # Update button styling based on state
        if is_ready:
            self.convert_button.setStyleSheet(
                "QPushButton { background-color: #4CAF50; color: white; border-radius: 6px; padding: 6px 12px; font-weight: bold; } "
                "QPushButton:hover { background-color: #45a049; } "
                "QPushButton:pressed { background-color: #3d8b40; }"
            )
        else:
            self.convert_button.setStyleSheet(
                "QPushButton { background-color: #cccccc; color: #666666; border-radius: 6px; padding: 6px 12px; font-weight: bold; } "
                "QPushButton:disabled { background-color: #cccccc; color: #666666; }"
            )

    def handle_input_settings_change(self):
        """Update input settings based on the selected radio button."""

        if self.radio_all_str.isChecked() and self.input_type != "parquet":
            self.input_settings = {"dtype": str}
        else:
            # Remove 'dtype' key for infer and for parquets
            self.input_settings = {}
        self.update_preview_table()

    def _snapshot_ui_state(self) -> dict:
        """Capture current UI selections so they can be restored after updates."""
        return {
            "split_mode": "no_split"
            if self.radio_no_split.isChecked()
            else "date"
            if self.radio_split_date.isChecked()
            else "length",
            "parts": self.parts_spinbox.value(),
            "timeframe": self.timeframe_combo.currentText(),
            "months": self.months_spinbox.value(),
            "input_dtype": "text" if self.radio_all_str.isChecked() else "infer",
            "output_type": "parquet" if self.radio_parquet.isChecked() else "csv",
            "date_col": self.date_col_combo.currentText(),
        }

    def _restore_ui_state(self, snapshot: dict, valid_date_columns: list[str]) -> None:
        """Restore UI selections from snapshot without emitting signals."""
        # Split radios
        self.radio_no_split.blockSignals(True)
        self.radio_split_date.blockSignals(True)
        self.radio_split_length.blockSignals(True)
        self.parts_spinbox.blockSignals(True)
        self.timeframe_combo.blockSignals(True)
        self.months_spinbox.blockSignals(True)
        self.radio_all_str.blockSignals(True)
        self.radio_infer_types.blockSignals(True)
        self.radio_parquet.blockSignals(True)
        self.radio_csv.blockSignals(True)
        self.date_col_combo.blockSignals(True)
        try:
            mode = snapshot.get("split_mode")
            if mode == "no_split":
                self.radio_no_split.setChecked(True)
            elif mode == "date":
                self.radio_split_date.setChecked(True)
            elif mode == "length":
                self.radio_split_length.setChecked(True)

            self.parts_spinbox.setValue(
                snapshot.get("parts", self.parts_spinbox.value())
            )
            tf = snapshot.get("timeframe")
            if tf:
                idx = self.timeframe_combo.findText(tf)
                if idx >= 0:
                    self.timeframe_combo.setCurrentIndex(idx)
            self.months_spinbox.setValue(
                snapshot.get("months", self.months_spinbox.value())
            )

            if snapshot.get("input_dtype") == "text":
                self.radio_all_str.setChecked(True)
            else:
                self.radio_infer_types.setChecked(True)

            if snapshot.get("output_type") == "parquet":
                self.radio_parquet.setChecked(True)
            else:
                self.radio_csv.setChecked(True)

            dc = snapshot.get("date_col")
            if dc and dc in valid_date_columns:
                self.date_col_combo.setCurrentText(dc)
        finally:
            self.radio_no_split.blockSignals(False)
            self.radio_split_date.blockSignals(False)
            self.radio_split_length.blockSignals(False)
            self.parts_spinbox.blockSignals(False)
            self.timeframe_combo.blockSignals(False)
            self.months_spinbox.blockSignals(False)
            self.radio_all_str.blockSignals(False)
            self.radio_infer_types.blockSignals(False)
            self.radio_parquet.blockSignals(False)
            self.radio_csv.blockSignals(False)
            self.date_col_combo.blockSignals(False)

    def update_preview_table(self):
        """Update the preview table with the contents of the selected input file."""

        def _set_preview(mode):
            if mode == 0:
                self.preview_table.setVisible(False)
                self.setFixedHeight(248)
            else:
                self.preview_table.setVisible(True)
                self.setMinimumHeight(320)
                self.setMaximumHeight(10000)
                # Enlarge window if needed
                if self.display_last == 0:
                    cur_size = self.size()
                    table_height = self.preview_table.minimumHeight()
                    self.resize(cur_size.width(), cur_size.height() + table_height)
            self.display_last = mode

        # Snapshot current UI state
        ui_snapshot = self._snapshot_ui_state()

        if not self.input_file:
            self.show_empty_preview()
            _set_preview(self.display_mode)
            self.df_size_label.setText("No data loaded")
            return
        # Check if we can use the cached DataFrame
        if (
            self._cached_df is not None
            and self._cached_df_file == self.input_file
            and self._cached_df_settings == str(self.input_settings)
        ):
            df = self._cached_df
        else:
            try:
                if self.input_file.endswith(".parquet"):
                    df = Convert.read("parquet", self.input_file, **self.input_settings)
                else:
                    df = Convert.read("csv", self.input_file, **self.input_settings)
                self._cached_df = df
                self._cached_df_file = self.input_file
                self._cached_df_settings = str(self.input_settings)
            except Exception as e:
                self.show_empty_preview()
                self.df_size_label.setText("No data loaded")
                QMessageBox.critical(
                    self, "Preview failed", f"Could not preview file:\n{e}"
                )
                return
        rows, cols = df.shape
        self.df_size_label.setText(f"DataFrame: {rows:,} rows × {cols:,} columns")
        # Update date columns and restore selections
        self.date_col_combo.blockSignals(True)
        try:
            prev_dc = ui_snapshot.get("date_col")
            self.date_col_combo.clear()
            columns_sorted = sorted(df.columns.tolist())
            if columns_sorted:
                self.date_col_combo.addItems(columns_sorted)
                if prev_dc in columns_sorted:
                    self.date_col_combo.setCurrentText(prev_dc)
        finally:
            self.date_col_combo.blockSignals(False)
        # Restore all other UI selections
        self._restore_ui_state(ui_snapshot, df.columns.tolist())

        _set_preview(self.display_mode)
        if self.display_mode == 0:  # OFF (mode == 0): hide table
            return
        if self.display_mode == 1:  # SHORT (mode == 1): show first 20 rows
            head = df.head(20)
        else:  # ON (mode == 2): show all
            head = df
        self.pandas_model.setDataFrame(head)
        self.preview_table.setEnabled(True)
        self.preview_table.setStyleSheet(
            "background-color: white; color: black; border: 1px solid #888; border-radius: 4px;"
        )

    def set_display_mode(self, mode):
        """Set the display mode for the preview table and update button states."""

        def _update_checked_states():
            self.display_off_btn.setChecked(mode == 0)
            self.display_short_btn.setChecked(mode == 1)
            self.display_on_btn.setChecked(mode == 2)

        def _set_down_and_enabled(down=True, enabled=True):
            for btn in [
                self.display_off_btn,
                self.display_short_btn,
                self.display_on_btn,
            ]:
                btn.setDown(down)
                btn.setEnabled(enabled)

        # Show loading feedback only on the pressed button, but disable all buttons
        _set_down_and_enabled(down=True, enabled=False)
        _update_checked_states()

        if mode == 0:
            self.display_off_btn.setText("...")
            self.display_off_btn.setDown(True)
        elif mode == 1:
            self.display_short_btn.setText("Wait...")
            self.display_short_btn.setDown(True)
        elif mode == 2:
            self.display_on_btn.setText("...")
            self.display_on_btn.setDown(True)
        QApplication.processEvents()  # Force UI update

        # Update display mode
        self.display_mode = mode
        _update_checked_states()

        # Restore button text and state
        self.display_off_btn.setText("OFF")
        self.display_short_btn.setText("SHORT")
        self.display_on_btn.setText("ON")

        _set_down_and_enabled(down=False, enabled=True)
        self.update_preview_table()

    def handle_split_change(self):
        """Update output settings based on the selected split option."""

        if self.radio_no_split.isChecked():
            self.output_settings = {}
            self.parts_spinbox.setEnabled(False)
            self.date_col_combo.setEnabled(False)
            self.timeframe_combo.setEnabled(False)
            self.months_spinbox.setVisible(False)
        elif self.radio_split_date.isChecked():
            self.output_settings = {
                "to_split": "date",
                "split_by": self.timeframe_combo.currentText(),
                "date_col": self.date_col_combo.currentText(),
                "month_n": self.months_spinbox.value(),
            }
            self.parts_spinbox.setEnabled(False)
            self.date_col_combo.setEnabled(True)
            self.timeframe_combo.setEnabled(True)
            # Show months spinbox only for "Last X Months" option
            self.months_spinbox.setVisible(
                self.timeframe_combo.currentText() == "Last X Months"
            )
        elif self.radio_split_length.isChecked():
            self.output_settings = {
                "to_split": "length",
                "split_by": self.parts_spinbox.value(),
            }
            self.parts_spinbox.setEnabled(True)
            self.date_col_combo.setEnabled(False)
            self.timeframe_combo.setEnabled(False)
            self.months_spinbox.setVisible(False)

    def handle_date_col_change(self):
        """Update output settings when the date column selection changes."""

        if self.radio_split_date.isChecked():
            self.output_settings = {
                "to_split": "date",
                "split_by": self.timeframe_combo.currentText(),
                "date_col": self.date_col_combo.currentText(),
            }

    def handle_timeframe_change(self):
        """Update output settings when the timeframe selection changes."""

        if self.radio_split_date.isChecked():
            self.output_settings = {
                "to_split": "date",
                "split_by": self.timeframe_combo.currentText(),
                "date_col": self.date_col_combo.currentText(),
                "month_n": self.months_spinbox.value(),
            }
            # Show months spinbox only for "Last X Months" option
            self.months_spinbox.setVisible(
                self.timeframe_combo.currentText() == "Last X Months"
            )

    def handle_parts_change(self):
        """Update output settings when the number of parts spinbox value changes."""

        if self.radio_split_length.isChecked():
            self.output_settings = {
                "to_split": "length",
                "split_by": self.parts_spinbox.value(),
            }

    def handle_months_change(self):
        """Update output settings when the months spinbox value changes."""

        if self.radio_split_date.isChecked():
            self.output_settings = {
                "to_split": "date",
                "split_by": self.timeframe_combo.currentText(),
                "date_col": self.date_col_combo.currentText(),
                "month_n": self.months_spinbox.value(),
            }

    def convert_file(self):
        """Convert the selected input file to the specified output format."""

        self.convert_button.setText("Saving...")
        self.convert_button.setEnabled(False)
        QApplication.processEvents()
        try:
            convert = Convert(
                input_file=self.input_file,
                output_file=self.output_file,
                input_type=self.input_type,
                output_type=self.output_type,
                input_settings=self.input_settings,
                output_settings=self.output_settings,
            )
            convert.convert()
            dialog_msg = f"File converted successfully:\n{self.output_file}"
            success = True
        except Exception as e:
            dialog_msg = f"Error converting file:\n{e}"
            success = False
        finally:
            if success:
                QMessageBox.information(self, "Success", dialog_msg)
            else:
                QMessageBox.critical(self, "Conversion failed", dialog_msg)
            self.convert_button.setText(self.convert_label)
            self.convert_button.setEnabled(True)
        return True

    def dragEnterEvent(self, event: QDragEnterEvent):
        """Handle drag enter events."""

        if event.mimeData().hasUrls():
            # Check if any of the URLs are valid file types
            for url in event.mimeData().urls():
                file_path = url.toLocalFile()
                if self.is_valid_file_type(file_path):
                    event.acceptProposedAction()
                    return
        event.ignore()

    def dropEvent(self, event: QDropEvent):
        """Handle drop events."""

        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                file_path = url.toLocalFile()
                if self.is_valid_file_type(file_path):
                    # Set the input file and update UI
                    self.input_file = file_path
                    file_tuple = self.input_file.rsplit(".", 1)
                    self.input_type = (
                        "parquet" if file_tuple[-1] == "parquet" else "csv"
                    )
                    # Reset output to prevent accidental overwrite when a new input is dropped
                    self.output_file = ""
                    self.output_line.setText("")
                    self.update_convert_button_state()
                    self.select_line.setText(self.input_file)
                    self.select_line.setStyleSheet(
                        "background-color: white; color: black;"
                    )
                    self.update_preview_table()
                    self.update_convert_button_state()
                    event.acceptProposedAction()
                    return
        event.ignore()

    def is_valid_file_type(self, file_path: str) -> bool:
        """Check if the file is a valid type for this app."""

        valid_extensions = [
            ".csv",
            ".gz",
            ".bz2",
            ".zip",
            ".xz",
            ".zst",
            ".tar",
            ".tar.gz",
            ".tar.xz",
            ".tar.bz2",
            ".parquet",
        ]
        return any(file_path.lower().endswith(ext) for ext in valid_extensions)


app = QApplication([])

window = MainWindow()
window.show()

app.exec()
