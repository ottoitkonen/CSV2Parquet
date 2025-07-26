from pandas import DataFrame, DateOffset, Timestamp, read_csv, read_parquet, to_datetime


class Convert:
    """
    Simple converter class to convert CSV files to Parquet and vice versa.

    Supports CSV in various formats (gz, bz2, zip, xz, zst, tar) and Parquet.
    Custom settings for reading and writing files can be provided through
    input_settings and output_settings.
    """

    def __init__(
        self,
        input_file: str,
        output_file: str,
        input_type: str | None = None,
        output_type: str | None = None,
        input_settings: dict | None = None,
        output_settings: dict | None = None,
    ):
        """
        Initialize the Convert class.

        :param input_file: Path to the input CSV file.
        :param output_file: Path to the output Parquet file.
        :param input_type: Type of the input file (csv or parquet).
        :param output_type: Type of the output file (csv or parquet).
        :param input_settings: Settings for reading the CSV file.
        :param output_settings: Settings for writing the Parquet file.
        """
        self.input_file = input_file
        self.output_file = output_file
        self.input_type = input_type or "csv"
        self.output_type = output_type or "parquet"
        self.input_settings = input_settings if input_settings else {}
        self.output_settings = output_settings if output_settings else {}
        self.date_splits = ("Last X Months", "Monthly", "Yearly")

    @staticmethod
    def read(input_type: str, input_file: str, **input_settings):
        """Read a file based on its type and return a DataFrame."""

        modules = {
            "csv": read_csv,
            "parquet": read_parquet,
        }
        module = modules.get(input_type)
        if module is None:
            raise ValueError(f"Unsupported file type: {input_type}")
        if input_type == "csv":
            low_memory = input_settings.get("low_memory", False)
            input_settings["low_memory"] = low_memory
        if input_type == "parquet":
            # not compatible with read_parquet
            _ = input_settings.pop("dtype", None)

        try:
            dataframe = module(input_file, **input_settings)
        except Exception as e:
            is_csv = input_type == "csv"
            encoding = input_settings.get("encoding", "utf-8")
            if is_csv and encoding == "utf-8":
                input_settings["encoding"] = "ISO-8859-1"
                dataframe = module(input_file, **input_settings)
            else:
                raise e
        return dataframe

    def save(self, dataframe: DataFrame, file_name: str, settings: dict):
        """Save a DataFrame to a file based on the output type."""

        keep_index = settings.get("index", False)
        if self.output_type == "parquet":
            dataframe = dataframe.reset_index(drop=not keep_index)
            dataframe.to_parquet(file_name, **settings)
        elif self.output_type == "csv":
            settings["index"] = keep_index
            dataframe.to_csv(file_name, **settings)

    def save_by_date(
        self,
        dataframe: DataFrame,
        date_col: str,
        split_by: str,
        settings: dict,
        **kwargs,
    ):
        """Split the DataFrame by date and save each part to a separate file."""

        date_series = to_datetime(dataframe[date_col])
        lmonth, monthly, yearly = self.date_splits
        file_name = self.output_file

        if split_by == lmonth:
            month_n = kwargs.get("month_n", 1)
            last_month = date_series.max()
            last_month = Timestamp(last_month.year, last_month.month, 1)
            last_month = last_month - DateOffset(months=month_n)
            dataframe = dataframe.loc[date_series > last_month]
            self.save(dataframe, file_name, settings)
        else:
            if self.output_type in file_name:
                file_name = file_name.rsplit(".", 1)[0]
            condition = (
                [date_series.dt.year]
                if split_by == yearly
                else [date_series.dt.year, date_series.dt.month]
                if split_by == monthly
                else []
            )
            grouped = dataframe.groupby(condition)
            for period, group in grouped:
                if split_by == monthly:
                    period = f"{period[0]}_{str(period[1]).zfill(2)}"
                else:
                    period = f"{period[0]}"
                part_name = f"{file_name}_{period}.{self.output_type}"
                self.save(group, part_name, settings)

    def save_by_parts(self, dataframe: DataFrame, parts: int, settings: dict):
        """Split the DataFrame into parts and save each part to a separate file."""

        data_length = len(dataframe)
        file_name = self.output_file
        if self.output_type in file_name:
            file_name = file_name.rsplit(".", 1)[0]
        for i in range(parts):
            start = int(i * data_length / parts)
            end = int((i + 1) * data_length / parts)
            part_name = f"{file_name}_part_{i + 1}.{self.output_type}"
            self.save(dataframe.iloc[start:end], part_name, settings)

    def convert(self):
        """Convert the input file to the output file format."""

        dataframe = self.read(self.input_type, self.input_file, **self.input_settings)
        # copy output_settings so that we won't modify a reference
        settings = {x[0]: x[1] for x in self.output_settings.items()}
        # to_split: 'date'/'length'/False
        to_split = settings.pop("to_split", None)
        # split_by: 'Yearly'/'Monthly'/'Last X Months'/int()
        split_by = settings.pop("split_by", None)
        date_col = settings.pop("date_col", None)
        # n of months for 'latest X months' option
        month_n = settings.pop("month_n", 1)

        if to_split:
            try:
                if (
                    (to_split == "date")
                    and (split_by in self.date_splits)
                    and (date_col in dataframe.columns)
                ):
                    self.save_by_date(
                        dataframe, date_col, split_by, settings, month_n=month_n
                    )
                elif (to_split == "length") and ((parts := int(split_by)) > 0):
                    self.save_by_parts(dataframe, parts, settings)
                else:
                    raise
            except Exception:
                raise Exception(
                    "Instructions to split the given table are not valid. Please confirm your settings!"
                )
        else:
            self.save(dataframe, self.output_file, settings)
