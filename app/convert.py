import pandas as pd


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
        file_type: str = "csv",
        input_settings: dict = None,
        output_settings: dict = None,
    ):
        """
        Initialize the Convert class.

        :param input_file: Path to the input CSV file.
        :param output_file: Path to the output Parquet file.
        :param file_type: Type of the input file (csv or parquet).
        :param input_settings: Settings for reading the CSV file.
        :param output_settings: Settings for writing the Parquet file.
        """
        self.input_file = input_file
        self.output_file = output_file
        self.file_type = file_type
        self.input_settings = input_settings if input_settings else {}
        self.output_settings = output_settings if output_settings else {}

    def convert(self):
        modules = {
            "csv": pd.read_csv,
            "parquet": pd.read_parquet,
        }
        module = modules.get(self.file_type)
        if module is None:
            raise ValueError(f"Unsupported file type: {self.file_type}")
        if self.file_type == "csv":
            low_memory = self.input_settings.get("low_memory", False)
            self.input_settings["low_memory"] = low_memory

        dataframe = module(self.input_file, **self.input_settings)

        if self.file_type == "csv":
            dataframe.to_parquet(self.output_file, **self.output_settings)
        elif self.file_type == "parquet":
            keep_index = self.output_settings.get("index", False)
            self.output_settings["index"] = keep_index
            dataframe.to_csv(self.output_file, **self.output_settings)
