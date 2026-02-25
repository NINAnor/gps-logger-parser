import csv
import os
import pathlib
from contextlib import contextmanager

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from chardet.universaldetector import UniversalDetector
from upath import UPath

MAX_SPEED = float(os.environ.get("MAX_SPEED", default="10"))


class ParserNotSupported(Exception):
    pass


class Parsable:
    def __init__(self, file_path: UPath) -> None:
        self._file_path = file_path

        if not self._file_path.exists():
            raise ValueError("File does not exists")

        self.encoding = self._detect_encoding()

    @contextmanager
    def get_stream(self, binary=False, errors="strict"):
        params = {
            "mode": "rb" if binary else "r",
            "encoding": None if binary else self.encoding,
            "errors": errors if not binary else None,
        }
        stream = self._file_path.open(**params)
        yield stream
        stream.close()

    def _detect_encoding(self):
        detector = UniversalDetector()
        with self.get_stream(binary=True) as stream:
            for line in stream.readlines():
                detector.feed(line)
                if detector.done:
                    break
            detector.close()
            print(detector.result)
            return detector.result["encoding"]


class Parser:
    DATATYPE = "generic_parser"
    OUTLIERS = {"speed_km_h": lambda x: x > MAX_SPEED}

    def __init__(self, parsable: Parsable):
        self.file = parsable
        self.data = []

    def _raise_not_supported(self, text):
        raise ParserNotSupported(f"{self.__class__.__name__}: {text}")

    def get_mappings(self):
        return {v: k for k, v in getattr(self, "MAPPINGS", {}).items() if v}

    def get_outliers(self):
        return getattr(self, "OUTLIERS", {})

    def harmonize_data(self, data):
        """
        Preserve original columns and remap values parsed using MAPPINGS.

        Original columns are kept with '__original__' prefix.
        Harmonized columns are created based on MAPPINGS.

        Args:
            data: DataFrame to harmonize

        Returns:
            Harmonized DataFrame with original columns prefixed
        """
        # First, preserve original columns by renaming them with __original__ prefix
        original_columns = {col: f"__original__{col}" for col in data.columns}
        data = data.rename(columns=original_columns)

        # Then create harmonized columns by copying and renaming from originals
        mappings = self.get_mappings()
        if mappings:
            # mappings is {source_col: harmonized_col}
            # We need to copy from __original__{source_col} to harmonized_col
            for source_col, harmonized_col in mappings.items():
                original_col_name = f"__original__{source_col}"
                if original_col_name in data.columns:
                    data[harmonized_col] = data[original_col_name]

        return data

    def get_harmonization_schema(self):
        """
        Return PyArrow schema for harmonized columns, or None for generic parsers.

        Override this method in subclasses to provide explicit schema for
        harmonized output.
        """
        return None

    def detect_outliers(self):
        """
        Apply conditions on fields to check for outliers
        """
        outliers = self.get_outliers()
        if outliers:

            def check_conditions(row):
                return any(
                    condition(row[k])
                    for k, condition in outliers.items()
                    if k in self.data
                )

            self.data["outlier"] = self.data.apply(check_conditions, axis=1)

    def as_table(self) -> pa.Table:
        self.data = self.harmonize_data(self.data)
        # self.detect_outliers()

        # Use explicit schema if provided by subclass harmonization
        # Note: schema only applies to harmonized columns, not __original__ ones
        schema = self.get_harmonization_schema()
        if schema:
            # Separate harmonized and original columns
            harmonized_cols = [
                col for col in self.data.columns if not col.startswith("__original__")
            ]
            original_cols = [
                col for col in self.data.columns if col.startswith("__original__")
            ]

            # Create table with explicit schema for harmonized columns
            harmonized_df = self.data[harmonized_cols]
            table = pa.Table.from_pandas(
                harmonized_df, schema=schema, preserve_index=False
            )

            # Add original columns without schema (let PyArrow infer types)
            if original_cols:
                original_df = self.data[original_cols]
                original_table = pa.Table.from_pandas(original_df, preserve_index=False)
                # Append original columns to the table
                for col_name in original_table.column_names:
                    table = table.append_column(
                        col_name, original_table.column(col_name)
                    )
        else:
            table = pa.Table.from_pandas(self.data, preserve_index=False)

        table = table.append_column(
            "_datatype", pa.array([self.DATATYPE] * len(table), pa.string())
        )
        table = table.append_column(
            "_parser", pa.array([self.__class__.__name__] * len(table), pa.string())
        )
        table = table.append_column(
            "_logger_file",
            pa.array([self.file._file_path.name] * len(table), pa.string()),
        )
        return table

    def write_parquet(self, path: pathlib.Path, filename: str | None = None):
        if filename:
            filename = pathlib.Path(filename)
        else:
            filename = self.file._file_path.name

        pq.write_table(self.as_table(), str(path / f"{filename}.parquet"))

    def write_csv(self, path):
        pacsv.write_csv(self.as_table(), str(path))


class CSVParser(Parser):
    DATATYPE = "generic_csv"
    FIELDS = []
    SEPARATOR = ","
    SKIP_INITIAL_SPACE = True
    HEADER = 1

    def _check_headers(self, header):
        if header != self.FIELDS:
            self._raise_not_supported(
                f"Stream have a header different than expected, "
                f"{header} != {self.FIELDS}"
            )

    def __init__(self, parsable: Parsable):
        super().__init__(parsable)

        with self.file.get_stream(binary=False) as stream:
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            reader = csv.reader(
                stream,
                delimiter=self.SEPARATOR,
                skipinitialspace=self.SKIP_INITIAL_SPACE,
            )
            header = next(reader)
            self._check_headers(header)
            stream.seek(0)

            self.data = pd.read_csv(
                stream,
                header=self.HEADER,
                names=self.FIELDS,
                sep=self.SEPARATOR,
                index_col=False,
            )


class ExcelParser(Parser):
    DATATYPE = "generic_excel"
    FIELDS = []
    SKIPROWS = 0

    def __init__(self, stream):
        super().__init__(stream)

        if "b" not in self.stream.mode:
            self._raise_not_supported("Stream is not binary")

        if "xls" not in pathlib.Path(self.stream.name).suffix:
            self._raise_not_supported("Extension is not xls")

        self.data = pd.read_excel(
            self.stream, header=0, index_col=False, skiprows=self.SKIPROWS
        )
        if set(self.data.columns.values) != set(self.FIELDS):
            self._raise_not_supported(
                "Field name not matching: "
                + str(
                    {
                        "missing": list(
                            set(self.data.columns.values) - set(self.FIELDS)
                        ),
                        "extra": list(set(self.FIELDS) - set(self.data.columns.values)),
                    }
                )
            )
