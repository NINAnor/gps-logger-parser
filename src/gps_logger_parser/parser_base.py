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

    def normalize_data(self):
        """
        Remap values parsed
        """
        mappings = self.get_mappings()
        if mappings:
            self.data = self.data.rename(mappings)

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
        self.normalize_data()
        # self.detect_outliers()
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

    def write_parquet(self, path: pathlib.Path, filename: str = None):
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
                f"Stream have a header different than expected, {header} != {self.FIELDS}"
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
