import csv
import json
import os
import pathlib
from contextlib import contextmanager

import geoarrow.pandas as _  # noqa: F401
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from chardet import UniversalDetector
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
            return detector.result["encoding"]


class Parser:
    DATATYPE = "generic_parser"

    def __init__(self, parsable: Parsable):
        self.file = parsable
        self.data = []
        self.harmonized_data = None

    def _raise_not_supported(self, text):
        raise ParserNotSupported(f"{self.__class__.__name__}: {text}")

    def get_mappings(self):
        mappings = {v: k for k, v in getattr(self, "MAPPINGS", {}).items()}

        if not mappings:
            raise NotImplementedError("Subclasses must provide a mapping")
        return mappings

    def harmonize_data(self, data):
        """
        Remap values parsed using MAPPINGS into harmonized column names.

        For each entry in MAPPINGS that has a source column, the source column
        value is copied into the harmonized column name. If the source is None
        but the harmonized column already exists in data (e.g. created by a
        subclass), it is preserved.

        Args:
            data: DataFrame to harmonize (a copy of self.data)

        Returns:
            Harmonized DataFrame with harmonized columns added
        """
        mappings = getattr(self, "MAPPINGS", {})

        if not mappings:
            raise NotImplementedError("Subclasses must provide a mapping")

        schema = self.get_harmonization_schema()
        df = pd.DataFrame(columns=schema.keys()).astype(schema)

        for harmonized_col, source_col in mappings.items():
            if source_col is not None and source_col in data.columns:
                df[harmonized_col.value] = data[source_col]
            elif harmonized_col.value in data.columns:
                df[harmonized_col.value] = data[harmonized_col.value]
            else:
                df[harmonized_col.value] = None

        return df

    def get_harmonization_schema(self) -> dict:
        """
        Return Pandas schema for harmonized columns

        Override this method in subclasses to provide explicit schema for
        harmonized output.
        """
        raise NotImplementedError(
            "Subclasses must implement get_harmonization_schema()"
        )

    def as_table(self) -> pa.Table:
        # Capture raw source data as JSON before any harmonization
        original_records = self.data.to_dict(orient="records")
        original_json = [json.dumps(row, default=str) for row in original_records]

        self.harmonized_data = self.harmonize_data(self.data.copy())

        if len(self.harmonized_data) == 0:
            raise ValueError("Harmonized data is empty, cannot create table")

        table = pa.Table.from_pandas(self.harmonized_data, preserve_index=False)

        table = table.append_column(
            "_original_data",
            pa.array(original_json, type=pa.json_(pa.large_utf8())),
        )
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
    HEADER = 0

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
