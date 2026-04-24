import csv
import io

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

from ..helpers import stream_chunk_match, stream_starts_with
from ..parser_base import CSVParser, Parsable, Parser
from .columns import TDRHarmonizedColumn
from .mixin import TDRHarmonizationMixin

ENDLINES = ["No Fast Log Data", "No Fast Data"]


def skip(row):
    if not row.text or row.text in ENDLINES:
        return "skip"
    return "error"


class TDRParser(TDRHarmonizationMixin, Parser):
    DATATYPE = "tdr"
    FIELDS = ["Date/Time Stamp", "Pressure", "Temp"]
    SEPARATOR = ","
    ALLOWED_META = ["Resolution"]
    HEAD = "\n?Comment\\s:-"
    MAX_READ = 1000
    MAPPINGS = {
        TDRHarmonizedColumn.TIMESTAMP: "Date/Time Stamp",
        TDRHarmonizedColumn.PRESSURE: "Pressure",
        TDRHarmonizedColumn.TEMPERATURE: "Temp",
        TDRHarmonizedColumn.DEPTH_M: None,
    }

    @classmethod
    def can_parse(cls, parsable):
        """Check if the file matches the expected HEAD regex pattern."""
        try:
            with parsable.get_stream(binary=False) as stream:
                if not stream.seekable():
                    return False
                return bool(stream_chunk_match(stream, 200, cls.HEAD))
        except (UnicodeDecodeError, OSError):
            return False

    def __init__(self, parsable: Parsable):
        super().__init__(parsable)
        meta = {}
        row_count = 0

        expected_row = self.SEPARATOR.join(self.FIELDS)

        with self.file.get_stream(binary=False) as stream:
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            if not stream_chunk_match(stream, 200, self.HEAD):
                self._raise_not_supported("Stream head different than expected")

            for row in stream:
                if row.strip() == expected_row:
                    break

                row_count += 1
                if "=" in row:
                    key, value = row.split("=")
                    if key in self.ALLOWED_META:
                        meta[key.strip()] = value.strip()

                if row_count > self.MAX_READ:
                    self._raise_not_supported(
                        f"Stream data not found after {self.MAX_READ} lines"
                    )

        parse_options = pacsv.ParseOptions(
            delimiter=self.SEPARATOR, invalid_row_handler=skip
        )
        read_options = pacsv.ReadOptions(
            skip_rows=row_count + 1,
            column_names=self.FIELDS,
        )
        convert_options = pacsv.ConvertOptions(
            include_columns=self.FIELDS,
            include_missing_columns=True,
        )
        with self.file.get_stream(binary=True) as stream:
            try:
                self.data = pacsv.read_csv(
                    stream,
                    parse_options=parse_options,
                    read_options=read_options,
                    convert_options=convert_options,
                ).to_pandas()
            except pa.lib.ArrowInvalid:
                pass
            else:
                for key, value in meta.items():
                    self.data[key] = [value] * len(self.data)
                return

        # Retry with an extra column to handle trailing comma in data rows
        read_options = pacsv.ReadOptions(
            skip_rows=row_count + 1,
            column_names=self.FIELDS + ["_trailing"],
        )
        with self.file.get_stream(binary=True) as stream:
            self.data = pacsv.read_csv(
                stream,
                parse_options=parse_options,
                read_options=read_options,
                convert_options=convert_options,
            ).to_pandas()

        for key, value in meta.items():
            self.data[key] = [value] * len(self.data)


class TDR2Parser(TDRParser):
    FIELDS = [
        "Time Stamp",
        "Pressure",
        "Temp",
    ]
    MAPPINGS = {
        TDRHarmonizedColumn.TIMESTAMP: "Time Stamp",
        TDRHarmonizedColumn.PRESSURE: "Pressure",
        TDRHarmonizedColumn.TEMPERATURE: "Temp",
        TDRHarmonizedColumn.DEPTH_M: None,
    }


class TDR2EuropeanDecimalParser(TDRParser):
    """Parser for TDR files where decimal values use commas as separators.

    Some TDR files use European-style decimal notation (e.g. ``-0,26`` instead
    of ``-0.26``) while also using comma as the CSV delimiter. This results in
    rows with 5 columns instead of the expected 3: the integer and decimal
    parts of Pressure and Temp are split into separate fields. This parser
    reads the 5 raw columns and recombines them into proper float values.
    """

    HEADER_FIELDS = ["Time Stamp", "Pressure", "Temp"]
    FIELDS = [
        "Time Stamp",
        "Pressure_int",
        "Pressure_dec",
        "Temp_int",
        "Temp_dec",
    ]
    MAPPINGS = {
        TDRHarmonizedColumn.TIMESTAMP: "Time Stamp",
        TDRHarmonizedColumn.PRESSURE: "Pressure",
        TDRHarmonizedColumn.TEMPERATURE: "Temp",
        TDRHarmonizedColumn.DEPTH_M: None,
    }

    def __init__(self, parsable: Parsable):
        # Override to use HEADER_FIELDS for header detection but FIELDS for reading
        Parser.__init__(self, parsable)
        meta = {}
        row_count = 0

        expected_row = self.SEPARATOR.join(self.HEADER_FIELDS)

        with self.file.get_stream(binary=False) as stream:
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            if not stream_chunk_match(stream, 200, self.HEAD):
                self._raise_not_supported("Stream head different than expected")

            for row in stream:
                if row.strip() == expected_row:
                    break

                row_count += 1
                if "=" in row:
                    key, value = row.split("=")
                    if key in self.ALLOWED_META:
                        meta[key.strip()] = value.strip()

                if row_count > self.MAX_READ:
                    self._raise_not_supported(
                        f"Stream data not found after {self.MAX_READ} lines"
                    )

        parse_options = pacsv.ParseOptions(
            delimiter=self.SEPARATOR, invalid_row_handler=skip
        )
        read_options = pacsv.ReadOptions(
            skip_rows=row_count + 1,
            column_names=self.FIELDS,
        )
        # Read integer parts as strings to preserve signs (e.g. "-0")
        convert_options = pacsv.ConvertOptions(
            column_types={
                "Pressure_int": pa.string(),
                "Temp_int": pa.string(),
            },
        )
        try:
            with self.file.get_stream(binary=True) as stream:
                self.data = pacsv.read_csv(
                    stream,
                    parse_options=parse_options,
                    read_options=read_options,
                    convert_options=convert_options,
                ).to_pandas()
        except pa.lib.ArrowInvalid as error:
            self._raise_not_supported(
                f"CSV data does not match expected 5-column European decimal "
                f"format: {error}"
            )

        for key, value in meta.items():
            self.data[key] = [value] * len(self.data)

        # Recombine split decimal columns into proper float values
        # by concatenating the integer and decimal parts as strings
        self.data["Pressure"] = (
            self.data["Pressure_int"] + "." + self.data["Pressure_dec"].astype(str)
        ).astype(float)
        self.data["Temp"] = (
            self.data["Temp_int"] + "." + self.data["Temp_dec"].astype(str)
        ).astype(float)
        self.data = self.data.drop(
            columns=["Pressure_int", "Pressure_dec", "Temp_int", "Temp_dec"]
        )


class PathtrackPressParser(TDRHarmonizationMixin, Parser):
    DATATYPE = "tdr"
    DIVIDER = "*" * 85 + "\n"
    HEAD = DIVIDER + "PathTrack Raw Pressure Data File Downloaded from Base Station"
    FIELDS = (
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "temperature",
        "temperature_decimal",
        "depth_mbar",
        "depth_mbar_decimal",
        "depth_m",
        "depth_m_decimal",
    )
    OUTLIERS = None
    SEPARATOR = ","
    MAPPINGS = {
        TDRHarmonizedColumn.TIMESTAMP: "timestamp",
        TDRHarmonizedColumn.PRESSURE: "depth_mbar_float",
        TDRHarmonizedColumn.TEMPERATURE: "temperature_float",
        TDRHarmonizedColumn.DEPTH_M: "depth_m_float",
    }

    @classmethod
    def can_parse(cls, parsable):
        """Check if the file starts with the expected HEAD bytes."""
        try:
            with parsable.get_stream(binary=False) as stream:
                if not stream.seekable():
                    return False
                return stream_starts_with(stream, cls.HEAD)
        except (UnicodeDecodeError, OSError):
            return False

    def harmonize_data(self, data):
        data["timestamp"] = pd.to_datetime(
            {
                "year": 2000 + data["year"],
                "month": data["month"],
                "day": data["day"],
                "hour": data["hour"],
                "minute": data["minute"],
                "second": data["second"],
            }
        )
        data["temperature_float"] = (
            data["temperature"] + data["temperature_decimal"] / 100
        )
        data["depth_mbar_float"] = data["depth_mbar"] + data["depth_mbar_decimal"] / 100
        data["depth_m_float"] = data["depth_m"] + data["depth_m_decimal"] / 100
        return super().harmonize_data(data)

    def __init__(self, parsable: Parsable):
        super().__init__(parsable)

        with self.file.get_stream(binary=False) as stream:
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            if not stream_starts_with(stream, self.HEAD):
                self._raise_not_supported("Stream head different than expected")

            _soi, _metadata, data = stream.read().split(self.DIVIDER, 2)

        content = io.StringIO(data)
        reader = csv.reader(content, delimiter=self.SEPARATOR)
        header = next(reader)
        if len(header) != len(self.FIELDS):
            self._raise_not_supported(
                f"Stream have a number of fields different than expected, "
                f"{len(header)} != {len(self.FIELDS)}"
            )

        self.data = pd.read_csv(
            content, header=0, names=self.FIELDS, sep=self.SEPARATOR, index_col=False
        )


class SimpleTDR(TDRHarmonizationMixin, CSVParser):
    DATATYPE = "tdr"
    FIELDS = [
        "Time Stamp",
        "Pressure",
        "Temp",
    ]
    MAPPINGS = {
        TDRHarmonizedColumn.TIMESTAMP: "Time Stamp",
        TDRHarmonizedColumn.PRESSURE: "Pressure",
        TDRHarmonizedColumn.TEMPERATURE: "Temp",
        TDRHarmonizedColumn.DEPTH_M: None,
    }


class SimpleTDRVariantDate(TDRHarmonizationMixin, CSVParser):
    DATATYPE = "tdr"
    FIELDS = [
        "Date/Time Stamp",
        "Pressure",
        "Temp",
    ]
    MAPPINGS = {
        TDRHarmonizedColumn.TIMESTAMP: "Date/Time Stamp",
        TDRHarmonizedColumn.PRESSURE: "Pressure",
        TDRHarmonizedColumn.TEMPERATURE: "Temp",
        TDRHarmonizedColumn.DEPTH_M: None,
    }


PARSERS = [
    TDRParser,
    TDR2EuropeanDecimalParser,
    TDR2Parser,
    PathtrackPressParser,
    SimpleTDR,
    SimpleTDRVariantDate,
]
