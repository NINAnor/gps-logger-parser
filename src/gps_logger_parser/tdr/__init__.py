import csv
import io

import pandas as pd
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

            for row in stream.readlines():
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
        read_options = pacsv.ReadOptions(skip_rows=row_count + 1)
        convert_options = pacsv.ConvertOptions(
            include_columns=self.FIELDS + ["empty"],
            include_missing_columns=True,
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
    TDR2Parser,
    PathtrackPressParser,
    SimpleTDR,
    SimpleTDRVariantDate,
]
