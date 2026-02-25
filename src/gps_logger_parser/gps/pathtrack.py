import csv
import io

import pandas as pd

from ..helpers import stream_starts_with
from ..parser_base import CSVParser, Parsable, Parser
from .columns import GPSHarmonizedColumn


class PathtrackParser(Parser):
    DATATYPE = "gps_pathtrack"
    DIVIDER = "*" * 85 + "\n"
    HEAD = DIVIDER + "PathTrack Archival Tracking System Results File"
    FIELDS = (
        "day",
        "month",
        "year",
        "hour",
        "minute",
        "second",
        "second_of_day",
        "satellites",
        "lat",
        "lon",
        "altitude",
        "clock_offset",
        "accuracy",  # HDOP?
        "battery",
        "unknown1",
        "unknown2",
    )
    SEPARATOR = ","

    MAPPINGS = {
        GPSHarmonizedColumn.ID: None,
        GPSHarmonizedColumn.DATE: "date",
        GPSHarmonizedColumn.TIME: "time",
        GPSHarmonizedColumn.LATITUDE: "lat",
        GPSHarmonizedColumn.LONGITUDE: "lon",
        GPSHarmonizedColumn.ALTITUDE: "altitude",
        GPSHarmonizedColumn.SPEED_KM_H: None,
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: "accuracy",
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: "satellites",
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def harmonize_data(self):
        self.data["time"] = (
            self.data["hour"].astype(str)
            + ":"
            + self.data["minute"].astype(str)
            + ":"
            + self.data["second"].astype(str)
        )
        self.data["date"] = (
            self.data["day"].astype(str)
            + "/"
            + self.data["month"].astype(str)
            + ":"
            + self.data["year"].astype(str)
        )
        return super().harmonize_data()

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
                content,
                header=0,
                names=self.FIELDS,
                sep=self.SEPARATOR,
                index_col=False,
            )


class PathtrackParserNoUnknown(PathtrackParser):
    FIELDS = (
        "day",
        "month",
        "year",
        "hour",
        "minute",
        "second",
        "second_of_day",
        "satellites",
        "lat",
        "lon",
        "altitude",
        "clock_offset",
        "accuracy",  # HDOP?
        "battery",
    )


class CSVPathtrack(CSVParser):
    DATATYPE = "gps_pathtrack"
    FIELDS = [
        "day",
        "month",
        "year",
        "hour",
        "minute",
        "second",
        "second_of_the_day",
        "satellites",
        "latitude",
        "longitude",
        "altitude",
        "clock_offset",
        "accuracy_indicator",
        "battery",
        "processing_parameterA",
        "processing_parameterB",
    ]
    SEPARATOR = ";"
    MAPPINGS = {
        GPSHarmonizedColumn.ID: None,
        GPSHarmonizedColumn.DATE: "date",
        GPSHarmonizedColumn.TIME: "time",
        GPSHarmonizedColumn.LATITUDE: "latitude",
        GPSHarmonizedColumn.LONGITUDE: "longitude",
        GPSHarmonizedColumn.ALTITUDE: "altitude",
        GPSHarmonizedColumn.SPEED_KM_H: None,
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: "accuracy_indicator",
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: "satellites",
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def harmonize_data(self):
        self.data["time"] = (
            self.data["hour"].astype(str)
            + ":"
            + self.data["minute"].astype(str)
            + ":"
            + self.data["second"].astype(str)
        )
        self.data["date"] = (
            self.data["day"].astype(str)
            + "/"
            + self.data["month"].astype(str)
            + ":"
            + self.data["year"].astype(str)
        )
        return super().harmonize_data()


PARSERS = [
    PathtrackParser,
    PathtrackParserNoUnknown,
    CSVPathtrack,
]
