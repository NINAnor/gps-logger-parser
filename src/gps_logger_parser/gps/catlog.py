import csv
import io

import pandas as pd

from ..helpers import stream_chunk_contains, stream_starts_with
from ..parser_base import CSVParser, Parsable
from .columns import GPSHarmonizedColumn


class GPSCatTrackParser(CSVParser):
    """
    Parser for a format, its a GPS CSV like format
    with the following fields
    """

    DATATYPE = "gps_cattrack"
    DIVIDER = "--------\n"
    START_WITH = "Name:CatLog"
    FIELDS = [
        "Date",
        "Time",
        "Latitude",
        "Longitude",
        "Altitude",
        "Satellites",
        "HDOP",
        "PDOP",
        "Temperature [C]",
        "Speed [km/h]",
        "TTFF",
        "SNR",
        "tbd",
    ]

    MAPPINGS = {
        GPSHarmonizedColumn.ID: "",
        GPSHarmonizedColumn.DATE: "Date",
        GPSHarmonizedColumn.TIME: "Time",
        GPSHarmonizedColumn.LATITUDE: "Latitude",
        GPSHarmonizedColumn.LONGITUDE: "Longitude",
        GPSHarmonizedColumn.ALTITUDE: "Altitude",
        GPSHarmonizedColumn.SPEED_KM_H: "Speed [km/h]",
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: "HDOP",
        GPSHarmonizedColumn.PDOP: "PDOP",
        GPSHarmonizedColumn.SATELLITES_COUNT: "Satellites",
        GPSHarmonizedColumn.TEMPERATURE: "Temperature [C]",
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def __init__(self, parsable: Parsable):
        self.file = parsable
        self.data = []

        with self.file.get_stream(binary=False) as stream:
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            if self.START_WITH and not stream_starts_with(stream, self.START_WITH):
                self._raise_not_supported("Stream must start with Name:CatLog")

            if self.DIVIDER:
                if stream_chunk_contains(stream, 500, self.DIVIDER):
                    _intro, data = stream.read().split(self.DIVIDER)
                    content = io.StringIO(data)
                else:
                    self._raise_not_supported(
                        f"Stream doesn't have the divider {self.DIVIDER}"
                    )
            else:
                content = stream

            reader = csv.reader(
                content,
                delimiter=self.SEPARATOR,
                skipinitialspace=self.SKIP_INITIAL_SPACE,
            )
            header = next(reader)
            if header != self.FIELDS:
                self._raise_not_supported(
                    f"Stream have fields different than expected, "
                    f"{header} != {self.FIELDS}"
                )

            self.data = pd.read_csv(
                content,
                header=0,
                names=self.FIELDS,
                sep=self.SEPARATOR,
                index_col=False,
            )


class GPSCatTrack2(GPSCatTrackParser):
    FIELDS = [
        "Date",
        "Time",
        "Latitude",
        "Longitude",
        "Altitude",
        "Satellites",
        "HDOP",
        "PDOP",
        "TTF [s]",
        "Info",
    ]
    DIVIDER = "-----\n"

    MAPPINGS = {
        GPSHarmonizedColumn.ID: "",
        GPSHarmonizedColumn.DATE: "Date",
        GPSHarmonizedColumn.TIME: "Time",
        GPSHarmonizedColumn.LATITUDE: "Latitude",
        GPSHarmonizedColumn.LONGITUDE: "Longitude",
        GPSHarmonizedColumn.ALTITUDE: "Altitude",
        GPSHarmonizedColumn.SPEED_KM_H: None,
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: "HDOP",
        GPSHarmonizedColumn.PDOP: "PDOP",
        GPSHarmonizedColumn.SATELLITES_COUNT: "Satellites",
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }


class GPSCatTrack3(GPSCatTrackParser):
    """
    This CatTrack logger has a wrong number of columns, 2 are missing
    """

    FIELDS = [
        "Date",
        "Time",
        "Latitude",
        "Longitude",
        "Altitude",
        "Satellites",
        "HDOP",
        "PDOP",
        "TTF [s]",
        "Info",
    ]
    DIVIDER = "--------\n"

    MAPPINGS = {
        GPSHarmonizedColumn.ID: "",
        GPSHarmonizedColumn.DATE: "Date",
        GPSHarmonizedColumn.TIME: "Time",
        GPSHarmonizedColumn.LATITUDE: "Latitude",
        GPSHarmonizedColumn.LONGITUDE: "Longitude",
        GPSHarmonizedColumn.ALTITUDE: "Altitude",
        GPSHarmonizedColumn.SPEED_KM_H: None,
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: "HDOP",
        GPSHarmonizedColumn.PDOP: "PDOP",
        GPSHarmonizedColumn.SATELLITES_COUNT: "Satellites",
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def __init__(self, parsable: Parsable):
        self.file = parsable
        self.data = []

        with self.file.get_stream(binary=False) as stream:
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            if self.START_WITH and not stream_starts_with(stream, self.START_WITH):
                self._raise_not_supported("Stream must start with Name:CatLog")

            if self.DIVIDER:
                if stream_chunk_contains(stream, 500, self.DIVIDER):
                    _intro, data = stream.read().split(self.DIVIDER)
                    content = io.StringIO(data)
                else:
                    self._raise_not_supported(
                        f"Stream doesn't have the divider {self.DIVIDER}"
                    )
            else:
                content = stream

            reader = csv.reader(
                content,
                delimiter=self.SEPARATOR,
                skipinitialspace=self.SKIP_INITIAL_SPACE,
            )
            header = next(reader)
            if header != self.FIELDS:
                self._raise_not_supported(
                    f"Stream have fields different than expected, "
                    f"{header} != {self.FIELDS}"
                )

            # Ensure that the headers are present, then ignore them
            names = self.FIELDS[:-2]

            self.data = pd.read_csv(
                content, header=0, names=names, sep=self.SEPARATOR, index_col=False
            )


PARSERS = [
    GPSCatTrackParser,
    GPSCatTrack2,
    GPSCatTrack3,
]
