import csv

import pandas as pd

from .columns import GPSHarmonizedColumn
from ..parser_base import CSVParser, Parsable, Parser

FIELDS = [
    "DataID",
    "ID",
    "Ring_nr",
    "Date",
    "Time",
    "Altitude",
    "Speed",
    "Course",
    "HDOP",
    "Latitude",
    "Longitude",
    "TripNr",
]

MAPPINGS = {
    GPSHarmonizedColumn.ID: "DataID",
    GPSHarmonizedColumn.DATE: "Date",
    GPSHarmonizedColumn.TIME: "Time",
    GPSHarmonizedColumn.LATITUDE: "Latitude",
    GPSHarmonizedColumn.LONGITUDE: "Longitude",
    GPSHarmonizedColumn.ALTITUDE: "Altitude",
    GPSHarmonizedColumn.SPEED_KM_H: "Speed",
    GPSHarmonizedColumn.TYPE: None,
    GPSHarmonizedColumn.DISTANCE: None,
    GPSHarmonizedColumn.COURSE: "Course",
    GPSHarmonizedColumn.HDOP: "HDOP",
    GPSHarmonizedColumn.PDOP: None,
    GPSHarmonizedColumn.SATELLITES_COUNT: None,
    GPSHarmonizedColumn.TEMPERATURE: None,
    GPSHarmonizedColumn.SOLAR_I_MA: None,
    GPSHarmonizedColumn.BAT_SOC_PCT: None,
    GPSHarmonizedColumn.RING_NR: "Ring_nr",
    GPSHarmonizedColumn.TRIP_NR: "TripNr",
}


class GPSUnknownFormatParser(CSVParser):
    """
    Parser for a format, its a GPS CSV like format
    with the following fields
    """

    DATATYPE = "gps_unknown"
    SEPARATOR = "\t"
    FIELDS = FIELDS

    MAPPINGS = MAPPINGS


class GPSUnknownFormatParserWithEmptyColumns(Parser):
    """
    Parser for a format, its a GPS CSV like format
    with the following fields
    """

    DATATYPE = "gps_unknown"
    SEPARATOR = "\t"
    FIELDS = FIELDS
    SKIP_INITIAL_SPACE = True

    MAPPINGS = MAPPINGS

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

            # Filter empty columns
            header = [c for c in header if c != ""]

            if header != self.FIELDS:
                self._raise_not_supported(
                    f"Stream have a header different than expected, {header} != {self.FIELDS}"
                )

            stream.seek(0)
            self.data = pd.read_csv(
                stream, header=1, names=self.FIELDS, sep=self.SEPARATOR, index_col=False
            )


PARSERS = [
    GPSUnknownFormatParser,
    GPSUnknownFormatParserWithEmptyColumns,
]
