import csv

import pandas as pd
import pyarrow.csv as pacsv

from ..parser_base import CSVParser, Parsable
from .columns import GPSHarmonizedColumn
from .mixin import GPSHarmonizationMixin


def skip(row):
    if row.text == "Power off command received.":
        return "skip"

    return "error"


class AXYTREKParser(GPSHarmonizationMixin, CSVParser):
    DATATYPE = "gps_axytrek"
    FIELDS = [
        "TagID",
        "Date",
        "Time",
        "X",
        "Y",
        "Z",
        "Activity",
        "Depth",
        "Temp. (?C)",
        "location-lat",
        "location-lon",
        "height-above-msl",
        "ground-speed",
        "satellite-count",
        "hdop",
        "maximum-signal-strength",
        "Sensor Raw",
        "Battery Voltage (V)",
    ]

    MAPPINGS = {
        GPSHarmonizedColumn.ID: "TagID",
        GPSHarmonizedColumn.TIMESTAMP: None,
        GPSHarmonizedColumn.LATITUDE: "location-lat",
        GPSHarmonizedColumn.LONGITUDE: "location-lon",
        GPSHarmonizedColumn.ALTITUDE: "height-above-msl",
        GPSHarmonizedColumn.SPEED_KM_H: "ground-speed",
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: "hdop",
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: "satellite-count",
        GPSHarmonizedColumn.TEMPERATURE: "Temp. (?C)",
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def harmonize_data(self, data):
        # Combine Date and Time columns into timestamp
        data["timestamp"] = pd.to_datetime(
            data["Date"] + " " + data["Time"], errors="coerce"
        )
        return super().harmonize_data(data)

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
            if header != self.FIELDS:
                self._raise_not_supported(
                    f"Stream have a header different than expected, "
                    f"{header} != {self.FIELDS}"
                )

        parse_options = pacsv.ParseOptions(
            delimiter=self.SEPARATOR, invalid_row_handler=skip
        )
        self.data = pacsv.read_csv(
            self.file._file_path, parse_options=parse_options
        ).to_pandas()


PARSERS = [
    AXYTREKParser,
]
