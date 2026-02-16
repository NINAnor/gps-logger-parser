import csv

import pyarrow.csv as pacsv

from ..parser_base import CSVParser, Parsable


def skip(row):
    if row.text == "Power off command received.":
        return "skip"

    return "error"


class AXYTREKParser(CSVParser):
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
        "id": "TagID",
        "date": "Date",
        "time": "Time",
        "latitude": "location-lat",
        "longitude": "location-lon",
        "altitude": "height-above-msl",
        "speed_km_h": "ground-speed",
        "type": None,
        "distance": None,
        "course": None,
        "hdop": "hdop",
        "pdop": None,
        "satellites_count": "satellite-count",
        "temperature": "Temp. (?C)",
        "solar_I_mA": None,
        "bat_soc_pct": None,
        "ring_nr": None,
        "trip_nr": None,
    }

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
                    f"Stream have a header different than expected, {header} != {self.FIELDS}"
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
