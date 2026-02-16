import csv
import io
import re

import pandas as pd

from ..helpers import stream_chunk_contains
from ..parser_base import Parsable, Parser


def signed(val, direction):
    val = float(val)
    if direction in ("S", "W"):
        return -val
    return val


# Earth&Ocean mGPS-2


class GPS2JMParser7_5(Parser):
    """
    Parser for 2Jm format v 7.5
    """

    DATATYPE = "gps_2jm"
    # TODO: define fields
    FIELDS = [str(x) for x in range(0, 13)]
    VERSION = "v7.5"
    SEPARATOR = " "
    ENDINGS = [
        "[EOF]",
        "---- End of data ----",
    ]

    # TODO: understand the fields first
    # MAPPINGS = {
    #     "id": "",
    #     "date": None,
    #     "time": None,
    #     "latitude": None,
    #     "longitude": None,
    #     "altitude": None,
    #     "speed_km_h": None,
    #     "type": None,
    #     "distance": None,
    #     "course": None,
    #     "hdop": None,
    #     "pdop": None,
    #     "satellites_count": None,
    #     "direction_deg": None,
    #     "temperature": None,
    #     "solar_I_mA": None,
    #     "bat_soc_pct": None,
    #     "ring_nr": None,
    #     "trip_nr": None,
    # }

    def _fix_content(self, data):
        return data

    def __init__(self, parsable: Parsable):
        super().__init__(parsable)

        with self.file.get_stream(binary=False, errors="backslashreplace") as stream:
            # TODO: check the first byte instead of the whole stream chunk
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            if not stream_chunk_contains(stream, 30, "2JmGPS-LOG"):
                self._raise_not_supported("Stream must start with 2JmGPS-LOG")

            groups = stream.read().split("\n\n")
            head = groups.pop(0)

            if self.VERSION not in head:
                self._raise_not_supported("Version not supported")

            data = None
            for group in groups:
                if group in self.ENDINGS:
                    break
                data = group

            data = self._fix_content(data)

            content = io.StringIO(data)

            reader = csv.reader(
                content, delimiter=self.SEPARATOR, skipinitialspace=True
            )
            header = next(reader)
            if len(header) != len(self.FIELDS):
                self._raise_not_supported(
                    f"Stream have fields different than expected, {len(header)} != {len(self.FIELDS)}"
                )

            self.data = pd.read_csv(
                content,
                header=0,
                names=self.FIELDS,
                sep=self.SEPARATOR,
                index_col=False,
            )


regex = re.compile(r"\s{2,10}", re.MULTILINE)


class GPS2JMParser8(GPS2JMParser7_5):
    VERSION = "v8"
    FIELDS = [
        "date",
        "time",
        "latitude",
        "latitude_decimal",
        "n",
        "longitude",
        "longitude_decimal",
        "e",
        "satellite",
        "voltage",
        "speed",
        "altitude",
        "distance",
    ]
    MAPPINGS = {
        "id": "",
        "date": "date",
        "time": "time",
        "latitude": None,
        "longitude": None,
        "altitude": "altitude",
        "speed_km_h": "speed",
        "type": None,
        "distance": "distance",
        "course": None,
        "hdop": None,
        "pdop": None,
        "satellites_count": "satellite",
        "direction_deg": None,
        "temperature": None,
        "solar_I_mA": None,
        "bat_soc_pct": None,
        "ring_nr": None,
        "trip_nr": None,
    }

    def _fix_content(self, data: str):
        """
        In version 8 there is a strange notation using the whitespace
        also to right align the number for a specific column
        In this case replace the multiple spaces
        """
        return regex.sub(" ", data)


class GPS2JMParser8Alternative(Parser):
    """
    Parser for 2Jm format v8

    instead of a .LOG file these files are ASCII encoded
    with a header structured
    """

    DATATYPE = "gps_2jm"
    # TODO: define fields
    FIELDS = [
        "UTC_date",
        "UTC_time",
        "Latitude",
        "Latitude_dir",
        "Longitude",
        "Longitude_dir",
        "satcount",
        "hdop",
        "speed_km_h",
        "altitude_m",
        "direction_deg",
    ]
    VERSION = "v8"
    SEPARATOR = " "

    # TODO: understand the fields first
    MAPPINGS = {
        "id": "",
        "date": "UTC_date",
        "time": "UTC_time",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "altitude": "altitude_m",
        "speed_km_h": "speed_km_h",
        "type": None,
        "distance": None,
        "course": None,
        "hdop": None,
        "pdop": None,
        "satellites_count": "satcount",
        "direction_deg": "direction_deg",
        "temperature": None,
        "solar_I_mA": None,
        "bat_soc_pct": None,
        "ring_nr": None,
        "trip_nr": None,
    }

    def _fix_content(self, data: str):
        """
        In version 8 there is a strange notation using the whitespace
        also to right align the number for a specific column
        In this case replace the multiple spaces
        """
        return regex.sub(" ", data)

    def __init__(self, parsable: Parsable):
        super().__init__(parsable)

        with self.file.get_stream(binary=False, errors="backslashreplace") as stream:
            # TODO: check the first byte instead of the whole stream chunk
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            if not stream_chunk_contains(
                stream, 50, "************* GPS DATA *************"
            ):
                self._raise_not_supported(
                    "Stream must start with ************* GPS DATA *************"
                )

            head, data = stream.read().split("\n\n\n\n")

            data = self._fix_content(data)

            content = io.StringIO(data)

            reader = csv.reader(
                content, delimiter=self.SEPARATOR, skipinitialspace=True
            )
            header = next(reader)
            if len(header) != len(self.FIELDS):
                self._raise_not_supported(
                    f"Stream have fields different than expected, {len(header)} != {len(self.FIELDS)}"
                )

            df = pd.read_csv(
                content,
                header=0,
                names=self.FIELDS,
                sep=self.SEPARATOR,
                index_col=False,
            )

            df["Latitude"] = [
                signed(v, d)
                for v, d in zip(df["Latitude"], df["Latitude_dir"], strict=False)
            ]
            df["Longitude"] = [
                signed(v, d)
                for v, d in zip(df["Longitude"], df["Longitude_dir"], strict=False)
            ]
            self.data = df


PARSERS = [
    GPS2JMParser7_5,
    GPS2JMParser8,
    GPS2JMParser8Alternative,
]
