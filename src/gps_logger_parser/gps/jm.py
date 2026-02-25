import csv
import io
import re

import pandas as pd

from ..helpers import stream_chunk_contains
from ..parser_base import Parsable, Parser
from .columns import GPSHarmonizedColumn
from .mixin import GPSHarmonizationMixin


def signed(val, direction):
    val = float(val)
    if direction in ("S", "W"):
        return -val
    return val


# Earth&Ocean mGPS-2


class GPS2JMParser7_5(GPSHarmonizationMixin, Parser):
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
        GPSHarmonizedColumn.ID: "",
        GPSHarmonizedColumn.TIMESTAMP: None,
        GPSHarmonizedColumn.LATITUDE: None,
        GPSHarmonizedColumn.LONGITUDE: None,
        GPSHarmonizedColumn.ALTITUDE: None,
        GPSHarmonizedColumn.SPEED_KM_H: None,
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: None,
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: None,
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def harmonize_data(self, data):
        # Combine start date with time from each row
        # start_date is in format DD.MM.YYYY, time is HH:MM:SS
        # Call parent harmonization first
        data = super().harmonize_data(data)

        # Then create the timestamp column after original columns are prefixed
        if hasattr(self, "start_date") and self.start_date:
            # time column is now __original__time
            data["timestamp"] = pd.to_datetime(
                self.start_date + " " + data["__original__time"],
                format="%d.%m.%Y %H:%M:%S",
                errors="coerce",
            )
        else:
            # Fallback if start_date not available
            data["timestamp"] = pd.to_datetime(
                data["__original__date"].astype(str) + " " + data["__original__time"],
                format="%d %H:%M:%S",
                errors="coerce",
            )
        return data

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

            full_content = stream.read()
            groups = full_content.split("\n\n")
            head = groups.pop(0)

            if self.VERSION not in head:
                self._raise_not_supported("Version not supported")

            # Extract STARTTIME date from any of the header groups
            self.start_date = None
            for group in groups:
                for line in group.split("\n"):
                    if "STARTTIME" in line:
                        # Format: STARTTIME .......: 29.06.2013 20:00:00
                        parts = line.split(":")
                        if len(parts) >= 2:
                            datetime_str = parts[1].strip()
                            # Extract just the date part (DD.MM.YYYY)
                            self.start_date = datetime_str.split()[0]
                        break
                if self.start_date:
                    break

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
                    f"Stream have fields different than expected, "
                    f"{len(header)} != {len(self.FIELDS)}"
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
        GPSHarmonizedColumn.ID: "",
        GPSHarmonizedColumn.TIMESTAMP: None,
        GPSHarmonizedColumn.LATITUDE: None,
        GPSHarmonizedColumn.LONGITUDE: None,
        GPSHarmonizedColumn.ALTITUDE: "altitude",
        GPSHarmonizedColumn.SPEED_KM_H: "speed",
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: "distance",
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: None,
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: "satellite",
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def harmonize_data(self, data):
        # Combine start date with time from each row
        # start_date is in format DD.MM.YYYY, time is HH:MM:SS
        # Call parent harmonization first
        data = super().harmonize_data(data)

        # Then create the timestamp column after original columns are prefixed
        if hasattr(self, "start_date") and self.start_date:
            # time column is now __original__time
            data["timestamp"] = pd.to_datetime(
                self.start_date + " " + data["__original__time"],
                format="%d.%m.%Y %H:%M:%S",
                errors="coerce",
            )
        else:
            raise NotImplementedError("Version 8 without start date is not supported")
        return data

    def _fix_content(self, data: str):
        """
        In version 8 there is a strange notation using the whitespace
        also to right align the number for a specific column
        In this case replace the multiple spaces
        """
        return regex.sub(" ", data)


class GPS2JMParser8Alternative(GPSHarmonizationMixin, Parser):
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
        GPSHarmonizedColumn.ID: "",
        GPSHarmonizedColumn.TIMESTAMP: None,
        GPSHarmonizedColumn.LATITUDE: "Latitude",
        GPSHarmonizedColumn.LONGITUDE: "Longitude",
        GPSHarmonizedColumn.ALTITUDE: "altitude_m",
        GPSHarmonizedColumn.SPEED_KM_H: "speed_km_h",
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: None,
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: "satcount",
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def harmonize_data(self, data):
        # Combine UTC_date and UTC_time columns into timestamp
        # Call parent harmonization first
        data = super().harmonize_data(data)

        # Then create timestamp from the prefixed original columns
        data["timestamp"] = pd.to_datetime(
            data["__original__UTC_date"] + " " + data["__original__UTC_time"],
            format="%d.%m.%Y %H:%M:%S",
            errors="coerce",
        )
        return data

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
                    f"Stream have fields different than expected, "
                    f"{len(header)} != {len(self.FIELDS)}"
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
