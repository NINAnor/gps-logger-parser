import pandas as pd

from ..parser_base import CSVParser
from .columns import GPSHarmonizedColumn
from .mixin import GPSHarmonizationMixin


class GPSUHo11(GPSHarmonizationMixin, CSVParser):
    DATATYPE = "gps_ho11"
    SEPARATOR = ";"
    FIELDS = [
        "ID",
        "Date",
        "Time",
        "DateTime",
        "Latitude",
        "Longitude",
        "Altitude",
        "Speed",
        "Course",
        "Type",
        "Distance",
        "DistAdj",
        "DistMax",
        "Tripnr",
    ]

    MAPPINGS = {
        GPSHarmonizedColumn.ID: "ID",
        GPSHarmonizedColumn.TIMESTAMP: None,
        GPSHarmonizedColumn.LATITUDE: "Latitude",
        GPSHarmonizedColumn.LONGITUDE: "Longitude",
        GPSHarmonizedColumn.ALTITUDE: "Altitude",
        GPSHarmonizedColumn.SPEED_KM_H: "Speed",
        GPSHarmonizedColumn.TYPE: "Type",
        GPSHarmonizedColumn.DISTANCE: "Distance",
        GPSHarmonizedColumn.COURSE: "Course",
        GPSHarmonizedColumn.HDOP: None,
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: None,
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: "Tripnr",
    }

    def harmonize_data(self, data):
        # Combine Date and Time columns into timestamp
        data["timestamp"] = pd.to_datetime(
            data["Date"] + " " + data["Time"],
            errors="raise",
            format="%d.%m.%Y %H:%M:%S",
        )

        for column in [
            "Latitude",
            "Longitude",
            "Altitude",
            "Speed",
            "Course",
            "Distance",
        ]:
            try:
                data[column] = data[column].str.replace(",", ".", regex=False)
                data[column] = pd.to_numeric(data[column], errors="coerce")
            except AttributeError:
                pass

        # Speed is provided in m/h, convert to km/h
        data["Speed"] = data["Speed"] / 1000

        return super().harmonize_data(data)


PARSERS = [
    GPSUHo11,
]
