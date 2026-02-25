import pandas as pd

from ..parser_base import CSVParser
from .columns import GPSHarmonizedColumn
from .mixin import GPSHarmonizationMixin


class IGotU_GT_Parser(GPSHarmonizationMixin, CSVParser):
    """
    Parser for IGotU_GT X version Logger
    """

    DATATYPE = "gps_igotugl"
    FIELDS = [
        "Date",
        "Time",
        "Latitude",
        "Longitude",
        "Altitude",
        "Speed",
        "Course",
        "Type",
        "Distance",
        "Essential",
    ]

    MAPPINGS = {
        GPSHarmonizedColumn.ID: None,
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
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def harmonize_data(self, data):
        # Combine Date and Time columns into timestamp
        data["timestamp"] = pd.to_datetime(
            data["Date"] + " " + data["Time"], errors="coerce"
        )
        return super().harmonize_data(data)


class IGotU_GT_TabSeparatedParser(IGotU_GT_Parser):
    """
    Parser separated by tabs
    """

    SEPARATOR = "\t"


class GPS_IGOTUGL(IGotU_GT_Parser):
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
    ]
    MAPPINGS = {
        GPSHarmonizedColumn.ID: "",
        GPSHarmonizedColumn.TIMESTAMP: None,
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


class GPS_IGOTUGL_INFO(GPS_IGOTUGL):
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


class GPS_IGOTUGL_SIMPLER(GPS_IGOTUGL):
    FIELDS = [
        "Date",
        "Time",
        "Latitude",
        "Longitude",
        "Altitude",
        "Satellites",
        "HDOP",
        "PDOP",
    ]


PARSERS = [
    IGotU_GT_Parser,
    IGotU_GT_TabSeparatedParser,
    GPS_IGOTUGL,
    GPS_IGOTUGL_SIMPLER,
    GPS_IGOTUGL_INFO,
]
