from ..parser_base import CSVParser
from .columns import GPSHarmonizedColumn
from .mixin import GPSHarmonizationMixin


class InterrexParser(GPSHarmonizationMixin, CSVParser):
    """
    Parser for Interrex Logger
    """

    DATATYPE = "gps_interrex"
    FIELDS = [
        "UUID",
        "Transmitting time",
        "Collecting time",
        "Longitude",
        "Latitude",
        "Altitude",
        "Altitude (Ellipsoid)",
        "Speed",
        "Course",
        "Satellite used",
        "Positioning mode",
        "HorAccuracy",
        "VerAccuracy",
        "GPS time consumption",
        "Data Source",
        "HDOP",
        "VDOP",
    ]

    MAPPINGS = {
        GPSHarmonizedColumn.ID: "UUID",
        GPSHarmonizedColumn.DATE: "Collecting time",
        GPSHarmonizedColumn.TIME: None,
        GPSHarmonizedColumn.LATITUDE: "Latitude",
        GPSHarmonizedColumn.LONGITUDE: "Longitude",
        GPSHarmonizedColumn.ALTITUDE: "Altitude",
        GPSHarmonizedColumn.SPEED_KM_H: "Speed",
        GPSHarmonizedColumn.TYPE: "Positioning mode",
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: "Course",
        GPSHarmonizedColumn.HDOP: "HDOP",
        GPSHarmonizedColumn.PDOP: "PDOP",
        GPSHarmonizedColumn.SATELLITES_COUNT: "Satellite used",
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }


PARSERS = [
    InterrexParser,
]
