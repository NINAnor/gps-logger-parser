from .columns import GPSHarmonizedColumn
from ..parser_base import CSVParser


class MatakiParser(CSVParser):
    DATATYPE = "gps_mataki"
    FIELDS = [
        "node",
        "datetime",
        "lat",
        "lon",
        "fix",
        "numsat",
        "hdop",
        "alt",
        "pressure",
        "temp",
        "voltage",
    ]

    MAPPINGS = {
        GPSHarmonizedColumn.ID: None,
        GPSHarmonizedColumn.DATE: "datetime",
        GPSHarmonizedColumn.TIME: None,
        GPSHarmonizedColumn.LATITUDE: "lat",
        GPSHarmonizedColumn.LONGITUDE: "lon",
        GPSHarmonizedColumn.ALTITUDE: "alt",
        GPSHarmonizedColumn.SPEED_KM_H: None,
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: "hdop",
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: "numsat",
        GPSHarmonizedColumn.TEMPERATURE: "temp",
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }


PARSERS = [
    MatakiParser,
]
