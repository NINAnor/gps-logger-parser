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
        GPSHarmonizedColumn.DATE: "Date",
        GPSHarmonizedColumn.TIME: "Time",
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


PARSERS = [
    GPSUHo11,
]
