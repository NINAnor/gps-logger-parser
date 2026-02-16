from ..parser_base import CSVParser


class IGotU_GT_Parser(CSVParser):
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
        "id": None,
        "date": "Date",
        "time": "Time",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "altitude": "Altitude",
        "speed_km_h": "Speed",
        "type": "Type",
        "distance": "Distance",
        "course": "Course",
        "hdop": None,
        "pdop": None,
        "satellites_count": None,
        "temperature": None,
        "solar_I_mA": None,
        "bat_soc_pct": None,
        "ring_nr": None,
        "trip_nr": None,
    }


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
        "id": "",
        "date": "Date",
        "time": "Time",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "altitude": "Altitude",
        "speed_km_h": None,
        "type": None,
        "distance": None,
        "course": None,
        "hdop": "HDOP",
        "pdop": "PDOP",
        "satellites_count": "Satellites",
        "temperature": None,
        "solar_I_mA": None,
        "bat_soc_pct": None,
        "ring_nr": None,
        "trip_nr": None,
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
