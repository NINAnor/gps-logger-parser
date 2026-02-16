from ..parser_base import CSVParser


class InterrexParser(CSVParser):
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
        "id": "UUID",
        "date": "Collecting time",
        "time": None,
        "latitude": "Latitude",
        "longitude": "Longitude",
        "altitude": "Altitude",
        "speed_km_h": "Speed",
        "type": "Positioning mode",
        "distance": None,
        "course": "Course",
        "hdop": "HDOP",
        "pdop": "PDOP",
        "satellites_count": "Satellite used",
        "temperature": None,
        "solar_I_mA": None,
        "bat_soc_pct": None,
        "ring_nr": None,
        "trip_nr": None,
    }


PARSERS = [
    InterrexParser,
]
