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
        "id": None,
        "date": "datetime",
        "time": None,
        "latitude": "lat",
        "longitude": "lon",
        "altitude": "alt",
        "speed_km_h": None,
        "type": None,
        "distance": None,
        "course": None,
        "hdop": "hdop",
        "pdop": None,
        "satellites_count": "numsat",
        "temperature": "temp",
        "solar_I_mA": None,
        "bat_soc_pct": None,
        "ring_nr": None,
        "trip_nr": None,
    }


PARSERS = [
    MatakiParser,
]
