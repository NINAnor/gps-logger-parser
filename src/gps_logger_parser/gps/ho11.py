from ..parser_base import CSVParser


class GPSUHo11(CSVParser):
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
        "id": "ID",
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
        "trip_nr": "Tripnr",
    }


PARSERS = [
    GPSUHo11,
]
