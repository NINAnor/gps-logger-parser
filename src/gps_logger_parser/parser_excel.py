from .parser_base import ExcelParser


class GPSUnknownFormatExcelParser(ExcelParser):
    """
    Parser for a format, its a GPS excel like format
    with the following fields
    """

    DATATYPE = "gps_unknown"
    FIELDS = [
        "ID",
        "DateTime",
        "Date",
        "Time",
        "Altitude",
        "Speed",
        "Type",
        "Course",
        "Distance",
        "Latitude",
        "Longitude",
        "Tripnr",
        "DistAdj",
        "DistMax",
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
