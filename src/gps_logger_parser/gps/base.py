from ..parser_base import CSVParser


class GPSParser(CSVParser):
    DATATYPE = "gps"
    FIELDS = [
        "device_id",
        "UTC_datetime",
        "UTC_date",
        "UTC_time",
        "datatype",
        "satcount",
        "U_bat_mV",
        "bat_soc_pct",
        "solar_I_mA",
        "hdop",
        "Latitude",
        "Longitude",
        "Altitude_m",
        "speed_km_h",
        "direction_deg",
        "temperature_C",
        "mag_x",
        "mag_y",
        "mag_z",
        "acc_x",
        "acc_y",
        "acc_z",
        "depth_m",
        "",
    ]

    MAPPINGS = {
        "id": "device_id",
        "date": "UTC_date",
        "time": "UTC_time",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "altitude": "Altitude_m",
        "speed_km_h": "speed_km_h",
        "type": "datatype",
        "distance": None,
        "course": "direction_deg",
        "hdop": "hdop",
        "pdop": None,
        "satellites_count": "satcount",
        "temperature": "temperature_C",
        "solar_I_mA": "solar_I_mA",
        "bat_soc_pct": "bat_soc_pct",
        "ring_nr": None,
        "trip_nr": None,
    }


PARSERS = [
    GPSParser,
]
