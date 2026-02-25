from ..parser_base import CSVParser
from .columns import GPSHarmonizedColumn
from .mixin import GPSHarmonizationMixin


class OrnitelaParser(GPSHarmonizationMixin, CSVParser):
    DATATYPE = "gps_ornitela"
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
        "MSL_altitude_m",
        "Reserved",
        "speed_km/h",
        "direction_deg",
        "int_temperature_C",
        "mag_x",
        "mag_y",
        "mag_z",
        "acc_x",
        "acc_y",
        "acc_z",
        "UTC_timestamp",
        "milliseconds",
        "light",
        "altimeter_m",
        "depth_m",
        "conductivity_mS/cm",
        "ext_temperature_C",
    ]

    MAPPINGS = {
        GPSHarmonizedColumn.ID: "device_id",
        GPSHarmonizedColumn.TIMESTAMP: "UTC_timestamp",
        GPSHarmonizedColumn.LATITUDE: "Latitude",
        GPSHarmonizedColumn.LONGITUDE: "Longitude",
        GPSHarmonizedColumn.ALTITUDE: "MSL_altitude_m",
        GPSHarmonizedColumn.SPEED_KM_H: "speed_km/h",
        GPSHarmonizedColumn.TYPE: "datatype",
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: "direction_deg",
        GPSHarmonizedColumn.HDOP: "hdop",
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: "satcount",
        GPSHarmonizedColumn.TEMPERATURE: "ext_temperature_C",
        GPSHarmonizedColumn.SOLAR_I_MA: "solar_I_mA",
        GPSHarmonizedColumn.BAT_SOC_PCT: "bat_soc_pct",
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }


class OrnitelaAlternativeParser(OrnitelaParser):
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
        "",
    ]

    MAPPINGS = {
        GPSHarmonizedColumn.ID: "device_id",
        GPSHarmonizedColumn.TIMESTAMP: "UTC_datetime",
        GPSHarmonizedColumn.LATITUDE: "Latitude",
        GPSHarmonizedColumn.LONGITUDE: "Longitude",
        GPSHarmonizedColumn.ALTITUDE: "Altitude_m",
        GPSHarmonizedColumn.SPEED_KM_H: "speed_km_h",
        GPSHarmonizedColumn.TYPE: "datatype",
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: "direction_deg",
        GPSHarmonizedColumn.HDOP: "hdop",
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: "satcount",
        GPSHarmonizedColumn.TEMPERATURE: "temperature_C",
        GPSHarmonizedColumn.SOLAR_I_MA: "solar_I_mA",
        GPSHarmonizedColumn.BAT_SOC_PCT: "bat_soc_pct",
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }


PARSERS = [
    OrnitelaParser,
    OrnitelaAlternativeParser,
]
