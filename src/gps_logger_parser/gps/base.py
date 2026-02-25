from ..parser_base import CSVParser
from .columns import GPSHarmonizedColumn
from .mixin import GPSHarmonizationMixin


class GPSParser(GPSHarmonizationMixin, CSVParser):
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
        GPSHarmonizedColumn.ID: "device_id",
        GPSHarmonizedColumn.DATE: "UTC_date",
        GPSHarmonizedColumn.TIME: "UTC_time",
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
    GPSParser,
]
