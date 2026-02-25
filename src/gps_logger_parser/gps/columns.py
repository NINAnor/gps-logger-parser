from enum import Enum


class GPSHarmonizedColumn(str, Enum):
    """Enum of harmonized GPS column names"""

    ID = "id"
    TIMESTAMP = "timestamp"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    ALTITUDE = "altitude"
    SPEED_KM_H = "speed_km_h"
    TYPE = "type"
    DISTANCE = "distance"
    COURSE = "course"
    HDOP = "hdop"
    PDOP = "pdop"
    SATELLITES_COUNT = "satellites_count"
    TEMPERATURE = "temperature"
    SOLAR_I_MA = "solar_I_mA"
    BAT_SOC_PCT = "bat_soc_pct"
    RING_NR = "ring_nr"
    TRIP_NR = "trip_nr"


# Pandas dtype mapping for each harmonized column
GPS_HARMONIZED_COLUMN_TYPES = {
    GPSHarmonizedColumn.ID: "object",
    GPSHarmonizedColumn.TIMESTAMP: "datetime64[ns]",
    GPSHarmonizedColumn.LATITUDE: "float64",
    GPSHarmonizedColumn.LONGITUDE: "float64",
    GPSHarmonizedColumn.ALTITUDE: "float64",
    GPSHarmonizedColumn.SPEED_KM_H: "float64",
    GPSHarmonizedColumn.TYPE: "object",
    GPSHarmonizedColumn.DISTANCE: "float64",
    GPSHarmonizedColumn.COURSE: "float64",
    GPSHarmonizedColumn.HDOP: "float64",
    GPSHarmonizedColumn.PDOP: "float64",
    GPSHarmonizedColumn.SATELLITES_COUNT: "Int64",
    GPSHarmonizedColumn.TEMPERATURE: "float64",
    GPSHarmonizedColumn.SOLAR_I_MA: "float64",
    GPSHarmonizedColumn.BAT_SOC_PCT: "float64",
    GPSHarmonizedColumn.RING_NR: "object",
    GPSHarmonizedColumn.TRIP_NR: "Int64",
}
