from enum import Enum

import pyarrow as pa


class GPSHarmonizedColumn(str, Enum):
    """Enum of harmonized GPS column names"""

    ID = "id"
    DATE = "date"
    TIME = "time"
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


# PyArrow type mapping for each harmonized column
GPS_HARMONIZED_COLUMN_TYPES = {
    GPSHarmonizedColumn.ID: pa.string(),
    GPSHarmonizedColumn.DATE: pa.string(),  # Can be date32() after parsing
    GPSHarmonizedColumn.TIME: pa.string(),  # Can be time64('us') after parsing
    GPSHarmonizedColumn.LATITUDE: pa.float64(),
    GPSHarmonizedColumn.LONGITUDE: pa.float64(),
    GPSHarmonizedColumn.ALTITUDE: pa.float64(),
    GPSHarmonizedColumn.SPEED_KM_H: pa.float64(),
    GPSHarmonizedColumn.TYPE: pa.string(),
    GPSHarmonizedColumn.DISTANCE: pa.float64(),
    GPSHarmonizedColumn.COURSE: pa.float64(),
    GPSHarmonizedColumn.HDOP: pa.float64(),
    GPSHarmonizedColumn.PDOP: pa.float64(),
    GPSHarmonizedColumn.SATELLITES_COUNT: pa.int64(),
    GPSHarmonizedColumn.TEMPERATURE: pa.float64(),
    GPSHarmonizedColumn.SOLAR_I_MA: pa.float64(),
    GPSHarmonizedColumn.BAT_SOC_PCT: pa.float64(),
    GPSHarmonizedColumn.RING_NR: pa.string(),
    GPSHarmonizedColumn.TRIP_NR: pa.int64(),
}
