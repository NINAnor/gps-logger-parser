from enum import Enum


class OtherSensorHarmonizedColumn(str, Enum):
    """Enum of harmonized column names for other sensor parsers"""

    ID = "id"
    TIMESTAMP_TRANSMIT = "timestamp_transmit"
    TIMESTAMP = "timestamp"
    TEMPERATURE = "temperature"
    LIGHT_INTENSITY = "light_intensity"
    VOLTAGE = "voltage"
    DATA_SOURCE = "data_source"


# Pandas dtype mapping for each harmonized column
OTHER_SENSOR_HARMONIZED_COLUMN_TYPES = {
    OtherSensorHarmonizedColumn.ID: "object",
    OtherSensorHarmonizedColumn.TIMESTAMP_TRANSMIT: "datetime64[ns]",
    OtherSensorHarmonizedColumn.TIMESTAMP: "datetime64[ns]",
    OtherSensorHarmonizedColumn.TEMPERATURE: "float64",
    OtherSensorHarmonizedColumn.LIGHT_INTENSITY: "float64",
    OtherSensorHarmonizedColumn.VOLTAGE: "float64",
    OtherSensorHarmonizedColumn.DATA_SOURCE: "object",
}
