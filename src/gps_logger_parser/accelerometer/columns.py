from enum import Enum


class AccelerometerHarmonizedColumn(str, Enum):
    """Enum of harmonized accelerometer column names"""

    TIMESTAMP = "timestamp"
    X = "x"
    Y = "y"
    Z = "z"


# Pandas dtype mapping for each harmonized column
ACCELEROMETER_HARMONIZED_COLUMN_TYPES = {
    AccelerometerHarmonizedColumn.TIMESTAMP: "datetime64[ns]",
    AccelerometerHarmonizedColumn.X: "float64",
    AccelerometerHarmonizedColumn.Y: "float64",
    AccelerometerHarmonizedColumn.Z: "float64",
}
