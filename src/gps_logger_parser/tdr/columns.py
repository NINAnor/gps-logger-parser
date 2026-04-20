from enum import Enum


class TDRHarmonizedColumn(str, Enum):
    """Enum of harmonized TDR column names"""

    TIMESTAMP = "timestamp"
    PRESSURE = "pressure"
    TEMPERATURE = "temperature"
    DEPTH_M = "depth_m"


# Pandas dtype mapping for each harmonized column
TDR_HARMONIZED_COLUMN_TYPES = {
    TDRHarmonizedColumn.TIMESTAMP: "datetime64[ns]",
    TDRHarmonizedColumn.PRESSURE: "float64",
    TDRHarmonizedColumn.TEMPERATURE: "float64",
    TDRHarmonizedColumn.DEPTH_M: "float64",
}
