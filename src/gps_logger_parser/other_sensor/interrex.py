from ..parser_base import CSVParser
from .columns import OtherSensorHarmonizedColumn
from .mixin import OtherSensorHarmonizationMixin


class InterrexEnvironmentParser(OtherSensorHarmonizationMixin, CSVParser):
    """
    Parser for Interrex Environment Data Logger
    """

    DATATYPE = "other_sensor"
    FIELDS = [
        "UUID",
        "Transmitting time",
        "Collecting time",
        "Temperature",
        "Light intensity",
        "Voltage",
        "Data Source",
    ]

    MAPPINGS = {
        OtherSensorHarmonizedColumn.ID: "UUID",
        OtherSensorHarmonizedColumn.TIMESTAMP_TRANSMIT: "Transmitting time",
        OtherSensorHarmonizedColumn.TIMESTAMP: "Collecting time",
        OtherSensorHarmonizedColumn.TEMPERATURE: "Temperature",
        OtherSensorHarmonizedColumn.LIGHT_INTENSITY: "Light intensity",
        OtherSensorHarmonizedColumn.VOLTAGE: "Voltage",
        OtherSensorHarmonizedColumn.DATA_SOURCE: "Data Source",
    }


PARSERS = [
    InterrexEnvironmentParser,
]
