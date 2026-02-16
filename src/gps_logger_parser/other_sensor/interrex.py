from ..parser_base import CSVParser


class InterrexEnvironmentParser(CSVParser):
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


PARSERS = [
    InterrexEnvironmentParser,
]
