from ..parser_base import CSVParser
from .mixin import GPSHarmonizationMixin


class EcotoneParser(GPSHarmonizationMixin, CSVParser):
    DATATYPE = "gps_ecotone"
    FIELDS = [x for x in range(0, 9)]
    SEPARATOR = ";"
    HEADER = 0

    def _check_headers(self, header):
        if len(header) != len(self.FIELDS):
            self._raise_not_supported(
                f"Stream have a header length different than expected, "
                f"{len(header)} != {len(self.FIELDS)}"
            )


PARSERS = [
    EcotoneParser,
]
