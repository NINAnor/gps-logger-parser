import logging
import traceback

from upath import UPath

from .accelerometer import PARSERS as ACCELEROMETER_PARSERS
from .gps import PARSERS as GPS_PARSERS
from .other_sensor import PARSERS as OTHER_SENSOR_PARSERS
from .parser_base import Parsable, ParserNotSupported
from .tdr import PARSERS as TDR_PARSERS

available_parsers = (
    GPS_PARSERS + ACCELEROMETER_PARSERS + TDR_PARSERS + OTHER_SENSOR_PARSERS
)

logger = logging.getLogger(__name__)


def detect_file(path: UPath, *args, logger=logger, **kwargs):
    parsable = Parsable(file_path=path)

    for parser in available_parsers:
        try:
            if not parser.can_parse(parsable):
                logger.debug(f"Skipped {parser.__name__}: can_parse returned False")
                continue
            result = parser(parsable)
            logger.info(f"Parsed with {parser}")
            return result
        except ParserNotSupported:
            logger.debug("Expected: " + traceback.format_exc())
        except Exception:
            logger.error(traceback.format_exc())

    raise NotImplementedError("File not supported")
