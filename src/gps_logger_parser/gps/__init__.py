from .axytrek import PARSERS as AXYTREK
from .base import PARSERS as BASE
from .catlog import PARSERS as CATLOG
from .ecotone import PARSERS as ECOTONE
from .gpx import GPXParser
from .ho11 import PARSERS as HO11
from .igotu import PARSERS as IGOUT
from .interrex import PARSERS as INTERREX
from .jm import PARSERS as JM
from .mataki import PARSERS as MATAKI
from .ornitela import PARSERS as ORNITELA
from .pathtrack import PARSERS as PATHTRACK
from .unknown import PARSERS as UNKNOWN

PARSERS = (
    [
        GPXParser,
    ]
    + IGOUT
    + CATLOG
    + BASE
    + JM
    + UNKNOWN
    + PATHTRACK
    + HO11
    + AXYTREK
    + INTERREX
    + ORNITELA
    + MATAKI
    + ECOTONE
)
