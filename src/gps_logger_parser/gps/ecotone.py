import geoarrow.pyarrow as ga
import numpy as np
import pandas as pd

from ..parser_base import CSVParser
from .columns import GPSHarmonizedColumn
from .mixin import GPSHarmonizationMixin


class EcotoneParser(GPSHarmonizationMixin, CSVParser):
    DATATYPE = "gps_ecotone"
    FIELDS = [
        "logger_id",
        "day",
        "month",
        "year",
        "hours",
        "minutes",
        "meters_north",
        "meters_east",
        "HDOP",
    ]
    SEPARATOR = ";"
    HEADER = 0
    MAPPINGS = {
        GPSHarmonizedColumn.ID: "logger_id",
        GPSHarmonizedColumn.TIMESTAMP: None,
        GPSHarmonizedColumn.LATITUDE: None,
        GPSHarmonizedColumn.LONGITUDE: None,
        GPSHarmonizedColumn.ALTITUDE: None,
        GPSHarmonizedColumn.SPEED_KM_H: None,
        GPSHarmonizedColumn.TYPE: None,
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: None,
        GPSHarmonizedColumn.HDOP: "HDOP",
        GPSHarmonizedColumn.PDOP: None,
        GPSHarmonizedColumn.SATELLITES_COUNT: None,
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def _check_headers(self, header):
        if len(header) != len(self.FIELDS):
            self._raise_not_supported(
                f"Stream have a header length different than expected, "
                f"{len(header)} != {len(self.FIELDS)}"
            )

    def harmonize_data(self, data):
        # Call parent harmonization — applies MAPPINGS, enforces GPS schema,
        # creates geometry, and drops raw source columns
        result = super().harmonize_data(data)

        result["timestamp"] = pd.to_datetime(
            data["year"].astype(str)
            + "/"
            + data["month"].astype(str)
            + "/"
            + data["day"].astype(str)
            + " "
            + data["hours"].astype(str)
            + ":"
            + data["minutes"].astype(str),
            format="%y/%m/%d %H:%M",
            errors="raise",
        )

        if (
            "meters_north" in data.columns
            and "meters_east" in data.columns
            and not data["meters_north"].isna().all()
            and not data["meters_east"].isna().all()
        ):
            # Create point geometries from meters_north/meters_east coordinates
            # GeoArrow expects (x, y) which is (longitude, latitude)
            points = ga.make_point(
                data["meters_east"].str[:-1].astype(float).values,
                data["meters_north"].str[:-1].astype(float).values,
                crs="EPSG:32633",  # UTM zone 33N
            )
            # Convert to pandas ArrowExtensionArray
            result["geometry"] = pd.array(
                points.to_pylist(), dtype=pd.ArrowDtype(points.type)
            )
        else:
            # Create empty geometry column if lat/lon don't exist or are all null
            empty_points = ga.make_point(
                np.full(len(data), np.nan),
                np.full(len(data), np.nan),
                crs="EPSG:32633",  # UTM zone 33N
            )
            result["geometry"] = pd.array(
                empty_points.to_pylist(), dtype=pd.ArrowDtype(empty_points.type)
            )

        return result


PARSERS = [
    EcotoneParser,
]
