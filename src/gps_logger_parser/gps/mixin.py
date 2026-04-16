"""
GPS-specific harmonization mixin for parsers.

This module provides the GPSHarmonizationMixin class that ensures all GPS
harmonized columns exist with correct types according to the
GPS_HARMONIZED_COLUMN_TYPES specification.
"""

import geoarrow.pyarrow as ga
import numpy as np
import pandas as pd

from .columns import GPS_HARMONIZED_COLUMN_TYPES


class GPSHarmonizationMixin:
    """
    Mixin to provide GPS-specific harmonization functionality.

    This mixin ensures that all GPS harmonized columns exist with correct types
    according to the GPS_HARMONIZED_COLUMN_TYPES specification.
    """

    def harmonize_data(self, data):
        """
        Remap values parsed and ensure all GPS harmonized columns exist
        with correct types

        Args:
            data: DataFrame to harmonize

        Returns:
            Harmonized DataFrame with all GPS columns and correct types
        """
        # First, apply standard column renaming
        data = super().harmonize_data(data)

        # Create geometry column from latitude and longitude (WGS84)
        # Note: This creates geometry from current lat/lon values
        # If parsers modify lat/lon after calling super(), they should
        # call _create_geometry_column() again
        data = self._create_geometry_column(data)
        return data

    def _create_geometry_column(self, data):
        """
        Create or update geometry column from latitude and longitude.

        This helper method can be called by subclasses after they've
        finished creating/modifying latitude and longitude columns.

        Args:
            data: DataFrame with latitude and longitude columns

        Returns:
            DataFrame with geometry column added/updated
        """
        # Only create if lat/lon columns exist and have valid (non-null) values
        if (
            "latitude" in data.columns
            and "longitude" in data.columns
            and not data["latitude"].isna().all()
            and not data["longitude"].isna().all()
        ):
            # Create point geometries from lat/lon coordinates
            # GeoArrow expects (x, y) which is (longitude, latitude)
            points = ga.make_point(
                data["longitude"].values,
                data["latitude"].values,
                crs="EPSG:4326",
            )
            # Convert to pandas ArrowExtensionArray
            data["geometry"] = pd.array(
                points.to_pylist(), dtype=pd.ArrowDtype(points.type)
            )
        else:
            # Create empty geometry column if lat/lon don't exist or are all null
            empty_points = ga.make_point(
                np.full(len(data), np.nan),
                np.full(len(data), np.nan),
                crs="EPSG:4326",
            )
            data["geometry"] = pd.array(
                empty_points.to_pylist(), dtype=pd.ArrowDtype(empty_points.type)
            )

        return data

    def get_harmonization_schema(self):
        """
        Return None - we use pandas types directly, not PyArrow schemas
        """
        return {
            harmonized_col.value: pd_dtype
            for harmonized_col, pd_dtype in GPS_HARMONIZED_COLUMN_TYPES.items()
        }
