"""
GPS-specific harmonization mixin for parsers.

This module provides the GPSHarmonizationMixin class that ensures all GPS
harmonized columns exist with correct types according to the
GPS_HARMONIZED_COLUMN_TYPES specification.
"""

import numpy as np
import pandas as pd

from .columns import GPS_HARMONIZED_COLUMN_TYPES, GPSHarmonizedColumn


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

        # Then ensure all GPS harmonized columns exist with correct types
        for harmonized_col in GPSHarmonizedColumn:
            col_name = harmonized_col.value
            pd_dtype = GPS_HARMONIZED_COLUMN_TYPES[harmonized_col]

            if col_name not in data.columns:
                # Add column with null values and appropriate dtype
                if pd_dtype == "float64":
                    data[col_name] = np.nan
                elif pd_dtype == "datetime64[ns]":
                    data[col_name] = pd.NaT
                else:
                    data[col_name] = None

            # Ensure column has the correct dtype
            col_exists = col_name in data.columns
            if col_exists and data[col_name].dtype != pd_dtype:
                # Convert to correct dtype, handling potential type conversion issues
                if pd_dtype == "object":
                    if data[col_name].dtype != "object":
                        data[col_name] = data[col_name].astype(str)
                elif pd_dtype == "float64":
                    data[col_name] = pd.to_numeric(data[col_name], errors="coerce")
                elif pd_dtype == "Int64":
                    # Convert to numeric first, then to nullable Int64
                    data[col_name] = pd.to_numeric(data[col_name], errors="coerce")
                    # Round to handle floating point values before conversion
                    data[col_name] = data[col_name].round().astype("Int64")
                elif pd_dtype == "datetime64[ns]":
                    data[col_name] = pd.to_datetime(data[col_name], errors="coerce")

        return data

    def get_harmonization_schema(self):
        """
        Return None - we use pandas types directly, not PyArrow schemas
        """
        return None
