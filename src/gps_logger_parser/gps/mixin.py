"""
GPS-specific harmonization mixin for parsers.

This module provides the GPSHarmonizationMixin class that ensures all GPS
harmonized columns exist with correct types according to the
GPS_HARMONIZED_COLUMN_TYPES specification.
"""

import numpy as np
import pandas as pd
import pyarrow as pa

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

            # Map PyArrow type to pandas dtype
            pa_type = GPS_HARMONIZED_COLUMN_TYPES[harmonized_col]
            if pa_type == pa.string():
                pd_dtype = "object"
            elif pa_type == pa.float64():
                pd_dtype = "float64"
            elif pa_type == pa.int64():
                pd_dtype = "Int64"  # Nullable integer
            else:
                pd_dtype = "object"

            if col_name not in data.columns:
                # Add column with null values and appropriate dtype
                data[col_name] = np.nan if pd_dtype == "float64" else None

            # Ensure column has the correct dtype
            col_exists = col_name in data.columns
            if col_exists and data[col_name].dtype != pd_dtype:
                # Convert to correct dtype, handling potential type
                # conversion issues
                if pd_dtype == "object":
                    data[col_name] = data[col_name].astype(str)
                elif pd_dtype == "float64":
                    data[col_name] = pd.to_numeric(data[col_name], errors="coerce")
                elif pd_dtype == "Int64":
                    data[col_name] = pd.to_numeric(
                        data[col_name], errors="coerce", downcast="integer"
                    ).astype("Int64")

        return data

    def get_harmonization_schema(self):
        """
        Return PyArrow schema with GPS harmonized columns
        """
        # Build schema with harmonized columns
        schema_fields = []
        for harmonized_col in GPSHarmonizedColumn:
            col_name = harmonized_col.value
            col_type = GPS_HARMONIZED_COLUMN_TYPES[harmonized_col]
            schema_fields.append(pa.field(col_name, col_type))

        return pa.schema(schema_fields)
