"""
Other-sensor-specific harmonization mixin for parsers.

This module provides the OtherSensorHarmonizationMixin class that ensures all
other-sensor harmonized columns exist with correct types according to the
OTHER_SENSOR_HARMONIZED_COLUMN_TYPES specification.
"""

from .columns import OTHER_SENSOR_HARMONIZED_COLUMN_TYPES


class OtherSensorHarmonizationMixin:
    """
    Mixin to provide other-sensor-specific harmonization functionality.

    This mixin ensures that all other-sensor harmonized columns exist with
    correct types according to the OTHER_SENSOR_HARMONIZED_COLUMN_TYPES
    specification.
    """

    def get_harmonization_schema(self) -> dict:
        """
        Return Pandas dtype schema for other-sensor harmonized columns.
        """
        return {
            harmonized_col.value: pd_dtype
            for harmonized_col, pd_dtype in OTHER_SENSOR_HARMONIZED_COLUMN_TYPES.items()
        }
