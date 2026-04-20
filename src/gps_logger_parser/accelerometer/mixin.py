"""
Accelerometer-specific harmonization mixin for parsers.

This module provides the AccelerometerHarmonizationMixin class that ensures all
accelerometer harmonized columns exist with correct types according to the
ACCELEROMETER_HARMONIZED_COLUMN_TYPES specification.
"""

from .columns import ACCELEROMETER_HARMONIZED_COLUMN_TYPES


class AccelerometerHarmonizationMixin:
    """
    Mixin to provide accelerometer-specific harmonization functionality.

    This mixin ensures that all accelerometer harmonized columns exist with
    correct types according to the ACCELEROMETER_HARMONIZED_COLUMN_TYPES
    specification.
    """

    def get_harmonization_schema(self) -> dict:
        return {
            harmonized_col.value: pd_dtype
            for harmonized_col, pd_dtype in (
                ACCELEROMETER_HARMONIZED_COLUMN_TYPES.items()
            )
        }
