"""
TDR-specific harmonization mixin for parsers.

This module provides the TDRHarmonizationMixin class that ensures all TDR
harmonized columns exist with correct types according to the
TDR_HARMONIZED_COLUMN_TYPES specification.
"""

from .columns import TDR_HARMONIZED_COLUMN_TYPES


class TDRHarmonizationMixin:
    """
    Mixin to provide TDR-specific harmonization functionality.

    This mixin ensures that all TDR harmonized columns exist with correct types
    according to the TDR_HARMONIZED_COLUMN_TYPES specification.
    """

    def get_harmonization_schema(self) -> dict:
        return {
            harmonized_col.value: pd_dtype
            for harmonized_col, pd_dtype in TDR_HARMONIZED_COLUMN_TYPES.items()
        }
