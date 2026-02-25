import os
import pathlib

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from ..gps.columns import GPS_HARMONIZED_COLUMN_TYPES, GPSHarmonizedColumn
from ..parser import detect_file

TESTS_DATA_PATH = pathlib.Path(os.environ.get("TEST_DATA_PATH"))
IGNORED_FILES = [
    ".gitkeep",
]
IGNORED_DIRS = [
    "gps_gpx",
    "gps_cattrack",
    "gps_igotugl",
    "gps_unknown",
    "gps_2jm",
    "accelerometer",
    "gps_pathtrack",
    "tdr",
    "gps_ho11",
    "gps_axytrek",
    "gps_ecotone",
]

# Define expected metadata columns that should be present in all harmonized outputs
REQUIRED_METADATA_COLUMNS = {
    "_datatype": pa.string(),
    "_parser": pa.string(),
    "_logger_file": pa.string(),
}


testdata_success = []
for directory in (TESTS_DATA_PATH / "success").iterdir():
    if directory.is_dir() and directory.name not in IGNORED_DIRS:
        for f in directory.iterdir():
            if not f.is_dir() and f.name not in IGNORED_FILES:
                testdata_success.append((f.name, f, directory.name))

testdata_fail = [
    (f.name, f) for f in (TESTS_DATA_PATH / "fail").iterdir() if not f.is_dir()
]


@pytest.mark.parametrize("file,path,file_format", testdata_success)
def test_parser_success(file, path, file_format):
    parser_instance = detect_file(path)
    assert parser_instance.DATATYPE == file_format
    table = parser_instance.as_table()
    assert table
    assert pc.all(pc.is_in(table.column("_datatype"), pa.array([file_format]))).as_py()
    assert pc.all(
        pc.is_in(
            table.column("_parser"), pa.array([parser_instance.__class__.__name__])
        )
    ).as_py()


@pytest.mark.parametrize("file,path", testdata_fail)
def test_parser_fail(file, path):
    with pytest.raises(NotImplementedError):
        detect_file(path)


@pytest.mark.parametrize("file,path,file_format", testdata_success)
def test_harmonized_output_schema(file, path, file_format):
    """
    Test that every success test input outputs a harmonized file with:
    1. Required metadata columns (_datatype, _parser, _logger_file) with correct types
    2. For GPS parsers: All harmonized columns present with correct PyArrow types
    """
    parser_instance = detect_file(path)
    table = parser_instance.as_table()

    # Check that all required metadata columns are present with correct types
    column_names = table.column_names
    for col_name, expected_type in REQUIRED_METADATA_COLUMNS.items():
        assert col_name in column_names, (
            f"Required column '{col_name}' not found in {file}"
        )
        actual_type = table.schema.field(col_name).type
        assert actual_type == expected_type, (
            f"Column '{col_name}' has type {actual_type}, expected {expected_type} in {file}"
        )

    # For GPS parsers, check that all harmonized columns exist with correct types
    if file_format.startswith("gps_"):
        # Check that all harmonized columns are present
        for harmonized_col in GPSHarmonizedColumn:
            col_name = harmonized_col.value
            assert col_name in column_names, (
                f"GPS parser {file} is missing harmonized column '{col_name}'"
            )

            # Check that each harmonized column has the correct PyArrow type
            actual_type = table.schema.field(col_name).type
            expected_type = GPS_HARMONIZED_COLUMN_TYPES[harmonized_col]

            # Allow nullable versions of types (e.g., int64 vs int64 with nulls)
            # PyArrow types should match exactly for our purposes
            assert actual_type == expected_type, (
                f"GPS parser {file}: column '{col_name}' has type {actual_type}, expected {expected_type}"
            )
