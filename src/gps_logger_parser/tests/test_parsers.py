import json
import os
import pathlib

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from upath import UPath

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
    "gps_ecotone",
    "other",
    "other_sensor",
    "gps_axytrek",
    "gps_interrex",
    "gps_ornitela",
    "gps_mataki",
]

# Define expected columns that should be present in all harmonized outputs
REQUIRED_METADATA_COLUMNS = {
    "_original_data": pa.json_(pa.large_utf8()),
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

testdata_s3 = []

S3_TEST_PATHS = TESTS_DATA_PATH / "s3.csv"
if S3_TEST_PATHS.exists():
    with S3_TEST_PATHS.open("r") as f:
        import csv

        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            testdata_s3.append((row["path"], row["endpoint_url"]))


@pytest.mark.timeout(10)
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


@pytest.mark.timeout(10)
@pytest.mark.parametrize("file,path,file_format", testdata_success)
def test_original_data_preserved(file, path, file_format):
    """
    Test that raw source data is preserved in the _original_data JSON column
    for all parser types.
    """
    parser_instance = detect_file(path)
    table = parser_instance.as_table()

    assert "_original_data" in table.column_names, (
        f"Parser {file} is missing the '_original_data' column"
    )

    # Verify the column type is json(large_utf8)
    actual_type = table.schema.field("_original_data").type
    assert actual_type == pa.json_(pa.large_utf8()), (
        f"Column '_original_data' has type {actual_type}, "
        f"expected {pa.json_(pa.large_utf8())} in {file}"
    )

    # Verify values are valid JSON objects with at least one key
    first_value = table.column("_original_data")[0].as_py()
    parsed = json.loads(first_value)
    assert isinstance(parsed, dict), (
        f"_original_data value is not a JSON object in {file}"
    )
    assert len(parsed) > 0, f"_original_data JSON object has no keys in {file}"


@pytest.mark.parametrize("path,endpoint_url", testdata_s3)
def test_parser_remote(path, endpoint_url):
    parser_instance = detect_file(UPath(path, anon=True, endpoint_url=endpoint_url))
    table = parser_instance.as_table()
    assert table


@pytest.mark.timeout(10)
@pytest.mark.parametrize("file,path", testdata_fail)
def test_parser_fail(file, path):
    with pytest.raises(NotImplementedError):
        detect_file(path)


@pytest.mark.timeout(10)
@pytest.mark.parametrize("file,path,file_format", testdata_success)
def test_harmonized_output_schema(file, path, file_format):
    """
    Test that every success test input outputs a harmonized file with:
    1. Required metadata columns (_datatype, _parser, _logger_file) with correct types
    2. For GPS parsers: All harmonized columns present
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

    schema = parser_instance.get_harmonization_schema()
    assert set(list(schema.keys()) + list(REQUIRED_METADATA_COLUMNS.keys())) == set(
        column_names
    )
