import pathlib

import geoarrow.pyarrow as ga
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import yaml
from tabulate import tabulate

from ..parser import detect_file

TESTS_DATA_PATH = pathlib.Path("tests")
TEST_CONFIG_PATH = TESTS_DATA_PATH / "config.yaml"

CONFIG = yaml.safe_load(TEST_CONFIG_PATH.open("r"))


# IGNORED_FILES = [
#     ".gitkeep",
# ]
# IGNORED_DIRS = [
#     "gps_gpx",
#     "gps_cattrack",
#     "gps_igotugl",
#     "gps_unknown",
#     "gps_2jm",
#     "accelerometer",
#     "gps_pathtrack",
#     "tdr",
#     "gps_ho11",
#     "gps_ecotone",
#     "other",
#     "other_sensor",
#     "gps_axytrek",
#     "gps_interrex",
#     "gps_ornitela",
#     "gps_mataki",
# ]

# Define expected columns that should be present in all harmonized outputs
REQUIRED_METADATA_COLUMNS = {
    "_original_data": pa.json_(pa.large_utf8()),
    "_datatype": pa.string(),
    "_parser": pa.string(),
    "_logger_file": pa.string(),
}

test_files = [
    (filename, TESTS_DATA_PATH / "files" / filename, conf)
    for filename, conf in CONFIG.get("files", {}).items()
    if conf.get("skip", False) is not True
    and (TESTS_DATA_PATH / "files" / filename).exists()
]


@pytest.mark.timeout(2)
@pytest.mark.parametrize("file,path,config", test_files)
def test_parsing(file, path, config):
    parser_instance = detect_file(path)
    assert parser_instance.DATATYPE == config["type"]


@pytest.mark.timeout(10)
@pytest.mark.parametrize("file,path,config", test_files)
def test_harmonizing(file, path, config):
    parser_instance = detect_file(path)
    table = parser_instance.as_table()
    assert table
    assert "_original_data" in table.column_names, (
        f"Parser {file} is missing the '_original_data' column"
    )
    if "expected_rows" in config:
        assert len(table) == config["expected_rows"], (
            f"Expected {config['expected_rows']} rows but got {len(table)} for {file}"
        )
    if "expected_valid_rows" in config:
        valid_mask = pc.invert(pc.is_null(table.column("timestamp")))
        if "geometry" in table.column_names:
            valid_mask = pc.and_(
                valid_mask, pc.invert(pc.is_null(table.column("geometry")))
            )
        valid_row_count = pc.sum(valid_mask).as_py()
        print(
            tabulate(
                table.drop("_original_data").to_pydict(),
                headers="keys",
                tablefmt="fancy_grid",
            )
        )
        assert valid_row_count == config["expected_valid_rows"], (
            f"Expected {config['expected_valid_rows']} valid rows but got "
            f"{valid_row_count} for {file}"
        )
    if "expected_empty_geom" in config:
        assert "geometry" in table.column_names, (
            f"Parser {file} is missing the 'geometry' column"
        )
        geometry = ga.as_geoarrow(table.column("geometry"))
        coordinates = tuple(ga.point_coords(geometry))
        empty_point_mask = pc.or_(
            pc.is_nan(coordinates[0]),
            pc.is_nan(coordinates[1]),
        )
        empty_row_count = pc.sum(empty_point_mask).as_py()
        assert empty_row_count == config["expected_empty_geom"], (
            f"Expected {config['expected_empty_geom']} empty rows but got "
            f"{empty_row_count} for {file}"
        )


gps_test_files = [
    (filename, TESTS_DATA_PATH / "files" / filename, conf)
    for filename, conf in CONFIG.get("files", {}).items()
    if conf.get("skip", False) is not True
    and conf.get("type", "").startswith("gps")
    and (TESTS_DATA_PATH / "files" / filename).exists()
]


@pytest.mark.timeout(10)
@pytest.mark.parametrize("file,path,config", gps_test_files)
def test_geometry_encoding_wkb(file, path, config):
    """Test that as_table() produces WKB-encoded geometry by default."""
    parser_instance = detect_file(path)
    table = parser_instance.as_table()
    assert "geometry" in table.column_names
    geometry_type = table.schema.field("geometry").type
    assert isinstance(geometry_type, ga.GeometryExtensionType)
    assert geometry_type.encoding == ga.Encoding.WKB


@pytest.mark.timeout(10)
@pytest.mark.parametrize("file,path,config", gps_test_files)
def test_geometry_encoding_geoarrow(file, path, config):
    """Test that as_table(geometry_encoding='geoarrow') preserves native encoding."""
    parser_instance = detect_file(path)
    table = parser_instance.as_table(geometry_encoding="geoarrow")
    assert "geometry" in table.column_names
    geometry_type = table.schema.field("geometry").type
    assert isinstance(geometry_type, ga.GeometryExtensionType)
    assert geometry_type.encoding == ga.Encoding.GEOARROW
