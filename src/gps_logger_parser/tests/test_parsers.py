import os
import pathlib

import pyarrow.compute as pc
import pytest

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

testdata_success = []
for directory in (TESTS_DATA_PATH / "success").iterdir():
    if directory.is_dir() and directory.name not in IGNORED_DIRS:
        for f in directory.iterdir():
            if not f.is_dir() and f.name not in IGNORED_FILES:
                testdata_success.append((f.name, f, directory.name))

testdata_fail = [
    (f.name, f) for f in (TESTS_DATA_PATH / "fail").iterdir() if not f.is_dir()
]


@pytest.mark.parametrize("file,path,format", testdata_success)
def test_parser_success(file, path, file_format):
    parser_instance = detect_file(path)
    assert parser_instance.DATATYPE == file_format
    table = parser_instance.as_table()
    assert table
    assert pc.isin(table.column("_datatype"), [file_format]).all().as_py()
    assert (
        pc.isin(table.column("_parser"), [parser_instance.__class__.__name__])
        .all()
        .as_py()
    )


@pytest.mark.parametrize("file,path", testdata_fail)
def test_parser_fail(file, path):
    with pytest.raises(NotImplementedError):
        detect_file(path)
