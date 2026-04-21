import pathlib

import pytest
import yaml

from ..parser import detect_file

TESTS_DATA_PATH = pathlib.Path("tests")
TEST_CONFIG_PATH = TESTS_DATA_PATH / "config.yaml"

CONFIG = yaml.safe_load(TEST_CONFIG_PATH.open("r"))

test_files = [
    pytest.param(
        TESTS_DATA_PATH / "files" / filename,
        conf,
        id=filename,
    )
    for filename, conf in CONFIG.get("files", {}).items()
    if conf.get("skip", False) is not True
    and (TESTS_DATA_PATH / "files" / filename).exists()
]


@pytest.mark.parametrize("path,config", test_files)
def test_bench_detect(benchmark, path, config):
    """Benchmark detect_file() — the parser detection loop."""
    result = benchmark(detect_file, path)
    assert result.DATATYPE == config["type"]


@pytest.mark.parametrize("path,config", test_files)
def test_bench_detect_and_harmonize(benchmark, path, config):
    """Benchmark detect_file() + as_table() — full pipeline."""

    def detect_and_harmonize():
        parser_instance = detect_file(path)
        return parser_instance.as_table()

    table = benchmark(detect_and_harmonize)
    assert table
    assert "_original_data" in table.column_names
