# GPS Logger Parser

A small Python library and CLI for parsing GPS logger files from multiple device formats
and exporting their data to a standardized Parquet (or CSV) format.

**Features**
- Detects and parses many GPS logger formats (see `src/gps_logger_parser/gps`)
- CLI and Python API for parsing single files or batching
- Outputs `parquet` (default) or CSV via the parser API

**Installation**
This project uses `uv`/`uvx` for environment and package management. Recommended development setup:

```bash
uv sync --dev
```

Run the CLI directly from the source (no install required):

```bash
uvx --from git+https://github.com/NINAnor/gps-logger-parser gps-logger-parser parse path/to/logger_file.txt -o ./out
```


**Command-line usage**
After installation the package exposes the `gps-logger-parser` command (entry point).

Parse a file and write a Parquet file into `./out`:

```bash
gps-logger-parser parse path/to/logger_file.txt -o ./out
```

(Alternative) run the CLI module directly without installing:

```bash
uv run gps-logger-parser parse path/to/logger_file.txt -o ./out
```

By default the parser writes a file named like the original input with a `.parquet` suffix.

**Python API**
Use the `detect_file` helper to obtain a parser instance and write output programmatically:

```python
from pathlib import Path
from gps_logger_parser.parser import detect_file

p = detect_file(Path("test_data/success/gps_gpx/example.gpx"))
p.write_parquet(Path("out"))
```

Parser instances expose `write_parquet(path, filename=None)` and helper methods to access
the parsed data as a PyArrow table via `as_table()`.

**Project layout & tests**
- Parsers are implemented under `src/gps_logger_parser/gps`, `accelerometer`, `tdr`, and `other_sensor`.
- Example test data is available in the `test_data` directory.
-- Run tests with `pytest` (via `uvx` after provisioning dev env):

```bash
uv sync --dev
uv run pytest
```

**Contributing**
Contributions and additional parsers are welcome. Please open an issue or a pull request with sample
files and expected outputs so a parser can be added or improved.

**License**
This project is licensed under the MIT License.
