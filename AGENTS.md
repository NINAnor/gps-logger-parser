# Agent Instructions for gps-logger-parser

## Critical Rules

- ALWAYS attempt to add a test case for changed behavior
- PREFER running specific tests over running the entire test suite
- ALWAYS read and copy the style of similar tests when adding new cases
- AVOID shortening variable names, e.g., use `version` instead of `ver`, and `file_path` instead of `fp`
- ALWAYS use `uv` to run commands
- ALWAYS make sure `ruff` does not return errors before finishing

---

## Project Overview

`gps_logger_parser` is a Python library and CLI for detecting and parsing GPS logger files
from various hardware vendors (Ornitela, AXYTrek, Mataki, Pathtrack, iGotU, etc.).
Each parser class detects whether a file matches its format by raising `ParserNotSupported`
if it does not, allowing `detect_file()` in `parser.py` to try them in sequence.

Source layout:
```
src/gps_logger_parser/
  parser.py          # detect_file() — tries all parsers in order
  parser_base.py     # Base classes: Parsable, Parser, CSVParser, ExcelParser
  cli.py             # Typer CLI entry point
  helpers.py         # Stream utilities: stream_starts_with, read_csv, etc.
  gps/               # GPS-specific parsers (one file per vendor/format)
  accelerometer/     # Accelerometer parsers
  tdr/               # TDR (time-depth recorder) parsers
  other_sensor/      # Other sensor parsers (e.g. Interrex environment)
  tests/
    test_parsers.py  # All pytest tests (parametrized over test_data/)
```

Test data lives in `test_data/` (git-ignored). Subdirectories under `test_data/success/`
map to `file_format` values used in the parametrized tests.

---

## Build, Lint, and Test Commands

All commands use `uv`. There is no Makefile.

```bash
# Install dependencies
uv sync

# Run the full test suite (requires test_data/ to be present)
TEST_DATA_PATH=./test_data uv run pytest

# Run a single test file
TEST_DATA_PATH=./test_data uv run pytest src/gps_logger_parser/tests/test_parsers.py

# Run a single test function by name
TEST_DATA_PATH=./test_data uv run pytest src/gps_logger_parser/tests/test_parsers.py::test_parser_success

# Run tests matching a keyword (e.g. a vendor name)
TEST_DATA_PATH=./test_data uv run pytest src/gps_logger_parser/tests/test_parsers.py -k "ornitela"

# Run a specific parametrized case by node ID
TEST_DATA_PATH=./test_data uv run pytest \
  "src/gps_logger_parser/tests/test_parsers.py::test_harmonized_output_schema[somefile.LOG-path-gps_ornitela]"

# Lint (auto-fix enabled by default via fix = true in pyproject.toml)
uv run ruff check .

# Format
uv run ruff format .

# Check for unused/missing dependencies
uv run deptry .

# Type checking
uv run ty check

# Run pre-commit hooks on all files
uv run pre-commit run --all-files

# Build and serve documentation
uv run mkdocs build
uv run mkdocs serve
```

---

## Code Style Guidelines

### Formatting

- **Ruff** is the sole formatter and linter — no Black, no isort, no flake8.
  - `ruff format` replaces Black.
  - `ruff check --select I` handles import sorting.
  - `fix = true` in `pyproject.toml` means ruff auto-fixes safe issues.
- 4-space indentation, UTF-8 encoding, LF line endings (enforced by `.editorconfig`).
- Trailing newline required; no trailing whitespace.

### Imports

- Absolute imports for the standard library and third-party packages.
- Relative imports for intra-package references (e.g., `from ..parser_base import CSVParser`,
  `from .columns import GPSHarmonizedColumn`).
- Import order (enforced by ruff/isort): stdlib → third-party → local relative.
- Group imports in that order with a blank line between groups.

### Type Annotations

- Use type hints on all public function/method signatures.
- Use the modern union syntax `X | Y` (not `Optional[X]` or `Union[X, Y]`) — the project
  targets Python ≥ 3.10.
- `str | None` is preferred over `Optional[str]`.
- `ty` (Astral's type checker) is available via `uv run ty check`, but is not gating in CI.

### Naming Conventions

| Category | Convention | Examples |
|---|---|---|
| Modules / packages | `snake_case` | `parser_base`, `gps_logger_parser` |
| Classes | `PascalCase` | `OrnitelaParser`, `GPSCatTrackParser`, `Parsable` |
| Class-level config attrs | `UPPER_SNAKE_CASE` | `DATATYPE`, `FIELDS`, `MAPPINGS`, `SEPARATOR` |
| Functions / methods | `snake_case` | `detect_file`, `harmonize_data`, `write_parquet` |
| Private methods | `_snake_case` | `_check_headers`, `_raise_not_supported` |
| Variables | `snake_case` | `file_path`, `row_count`, `parse_options` |
| Enum members | `UPPER_SNAKE_CASE` | `GPSHarmonizedColumn.LATITUDE` |
| Unused variables | leading `_` | `_soi`, `_metadata` |

Parser class names may embed brand/device names verbatim when that aids discoverability
(e.g., `IGotU_GT_Parser`, `GPS2JMParser7_5`, `AXYTREKParser`).

### Error Handling

Three established patterns — follow them consistently:

1. **`ParserNotSupported`** — raised inside `__init__` when a file does not match the
   parser's expected format. Use `self._raise_not_supported("reason")` (defined in
   `parser_base.py`). The `detect_file()` loop catches only this exception to advance
   to the next parser candidate.

2. **`ValueError`** — used for hard precondition failures at call-site boundaries, e.g.
   `Parsable.__init__` raising when the file does not exist.

3. **Guard-at-the-top pattern** — validate the stream at the very start of `__init__`
   with early raises (check seekable, check `HEAD`, check headers) before doing any
   substantial work.

Do not use bare `except Exception` outside of `detect_file()`'s top-level fallback.
Pass structured messages to `self._raise_not_supported()` rather than raising raw
`ParserNotSupported` directly.

### Parser Implementation Pattern

New parsers must follow this structure:

```python
class MyVendorParser(CSVParser):
    DATATYPE = "gps"               # "gps", "tdr", "accelerometer", etc.
    FIELDS = (...)                  # Tuple of expected column headers
    MAPPINGS = {...}                # Maps raw columns -> GPSHarmonizedColumn
    SEPARATOR = ","                 # CSV separator character
    HEAD = b"..."                   # Bytes the file must start with (for fast rejection)

    def __init__(self, parsable: Parsable) -> None:
        super().__init__(parsable)  # Calls _check_headers, may raise ParserNotSupported
```

After writing a new parser, register it in the appropriate `__init__.py`'s `PARSERS` list
(e.g., `src/gps_logger_parser/gps/__init__.py`).

### Testing

- Tests live in `src/gps_logger_parser/tests/test_parsers.py`.
- Tests are parametrized — read existing parametrize decorators before adding cases.
- The `TEST_DATA_PATH` environment variable must point to `test_data/`.
- `S101` (assert usage), `E501` (line length), and a few other rules are suppressed in
  test files — raw `assert` statements are correct and expected.
- When adding a new parser, add at least one file to `test_data/success/<format>/` and
  a corresponding parametrized entry in `test_parser_success` and
  `test_harmonized_output_schema`.
- Copy the exact style and fixture usage of adjacent test cases — do not invent new
  fixture patterns.
