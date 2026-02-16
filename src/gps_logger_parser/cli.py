import pathlib

import typer

from .parser import detect_file

app = typer.Typer(
    help="A CLI tool to parse GPS logger files and output them in a standardized format"
)

_output_option = typer.Option(
    ".", "--output", "-o", help="Directory to save the parsed output (as parquet)"
)
_verbose_option = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
_file_argument = typer.Argument(
    ..., help="Path to the GPS logger file to parse"
)


@app.command()
def parse(
    file: pathlib.Path = _file_argument,
    output: pathlib.Path = _output_option,
    verbose: bool = _verbose_option,
):
    parser_instance = detect_file(file)
    parser_instance.write_parquet(output)


if __name__ == "__main__":
    app()
