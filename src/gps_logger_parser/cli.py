import logging

import typer
from upath import UPath

from .logger import configure_logger
from .parser import detect_file

app = typer.Typer(
    help="A CLI tool to parse GPS logger files and output them in a standardized format"
)

_output_option = typer.Option(
    ".", "--output", "-o", help="Directory to save the parsed output (as parquet)"
)
_verbose_option = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
_file_argument = typer.Argument(..., help="Path to the GPS logger file to parse")


@app.command()
def parse(
    file: str = _file_argument,
    output: str = _output_option,
    verbose: bool = _verbose_option,
    s3_endpoint: str = typer.Option(
        None, "--s3-endpoint", help="Custom S3 endpoint URL"
    ),
):
    params = {}

    logging_level = logging.DEBUG if verbose else logging.INFO
    logger = configure_logger(logging_level=logging_level)

    if file.startswith("s3://"):
        if s3_endpoint:
            params["endpoint_url"] = s3_endpoint
        params["anon"] = True

    parser_instance = detect_file(UPath(file, **params), logger=logger)
    parser_instance.write_parquet(UPath(output))


if __name__ == "__main__":
    app()
