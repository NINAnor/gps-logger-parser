import datetime
import re

import pyarrow.csv as pacsv

from ..helpers import stream_starts_with
from ..parser_base import Parsable, Parser


def skip(row):
    return "error"


class AcceleratorParser(Parser):
    DATATYPE = "accelerometer"
    FIELDS = ["X", "Y", "Z"]
    HEAD = "ACCELERATION DATA"
    SEPARATOR = ","
    DATE_REGEX = r"START DATE\s+((\d{4})\/\s*(\d{1,2})\/\s*(\d{1,2}))"
    TIME_REGEX = r"START TIME\s+((\d{2}):(\d{2}):(\d{2}))"
    STRP_FORMAT = "%Y/%m/%d %H:%M:%S"
    FREQUENCY_REGEX = r"\s*(\d*)\smsec\/point"
    DELTA_ATTR = "milliseconds"
    MAX_READ = 30

    def __init__(self, parsable: Parsable):
        super().__init__(parsable)

        row_count = 0
        intro = ""

        with self.file.get_stream(binary=False) as stream:
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            if not stream_starts_with(stream, self.HEAD):
                self._raise_not_supported("Stream head different than expected")

            stream.seek(0)

            for row in stream.readlines():
                if [v.strip() for v in row.split(",")] == self.FIELDS:
                    break
                row_count += 1
                intro += row

                if row_count > self.MAX_READ:
                    self._raise_not_supported(
                        f"Expected data not found after {self.MAX_READ} lines"
                    )

        parse_options = pacsv.ParseOptions(
            delimiter=self.SEPARATOR, invalid_row_handler=skip
        )
        read_options = pacsv.ReadOptions(skip_rows=row_count + 1)
        convert_options = pacsv.ConvertOptions(
            include_columns=self.FIELDS + ["empty"],
            include_missing_columns=True,
        )
        self.data = pacsv.read_csv(
            self.file._file_path,
            parse_options=parse_options,
            read_options=read_options,
            convert_options=convert_options,
        ).to_pandas()

        frequency = self.get_frequency(intro)
        start = self.get_start_datetime(intro)
        self.data["datetime"] = [
            start + (frequency * index) for index in range(len(self.data))
        ]

    def get_start_datetime(self, intro):
        date, year, month, day = re.search(self.DATE_REGEX, intro).groups()
        time, hour, minutes, seconds = re.search(self.TIME_REGEX, intro).groups()

        return datetime.datetime.strptime(
            f'{date.replace(" ", "")} {time}', self.STRP_FORMAT
        )

    def get_frequency(self, intro):
        frequency = re.search(self.FREQUENCY_REGEX, intro).group(1)
        delta = {self.DELTA_ATTR: int(frequency)}
        return datetime.timedelta(**delta)


class AcceleratorDDMMYYParser(AcceleratorParser):
    DATE_REGEX = r"START DATE\s+((\d{1,2})\/\s*(\d{1,2})\/\s*(\d{1,2}))"
    STRP_FORMAT = "%d/%m/%y %H:%M:%S"


PARSERS = [
    AcceleratorParser,
    AcceleratorDDMMYYParser,
]
