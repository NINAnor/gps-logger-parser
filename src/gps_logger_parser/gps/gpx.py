import gpxpy
import pandas as pd

from ..helpers import stream_chunk_contains
from ..parser_base import Parser


class GPXParser(Parser):
    DATATYPE = "gps_gpx"
    FIELDS = [
        "latitude",
        "longitude",
        "elevation",
        "time",
        "satellites",
        "horizontal_dilution",  # hdop
        "course",
        "speed",
        "type",
        "position_dilution",  # pdop
    ]

    MAPPINGS = {
        "id": None,
        "date": "date",
        "time": "time",
        "latitude": "latitude",
        "longitude": "longitude",
        "altitude": "elevation",
        "speed_km_h": "speed",
        "type": "type",
        "distance": None,
        "course": "course",
        "hdop": "horizontal_dilution",
        "pdop": "position_dilution",
        "satellites_count": "satellites",
        "temperature": None,
        "solar_I_mA": None,
        "bat_soc_pct": None,
        "ring_nr": None,
        "trip_nr": None,
    }

    def normalize_data(self):
        self.data["datetime"] = pd.to_datetime(self.data["time"])
        self.data["date"] = self.data["datetime"].dt.date
        self.data["time"] = self.data["datetime"].dt.time
        return super().normalize_data()

    def __init__(self, stream):
        super().__init__(stream)

        with self.file.get_stream(binary=False) as stream:
            if not stream.seekable():
                self._raise_not_supported("Stream not seekable")

            if not stream_chunk_contains(stream, 30, "<?xml"):
                self._raise_not_supported("Stream does not start with <?xml")

            gpx = gpxpy.parse(stream)
            points = []
            for track in gpx.tracks:
                for segment in track.segments:
                    for point in segment.points:
                        points.append(getattr(point, f) for f in self.FIELDS)

            self.data = pd.DataFrame(points, columns=self.FIELDS)
