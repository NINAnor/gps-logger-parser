import gpxpy
import pandas as pd

from ..helpers import stream_chunk_contains
from ..parser_base import Parser
from .columns import GPSHarmonizedColumn


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
        GPSHarmonizedColumn.ID: None,
        GPSHarmonizedColumn.DATE: "date",
        GPSHarmonizedColumn.TIME: "time",
        GPSHarmonizedColumn.LATITUDE: "latitude",
        GPSHarmonizedColumn.LONGITUDE: "longitude",
        GPSHarmonizedColumn.ALTITUDE: "elevation",
        GPSHarmonizedColumn.SPEED_KM_H: "speed",
        GPSHarmonizedColumn.TYPE: "type",
        GPSHarmonizedColumn.DISTANCE: None,
        GPSHarmonizedColumn.COURSE: "course",
        GPSHarmonizedColumn.HDOP: "horizontal_dilution",
        GPSHarmonizedColumn.PDOP: "position_dilution",
        GPSHarmonizedColumn.SATELLITES_COUNT: "satellites",
        GPSHarmonizedColumn.TEMPERATURE: None,
        GPSHarmonizedColumn.SOLAR_I_MA: None,
        GPSHarmonizedColumn.BAT_SOC_PCT: None,
        GPSHarmonizedColumn.RING_NR: None,
        GPSHarmonizedColumn.TRIP_NR: None,
    }

    def harmonize_data(self):
        self.data["datetime"] = pd.to_datetime(self.data["time"])
        self.data["date"] = self.data["datetime"].dt.date
        self.data["time"] = self.data["datetime"].dt.time
        return super().harmonize_data()

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
