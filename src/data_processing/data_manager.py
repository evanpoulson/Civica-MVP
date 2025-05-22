import geopandas as gpd
import requests
import io
from pathlib import Path
import datetime

class DataManager:
    LAND_USE_DISTRICTS_URL = "https://data.calgary.ca/resource/qe6k-p9nh.geojson?$limit=20000"
    CACHED_DISTRICTS_FILE_PATH = Path("data/raw/land_use_districts.geojson")
    CACHE_EXPIRY_DAYS = 7

    @classmethod
    def get_districts(cls):
        if cls.CACHED_DISTRICTS_FILE_PATH.exists():
            file_modification_time = datetime.datetime.fromtimestamp(cls.CACHED_DISTRICTS_FILE_PATH.stat().st_mtime)
            if (datetime.datetime.now() - file_modification_time).days < cls.CACHE_EXPIRY_DAYS:
                return gpd.read_file(str(cls.CACHED_DISTRICTS_FILE_PATH))

        # otherwise fetch fresh
        response = requests.get(cls.LAND_USE_DISTRICTS_URL)
        response.raise_for_status()
        geodataframe = gpd.read_file(response.text)
        cls.CACHED_DISTRICTS_FILE_PATH.parent.mkdir(exist_ok=True)
        geodataframe.to_file(str(cls.CACHED_DISTRICTS_FILE_PATH), driver="GeoJSON")
        return geodataframe
    