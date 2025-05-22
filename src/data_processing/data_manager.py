import geopandas as gpd
import requests
import io
from pathlib import Path
import datetime

class DataManager:
    LAND_USE_DISTRICTS_URL = "https://data.calgary.ca/resource/qe6k-p9nh.geojson?$limit=20000"
    CACHED_DISTRICTS_FILE_PATH = Path("data/raw/land_use_districts.geojson")
    CACHE_EXPIRY_DAYS = 7
    TARGET_CRS = "EPSG:3400"  # Calgary's standard CRS (UTM Zone 12N)

    @classmethod
    def get_districts(cls):
        if cls.CACHED_DISTRICTS_FILE_PATH.exists():
            file_modification_time = datetime.datetime.fromtimestamp(cls.CACHED_DISTRICTS_FILE_PATH.stat().st_mtime)
            if (datetime.datetime.now() - file_modification_time).days < cls.CACHE_EXPIRY_DAYS:
                geodataframe = gpd.read_file(str(cls.CACHED_DISTRICTS_FILE_PATH))
                # Verify CRS is correct
                if geodataframe.crs != cls.TARGET_CRS:
                    geodataframe = geodataframe.to_crs(cls.TARGET_CRS)
                return geodataframe

        # otherwise fetch fresh
        response = requests.get(cls.LAND_USE_DISTRICTS_URL)
        response.raise_for_status()
        geodataframe = gpd.read_file(response.text)
        
        # Set source CRS (WGS84) and transform to target CRS
        geodataframe.set_crs(epsg=4326, inplace=True)
        geodataframe = geodataframe.to_crs(cls.TARGET_CRS)
        
        cls.CACHED_DISTRICTS_FILE_PATH.parent.mkdir(exist_ok=True)
        geodataframe.to_file(str(cls.CACHED_DISTRICTS_FILE_PATH), driver="GeoJSON")
        return geodataframe
    