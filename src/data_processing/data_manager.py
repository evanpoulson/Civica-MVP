"""
Data Manager for Civica Calgary Zoning Simulation

This module handles data acquisition and caching for the simulation.
It provides methods to fetch and cache geospatial data from Calgary's Open Data Portal,
ensuring data is in the correct coordinate system and efficiently cached.
"""

import geopandas as gpd
import requests
import io
from pathlib import Path
import datetime

class DataManager:
    # API endpoints for Calgary's data
    LAND_USE_DISTRICTS_URL = "https://data.calgary.ca/resource/qe6k-p9nh.geojson?$limit=20000"
    PARCEL_BOUNDARIES_URL = "https://data.calgary.ca/resource/4bsw-nn7w.geojson"
    
    # Paths where the cached data will be stored
    CACHED_DISTRICTS_FILE_PATH = Path("data/raw/land_use_districts.geojson")
    CACHED_PARCELS_FILE_PATH = Path("data/raw/parcel_boundaries.geojson")
    
    # Number of days before cached data is considered stale
    CACHE_EXPIRY_DAYS = 7
    
    # Target coordinate reference system (UTM Zone 12N - Calgary's standard)
    TARGET_CRS = "EPSG:3400"

    @classmethod
    def get_districts(cls):
        """
        Fetches land use districts data, using cached data if available and not stale.
        
        Returns:
            geopandas.GeoDataFrame: Land use districts data in the target CRS (EPSG:3400)
        """
        # Check if we have a cached file that's not too old
        if cls.CACHED_DISTRICTS_FILE_PATH.exists():
            file_modification_time = datetime.datetime.fromtimestamp(cls.CACHED_DISTRICTS_FILE_PATH.stat().st_mtime)
            if (datetime.datetime.now() - file_modification_time).days < cls.CACHE_EXPIRY_DAYS:
                # Load from cache and verify CRS
                geodataframe = gpd.read_file(str(cls.CACHED_DISTRICTS_FILE_PATH))
                if geodataframe.crs != cls.TARGET_CRS:
                    geodataframe = geodataframe.to_crs(cls.TARGET_CRS)
                return geodataframe

        # If no valid cache exists, fetch fresh data from the API
        response = requests.get(cls.LAND_USE_DISTRICTS_URL)
        response.raise_for_status()  # Raise exception for bad status codes
        
        # Convert API response to GeoDataFrame
        geodataframe = gpd.read_file(response.text)
        
        # Ensure data is in the correct coordinate system
        # First set the source CRS (WGS84) and then transform to target CRS
        geodataframe.set_crs(epsg=4326, inplace=True)
        geodataframe = geodataframe.to_crs(cls.TARGET_CRS)
        
        # Save to cache for future use
        cls.CACHED_DISTRICTS_FILE_PATH.parent.mkdir(exist_ok=True)
        geodataframe.to_file(str(cls.CACHED_DISTRICTS_FILE_PATH), driver="GeoJSON")
        
        return geodataframe

    @classmethod
    def get_parcel_boundaries(cls):
        """
        Fetches parcel boundary data, using cached data if available and not stale.
        
        Returns:
            geopandas.GeoDataFrame: Parcel boundary data in the target CRS (EPSG:3400)
        """
        # Check if we have a cached file that's not too old
        if cls.CACHED_PARCELS_FILE_PATH.exists():
            file_modification_time = datetime.datetime.fromtimestamp(cls.CACHED_PARCELS_FILE_PATH.stat().st_mtime)
            if (datetime.datetime.now() - file_modification_time).days < cls.CACHE_EXPIRY_DAYS:
                # Load from cache and verify CRS
                geodataframe = gpd.read_file(str(cls.CACHED_PARCELS_FILE_PATH))
                if geodataframe.crs != cls.TARGET_CRS:
                    geodataframe = geodataframe.to_crs(cls.TARGET_CRS)
                return geodataframe

        # If no valid cache exists, fetch fresh data from the API
        response = requests.get(cls.PARCEL_BOUNDARIES_URL)
        response.raise_for_status()  # Raise exception for bad status codes
        
        # Convert API response to GeoDataFrame
        geodataframe = gpd.read_file(response.text)
        
        # Ensure data is in the correct coordinate system
        # First set the source CRS (WGS84) and then transform to target CRS
        geodataframe.set_crs(epsg=4326, inplace=True)
        geodataframe = geodataframe.to_crs(cls.TARGET_CRS)
        
        # Save to cache for future use
        cls.CACHED_PARCELS_FILE_PATH.parent.mkdir(exist_ok=True)
        geodataframe.to_file(str(cls.CACHED_PARCELS_FILE_PATH), driver="GeoJSON")
        
        return geodataframe
    