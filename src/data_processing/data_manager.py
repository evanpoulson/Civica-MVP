"""
Data Manager for Civica Calgary Zoning Simulation

This module handles data acquisition and caching for the simulation.
It provides methods to fetch and cache geospatial data from Calgary's Open Data Portal,
ensuring data is in the correct coordinate system and efficiently cached.
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path
import datetime
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataManager:
    """Manages data acquisition and caching for Calgary's geospatial data."""
    
    # API endpoints for Calgary's data
    LAND_USE_DISTRICTS_URL = "https://data.calgary.ca/resource/qe6k-p9nh.geojson?$limit=100000"
    PARCEL_BOUNDARIES_URL = "https://data.calgary.ca/resource/4bsw-nn7w.geojson"
    
    # Cache configuration
    CACHE_DIR = Path("data/raw")
    CACHED_DISTRICTS_FILE = CACHE_DIR / "land_use_districts.geojson"
    CACHED_PARCELS_FILE = CACHE_DIR / "parcel_boundaries.geojson"
    CACHE_EXPIRY_DAYS = 7
    
    # Coordinate reference system (UTM Zone 12N - Calgary's standard)
    TARGET_CRS = "EPSG:3400"
    SOURCE_CRS = "EPSG:4326"  # WGS84

    @classmethod
    def _is_cache_valid(cls, cache_path: Path) -> bool:
        """
        Check if cached data exists and is not expired.
        
        Args:
            cache_path: Path to the cached file
            
        Returns:
            bool: True if cache is valid, False otherwise
        """
        if not cache_path.exists():
            return False
            
        file_age = datetime.datetime.now() - datetime.datetime.fromtimestamp(
            cache_path.stat().st_mtime
        )
        return file_age.days < cls.CACHE_EXPIRY_DAYS

    @classmethod
    def _load_from_cache(cls, cache_path: Path) -> Optional[gpd.GeoDataFrame]:
        """
        Load data from cache if it exists and is valid.
        
        Args:
            cache_path: Path to the cached file
            
        Returns:
            Optional[gpd.GeoDataFrame]: Cached data if valid, None otherwise
        """
        if not cls._is_cache_valid(cache_path):
            return None
            
        try:
            gdf = gpd.read_file(str(cache_path))
            if gdf.crs != cls.TARGET_CRS:
                gdf = gdf.to_crs(cls.TARGET_CRS)
            return gdf
        except Exception as e:
            logger.warning(f"Error loading cache from {cache_path}: {e}")
            return None

    @classmethod
    def _save_to_cache(cls, gdf: gpd.GeoDataFrame, cache_path: Path) -> None:
        """
        Save GeoDataFrame to cache.
        
        Args:
            gdf: GeoDataFrame to cache
            cache_path: Path where to save the cache
        """
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            gdf.to_file(str(cache_path), driver="GeoJSON")
            logger.info(f"Data cached to {cache_path}")
        except Exception as e:
            logger.error(f"Error saving cache to {cache_path}: {e}")

    @classmethod
    def _ensure_crs(cls, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Ensure GeoDataFrame is in the target coordinate system.
        
        Args:
            gdf: Input GeoDataFrame
            
        Returns:
            gpd.GeoDataFrame: GeoDataFrame in target CRS
        """
        if gdf.crs != cls.TARGET_CRS:
            gdf.set_crs(epsg=4326, inplace=True)
            gdf = gdf.to_crs(cls.TARGET_CRS)
        return gdf

    @classmethod
    def get_districts(cls) -> gpd.GeoDataFrame:
        """
        Fetch land use districts data, using cached data if available and not stale.
        
        Returns:
            gpd.GeoDataFrame: Land use districts data in the target CRS
        """
        # Try to load from cache first
        cached_data = cls._load_from_cache(cls.CACHED_DISTRICTS_FILE)
        if cached_data is not None:
            return cached_data

        # Fetch fresh data if cache miss
        try:
            gdf = gpd.read_file(cls.LAND_USE_DISTRICTS_URL)
            gdf = cls._ensure_crs(gdf)
            cls._save_to_cache(gdf, cls.CACHED_DISTRICTS_FILE)
            return gdf
        except Exception as e:
            logger.error(f"Error fetching land use districts: {e}")
            raise

    @classmethod
    def get_parcel_boundaries(cls) -> gpd.GeoDataFrame:
        """
        Fetch parcel boundary data, using cached data if available and not stale.
        Handles API pagination to ensure all records are fetched.
        
        Returns:
            gpd.GeoDataFrame: Parcel boundary data in the target CRS
        """
        # Try to load from cache first
        cached_data = cls._load_from_cache(cls.CACHED_PARCELS_FILE)
        if cached_data is not None:
            return cached_data

        # Fetch fresh data if cache miss
        try:
            limit = 100000  # Socrata's maximum limit per request
            offset = 0
            all_data = []
            
            while True:
                url = f"{cls.PARCEL_BOUNDARIES_URL}?$limit={limit}&$offset={offset}"
                logger.info(f"Fetching records {offset:,} to {offset + limit:,}...")
                
                chunk = gpd.read_file(url)
                if chunk.empty:
                    break
                    
                all_data.append(chunk)
                offset += limit
                
                if len(chunk) < limit:  # Last chunk
                    break
            
            if not all_data:
                raise ValueError("No parcel data fetched from API")
            
            # Combine all chunks and ensure correct CRS
            gdf = gpd.GeoDataFrame(pd.concat(all_data, ignore_index=True))
            gdf = cls._ensure_crs(gdf)
            
            # Cache the complete dataset
            cls._save_to_cache(gdf, cls.CACHED_PARCELS_FILE)
            return gdf
            
        except Exception as e:
            logger.error(f"Error fetching parcel boundaries: {e}")
            raise
    