"""
Configuration settings for the data processing pipeline.
"""

from typing import List, Dict

# Required columns for each dataset
REQUIRED_ZONING_COLUMNS: List[str] = ['lu_bylaw', 'lu_code', 'dc_bylaw', 'dc_site_no']
REQUIRED_PARCEL_COLUMNS: List[str] = ['roll_numbe', 'address', 'land_use_d', 'property_t']

# File paths and names
ZONING_SHP_NAME: str = "LandUseDistricts.shp"
PARCELS_SHP_NAME: str = "PropertyAssessments.shp"
REQUIRED_SHAPEFILE_EXTENSIONS: List[str] = ['.shx', '.dbf', '.prj']

# Coordinate reference system
TARGET_CRS: str = "EPSG:32612"  # UTM Zone 12N

# Column prefixes for filtering
ZONING_COLUMN_PREFIXES: List[str] = ['ZONE_', 'LAND_USE_', 'lu_', 'dc_'] 