"""
Validation classes for the data processing pipeline.
"""

from typing import Tuple, List
import geopandas as gpd
from shapely.validation import make_valid

from .config import REQUIRED_ZONING_COLUMNS, REQUIRED_PARCEL_COLUMNS

class GeometryValidator:
    """Validates and fixes geometry issues in GeoDataFrames."""
    
    def validate(self, gdf: gpd.GeoDataFrame) -> Tuple[bool, List[str]]:
        """
        Validate geometries in a GeoDataFrame.
        Returns (is_valid, list_of_issues)
        """
        issues = []
        
        # Check for null geometries
        null_geoms = gdf['geometry'].isnull().sum()
        if null_geoms > 0:
            issues.append(f"Found {null_geoms} null geometries")
        
        # Check for invalid geometries
        invalid_geoms = ~gdf['geometry'].is_valid
        if invalid_geoms.any():
            issues.append(f"Found {invalid_geoms.sum()} invalid geometries")
        
        # Check for empty geometries
        empty_geoms = gdf['geometry'].is_empty
        if empty_geoms.any():
            issues.append(f"Found {empty_geoms.sum()} empty geometries")
        
        # Check for self-intersections
        self_intersects = gdf['geometry'].apply(lambda x: not x.is_simple if hasattr(x, 'is_simple') else False)
        if self_intersects.any():
            issues.append(f"Found {self_intersects.sum()} self-intersecting geometries")
        
        return len(issues) == 0, issues
    
    def fix_issues(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Fix common geometry issues in the GeoDataFrame."""
        # Make invalid geometries valid
        gdf['geometry'] = gdf['geometry'].apply(lambda x: make_valid(x) if not x.is_valid else x)
        
        # Remove empty geometries
        gdf = gdf[~gdf['geometry'].is_empty]
        
        return gdf

class ColumnValidator:
    """Validates column presence and data quality."""
    
    def __init__(self):
        self.required_zoning_columns = REQUIRED_ZONING_COLUMNS
        self.required_parcel_columns = REQUIRED_PARCEL_COLUMNS
    
    def validate(self, gdf: gpd.GeoDataFrame, required_columns: List[str]) -> Tuple[bool, List[str]]:
        """
        Validate required columns exist and have valid data.
        Returns (is_valid, list_of_issues)
        """
        issues = []
        
        # Check for missing columns
        missing_cols = [col for col in required_columns if col not in gdf.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
        
        # Check for columns with all null values
        for col in required_columns:
            if col in gdf.columns and gdf[col].isnull().all():
                issues.append(f"Column {col} contains all null values")
        
        return len(issues) == 0, issues

class ZoningCodeValidator:
    """Validates zoning codes in the dataset."""
    
    def validate(self, gdf: gpd.GeoDataFrame) -> Tuple[bool, List[str]]:
        """
        Validate zoning codes follow expected patterns.
        Returns (is_valid, list_of_issues)
        """
        issues = []
        
        if 'lu_code' in gdf.columns:
            # Check for null zoning codes
            null_codes = gdf['lu_code'].isnull().sum()
            if null_codes > 0:
                issues.append(f"Found {null_codes} null zoning codes")
            
            # Check for empty zoning codes
            empty_codes = (gdf['lu_code'] == '').sum()
            if empty_codes > 0:
                issues.append(f"Found {empty_codes} empty zoning codes")
            
            # Check for unexpected characters in zoning codes
            invalid_chars = gdf['lu_code'].str.contains(r'[^A-Z0-9-]', na=False)
            if invalid_chars.any():
                issues.append(f"Found {invalid_chars.sum()} zoning codes with invalid characters")
        
        return len(issues) == 0, issues 