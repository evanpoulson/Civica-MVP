"""
Spatial validation and relationship checking for the data processing pipeline.
"""

from typing import Tuple, List
import geopandas as gpd

class SpatialRelationshipValidator:
    """Validates spatial relationships between datasets."""
    
    def validate(self, zoning_gdf: gpd.GeoDataFrame, parcels_gdf: gpd.GeoDataFrame) -> Tuple[bool, List[str]]:
        """
        Validate spatial relationships between zoning and parcels.
        Returns (is_valid, list_of_issues)
        """
        issues = []
        
        # Check for overlapping zoning districts
        zoning_overlaps = zoning_gdf.overlay(zoning_gdf, how='intersection')
        if len(zoning_overlaps) > len(zoning_gdf):
            issues.append("Found overlapping zoning districts")
        
        # Check for parcels not in any zoning district
        parcels_in_zoning = gpd.sjoin(parcels_gdf, zoning_gdf, how='left', predicate='within')
        unzoned_parcels = parcels_in_zoning['index_right'].isnull().sum()
        if unzoned_parcels > 0:
            issues.append(f"Found {unzoned_parcels} parcels not in any zoning district")
        
        return len(issues) == 0, issues 