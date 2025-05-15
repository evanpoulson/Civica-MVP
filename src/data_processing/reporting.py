"""
Data quality reporting functionality for the data processing pipeline.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict

import geopandas as gpd

class DataQualityReporter:
    """Generates and saves data quality reports."""
    
    def __init__(self, reports_dir: Path):
        """Initialize the reporter with a reports directory."""
        self.reports_dir = reports_dir
        self.reports_dir.mkdir(exist_ok=True)
    
    def generate_report(self, zoning_gdf: gpd.GeoDataFrame, parcels_gdf: gpd.GeoDataFrame, joined_gdf: gpd.GeoDataFrame) -> Dict:
        """
        Generate a comprehensive data quality report for the processed data.
        Returns a dictionary containing the report data.
        """
        report = {
            "timestamp": datetime.now().isoformat(),
            "input_data": {
                "zoning": self._generate_dataset_report(zoning_gdf, "zoning"),
                "parcels": self._generate_dataset_report(parcels_gdf, "parcels")
            },
            "output_data": self._generate_dataset_report(joined_gdf, "joined"),
            "spatial_metrics": self._generate_spatial_metrics(zoning_gdf, parcels_gdf, joined_gdf),
            "data_completeness": self._generate_completeness_metrics(zoning_gdf, parcels_gdf, joined_gdf)
        }
        
        # Save the report
        report_path = self.reports_dir / f"data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        return report
    
    def _generate_dataset_report(self, gdf: gpd.GeoDataFrame, dataset_name: str) -> Dict:
        """Generate a report for a single dataset."""
        return {
            "feature_count": len(gdf),
            "crs": str(gdf.crs),
            "columns": {
                col: {
                    "null_count": int(gdf[col].isnull().sum()),
                    "null_percentage": float(gdf[col].isnull().mean() * 100),
                    "unique_values": int(gdf[col].nunique()) if gdf[col].dtype != 'geometry' else None,
                    "data_type": str(gdf[col].dtype)
                }
                for col in gdf.columns
            },
            "geometry_types": gdf.geometry.geom_type.value_counts().to_dict(),
            "total_area": float(gdf.geometry.area.sum()) if not gdf.empty else 0.0
        }
    
    def _generate_spatial_metrics(self, zoning_gdf: gpd.GeoDataFrame, parcels_gdf: gpd.GeoDataFrame, joined_gdf: gpd.GeoDataFrame) -> Dict:
        """Generate spatial metrics for the datasets."""
        return {
            "zoning_coverage": {
                "total_area": float(zoning_gdf.geometry.area.sum()),
                "number_of_districts": len(zoning_gdf),
                "average_district_size": float(zoning_gdf.geometry.area.mean())
            },
            "parcel_coverage": {
                "total_area": float(parcels_gdf.geometry.area.sum()),
                "number_of_parcels": len(parcels_gdf),
                "average_parcel_size": float(parcels_gdf.geometry.area.mean())
            },
            "join_metrics": {
                "parcels_with_zoning": len(joined_gdf[joined_gdf.index_right.notnull()]),
                "parcels_without_zoning": len(joined_gdf[joined_gdf.index_right.isnull()]),
                "coverage_percentage": float(len(joined_gdf[joined_gdf.index_right.notnull()]) / len(parcels_gdf) * 100)
            }
        }
    
    def _generate_completeness_metrics(self, zoning_gdf: gpd.GeoDataFrame, parcels_gdf: gpd.GeoDataFrame, joined_gdf: gpd.GeoDataFrame) -> Dict:
        """Generate data completeness metrics."""
        return {
            "zoning": {
                "required_columns": {
                    col: {
                        "completeness": float(1 - zoning_gdf[col].isnull().mean()) * 100
                    }
                    for col in ['lu_bylaw', 'lu_code', 'dc_bylaw', 'dc_site_no']
                }
            },
            "parcels": {
                "required_columns": {
                    col: {
                        "completeness": float(1 - parcels_gdf[col].isnull().mean()) * 100
                    }
                    for col in ['roll_numbe', 'address', 'land_use_d', 'property_t']
                }
            },
            "joined_data": {
                "spatial_join_completeness": float(len(joined_gdf[joined_gdf.index_right.notnull()]) / len(parcels_gdf) * 100),
                "missing_zoning_info": float(len(joined_gdf[joined_gdf.index_right.isnull()]) / len(parcels_gdf) * 100)
            }
        } 