"""
Main data processor class for the Civica project.
Coordinates the data processing workflow.
"""

import logging
from pathlib import Path
from typing import Tuple
import time
import os
import multiprocessing
from functools import partial

import geopandas as gpd

from .validators import GeometryValidator, ColumnValidator, ZoningCodeValidator
from .spatial import SpatialRelationshipValidator
from .reporting import DataQualityReporter
from .utils import FileManager

logger = logging.getLogger(__name__)

class CalgaryDataProcessor:
    def __init__(self, data_dir: str = "data"):
        """Initialize the data processor with a data directory."""
        self.file_manager = FileManager(data_dir)
        self.geometry_validator = GeometryValidator()
        self.column_validator = ColumnValidator()
        self.zoning_validator = ZoningCodeValidator()
        self.spatial_validator = SpatialRelationshipValidator()
        self.reporter = DataQualityReporter(self.file_manager.reports_dir)
        self.n_cores = max(1, multiprocessing.cpu_count() - 1)
    
    def _read_geodata(self, path, usecols=None):
        """Read geospatial data efficiently, using GeoParquet if available and up-to-date."""
        parquet_path = Path(str(path).replace('.shp', '.parquet'))
        shp_mtime = os.path.getmtime(path)
        if parquet_path.exists() and os.path.getmtime(parquet_path) > shp_mtime:
            logger.info(f"Reading {parquet_path} (cached Parquet)")
            gdf = gpd.read_parquet(parquet_path, columns=usecols)
        else:
            logger.info(f"Reading {path} (shapefile)")
            gdf = gpd.read_file(path)
            if usecols:
                gdf = gdf[usecols + ['geometry']]
            logger.info(f"Saving Parquet cache to {parquet_path}")
            gdf.to_parquet(parquet_path)
        return gdf

    def process_data(self) -> gpd.GeoDataFrame:
        """Process the data to create a harmonized dataset."""
        start_total = time.time()
        # Validate input files
        zoning_shp, parcels_shp = self.file_manager.validate_input_files()
        
        # Read the shapefiles (with timing)
        t0 = time.time()
        # Read only necessary columns for efficiency
        zoning_gdf = self._read_geodata(zoning_shp, usecols=self.column_validator.required_zoning_columns)
        parcels_gdf = self._read_geodata(parcels_shp, usecols=self.column_validator.required_parcel_columns)
        logger.info(f"File reading took {time.time() - t0:.2f} seconds")
        
        # Validate and fix geometries (vectorized and parallelized)
        t0 = time.time()
        zoning_gdf = self._validate_and_fix_geometries(zoning_gdf, "zoning")
        parcels_gdf = self._validate_and_fix_geometries(parcels_gdf, "parcels")
        logger.info(f"Geometry validation/fixing took {time.time() - t0:.2f} seconds")
        
        # Validate columns
        t0 = time.time()
        self._validate_columns(zoning_gdf, parcels_gdf)
        logger.info(f"Column validation took {time.time() - t0:.2f} seconds")
        
        # Validate zoning codes
        t0 = time.time()
        self._validate_zoning_codes(zoning_gdf)
        logger.info(f"Zoning code validation took {time.time() - t0:.2f} seconds")
        
        # Ensure consistent CRS
        t0 = time.time()
        zoning_gdf, parcels_gdf = self._ensure_consistent_crs(zoning_gdf, parcels_gdf)
        logger.info(f"CRS harmonization took {time.time() - t0:.2f} seconds")
        
        # Validate spatial relationships
        t0 = time.time()
        self._validate_spatial_relationships(zoning_gdf, parcels_gdf)
        logger.info(f"Spatial relationship validation took {time.time() - t0:.2f} seconds")
        
        # Perform spatial join and clean up
        t0 = time.time()
        joined_gdf = self._perform_spatial_join(zoning_gdf, parcels_gdf)
        logger.info(f"Spatial join took {time.time() - t0:.2f} seconds")
        
        # Generate data quality report
        t0 = time.time()
        self.reporter.generate_report(zoning_gdf, parcels_gdf, joined_gdf)
        logger.info(f"Data quality report generation took {time.time() - t0:.2f} seconds")
        
        # Save the processed data
        output_path = self.file_manager.processed_dir / "calgary_parcels_with_zoning.geojson"
        logger.info(f"Saving processed data to {output_path}")
        joined_gdf.to_file(output_path, driver="GeoJSON")
        
        logger.info(f"Total pipeline time: {time.time() - start_total:.2f} seconds")
        return joined_gdf
    
    def _validate_and_fix_geometries(self, gdf: gpd.GeoDataFrame, dataset_name: str) -> gpd.GeoDataFrame:
        """Validate and fix geometries in a dataset (vectorized and parallelized)."""
        logger.info(f"Validating {dataset_name} geometries...")
        # Vectorized check for validity
        invalid_mask = ~gdf.geometry.is_valid
        if invalid_mask.any():
            logger.warning(f"{dataset_name.title()} geometry issues found: {invalid_mask.sum()} invalid geometries")
            # Parallelize geometry fixing for large datasets
            if len(gdf) > 10000:
                with multiprocessing.Pool(self.n_cores) as pool:
                    fixed_geoms = pool.map(lambda x: x.buffer(0) if not x.is_valid else x, gdf.loc[invalid_mask, 'geometry'])
                gdf.loc[invalid_mask, 'geometry'] = fixed_geoms
            else:
                gdf.loc[invalid_mask, 'geometry'] = gdf.loc[invalid_mask, 'geometry'].apply(lambda x: x.buffer(0) if not x.is_valid else x)
            # Remove empty geometries
            gdf = gdf[~gdf.geometry.is_empty]
        return gdf
    
    def _validate_columns(self, zoning_gdf: gpd.GeoDataFrame, parcels_gdf: gpd.GeoDataFrame) -> None:
        """Validate required columns in both datasets."""
        logger.info("Validating columns...")
        
        # Validate zoning columns
        zoning_valid, zoning_issues = self.column_validator.validate(
            zoning_gdf, 
            self.column_validator.required_zoning_columns
        )
        if not zoning_valid:
            logger.error("Zoning column issues found:")
            for issue in zoning_issues:
                logger.error(f"- {issue}")
            raise ValueError("Missing required zoning columns")
        
        # Validate parcel columns
        parcels_valid, parcels_issues = self.column_validator.validate(
            parcels_gdf, 
            self.column_validator.required_parcel_columns
        )
        if not parcels_valid:
            logger.error("Parcel column issues found:")
            for issue in parcels_issues:
                logger.error(f"- {issue}")
            raise ValueError("Missing required parcel columns")
    
    def _validate_zoning_codes(self, zoning_gdf: gpd.GeoDataFrame) -> None:
        """Validate zoning codes in the zoning dataset."""
        logger.info("Validating zoning codes...")
        is_valid, issues = self.zoning_validator.validate(zoning_gdf)
        if not is_valid:
            logger.warning("Zoning code issues found:")
            for issue in issues:
                logger.warning(f"- {issue}")
    
    def _ensure_consistent_crs(self, zoning_gdf: gpd.GeoDataFrame, parcels_gdf: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Ensure both datasets are in the same CRS."""
        target_crs = "EPSG:32612"
        
        if zoning_gdf.crs != target_crs:
            logger.info(f"Reprojecting zoning data to {target_crs}")
            zoning_gdf = zoning_gdf.to_crs(target_crs)
        
        if parcels_gdf.crs != target_crs:
            logger.info(f"Reprojecting parcels data to {target_crs}")
            parcels_gdf = parcels_gdf.to_crs(target_crs)
        
        return zoning_gdf, parcels_gdf
    
    def _validate_spatial_relationships(self, zoning_gdf: gpd.GeoDataFrame, parcels_gdf: gpd.GeoDataFrame) -> None:
        """Validate spatial relationships between datasets."""
        logger.info("Validating spatial relationships...")
        is_valid, issues = self.spatial_validator.validate(zoning_gdf, parcels_gdf)
        if not is_valid:
            logger.warning("Spatial relationship issues found:")
            for issue in issues:
                logger.warning(f"- {issue}")
    
    def _perform_spatial_join(self, zoning_gdf: gpd.GeoDataFrame, parcels_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Perform spatial join and clean up the result."""
        logger.info("Performing spatial join...")
        # Ensure spatial indexes are built
        zoning_gdf.sindex
        parcels_gdf.sindex
        # Use the most restrictive predicate for efficiency
        joined_gdf = gpd.sjoin(parcels_gdf, zoning_gdf, how="left", predicate="within")
        
        # Clean up the joined dataset
        columns_to_keep = ['geometry']
        
        # Add zoning columns if they exist
        zoning_columns = [
            col for col in joined_gdf.columns 
            if any(col.startswith(prefix) for prefix in ['ZONE_', 'LAND_USE_', 'lu_', 'dc_'])
        ]
        if not zoning_columns:
            logger.warning("No zoning columns found in the joined dataset. Keeping all columns.")
            columns_to_keep.extend(joined_gdf.columns.difference(['geometry']))
        else:
            columns_to_keep.extend(zoning_columns)
            # Add essential parcel columns
            parcel_columns = ['roll_numbe', 'address', 'land_use_d', 'property_t']
            columns_to_keep.extend([col for col in parcel_columns if col in joined_gdf.columns])
        
        return joined_gdf[columns_to_keep] 