"""
ETL Pipeline for Civica Calgary Zoning Simulation

This module handles the Extract, Transform, and Load (ETL) operations for processing
Calgary's zoning and parcel data. It includes functions for:
- Extracting raw geospatial data
- Transforming and harmonizing data formats
- Loading processed data into the appropriate format for simulation
""" 

import pandas as pd
import geopandas as gpd
import requests
from pathlib import Path
import matplotlib.pyplot as plt
from .data_manager import DataManager
import logging

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler if no handlers exist
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

def filter_land_use_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Filter the land use districts dataset to keep only essential columns.
    
    Args:
        gdf: Input GeoDataFrame containing land use districts
        
    Returns:
        gpd.GeoDataFrame: Filtered GeoDataFrame with only essential columns
    """
    essential_columns = ['lu_code', 'description', 'lu_bylaw', 'geometry']
    return gdf[essential_columns]

def get_land_use_data():
    """
    Extract land use districts data from Calgary's Open Data Portal.
    Returns a GeoPandas DataFrame containing the land use districts.
    """
    # Get the data using DataManager
    districts = DataManager.get_districts()
    # Filter to keep only essential columns
    return filter_land_use_columns(districts)

def filter_parcel_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Filter the parcel boundaries dataset to keep only essential columns.
    
    Args:
        gdf: Input GeoDataFrame containing parcel boundaries
        
    Returns:
        gpd.GeoDataFrame: Filtered GeoDataFrame with only essential columns
    """
    essential_columns = ['land_size_sm', 'property_type', 'unique_key', 'comm_name', 
                        'sub_property_use', 'address', 'geometry']
    return gdf[essential_columns]

def get_parcel_data():
    """
    Extract parcel boundary data from Calgary's Open Data Portal.
    Returns a GeoPandas DataFrame containing the parcel boundaries.
    """
    # Get the data using DataManager
    parcels = DataManager.get_parcel_boundaries()
    # Filter to keep only essential columns
    return filter_parcel_columns(parcels)

def save_processed_data(gdf: gpd.GeoDataFrame, filename: str):
    """
    Save processed GeoDataFrame to the processed data directory.
    
    Args:
        gdf: GeoDataFrame to save
        filename: Name of the file to save (without extension)
    """
    # Create processed data directory if it doesn't exist
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Save the data
    output_path = processed_dir / f"{filename}.geojson"
    gdf.to_file(output_path, driver='GeoJSON')
    logger.info(f"Saved {filename} to {output_path}")

if __name__ == "__main__":
    try:
        # Get the land use data
        districts = get_land_use_data()
        logger.info(f"Processed land use districts dataset with {len(districts)} rows")
        
        # Get the parcel boundaries data
        parcels = get_parcel_data()
        logger.info(f"Processed parcel boundaries dataset with {len(parcels)} rows")
        
        # Save processed datasets
        save_processed_data(districts, "land_use_districts")
        save_processed_data(parcels, "parcel_boundaries")
        
        logger.info("Data processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")

