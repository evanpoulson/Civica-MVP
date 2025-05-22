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

def get_land_use_data():
    """
    Extract land use districts data from Calgary's Open Data Portal.
    Returns a GeoPandas DataFrame containing the land use districts.
    """
    # Get the data using DataManager
    return DataManager.get_districts()

def get_parcel_data():
    """
    Extract parcel boundary data from Calgary's Open Data Portal.
    Returns a GeoPandas DataFrame containing the parcel boundaries.
    """
    # Get the data using DataManager
    return DataManager.get_parcel_boundaries()

if __name__ == "__main__":
    try:
        # Get the land use data
        districts = get_land_use_data()
        
        # Get the parcel boundaries data
        parcels = get_parcel_data()
        
        # Create a figure with two subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 7))
        
        # Plot land use districts
        districts.plot(ax=ax1)
        ax1.set_title('Land Use Districts')
        
        # Plot parcel boundaries
        parcels.plot(ax=ax2)
        ax2.set_title('Parcel Boundaries')
        
        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        print(f"Error: {str(e)}")

