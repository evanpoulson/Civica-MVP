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
from data_processing.data_manager import DataManager

def get_land_use_data():
    """
    Extract land use districts data from Calgary's Open Data Portal.
    Returns a GeoPandas DataFrame containing the land use districts.
    """
    # Get the data using DataManager
    return DataManager.get_districts()

if __name__ == "__main__":
    try:
        # Get the land use data
        districts = get_land_use_data()
        
        districts.plot()
        plt.show()
        
    except Exception as e:
        print(f"Error: {str(e)}")

