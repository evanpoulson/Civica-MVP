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

def get_land_use_data():
    """
    Extract land use districts data from Calgary's Open Data Portal.
    Returns a GeoPandas DataFrame containing the land use districts.
    """
    # Get the GeoJSON data directly from the API endpoint
    url = "https://data.calgary.ca/resource/qe6k-p9nh.geojson?$limit=20000"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    
    # Read directly into GeoPandas
    gdf = gpd.read_file(response.text)
    
    # First set the source CRS (WGS84)
    gdf.set_crs(epsg=4326, inplace=True)
    
    # Then transform to Calgary's standard CRS (UTM Zone 12N)
    gdf = gdf.to_crs(epsg=3400)
    
    return gdf

if __name__ == "__main__":
    try:
        # Get the land use data
        districts = get_land_use_data()
        
        districts.plot()
        plt.show()
        
    except Exception as e:
        print(f"Error: {str(e)}")

