import pytest
import shutil
import time
import psutil
import os
from pathlib import Path
from src.data_processor import CalgaryDataProcessor
import geopandas as gpd
from shapely.geometry import Point, Polygon
import numpy as np

def test_data_processor_initialization(tmp_path):
    processor = CalgaryDataProcessor(data_dir=str(tmp_path))
    assert processor.raw_dir.exists()
    assert processor.processed_dir.exists()
    assert processor.zoning_dir.exists()
    assert processor.parcels_dir.exists()

def test_validate_input_files_missing(tmp_path):
    processor = CalgaryDataProcessor(data_dir=str(tmp_path))
    with pytest.raises(FileNotFoundError):
        processor.validate_input_files()

def test_missing_companion_files(tmp_path):
    processor = CalgaryDataProcessor(data_dir=str(tmp_path))
    # Create empty .shp files for zoning and parcels
    processor.zoning_shp.touch()
    processor.parcels_shp.touch()
    # Only .shp files exist, so companion files are missing
    with pytest.raises(FileNotFoundError) as excinfo:
        processor.validate_input_files()
    assert any(ext in str(excinfo.value) for ext in ['.shx', '.dbf', '.prj'])

def test_process_data_and_output():
    processor = CalgaryDataProcessor()
    # This assumes the real data is present in data/raw/zoning and data/raw/parcels
    processed_gdf = processor.process_data()
    output_path = processor.processed_dir / "calgary_parcels_with_zoning.geojson"
    assert output_path.exists(), "Output file does not exist."
    # Reload with geopandas to check structure
    gdf = gpd.read_file(output_path)
    # Check for expected columns
    expected_columns = {'lu_bylaw', 'lu_code', 'dc_bylaw', 'dc_site_no', 'roll_numbe', 'address', 'land_use_d', 'property_t', 'geometry'}
    assert expected_columns.issubset(set(gdf.columns)), f"Missing expected columns: {expected_columns - set(gdf.columns)}"
    # Check for missing geometries
    assert gdf['geometry'].notnull().all(), "There are missing geometries in the output."
    # Check for at least one feature
    assert len(gdf) > 0, "No features found in the output."

def test_process_data_missing_files(tmp_path):
    processor = CalgaryDataProcessor(data_dir=str(tmp_path))
    with pytest.raises(FileNotFoundError):
        processor.process_data()

def test_empty_shapefile(tmp_path):
    """Test handling of empty shapefiles."""
    processor = CalgaryDataProcessor(data_dir=str(tmp_path))
    
    # Create empty GeoDataFrames with required columns
    empty_gdf = gpd.GeoDataFrame(
        geometry=[],
        data={'lu_bylaw': [], 'lu_code': [], 'dc_bylaw': [], 'dc_site_no': []},
        crs="EPSG:32612"
    )
    
    # Save empty shapefiles
    empty_gdf.to_file(processor.zoning_shp)
    empty_gdf.to_file(processor.parcels_shp)
    
    # Create companion files
    for shp_file in [processor.zoning_shp, processor.parcels_shp]:
        base = shp_file.with_suffix('')
        for ext in ['.shx', '.dbf', '.prj']:
            (base.with_suffix(ext)).touch()
    
    # Process should complete but return empty result
    result = processor.process_data()
    assert len(result) == 0, "Empty input should produce empty output"

def test_invalid_crs(tmp_path):
    """Test handling of invalid CRS."""
    processor = CalgaryDataProcessor(data_dir=str(tmp_path))
    
    # Create GeoDataFrames with missing CRS
    invalid_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0)],
        crs=None  # No CRS set
    )
    
    # Save shapefiles
    invalid_gdf.to_file(processor.zoning_shp)
    invalid_gdf.to_file(processor.parcels_shp)
    
    # Create companion files
    for shp_file in [processor.zoning_shp, processor.parcels_shp]:
        base = shp_file.with_suffix('')
        for ext in ['.shx', '.dbf', '.prj']:
            (base.with_suffix(ext)).touch()
    
    # Process should handle missing CRS gracefully
    with pytest.raises(Exception) as excinfo:
        processor.process_data()
    assert "Cannot transform naive geometries" in str(excinfo.value)

def test_duplicate_geometries(tmp_path):
    """Test handling of duplicate geometries."""
    processor = CalgaryDataProcessor(data_dir=str(tmp_path))
    
    # Create GeoDataFrames with duplicate geometries
    duplicate_geom = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    duplicate_gdf = gpd.GeoDataFrame(
        geometry=[duplicate_geom, duplicate_geom],
        crs="EPSG:32612"
    )
    
    # Save shapefiles
    duplicate_gdf.to_file(processor.zoning_shp)
    duplicate_gdf.to_file(processor.parcels_shp)
    
    # Create companion files
    for shp_file in [processor.zoning_shp, processor.parcels_shp]:
        base = shp_file.with_suffix('')
        for ext in ['.shx', '.dbf', '.prj']:
            (base.with_suffix(ext)).touch()
    
    # Process should handle duplicates without error
    result = processor.process_data()
    assert len(result) > 0, "Should process duplicate geometries"

def test_large_dataset_performance(tmp_path):
    """Test performance with large datasets."""
    processor = CalgaryDataProcessor(data_dir=str(tmp_path))
    
    # Create large GeoDataFrames (1000 features)
    n_features = 1000
    geometries = [Polygon([(i, i), (i+1, i), (i+1, i+1), (i, i+1)]) for i in range(n_features)]
    large_gdf = gpd.GeoDataFrame(
        geometry=geometries,
        crs="EPSG:32612"
    )
    
    # Save shapefiles
    large_gdf.to_file(processor.zoning_shp)
    large_gdf.to_file(processor.parcels_shp)
    
    # Create companion files
    for shp_file in [processor.zoning_shp, processor.parcels_shp]:
        base = shp_file.with_suffix('')
        for ext in ['.shx', '.dbf', '.prj']:
            (base.with_suffix(ext)).touch()
    
    # Measure processing time and memory usage
    process = psutil.Process(os.getpid())
    start_mem = process.memory_info().rss / 1024 / 1024  # MB
    start_time = time.time()
    
    result = processor.process_data()
    
    end_time = time.time()
    end_mem = process.memory_info().rss / 1024 / 1024  # MB
    
    processing_time = end_time - start_time
    memory_usage = end_mem - start_mem
    
    # Assert reasonable performance
    assert processing_time < 30, f"Processing took too long: {processing_time:.2f} seconds"
    assert memory_usage < 1000, f"Memory usage too high: {memory_usage:.2f} MB"
    assert len(result) > 0, "Should process large dataset successfully"

def test_invalid_column_names(tmp_path):
    """Test handling of invalid column names."""
    processor = CalgaryDataProcessor(data_dir=str(tmp_path))
    
    # Create GeoDataFrames with invalid column names
    invalid_cols_gdf = gpd.GeoDataFrame(
        geometry=[Point(0, 0)],
        data={'invalid!@#': [1]},
        crs="EPSG:32612"
    )
    
    # Save shapefiles
    invalid_cols_gdf.to_file(processor.zoning_shp)
    invalid_cols_gdf.to_file(processor.parcels_shp)
    
    # Create companion files
    for shp_file in [processor.zoning_shp, processor.parcels_shp]:
        base = shp_file.with_suffix('')
        for ext in ['.shx', '.dbf', '.prj']:
            (base.with_suffix(ext)).touch()
    
    # Process should handle invalid column names gracefully
    result = processor.process_data()
    assert 'geometry' in result.columns, "Should preserve geometry column" 