import os
import geopandas as gpd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CalgaryDataProcessor:
    def __init__(self, data_dir: str = "data"):
        """Initialize the data processor with a data directory."""
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        
        # Create directories if they don't exist
        self.raw_dir.mkdir(exist_ok=True)
        self.processed_dir.mkdir(exist_ok=True)
        
        # Define expected file paths
        self.zoning_dir = self.raw_dir / "zoning"
        self.parcels_dir = self.raw_dir / "parcels"
        
        # Create subdirectories
        self.zoning_dir.mkdir(exist_ok=True)
        self.parcels_dir.mkdir(exist_ok=True)
        
        # Define expected file names
        self.zoning_shp = self.zoning_dir / "LandUseDistricts.shp"
        self.parcels_shp = self.parcels_dir / "PropertyAssessments.shp"
    
    def validate_input_files(self):
        """Validate that required input files exist and have the correct names."""
        # Check zoning files
        if not self.zoning_shp.exists():
            raise FileNotFoundError(
                f"Zoning shapefile not found at {self.zoning_shp}. "
                "Please ensure the file is named 'LandUseDistricts.shp' and is in the correct directory."
            )
        logger.info(f"Found zoning shapefile: {self.zoning_shp}")
        
        # Check parcel files
        if not self.parcels_shp.exists():
            raise FileNotFoundError(
                f"Parcels shapefile not found at {self.parcels_shp}. "
                "Please ensure the file is named 'PropertyAssessments.shp' and is in the correct directory."
            )
        logger.info(f"Found parcels shapefile: {self.parcels_shp}")
        
        # Check for required companion files
        required_extensions = ['.shx', '.dbf', '.prj']
        for shp_file in [self.zoning_shp, self.parcels_shp]:
            base_name = shp_file.with_suffix('')
            for ext in required_extensions:
                companion_file = base_name.with_suffix(ext)
                if not companion_file.exists():
                    raise FileNotFoundError(
                        f"Required companion file {companion_file} is missing. "
                        "Please ensure all shapefile components are present."
                    )
        
        return self.zoning_shp, self.parcels_shp
    
    def process_data(self):
        """Process the data to create a harmonized dataset."""
        # Validate input files
        zoning_shp, parcels_shp = self.validate_input_files()
        
        # Read the shapefiles
        logger.info("Reading shapefiles...")
        zoning_gdf = gpd.read_file(zoning_shp)
        parcels_gdf = gpd.read_file(parcels_shp)
        
        # Log CRS information
        logger.info(f"Zoning CRS: {zoning_gdf.crs}")
        logger.info(f"Parcels CRS: {parcels_gdf.crs}")
        
        # Ensure both datasets are in the same CRS (using EPSG:32612 for UTM Zone 12N)
        target_crs = "EPSG:32612"
        if zoning_gdf.crs != target_crs:
            logger.info(f"Reprojecting zoning data to {target_crs}")
            zoning_gdf = zoning_gdf.to_crs(target_crs)
        if parcels_gdf.crs != target_crs:
            logger.info(f"Reprojecting parcels data to {target_crs}")
            parcels_gdf = parcels_gdf.to_crs(target_crs)
        
        # Log column information
        logger.info("Zoning columns: " + ", ".join(zoning_gdf.columns))
        logger.info("Parcels columns: " + ", ".join(parcels_gdf.columns))
        
        # Perform spatial join
        logger.info("Performing spatial join...")
        joined_gdf = gpd.sjoin(parcels_gdf, zoning_gdf, how="left", predicate="within")
        
        # Clean up the joined dataset
        # Keep only essential columns
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
        
        joined_gdf = joined_gdf[columns_to_keep]
        
        # Save the processed data
        output_path = self.processed_dir / "calgary_parcels_with_zoning.geojson"
        logger.info(f"Saving processed data to {output_path}")
        joined_gdf.to_file(output_path, driver="GeoJSON")
        
        return joined_gdf

    def inspect_output(self, n=5):
        """Utility to inspect the processed output file."""
        output_path = self.processed_dir / "calgary_parcels_with_zoning.geojson"
        if not output_path.exists():
            logger.error(f"Output file {output_path} does not exist.")
            return
        logger.info(f"Inspecting output file: {output_path}")
        gdf = gpd.read_file(output_path)
        logger.info(f"Number of features: {len(gdf)}")
        logger.info(f"Columns: {list(gdf.columns)}")
        logger.info(f"Sample data:\n{gdf.head(n)}")
        missing_geom = gdf['geometry'].isnull().sum()
        logger.info(f"Missing geometries: {missing_geom}")
        return gdf

def main():
    """Main function to run the data processing pipeline."""
    processor = CalgaryDataProcessor()
    
    try:
        # Process the data
        logger.info("Starting data processing...")
        processed_data = processor.process_data()
        
        logger.info("Data processing complete!")
        logger.info(f"Processed data saved to: {processor.processed_dir}/calgary_parcels_with_zoning.geojson")
        
    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        logger.error("Please ensure you have downloaded and placed the required shapefiles in the correct directories.")
        logger.error("See data/README.md for download instructions.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main() 