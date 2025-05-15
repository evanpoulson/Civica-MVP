"""
Utility functions and classes for the data processing pipeline.
"""

import logging
from pathlib import Path
from typing import Tuple

from .config import (
    ZONING_SHP_NAME,
    PARCELS_SHP_NAME,
    REQUIRED_SHAPEFILE_EXTENSIONS
)

logger = logging.getLogger(__name__)

class FileManager:
    """Manages file operations and directory structure."""
    
    def __init__(self, data_dir: str = "data"):
        """Initialize the file manager with a data directory."""
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.reports_dir = self.data_dir / "reports"
        
        # Create directories if they don't exist
        self.raw_dir.mkdir(exist_ok=True)
        self.processed_dir.mkdir(exist_ok=True)
        self.reports_dir.mkdir(exist_ok=True)
        
        # Define expected file paths
        self.zoning_dir = self.raw_dir / "zoning"
        self.parcels_dir = self.raw_dir / "parcels"
        
        # Create subdirectories
        self.zoning_dir.mkdir(exist_ok=True)
        self.parcels_dir.mkdir(exist_ok=True)
        
        # Define expected file names
        self.zoning_shp = self.zoning_dir / ZONING_SHP_NAME
        self.parcels_shp = self.parcels_dir / PARCELS_SHP_NAME
    
    def validate_input_files(self) -> Tuple[Path, Path]:
        """Validate that required input files exist and have the correct names."""
        # Check zoning files
        if not self.zoning_shp.exists():
            raise FileNotFoundError(
                f"Zoning shapefile not found at {self.zoning_shp}. "
                f"Please ensure the file is named '{ZONING_SHP_NAME}' and is in the correct directory."
            )
        logger.info(f"Found zoning shapefile: {self.zoning_shp}")
        
        # Check parcel files
        if not self.parcels_shp.exists():
            raise FileNotFoundError(
                f"Parcels shapefile not found at {self.parcels_shp}. "
                f"Please ensure the file is named '{PARCELS_SHP_NAME}' and is in the correct directory."
            )
        logger.info(f"Found parcels shapefile: {self.parcels_shp}")
        
        # Check for required companion files
        for shp_file in [self.zoning_shp, self.parcels_shp]:
            base_name = shp_file.with_suffix('')
            for ext in REQUIRED_SHAPEFILE_EXTENSIONS:
                companion_file = base_name.with_suffix(ext)
                if not companion_file.exists():
                    raise FileNotFoundError(
                        f"Required companion file {companion_file} is missing. "
                        "Please ensure all shapefile components are present."
                    )
        
        return self.zoning_shp, self.parcels_shp 