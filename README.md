# Civica: Calgary Zoning Simulation MVP

**Lead:** Evan Poulson

## Overview
Civica is an interactive civic simulation platform designed to help urban planners, community leaders, and the public visualize the impact of zoning policy changes in Calgary. The MVP enables users to select a zoning district, specify a new allowed residential density, and see how that change would affect housing capacity and land use in that area.

## Features
- Upload and process Calgary zoning and parcel data
- Harmonize geospatial data and perform spatial joins
- Stateless, testable simulation logic for zoning changes
- FastAPI backend for simulation API
- Streamlit front-end for interactive exploration
- Modular, reproducible, and open-source design

## Project Structure
```
Civica-MVP/
├── data/
│   ├── raw/         # Raw geospatial data
│   └── processed/   # Cleaned/processed files
├── requirements.txt
├── src/
│   ├── data_processing/  # Data processing and validation logic
│   │   ├── processor.py  # Main data processing pipeline
│   │   ├── validators.py # Data validation utilities
│   │   ├── utils.py      # Helper functions
│   │   ├── config.py     # Configuration settings
│   │   ├── reporting.py  # Data quality reporting
│   │   └── spatial.py    # Spatial operations
│   ├── core/        # Core simulation logic (to be implemented)
│   ├── api/         # FastAPI endpoints (to be implemented)
│   └── frontend/    # Streamlit UI code (to be implemented)
├── tests/           # Unit and integration tests
└── venv/            # Python virtual environment
```

## Setup Instructions
1. **Install Python 3.11** (recommended via Homebrew on macOS):
   ```sh
   brew install python@3.11
   ```
2. **Create and activate a virtual environment:**
   ```sh
   python3.11 -m venv venv
   source venv/bin/activate
   ```
3. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```
4. **Project directories:**
   - Place raw geospatial data in `data/raw/`
   - Processed/cleaned data will be stored in `data/processed/`

## Conventions & Best Practices
- All simulation logic is stateless and pure
- All geospatial data uses a common, explicitly defined CRS
- API uses FastAPI and Pydantic for validation
- UI is built with Streamlit and streamlit-folium
- Follow PEP 8 and open-source geospatial standards

## License
[MIT License](LICENSE) 