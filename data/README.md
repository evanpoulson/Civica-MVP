# Data Directory

This directory contains the geospatial data used by the Civica project.

## Directory Structure

```
data/
├── raw/              # Original, unmodified data files
│   ├── zoning/       # Zoning district shapefiles
│   └── parcels/      # Parcel boundary shapefiles
├── processed/        # Processed and harmonized datasets
└── README.md         # This file
```

## Required Datasets

### 1. Zoning Districts
- **Source:** [Calgary Open Data Portal - Land Use Districts](https://data.calgary.ca/Base-Maps/Land-Use-Districts/qe6k-p9nh)
- **Format:** Shapefile (.shp, .shx, .dbf, .prj)
- **API Endpoint:** https://data.calgary.ca/resource/qe6k-p9nh.geojson
- **Download Instructions:**
  1. Visit the source URL
  2. Click "Export" button
  3. Select "Shapefile" format
  4. Save the downloaded zip file
  5. Extract contents to `data/raw/zoning/`
- **Required Files:**
  - LandUseDistricts.shp
  - LandUseDistricts.shx
  - LandUseDistricts.dbf
  - LandUseDistricts.prj

### 2. Parcel Boundaries
- **Source:** [Calgary Open Data Portal - Current Year Property Assessments](https://data.calgary.ca/Government/Current-Year-Property-Assessments-Parcel-/4bsw-nn7w)
- **Format:** Shapefile (.shp, .shx, .dbf, .prj)
- **API Endpoint:** https://data.calgary.ca/resource/4bsw-nn7w.geojson
- **Download Instructions:**
  1. Visit the source URL
  2. Click "Export" button
  3. Select "Shapefile" format
  4. Save the downloaded zip file
  5. Extract contents to `data/raw/parcels/`
- **Required Files:**
  - PropertyAssessments.shp
  - PropertyAssessments.shx
  - PropertyAssessments.dbf
  - PropertyAssessments.prj

## Data Processing

The data processing pipeline will:
1. Read the shapefiles from the `raw` directory
2. Harmonize coordinate reference systems (CRS)
3. Perform spatial joins to assign zoning districts to parcels
4. Export the processed data to `processed/calgary_parcels_with_zoning.geojson`

## Data Provenance

When downloading new data, please update this section with:
- Download date
- Source URL
- Any relevant metadata or notes

Example:
```
Zoning Districts:
- Downloaded: 2024-03-15
- Source: https://data.calgary.ca/Base-Maps/Land-Use-Districts/qe6k-p9nh
- Notes: Using 2023 version of the land use districts

Parcel Boundaries:
- Downloaded: 2024-03-15
- Source: https://data.calgary.ca/Government/Current-Year-Property-Assessments-Parcel-/4bsw-nn7w
- Notes: Using current year property assessment data
``` 