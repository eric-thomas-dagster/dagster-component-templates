# Image Metadata Extractor Component

Extract technical metadata from images: dimensions, format, color mode, file size, and EXIF data including GPS coordinates, camera model, and capture date. Adds metadata as prefixed columns to the DataFrame.

## Overview

The Image Metadata Extractor Component uses PIL/Pillow to read image technical properties and EXIF metadata. GPS EXIF tags are automatically converted from degrees/minutes/seconds to decimal latitude and longitude. All output columns are prefixed for easy identification.

## Features

- **Basic metadata**: Width, height, format (JPEG/PNG), color mode (RGB/L), file size
- **EXIF data**: Capture date, camera make and model
- **GPS parsing**: Converts DMS EXIF GPS tags to decimal lat/lon
- **Configurable prefix**: Namespace output columns with a custom prefix
- **Optional histogram**: Color histogram for image color analysis

## Use Cases

1. **Photo library enrichment**: Add metadata for organization and search
2. **GDPR GPS auditing**: Find images with embedded location data
3. **Camera inventory**: Aggregate usage stats by camera model
4. **Dataset quality control**: Verify dimensions and formats meet requirements

## Prerequisites

- `Pillow>=9.0.0`

## Configuration

### Basic Metadata

```yaml
type: dagster_component_templates.ImageMetadataExtractorComponent
attributes:
  asset_name: image_metadata
  upstream_asset_key: raw_images
  image_column: file_path
  extract_exif: true
  extract_gps: true
  output_prefix: "img_"
```

### Dimensions Only (No EXIF)

```yaml
type: dagster_component_templates.ImageMetadataExtractorComponent
attributes:
  asset_name: image_dimensions
  upstream_asset_key: product_images
  image_column: image_path
  extract_exif: false
  extract_gps: false
  output_prefix: ""
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `image_column` | string | required | Column with image file paths |
| `extract_exif` | boolean | `true` | Extract EXIF metadata |
| `extract_gps` | boolean | `true` | Parse GPS to lat/lon |
| `output_prefix` | string | `img_` | Column name prefix |
| `include_histogram` | boolean | `false` | Add color histogram column |
| `group_name` | string | `None` | Asset group |

## Output Columns

With default prefix `img_`:

| Column | Description |
|--------|-------------|
| `img_width` | Image width in pixels |
| `img_height` | Image height in pixels |
| `img_format` | Image format (JPEG, PNG, etc.) |
| `img_mode` | Color mode (RGB, L, RGBA, etc.) |
| `img_file_size_bytes` | File size in bytes |
| `img_capture_date` | EXIF DateTimeOriginal or DateTime |
| `img_camera_model` | EXIF camera model/make |
| `img_gps_lat` | GPS latitude (decimal degrees) |
| `img_gps_lon` | GPS longitude (decimal degrees) |
| `img_histogram` | Color histogram (if enabled) |

## Troubleshooting

- **No EXIF data**: Many images (screenshots, web images) don't have EXIF; columns will be `None`
- **GPS always None**: EXIF GPS is only present in photos taken on GPS-enabled devices
- **Slow on large files**: Histogram computation reads full pixel data; disable if not needed
