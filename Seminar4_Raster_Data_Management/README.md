# Introduction of raster file format


## 1. Netcdf

**NetCDF (Network Common Data Form)** is a `binary, self-describing, machine-independent format` for storing 
**array-oriented scientific** data, commonly used in climatology, meteorology, oceanography, and remote sensing.

A `NetCDF` file defines:
- **dimensions**: It defines the shape of data arrays.
- **variables**: It stores the actual data.
- **coordinates**: 1D or 2D variables that define position.
- **attributes**: It stores the metadata at the file or variable level.

## 1.1 Full example 

```python
import xarray as xr
import numpy as np
import pandas as pd
import pathlib

file_root_path = (pathlib.Path.cwd().parent.parent / "data/tmp/netcdf_test_files").as_posix() 

# Define dimensions
time = pd.date_range("2025-01-01", periods=3)
lat = np.array([10.0, 20.0], dtype=np.float32)
lon = np.array([30.0, 40.0], dtype=np.float32)

# Generate dummy temperature data
temperature_data = np.random.uniform(280, 300, size=(3, 2, 2)).astype(np.float32)
humidity_data = np.random.uniform(30, 80, size=(3, 2, 2)).astype(np.float32)

# Create dataset
ds = xr.Dataset(
    data_vars={
        "temperature": (["time", "lat", "lon"], temperature_data),
        "humidity": (["time", "lat", "lon"], humidity_data),
    },
    coords={
        "time": ("time", time),
        "lat": ("lat", lat),
        "lon": ("lon", lon),
    },
    attrs={
        "title": "Temperature sample Dataset",
        "institution": "CASD Meteorological Center",
        "source": "Simulated data",
        "history": "Created 2025-06-16",
        "Conventions": "CF-1.8"
    }
)

# Add variable attributes
ds["temperature"].attrs = {
    "long_name": "Surface Air Temperature",
    "units": "K",
    "_FillValue": -999.0
}
ds["humidity"].attrs = {
    "long_name": "Surface Air Humidity",
    "units": "percentage",
    "_FillValue": -1.0
}
ds["time"].attrs = {
    "long_name": "time"
}
ds["time"].encoding = {
    "units": "days since 2000-01-01 00:00:00",
    "calendar": "standard"
}

ds["lat"].attrs = {
    "units": "degrees_north",
    "long_name": "latitude"
}
ds["lon"].attrs = {
    "units": "degrees_east",
    "long_name": "longitude"
}

# Save as NetCDF
print(ds)
ds.to_netcdf(f'{file_root_path}/day{i+1}.nc',engine='netcdf4')

```


## 1.2 Optimization options

### 1.2.1. Reduce Data Precision and compression

We can use `scale_factor, add_offset, and convert float64 → float32` to reduce precision to save some disk space.

Update your encoding:

encoding = {
    'temperature': {
        'dtype': 'float32',
        'scale_factor': 0.01,     # 0.01°C resolution
        'add_offset': 0,
        '_FillValue': -999,
        'zlib': True,
        'complevel': 4,
        'chunksizes': (1, 5, 5)
    }
}
This alone will cut the variable size by ~50% (8 bytes → 4 bytes per value, plus compression).

> Can't use float16, netcdf4 does not support float16.

### 1.2.2. Use Only Shared 1D Coordinates

Before concatenation, drop any 2D coordinates or attributes that are being stored redundantly:

```python
# Example cleanup for each file
ds = ds.drop_vars([var for var in ds.data_vars if var not in ['temperature']])
for coord in ds.coords:
    if coord not in ['lat', 'lon']:
        ds = ds.drop_vars(coord)

# Ensure lat and lon are 1D and identical across files:
assert np.allclose(data_list[0]['lat'], ds['lat'])
assert np.allclose(data_list[0]['lon'], ds['lon'])

```
### 1.2.3. Disable Unnecessary Attributes or Metadata

Attributes like long_name, history, conventions, and software version bloat the file:

```python
combined.attrs = {}  # remove global attributes
combined['temperature'].attrs = {}  # clear variable metadata
```

### 1.2.4. Save with Classic Format (More Compatible & Compact)
```python
combined.to_netcdf(
    f"{file_root_path}/merged_temperature.nc",
    engine='netcdf4',
    format='NETCDF4_CLASSIC',
    encoding=encoding
)
```
