#!/usr/bin/env python3
"""
Interactive OPeNDAP URL Builder for GEOS-FP Data
Allows users to build subset URLs with tab-completion and guided prompts
Supports historical data access and export to curl/wget commands
"""

import xarray as xr
import numpy as np
from datetime import datetime, timedelta
import sys
import os
import requests
from pathlib import Path

# GEOS-FP Products catalog
PRODUCTS_CATALOG = {
    'inst1_2d_hwl_Nx': '2d,1-Hourly,Instantaneous,Single-Level,Forecast,Hyperwall',
    'inst1_2d_lfo_Nx': 'GEOS5 FP 2d time-averaged land surface forcing',
    'inst1_2d_smp_Nx': 'GEOS5 FP 2d instantaneous diagnostics for SMAP',
    'inst1_2d_wwi_Nx': '2d,1-Hourly,Instantaneous,Single-Level,Forecast,WorldWinds',
    'inst3_2d_met_Nx': '2d,3-Hourly,Instantaneous,Single-Level,Forecast,Forecast Fields',
    'inst3_2d_smp_Nx': 'GEOS5 FP 2d instantaneous diagnostics for SMAP',
    'inst3_3d_aer_Np': '3d,3-Hourly,Instantaneous,Pressure-Level,Forecast,Aerosol Concentrations',
    'inst3_3d_aer_Nv': 'GEOS5 FP 3d instantaneous aerosol diagnostics',
    'inst3_3d_asm_Cp': '3d,3-Hourly,Instantaneous,Pressure-Level,Forecast Fields',
    'inst3_3d_asm_Np': 'GEOS5 FP 3d assimilated state on pressure levels',
    'inst3_3d_asm_Nv': 'GEOS5 FP 3d assimilated state on native levels',
    'inst3_3d_chm_Np': '3d,3-Hourly,Instantaneous,Pressure-Level,Forecast,Chemistry',
    'inst3_3d_ext_Np': '3d,3-Hourly,Instantaneous,Pressure-Level,Forecast,Aerosol Extinction',
    'inst3_3d_tag_Np': 'GEOS5 FP 3d tag',
    'tavg1_2d_flx_Nx': 'GEOS5 FP 2d time-averaged surface flux diagnostics',
    'tavg1_2d_lfo_Nx': 'GEOS5 FP 2d instantaneous land surface forcing',
    'tavg1_2d_lnd_Nx': 'GEOS5 FP 2d time-averaged land surface diagnostics',
    'tavg1_2d_rad_Nx': 'GEOS5 FP 2d time-averaged radiation diagnostics',
    'tavg1_2d_slv_Nx': 'GEOS5 FP 2d time-averaged single level diagnostics',
    'tavg3_2d_aer_Nx': 'GEOS5 FP 2d time-averaged primary aerosol diagnostics',
    'tavg3_2d_smp_Nx': 'GEOS5 FP 2d time-averaged diagnostics for SMAP',
    'tavg3_3d_asm_Nv': 'GEOS5.10.0 FP 3d time-averaged assimilated state on native levels',
}

BASE_URL = "https://opendap.nccs.nasa.gov/dods/GEOS-5/fp/0.25_deg"


def print_header(text):
    """Print formatted header"""
    print(f"\n{'=' * 70}")
    print(f"  {text}")
    print(f"{'=' * 70}\n")


def print_section(text):
    """Print formatted section header"""
    print(f"\n{'-' * 70}")
    print(f"  {text}")
    print(f"{'-' * 70}\n")


def check_product_availability(product, base_url, timeout=5):
    """Check if a product is available on the OPeNDAP server"""
    test_url = f"{base_url}/{product}.latest.dds"
    
    try:
        response = requests.head(test_url, timeout=timeout, allow_redirects=True)
        return response.status_code == 200
    except:
        # If HEAD fails, try GET on the DDS endpoint
        try:
            response = requests.get(test_url, timeout=timeout)
            return response.status_code == 200 and 'Dataset' in response.text
        except:
            return False


def validate_products(show_progress=True):
    """Validate which products are actually available"""
    if show_progress:
        print_section("Validating Product Availability")
        print("Checking which products are accessible on the OPeNDAP server...")
        print("(This may take a moment)\n")
    
    available_products = {}
    
    for i, (product, description) in enumerate(PRODUCTS_CATALOG.items(), 1):
        if show_progress:
            print(f"[{i:2d}/{len(PRODUCTS_CATALOG)}] Checking {product}...", end=' ')
            sys.stdout.flush()
        
        if check_product_availability(product, BASE_URL):
            available_products[product] = description
            if show_progress:
                print("✓")
        else:
            if show_progress:
                print("✗ (not available)")
    
    if show_progress:
        print(f"\n✓ Found {len(available_products)} available products")
    
    return available_products


def select_product(products=None):
    """Let user select a product"""
    print_header("GEOS-FP Product Selection")
    
    if products is None:
        # Use all products from catalog without validation
        products = PRODUCTS_CATALOG
        print("⚠ Note: Not all products may be available\n")
    
    if not products:
        print("✗ No products available. Please check your connection.")
        sys.exit(1)
    
    print(f"Available products ({len(products)}):\n")
    products_list = list(products.keys())
    
    for i, (product, description) in enumerate(products.items(), 1):
        print(f"{i:2d}. {product:20s} - {description}")
    
    while True:
        try:
            choice = input("\nEnter product number (or 'q' to quit): ").strip()
            if choice.lower() == 'q':
                sys.exit(0)
            
            idx = int(choice) - 1
            if 0 <= idx < len(products_list):
                selected = products_list[idx]
                print(f"\n✓ Selected: {selected}")
                return selected
            else:
                print(f"Please enter a number between 1 and {len(products_list)}")
        except ValueError:
            print("Invalid input. Please enter a number.")


def select_data_source(product):
    """Select between latest data or historical archive"""
    print_section("Data Source Selection")
    
    print("Options:")
    print("  1. Latest data (rolling ~10 days)")
    print("  2. Historical archive (specific dates)")
    
    while True:
        choice = input("\nEnter option (1-2): ").strip()
        
        if choice == '1':
            return 'latest', None
        
        elif choice == '2':
            return 'historical', get_historical_dates(product)
        
        else:
            print("Invalid option. Please choose 1 or 2.")


def get_historical_dates(product):
    """Get specific date or date range for historical data"""
    print_section("Historical Data Selection")
    
    print("GEOS-FP historical data is organized by date:")
    print("  Structure: PRODUCT/YEAR/MONTH/DAY/")
    print("  Example: inst3_3d_asm_Np/Y2024/M01/D15/\n")
    
    print("Options:")
    print("  1. Single date")
    print("  2. Date range (multiple files)")
    
    while True:
        choice = input("\nEnter option (1-2): ").strip()
        
        if choice == '1':
            date_str = input("Enter date (YYYY-MM-DD): ").strip()
            try:
                date = datetime.strptime(date_str, "%Y-%m-%d")
                return [date]
            except ValueError:
                print("Invalid date format. Use YYYY-MM-DD")
                continue
        
        elif choice == '2':
            start_str = input("Start date (YYYY-MM-DD): ").strip()
            end_str = input("End date (YYYY-MM-DD): ").strip()
            try:
                start_date = datetime.strptime(start_str, "%Y-%m-%d")
                end_date = datetime.strptime(end_str, "%Y-%m-%d")
                
                if start_date > end_date:
                    print("Start date must be before end date")
                    continue
                
                # Generate list of dates
                dates = []
                current = start_date
                while current <= end_date:
                    dates.append(current)
                    current += timedelta(days=1)
                
                print(f"\n✓ Selected {len(dates)} days from {start_date.date()} to {end_date.date()}")
                return dates
                
            except ValueError:
                print("Invalid date format. Use YYYY-MM-DD")
                continue
        
        else:
            print("Invalid option. Please choose 1 or 2.")


def get_historical_files(product, date):
    """Construct path for historical data"""
    year = f"Y{date.year}"
    month = f"M{date.month:02d}"
    day = f"D{date.day:02d}"
    
    date_str = date.strftime("%Y%m%d")
    base_path = f"{BASE_URL}/{product}/{year}/{month}/{day}"
    
    return base_path, date_str


def get_dataset_info(product, source_type, dates=None):
    """Connect to OPeNDAP and get dataset structure"""
    print_section("Connecting to OPeNDAP server...")
    
    if source_type == 'latest':
        url = f"{BASE_URL}/{product}.latest"
        print(f"URL: {url}")
        
        try:
            print("Loading dataset metadata (this may take a moment)...")
            ds = xr.open_dataset(url)
            print("✓ Connected successfully!\n")
            return ds, url
        except Exception as e:
            print(f"✗ Error connecting to OPeNDAP: {e}")
            print("\nThis product may not be available via the .latest endpoint.")
            print("Suggestions:")
            print("  1. Try a different product")
            print("  2. Try historical data access")
            print("  3. Check https://opendap.nccs.nasa.gov/dods/GEOS-5/fp/0.25_deg/ for available datasets")
            sys.exit(1)
    
    else:  # historical
        # For historical, connect to first date to get structure
        date = dates[0]
        base_path, date_str = get_historical_files(product, date)
        
        print(f"Checking historical data for {date.date()}...")
        print(f"Path: {base_path}/")
        
        # Try different common filename patterns
        patterns = [
            # Daily aggregation (preferred)
            f"{BASE_URL}/{product}/{year}/M{date.month:02d}/{product}.daily.{date_str}",
            # Individual hourly files
            f"{base_path}/GEOS.fp.asm.{product}.{date_str}_0000.V01.nc4",
            f"{base_path}/GEOS.fp.asm.{product}.{date_str}_00z.V01.nc4",
            f"{base_path}/GEOS.fp.fcst.{product}.{date_str}_00+{date_str}_0000.V01.nc4",
            f"{base_path}/{product}.{date_str}.nc4",
            # Alternative structures
            f"{BASE_URL}/{product}/Y{date.year}/M{date.month:02d}/D{date.day:02d}/{product}.{date_str}.nc4",
        ]
        
        ds = None
        working_url = None
        
        for pattern in patterns:
            try:
                print(f"  Trying: {pattern.split('/')[-1]}")
                ds = xr.open_dataset(pattern)
                working_url = pattern
                print("  ✓ Success!")
                break
            except Exception as e:
                continue
        
        if ds is None:
            print("\n✗ Could not find data for this date with standard patterns.")
            print("\nTroubleshooting options:")
            print(f"  1. Browse manually: {base_path.replace('.nc4', '')}/")
            print("  2. Check the NCCS data portal for the exact file structure")
            print("  3. Use .latest endpoint for recent data")
            
            manual = input("\nEnter full OPeNDAP URL manually? (y/n): ").strip().lower()
            if manual == 'y':
                url = input("URL: ").strip()
                try:
                    ds = xr.open_dataset(url)
                    working_url = url
                    print("✓ Connected successfully!\n")
                except Exception as e:
                    print(f"✗ Error: {e}")
                    sys.exit(1)
            else:
                sys.exit(1)
        
        return ds, working_url


def display_dataset_info(ds):
    """Display comprehensive dataset information"""
    print_section("Dataset Structure")
    
    print("DIMENSIONS:")
    for dim, size in ds.dims.items():
        print(f"  {dim:15s}: {size}")
    
    print("\nCOORDINATES:")
    for coord in ds.coords:
        coord_data = ds[coord]
        if coord_data.size > 0:
            if coord == 'time':
                try:
                    times = coord_data.values
                    print(f"  {coord:15s}: {times[0]} to {times[-1]} ({len(times)} steps)")
                except:
                    print(f"  {coord:15s}: {coord_data.size} values")
            else:
                try:
                    min_val = float(coord_data.min().values)
                    max_val = float(coord_data.max().values)
                    print(f"  {coord:15s}: {min_val:.2f} to {max_val:.2f}")
                except:
                    print(f"  {coord:15s}: {coord_data.size} values")
    
    print("\nDATA VARIABLES:")
    data_vars = [v for v in ds.data_vars]
    for i, var in enumerate(data_vars, 1):
        var_data = ds[var]
        dims_str = ', '.join([f"{d}={ds.dims[d]}" for d in var_data.dims])
        try:
            long_name = var_data.attrs.get('long_name', 'No description')
            units = var_data.attrs.get('units', '')
            units_str = f" [{units}]" if units else ""
            print(f"  {i:2d}. {var:20s} ({dims_str})")
            print(f"      {long_name}{units_str}")
        except:
            print(f"  {i:2d}. {var:20s} ({dims_str})")


def select_variables(ds):
    """Let user select variables"""
    print_section("Variable Selection")
    
    data_vars = [v for v in ds.data_vars]
    
    print("Enter variable numbers separated by commas (e.g., 1,3,5)")
    print("Or enter variable names separated by commas (e.g., T,U,V)")
    print("Enter 'all' to select all variables")
    print("Enter 'list' to see the variables again\n")
    
    while True:
        choice = input("Variables: ").strip()
        
        if choice.lower() == 'q':
            sys.exit(0)
        
        if choice.lower() == 'list':
            display_dataset_info(ds)
            continue
        
        if choice.lower() == 'all':
            print(f"✓ Selected all {len(data_vars)} variables")
            return data_vars
        
        # Try parsing as numbers
        try:
            indices = [int(x.strip()) - 1 for x in choice.split(',')]
            selected = [data_vars[i] for i in indices if 0 <= i < len(data_vars)]
            if selected:
                print(f"✓ Selected: {', '.join(selected)}")
                return selected
            else:
                print("Invalid indices. Please try again.")
        except ValueError:
            # Try parsing as variable names
            selected = [v.strip() for v in choice.split(',')]
            valid = [v for v in selected if v in data_vars]
            if valid:
                print(f"✓ Selected: {', '.join(valid)}")
                return valid
            else:
                print(f"Invalid variable names. Available: {', '.join(data_vars)}")


def get_time_indices(ds):
    """Get time range from user"""
    print_section("Time Selection")
    
    times = ds.time.values
    print(f"Available time range:")
    print(f"  Start: {times[0]}")
    print(f"  End:   {times[-1]}")
    print(f"  Total: {len(times)} time steps\n")
    
    print("Options:")
    print("  1. All times")
    print("  2. Latest time only")
    print("  3. First N times")
    print("  4. Last N times")
    print("  5. Specific index range")
    
    while True:
        choice = input("\nEnter option (1-5): ").strip()
        
        if choice == '1':
            return 0, len(times) - 1, 1
        
        elif choice == '2':
            return len(times) - 1, len(times) - 1, 1
        
        elif choice == '3':
            n = int(input("How many time steps? "))
            n = min(n, len(times))
            return 0, n - 1, 1
        
        elif choice == '4':
            n = int(input("How many time steps? "))
            n = min(n, len(times))
            return len(times) - n, len(times) - 1, 1
        
        elif choice == '5':
            start = int(input(f"Start index (0-{len(times)-1}): "))
            end = int(input(f"End index ({start}-{len(times)-1}): "))
            stride = input("Stride (default=1): ").strip()
            stride = int(stride) if stride else 1
            return start, end, stride
        
        else:
            print("Invalid option. Please choose 1-5.")


def get_level_indices(ds, variables):
    """Get vertical level range if applicable"""
    # Check if any selected variable has a level dimension
    has_levels = any('lev' in ds[v].dims for v in variables)
    
    if not has_levels:
        return None
    
    print_section("Vertical Level Selection")
    
    levels = ds.lev.values
    print(f"Available levels: {len(levels)} pressure levels")
    print(f"  Range: {levels[0]:.1f} to {levels[-1]:.1f} hPa\n")
    
    # Display some key levels
    print("Common levels:")
    common = [1000, 925, 850, 700, 500, 300, 250, 200, 100, 50, 10]
    for target in common:
        if target >= levels.min() and target <= levels.max():
            idx = np.argmin(np.abs(levels - target))
            print(f"  {target:4.0f} hPa: index {idx}")
    
    print("\nOptions:")
    print("  1. All levels")
    print("  2. Single level")
    print("  3. Level range (by index)")
    print("  4. Pressure range (in hPa)")
    
    while True:
        choice = input("\nEnter option (1-4): ").strip()
        
        if choice == '1':
            return 0, len(levels) - 1, 1
        
        elif choice == '2':
            lev = float(input("Enter pressure level (hPa): "))
            idx = np.argmin(np.abs(levels - lev))
            print(f"Closest level: {levels[idx]:.1f} hPa (index {idx})")
            return idx, idx, 1
        
        elif choice == '3':
            start = int(input(f"Start index (0-{len(levels)-1}): "))
            end = int(input(f"End index ({start}-{len(levels)-1}): "))
            return start, end, 1
        
        elif choice == '4':
            p_start = float(input("Start pressure (hPa): "))
            p_end = float(input("End pressure (hPa): "))
            idx_start = np.argmin(np.abs(levels - p_start))
            idx_end = np.argmin(np.abs(levels - p_end))
            if idx_start > idx_end:
                idx_start, idx_end = idx_end, idx_start
            print(f"Selected: {levels[idx_start]:.1f} to {levels[idx_end]:.1f} hPa")
            return idx_start, idx_end, 1
        
        else:
            print("Invalid option. Please choose 1-4.")


def get_spatial_indices(ds):
    """Get spatial subset from user"""
    print_section("Spatial Subset Selection")
    
    lats = ds.lat.values
    lons = ds.lon.values
    
    print(f"Available spatial range:")
    print(f"  Latitude:  {lats.min():.2f} to {lats.max():.2f}")
    print(f"  Longitude: {lons.min():.2f} to {lons.max():.2f}")
    print(f"  Resolution: ~0.25°\n")
    
    print("Options:")
    print("  1. Global (all points)")
    print("  2. Specify lat/lon range")
    print("  3. Coarser resolution (with stride)")
    
    while True:
        choice = input("\nEnter option (1-3): ").strip()
        
        if choice == '1':
            return (0, len(lats) - 1, 1), (0, len(lons) - 1, 1)
        
        elif choice == '2':
            lat_min = float(input("Minimum latitude: "))
            lat_max = float(input("Maximum latitude: "))
            lon_min = float(input("Minimum longitude: "))
            lon_max = float(input("Maximum longitude: "))
            
            lat_idx_min = np.argmin(np.abs(lats - lat_min))
            lat_idx_max = np.argmin(np.abs(lats - lat_max))
            lon_idx_min = np.argmin(np.abs(lons - lon_min))
            lon_idx_max = np.argmin(np.abs(lons - lon_max))
            
            if lat_idx_min > lat_idx_max:
                lat_idx_min, lat_idx_max = lat_idx_max, lat_idx_min
            if lon_idx_min > lon_idx_max:
                lon_idx_min, lon_idx_max = lon_idx_max, lon_idx_min
            
            print(f"\nSelected region:")
            print(f"  Lat: {lats[lat_idx_min]:.2f} to {lats[lat_idx_max]:.2f}")
            print(f"  Lon: {lons[lon_idx_min]:.2f} to {lons[lon_idx_max]:.2f}")
            
            stride = input("Spatial stride (1=full res, 2=half res, etc., default=1): ").strip()
            stride = int(stride) if stride else 1
            
            return (lat_idx_min, lat_idx_max, stride), (lon_idx_min, lon_idx_max, stride)
        
        elif choice == '3':
            stride = int(input("Stride factor (2=1°, 4=2°, 8=4° resolution): "))
            return (0, len(lats) - 1, stride), (0, len(lons) - 1, stride)
        
        else:
            print("Invalid option. Please choose 1-3.")


def build_opendap_url(base_url, variables, ds, time_range, level_range, spatial_ranges):
    """Build the final OPeNDAP URL with constraints"""
    
    url = f"{base_url}.nc?"
    
    constraints = []
    
    # Build variable constraints
    for var in variables:
        var_dims = ds[var].dims
        constraint = var
        
        # Add dimension slices in order
        for dim in var_dims:
            if dim == 'time':
                t_start, t_end, t_stride = time_range
                if t_stride == 1:
                    constraint += f"[{t_start}:{t_end}]"
                else:
                    constraint += f"[{t_start}:{t_stride}:{t_end}]"
            
            elif dim == 'lev' and level_range:
                l_start, l_end, l_stride = level_range
                if l_start == l_end:
                    constraint += f"[{l_start}]"
                elif l_stride == 1:
                    constraint += f"[{l_start}:{l_end}]"
                else:
                    constraint += f"[{l_start}:{l_stride}:{l_end}]"
            
            elif dim == 'lat':
                lat_range = spatial_ranges[0]
                lat_start, lat_end, lat_stride = lat_range
                if lat_stride == 1:
                    constraint += f"[{lat_start}:{lat_end}]"
                else:
                    constraint += f"[{lat_start}:{lat_stride}:{lat_end}]"
            
            elif dim == 'lon':
                lon_range = spatial_ranges[1]
                lon_start, lon_end, lon_stride = lon_range
                if lon_stride == 1:
                    constraint += f"[{lon_start}:{lon_end}]"
                else:
                    constraint += f"[{lon_start}:{lon_stride}:{lon_end}]"
        
        constraints.append(constraint)
    
    # Add coordinate variables
    t_start, t_end, t_stride = time_range
    if t_stride == 1:
        constraints.append(f"time[{t_start}:{t_end}]")
    else:
        constraints.append(f"time[{t_start}:{t_stride}:{t_end}]")
    
    if level_range and 'lev' in ds.coords:
        l_start, l_end, l_stride = level_range
        if l_start == l_end:
            constraints.append(f"lev[{l_start}]")
        elif l_stride == 1:
            constraints.append(f"lev[{l_start}:{l_end}]")
        else:
            constraints.append(f"lev[{l_start}:{l_stride}:{l_end}]")
    
    lat_start, lat_end, lat_stride = spatial_ranges[0]
    if lat_stride == 1:
        constraints.append(f"lat[{lat_start}:{lat_end}]")
    else:
        constraints.append(f"lat[{lat_start}:{lat_stride}:{lat_end}]")
    
    lon_start, lon_end, lon_stride = spatial_ranges[1]
    if lon_stride == 1:
        constraints.append(f"lon[{lon_start}:{lon_end}]")
    else:
        constraints.append(f"lon[{lon_start}:{lon_stride}:{lon_end}]")
    
    url += ','.join(constraints)
    
    return url


def estimate_data_size(ds, variables, time_range, level_range, spatial_ranges):
    """Estimate download size"""
    
    t_start, t_end, t_stride = time_range
    n_times = len(range(t_start, t_end + 1, t_stride))
    
    lat_start, lat_end, lat_stride = spatial_ranges[0]
    n_lats = len(range(lat_start, lat_end + 1, lat_stride))
    
    lon_start, lon_end, lon_stride = spatial_ranges[1]
    n_lons = len(range(lon_start, lon_end + 1, lon_stride))
    
    if level_range:
        l_start, l_end, l_stride = level_range
        n_levs = len(range(l_start, l_end + 1, l_stride))
    else:
        n_levs = 1
    
    # Estimate 4 bytes per float32 value
    total_values = len(variables) * n_times * n_levs * n_lats * n_lons
    size_bytes = total_values * 4
    
    # Convert to human readable
    if size_bytes < 1024:
        size_str = f"{size_bytes} B"
    elif size_bytes < 1024**2:
        size_str = f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024**3:
        size_str = f"{size_bytes/1024**2:.1f} MB"
    else:
        size_str = f"{size_bytes/1024**3:.1f} GB"
    
    print(f"\nEstimated data size:")
    print(f"  Variables: {len(variables)}")
    print(f"  Time steps: {n_times}")
    if level_range:
        print(f"  Levels: {n_levs}")
    print(f"  Spatial: {n_lats} × {n_lons}")
    print(f"  Total values: {total_values:,}")
    print(f"  Approximate size: {size_str}")
    
    return size_bytes


def generate_download_scripts(urls, output_dir="downloads"):
    """Generate curl and wget scripts for batch downloading"""
    print_section("Download Script Generation")
    
    # Create output directory name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    script_base = f"geos_fp_download_{timestamp}"
    
    # Generate bash script with curl
    curl_script = f"{script_base}_curl.sh"
    with open(curl_script, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("# GEOS-FP Data Download Script (using curl)\n")
        f.write(f"# Generated: {datetime.now()}\n")
        f.write(f"# Total files: {len(urls)}\n\n")
        f.write(f"# Create output directory\n")
        f.write(f"mkdir -p {output_dir}\n\n")
        
        for i, url in enumerate(urls, 1):
            filename = f"geos_fp_subset_{i:04d}.nc"
            f.write(f"# File {i}/{len(urls)}\n")
            f.write(f'echo "Downloading {filename}..."\n')
            f.write(f'curl -o "{output_dir}/{filename}" "{url}"\n')
            f.write(f'if [ $? -eq 0 ]; then\n')
            f.write(f'    echo "  ✓ Success"\n')
            f.write(f'else\n')
            f.write(f'    echo "  ✗ Failed"\n')
            f.write(f'fi\n\n')
        
        f.write('echo "Download complete!"\n')
    
    # Make executable
    os.chmod(curl_script, 0o755)
    
    # Generate bash script with wget
    wget_script = f"{script_base}_wget.sh"
    with open(wget_script, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("# GEOS-FP Data Download Script (using wget)\n")
        f.write(f"# Generated: {datetime.now()}\n")
        f.write(f"# Total files: {len(urls)}\n\n")
        f.write(f"# Create output directory\n")
        f.write(f"mkdir -p {output_dir}\n\n")
        
        for i, url in enumerate(urls, 1):
            filename = f"geos_fp_subset_{i:04d}.nc"
            f.write(f"# File {i}/{len(urls)}\n")
            f.write(f'echo "Downloading {filename}..."\n')
            f.write(f'wget -O "{output_dir}/{filename}" "{url}"\n')
            f.write(f'if [ $? -eq 0 ]; then\n')
            f.write(f'    echo "  ✓ Success"\n')
            f.write(f'else\n')
            f.write(f'    echo "  ✗ Failed"\n')
            f.write(f'fi\n\n')
        
        f.write('echo "Download complete!"\n')
    
    # Make executable
    os.chmod(wget_script, 0o755)
    
    # Generate Python script
    python_script = f"{script_base}_python.py"
    with open(python_script, 'w') as f:
        f.write("#!/usr/bin/env python3\n")
        f.write('"""GEOS-FP Data Download Script (using Python)"""\n')
        f.write(f"# Generated: {datetime.now()}\n")
        f.write(f"# Total files: {len(urls)}\n\n")
        f.write("import xarray as xr\n")
        f.write("from pathlib import Path\n")
        f.write("import sys\n\n")
        f.write(f"output_dir = Path('{output_dir}')\n")
        f.write("output_dir.mkdir(exist_ok=True)\n\n")
        f.write("urls = [\n")
        for url in urls:
            f.write(f'    "{url}",\n')
        f.write("]\n\n")
        f.write("print(f'Downloading {len(urls)} files...')\n\n")
        f.write("for i, url in enumerate(urls, 1):\n")
        f.write("    filename = output_dir / f'geos_fp_subset_{i:04d}.nc'\n")
        f.write("    print(f'[{i}/{len(urls)}] Downloading {filename.name}...')\n")
        f.write("    try:\n")
        f.write("        ds = xr.open_dataset(url)\n")
        f.write("        ds.load().to_netcdf(filename)\n")
        f.write("        print(f'  ✓ Success')\n")
        f.write("    except Exception as e:\n")
        f.write("        print(f'  ✗ Failed: {e}')\n\n")
        f.write("print('Download complete!')\n")
    
    # Make executable
    os.chmod(python_script, 0o755)
    
    # Generate URL list file
    url_list = f"{script_base}_urls.txt"
    with open(url_list, 'w') as f:
        f.write(f"# GEOS-FP OPeNDAP URLs\n")
        f.write(f"# Generated: {datetime.now()}\n")
        f.write(f"# Total URLs: {len(urls)}\n\n")
        for url in urls:
            f.write(f"{url}\n")
    
    print(f"✓ Generated download scripts:")
    print(f"  - {curl_script} (curl)")
    print(f"  - {wget_script} (wget)")
    print(f"  - {python_script} (Python)")
    print(f"  - {url_list} (URL list)")
    print(f"\nTo use:")
    print(f"  bash {curl_script}")
    print(f"  # or")
    print(f"  bash {wget_script}")
    print(f"  # or")
    print(f"  python {python_script}")
    
    return curl_script, wget_script, python_script, url_list


def main():
    """Main interactive workflow"""
    
    print_header("GEOS-FP OPeNDAP URL Builder")
    print("This tool helps you build subset URLs for GEOS-FP data")
    print("from the NCCS OPeNDAP server.\n")
    
    # Ask if user wants to validate products first
    print("Options:")
    print("  1. Show all products (faster, but some may not be available)")
    print("  2. Validate product availability first (slower, but shows only working products)")
    
    validate = input("\nEnter option (1-2, default=1): ").strip()
    
    if validate == '2':
        available_products = validate_products(show_progress=True)
        products = available_products
    else:
        products = PRODUCTS_CATALOG
    
    # Step 1: Select product
    product = select_product(products)
    
    # Step 2: Select data source (latest or historical)
    source_type, dates = select_data_source(product)
    
    # Step 3: Connect and get info
    ds, base_url = get_dataset_info(product, source_type, dates)
    display_dataset_info(ds)
    
    # Step 4: Select variables
    variables = select_variables(ds)
    
    # Step 5: Time selection
    time_range = get_time_indices(ds)
    
    # Step 6: Level selection (if applicable)
    level_range = get_level_indices(ds, variables)
    
    # Step 7: Spatial selection
    spatial_ranges = get_spatial_indices(ds)
    
    # Build URL(s)
    print_section("Building OPeNDAP URL(s)...")
    
    urls = []
    
    if source_type == 'latest':
        # Single URL for latest data
        url = build_opendap_url(base_url, variables, ds, 
                               time_range, level_range, spatial_ranges)
        urls.append(url)
        
        # Estimate size
        estimate_data_size(ds, variables, time_range, level_range, spatial_ranges)
        
    else:  # historical
        # Multiple URLs for historical data
        print(f"Building URLs for {len(dates)} date(s)...")
        
        for date in dates:
            base_path, date_str = get_historical_files(product, date)
            # Use the base_url from the connection test
            date_base = base_url.rsplit('.nc', 1)[0] if '.nc' in base_url else base_url
            url = build_opendap_url(date_base, variables, ds,
                                   time_range, level_range, spatial_ranges)
            urls.append(url)
        
        print(f"✓ Generated {len(urls)} URLs")
        
        # Estimate total size
        size_per_file = estimate_data_size(ds, variables, time_range, 
                                          level_range, spatial_ranges)
        total_size = size_per_file * len(urls)
        
        if total_size < 1024**3:
            total_str = f"{total_size/1024**2:.1f} MB"
        else:
            total_str = f"{total_size/1024**3:.1f} GB"
        
        print(f"\nTotal estimated size for all files: {total_str}")
    
    # Display final URL(s)
    print_header("Generated OPeNDAP URL(s)")
    
    if len(urls) == 1:
        print(urls[0])
        print()
    else:
        print(f"Generated {len(urls)} URLs:\n")
        for i, url in enumerate(urls[:5], 1):  # Show first 5
            print(f"{i}. {url}")
        if len(urls) > 5:
            print(f"... and {len(urls) - 5} more")
        print()
    
    # Export options
    print_section("Export Options")
    
    print("Would you like to:")
    print("  1. Test download the data")
    print("  2. Generate download scripts (curl/wget/Python)")
    print("  3. Save URLs to file")
    print("  4. All of the above")
    print("  5. Skip")
    
    choice = input("\nEnter option (1-5): ").strip()
    
    if choice in ['1', '4']:
        # Test download
        print("\nTesting connection...")
        try:
            test_ds = xr.open_dataset(urls[0])
            print("✓ Success! URL is valid and accessible.")
            print(f"\nDataset preview:")
            print(test_ds)
            
            if len(urls) == 1:
                save = input("\nSave data to file? (y/n): ").strip().lower()
                if save == 'y':
                    filename = input("Filename (default: subset.nc): ").strip()
                    filename = filename if filename else "subset.nc"
                    print(f"Downloading to {filename}...")
                    test_ds.load().to_netcdf(filename)
                    print(f"✓ Saved to {filename}")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    if choice in ['2', '4']:
        # Generate scripts
        generate_download_scripts(urls)
    
    if choice in ['3', '4']:
        # Save URLs
        filename = input("\nURL list filename (default: urls.txt): ").strip()
        filename = filename if filename else "urls.txt"
        with open(filename, 'w') as f:
            f.write(f"# GEOS-FP OPeNDAP URLs\n")
            f.write(f"# Generated: {datetime.now()}\n\n")
            for url in urls:
                f.write(f"{url}\n")
        print(f"✓ Saved {len(urls)} URL(s) to {filename}")
    
    print("\n" + "="*70)
    print("Done!")
    print("="*70 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
