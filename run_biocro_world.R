start_time <- Sys.time()
# location of the library
#my_lib <- "/storage/home/u112201008/climate_data_crop_models/library"
library(ncdf4)
library(dplyr)
library(tibble)
library(BioCro)
library(parallel)

# Define indices and dates

# latitude indices for India
lat_idx <- 1:359
# longitude indices for India
lon_idx <- 1:719
# time indices
time_idx <- 210:211   

# Create necessary vectors (here I have replicated values for twenty four hours of a day)
year <- rep(2021, each = 24 * length(time_idx))       
day <- rep(time_idx, each = 24)
zen <- rep(117.0, each = 24 * length(time_idx))
solar <- rep(24.5, each = 24 * length(time_idx))
netsolar <- rep(20.80, each = 24 * length(time_idx))
day_length <- rep(14.4, each = 24 * length(time_idx))
time_zone_offset <- rep(5.30, each = 24 * length(time_idx))
hour_index <- rep(0:23, times = length(time_idx))

# List of NetCDF files
file_list <- list(
"D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_rlds_landonly_daily_2021_2030.nc",
"D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_rsds_landonly_daily_2021_2030.nc",
"D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_tasmax_landonly_daily_2021_2030.nc",
"D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_hurs_landonly_daily_2021_2030.nc",
"D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_sfcwind_landonly_daily_2021_2030.nc",
"C:\\Users\\nandh\\Downloads\\isimip-download-2cfcdcb6e30c17b9082c5094659204f76efb7350\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_pr_landonly_daily_2021_2030.nc"
)

# List of headings for columns of a dataframe 

# This matches the variables with the columns of the dataframe as required in BioCro. 
column_heads <- list('rlds' = 'dw_solar', 'rsds' = 'up_solar', 'tasmax' = 'temp', 'hurs' = 'rh', 'sfcwind' = 'windspeed', 'pr' = 'precip')  
                                                                                                                                            
# Function to extract data for a specific variable and grid
extract_grid_data <- function(nc, var, lat_idx, lon_idx, dataframe) {
  # If the variable is present in dataset, extract the data
  if (var %in% names(nc$var)) {                                                   
    var_data <- ncvar_get(nc, var, start = c(lon_idx, lat_idx, min(time_idx)), count = c(1, 1, length(time_idx)))
    # Relative humidity is available in the units of percentage, BioCro needs it as a value from 0 to 1. Percentage is converted to 0 to 1.
    if (var == 'hurs') {
      var_data <- var_data / 100
    }
    else if (var  == 'pr'){
      var_data <- var_data*1000}
    else if (var == 'tasmax'){
      var_data <- var_data - 273
    }
    #  Repeat the daily data twenty four times ( to simulate for twenty four hours of a day ).
    var_data_hourly <- rep(var_data, each = 24)
    # Add the extracted data as a column to the required dataframe
    dataframe <- dataframe %>% mutate(!!sym(column_heads[[var]]) := var_data_hourly)
    return(dataframe)
  } else {
    stop(sprintf("Error: %s is not found in the given dataset", var))  }
}

# Function to process a single grid cell
process_grid_cell <- function(i, j) {
  # Create a key for each latitude and longitude pair to access the results with this key.
  # For example, the grid key for latitude index 1 and longitude index 60 will be lat_1_lon_60
  grid_key <- paste("lat", i, "lon", j, sep = "_")
  # Create a dataframe and add values to the first few columns
  grid_data <- tibble(year = year, doy = day, hour = hour_index, zen = zen, solar = solar, netsolar = netsolar)
  for (file in file_list) {
    # open the file
    nc <- nc_open(file)
    # Get the name of the variable other than lat, lon and time from the file. 
    variables <- setdiff(names(nc$var), c("lat", "lon", "time"))
    #  Extract the data 
    for (var in variables) {
      grid_data <- extract_grid_data(nc, var, i, j, grid_data)
    }
    nc_close(nc)
  }
  # Add the final columns to the dataframe
  grid_data <- grid_data %>% mutate(day_length = day_length, time_zone_offset = time_zone_offset)
  #  Update the values of latitude and longitude in the parameters (Here the latitude and longitude indices are passed on)
  soybean$parameters$lon <- i
  soybean$parameters$lat <- j
  #  Run BioCro to get the results
  result <- run_biocro(soybean$initial_values, soybean$parameters, grid_data,
                       soybean$direct_modules, soybean$differential_modules, soybean$ode_solver)
  # Get the data for Grain
  yield <- result$Grain
  # Return the grid_key, the yield(result$Grain) and grid_data (the input dataframe for the given latitude and longitude)
  result_yield <- list(grid_key = grid_key, yield = yield)
  return(result_yield)
}

# Setup parallel backend

# Detect the total available cores
num_cores <- detectCores()
# Uncomment the following line to vary the number of cores (This should be a number less than or equal to the total available cores)
num_cores <- num_cores - 1
cl <- makeCluster(num_cores)
# Define the necessary objects that should be exported to the cluster 
required_objects <- c("lat_idx", "lon_idx", "time_idx", "year", "day", "zen", "solar", "netsolar", "day_length",
                      "time_zone_offset", "hour_index", "file_list", "column_heads", "extract_grid_data", "process_grid_cell")

# Check if the objects exist
missing_objects <- required_objects[!sapply(required_objects, exists)]
if (length(missing_objects) > 0) {
  stop(paste("The following objects are missing:", paste(missing_objects, collapse = ", ")))
}

# Export the objects to the worker nodes
# Here the objects are exported from the Global environment
clusterExport(cl, required_objects, envir = .GlobalEnv)

# Load the libraries in the worker nodes
clusterEvalQ(cl, {
  library(ncdf4)
  library(dplyr)
  library(tibble)
  library(BioCro)
  
  # Check if the libraries are loaded
  list(
    ncdf4 = requireNamespace("ncdf4", quietly = TRUE),
    dplyr = requireNamespace("dplyr", quietly = TRUE),
    tibble = requireNamespace("tibble", quietly = TRUE),
    BioCro = requireNamespace("BioCro", quietly = TRUE)
  )
})

# Run the processing in parallel
results <- parLapply(cl, lat_idx, function(i) {
  lapply(lon_idx, function(j) {
    process_grid_cell(i, j)
  })
})

# Stop the cluster
stopCluster(cl)

# Combine results
file <- file_list[[1]]
nc <- nc_open(file)
lon <- ncvar_get(nc, "lon")
lat <- ncvar_get(nc, "lat")
dim_lon <- ncdim_def("lon", "degrees_east", lon)
dim_lat <- ncdim_def("lat", "degrees_north", lat)
yield <- ncvar_def("yield", "kg", list(dim_lon, dim_lat), -9999, longname="Yield at the end of growing season")
output_file <- "C:\\Users\\nandh\\OneDrive\\Desktop\\ClimateData_IITPKD_HPC\\output_files\\yield_world_2021laptop.nc"
nc_new <- nc_create(output_file, list(yield))

# Combine results
grid_data_list <- list()
for (res in results) {
  for (grid_res in res) {
    grid_data_list <-c(grid_data_list, grid_res$yield[length(time_idx)*24]) 
  }
}
yield_val <- matrix(grid_data_list, nrow = length(lat_idx), ncol = length(lon_idx), byrow = TRUE)
ncvar_put(nc_new, yield, yield_val)
ncatt_put(nc_new, "lon", "units", "degrees_east")
ncatt_put(nc_new, "lat", "units", "degrees_north")
nc_close(nc_new)
nc_close(nc)


# Compute the time elapsed for the running
end_time <- Sys.time()
elapsed_time <- end_time - start_time
elapsed_seconds <- as.numeric(elapsed_time, units = "secs")
print(print(paste("Elapsed time:", round(elapsed_seconds, digits = 2))))

