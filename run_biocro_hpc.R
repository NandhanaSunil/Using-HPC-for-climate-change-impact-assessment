my_lib <- "/storage/home/u112201008/climate_data_crop_models/library"
library(ncdf4, lib.loc = my_lib)
library(dplyr, lib.loc = my_lib)
library(tibble, lib.loc = my_lib)
library(BioCro, lib.loc = my_lib)
library(parallel)

# Define indices and dates

# latitude indices for India
lat_idx <- 1:61  
# longitude indices for India
lon_idx <- 1:61  
# time indices
time_idx <- 1:120   

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
  "/storage/home/u112201008/climate_data_crop_models/input_files/gfdl-esm4_r1i1p1f1_w5e5_ssp585_rlds_lat7.25to37.25lon67.75to97.75_daily_2021_2030.nc",
  "/storage/home/u112201008/climate_data_crop_models/input_files/gfdl-esm4_r1i1p1f1_w5e5_ssp585_rsds_lat7.25to37.25lon67.75to97.75_daily_2021_2030.nc",
  "/storage/home/u112201008/climate_data_crop_models/input_files/gfdl-esm4_r1i1p1f1_w5e5_ssp585_tasmax_lat7.25to37.25lon67.75to97.75_daily_2021_2030.nc",
  "/storage/home/u112201008/climate_data_crop_models/input_files/gfdl-esm4_r1i1p1f1_w5e5_ssp585_hurs_lat7.25to37.25lon67.75to97.75_daily_2021_2030.nc",
  "/storage/home/u112201008/climate_data_crop_models/input_files/gfdl-esm4_r1i1p1f1_w5e5_ssp585_sfcwind_lat7.25to37.25lon67.75to97.75_daily_2021_2030.nc",
  "/storage/home/u112201008/climate_data_crop_models/input_files/gfdl-esm4_r1i1p1f1_w5e5_picontrol_pr_lat7.25to37.25lon67.75to97.75_daily_2021_2030.nc"
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
  result_yield <- list(grid_key = grid_key, yield = yield, grid_data = grid_data)
  return(result_yield)
}

# Setup parallel backend

# Detect the total available cores
num_cores <- detectCores()
# Uncomment the following line to vary the number of cores (This should be a number less than or equal to the total available cores)
# num_cores <- 10
cl <- makeCluster(num_cores)
# Define the necessary objects that should be exported to the cluster 
required_objects <- c("my_lib", "lat_idx", "lon_idx", "time_idx", "year", "day", "zen", "solar", "netsolar", "day_length",
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
  library(ncdf4, lib.loc = my_lib)
  library(dplyr, lib.loc = my_lib)
  library(tibble, lib.loc = my_lib)
  library(BioCro, lib.loc = my_lib)
  
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
grid_data_list <- list()
for (res in results) {
  for (grid_res in res) {
    grid_data_list[[grid_res$grid_key]] <- grid_res$yield
  }
}

# Print the resulting list of dataframes for each grid
# print(grid_data_list)
# Write the obtained result as a csv file. (Here, the result for lat_indx =1 and lon_idx = 60 is accessed using the key lat_1_lon_60 and written to a csv file)
write.csv(grid_data_list[['lat_1_lon_60']], "/storage/home/u112201008/climate_data_crop_models/output_files/yield_1_60.csv")

# Compute the time elapsed for the running
end_time <- Sys.time()
elapsed_time <- end_time - start_time
elapsed_seconds <- as.numeric(elapsed_time, units = "secs")
print(print(paste("Elapsed time:", round(elapsed_seconds, digits = 2))))
