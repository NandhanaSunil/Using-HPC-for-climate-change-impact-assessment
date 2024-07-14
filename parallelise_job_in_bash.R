# Get the array index from the command line argument
args <- commandArgs(trailingOnly = TRUE)
array_index <- as.integer(args[1])

# Define the range of indices each job will handle
num_jobs <- 24  # Number of jobs in the array
total_indices <- 720  # Total number of longitude indices

# Compute the subset of indices for this job
indices_per_job <- ceiling(total_indices / num_jobs)
start_idx <- (array_index - 1) * indices_per_job + 1
end_idx <- min(array_index * indices_per_job, total_indices)

# Subset the longitude indices for this job
lon_idx_subset <- lon_idx[start_idx:end_idx]

# The rest of your code remains mostly unchanged, but use lon_idx_subset instead of lon_idx
lat_idx <- 1:360  # Latitude indices for India
time_idx <- 210:211  # Time indices

# Create necessary vectors for 24 hours of a day
n_hours <- 24
n_days <- length(time_idx)
n_total <- n_hours * n_days

year <- rep(2021, n_total)
day <- rep(time_idx, each = n_hours)
zen <- rep(117.0, n_total)
solar <- rep(24.5, n_total)
netsolar <- rep(20.80, n_total)
day_length <- rep(14.4, n_total)
time_zone_offset <- rep(5.30, n_total)
hour_index <- rep(0:23, n_days)

# List of NetCDF files
file_list <- list(
  "D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_rlds_landonly_daily_2021_2030.nc",
  "D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_rsds_landonly_daily_2021_2030.nc",
  "D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_tasmax_landonly_daily_2021_2030.nc",
  "D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_hurs_landonly_daily_2021_2030.nc",
  "D:\\weather data for parallel processing\\LandOnly\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_sfcwind_landonly_daily_2021_2030.nc",
  "C:\\Users\\nandh\\Downloads\\isimip-download-2cfcdcb6e30c17b9082c5094659204f76efb7350\\gfdl-esm4_r1i1p1f1_w5e5_ssp585_pr_landonly_daily_2021_2030.nc"
)

# Column headers matching the variables in BioCro
column_heads <- list('rlds' = 'dw_solar', 'rsds' = 'up_solar', 'tasmax' = 'temp', 'hurs' = 'rh', 'sfcwind' = 'windspeed', 'pr' = 'precip')

# Function to extract data for a specific variable and grid
extract_grid_data <- function(nc, var, lat_idx, lon_idx, dataframe) {
  if (var %in% names(nc$var)) {
    var_data <- ncvar_get(nc, var, start = c(lon_idx, lat_idx, min(time_idx)), count = c(1, 1, length(time_idx)))
    var_data <- switch(var,
                       'hurs' = var_data / 100,
                       'pr' = var_data * 1000,
                       'tasmax' = var_data - 273,
                       var_data)
    var_data_hourly <- rep(var_data, each = 24)
    dataframe <- dataframe %>% mutate(!!sym(column_heads[[var]]) := var_data_hourly)
    return(dataframe)
  } else {
    stop(sprintf("Error: %s is not found in the given dataset", var))
  }
}

# Function to process a single grid cell
process_grid_cell <- function(i, j) {
  grid_key <- paste("lat", i, "lon", j, sep = "_")
  grid_data <- tibble(year = year, doy = day, hour = hour_index, zen = zen, solar = solar, netsolar = netsolar)
  for (file in file_list) {
    nc <- nc_open(file)
    variables <- setdiff(names(nc$var), c("lat", "lon", "time"))
    for (var in variables) {
      grid_data <- extract_grid_data(nc, var, i, j, grid_data)
    }
    nc_close(nc)
  }
  grid_data <- grid_data %>% mutate(day_length = day_length, time_zone_offset = time_zone_offset)
  soybean$parameters$lon <- i
  soybean$parameters$lat <- j
  result <- run_biocro(soybean$initial_values, soybean$parameters, grid_data,
                       soybean$direct_modules, soybean$differential_modules, soybean$ode_solver)
  list(grid_key = grid_key, yield = result$Grain)
}

# Process the subset of grid cells for this job
results <- list()
for (i in lat_idx) {
  for (j in lon_idx_subset) {
    results[[paste("lat", i, "lon", j, sep = "_")]] <- process_grid_cell(i, j)
  }
}

# Save or return results as needed
saveRDS(results, file = paste0("results_", array_index, ".rds"))

# Combine results from all job array tasks
all_results <- list()
for (i in 1:24) {
  result_file <- paste0("results_", i, ".rds")
  if (file.exists(result_file)) {
    results <- readRDS(result_file)
    all_results <- c(all_results, results)
  }
}



