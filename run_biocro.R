# install.packages("BioCro")

library(BioCro)
start_time <- Sys.time()
#result = run_biocro(soybean$initial_values, soybean$parameters, soybean_weather$'2002',
                           #soybean$direct_modules, soybean$differential_modules, soybean$ode_solver)
parameters <- soybean$parameters
weatherb <- soybean_weather
yield = result$Grain
end_time <- Sys.time()
elapsed_time <- as.numeric(end_time - start_time)
# write.csv(yield, "yield.csv")

