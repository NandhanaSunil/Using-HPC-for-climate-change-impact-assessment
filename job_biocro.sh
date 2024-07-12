#!/bin/bash
#PBS -N BioCro
#PBS -q tiny
#PBS -l walltime=02:00:00
#PBS -l select=1:ncpus=24 
#PBS -j oe
#PBS -o output.log
cd $PBS_O_WORKDIR 
module load R/4.1.0
module load netcdf/gcc/64/4.4.0

Rscript "/storage/home/u112201008/climate_data_crop_models/codes/run_biocro_hpc.R"