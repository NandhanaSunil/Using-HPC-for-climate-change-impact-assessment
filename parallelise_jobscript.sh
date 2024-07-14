#!/bin/bash
#PBS -N BioCro
#PBS -q tiny
#PBS -l walltime=02:00:00
#PBS -l select=1:ncpus=1
#PBS -J 1-24                                        # This creates a job array with 24 jobs
#PBS -j oe
#PBS -o output.log
#PBS -m abe
#PBS -M email@gmail.com

# Change to the directory from which the job was submitted
cd $PBS_O_WORKDIR

# Load the necessary modules
module load R/4.1.0
module load netcdf/gcc/64/4.4.0

# Define the R script path
R_SCRIPT="path/to/the/script/run_biocro_hpc.R"

# Pass the PBS_ARRAY_INDEX to the R script to handle different subsets of data
Rscript $R_SCRIPT $PBS_ARRAY_INDEX
