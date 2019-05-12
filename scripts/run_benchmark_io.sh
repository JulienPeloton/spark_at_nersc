#!/bin/bash
#SBATCH -p debug
#SBATCH -N 10
#SBATCH -t 00:15:00
#SBATCH -J sparkFITS
#SBATCH -C haswell
#SBATCH --image=nersc/spark-2.3.0:v1
#SBATCH -A m1727

module load spark

# dataset is 116 GB
# On Cori, each loop takes approx 30 seconds to read the data on 320 cores
start-all.sh
shifter spark-submit \
  --master $SPARKURL \
  --driver-memory 15g --executor-memory 50g --executor-cores 32 --total-executor-cores 320 \
  --packages com.github.astrolabsoftware:spark-fits_2.11:0.7.3 \
  benchmark_io.py \
  -inputpath /global/cscratch1/sd/peloton/LSST10Y_striped_small  \
  -nloops 10 \
  -log_level ERROR
stop-all.sh
