#!/bin/bash
#SBATCH -p debug
#SBATCH -N 2
#SBATCH -t 00:30:00
#SBATCH -J sparkFITS
#SBATCH -C haswell
#SBATCH --image=nersc/spark-2.3.0:v1
#SBATCH -A m1727

module load spark

# dataset is 3.5 GB, or ~150 million points
start-all.sh
shifter spark-submit \
  --master $SPARKURL \
  --driver-memory 15g --executor-memory 50g --executor-cores 32 --total-executor-cores 64 \
  --packages com.github.astrolabsoftware:spark-fits_2.11:0.7.3,com.github.astrolabsoftware:spark3d_2.11:0.3.1 \
  --jars ../lib/jhealpix.jar \
  --py-files ../lib/pyspark3d-0.3.1-py3.6.egg \
  benchmark_communication.py \
  -inputpath  /global/cscratch1/sd/peloton/LSST10Y_striped_small/out_srcs_s1_0.fits \
  -nloops 10 \
  -log_level ERROR
stop-all.sh
