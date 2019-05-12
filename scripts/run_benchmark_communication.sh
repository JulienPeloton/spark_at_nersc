#!/bin/bash

spark-submit \
  --master local[*] \
  --packages com.github.astrolabsoftware:spark-fits_2.11:0.7.3,com.github.astrolabsoftware:spark3d_2.11:0.3.1 \
  --jars ../lib/jhealpix.jar \
  benchmark_communication.py \
  -inputpath  ../test_data/cartesian_points.fits \
  -nloops 100 \
  -log_level ERROR
