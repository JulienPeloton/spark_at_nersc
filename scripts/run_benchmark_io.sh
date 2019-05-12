#!/bin/bash

spark-submit \
  --master local[*] \
  --packages com.github.astrolabsoftware:spark-fits_2.11:0.7.3 \
  benchmark_io.py \
  -inputpath  ../test_data/test_file.fits \
  -nloops 100 \
  -log_level ERROR
