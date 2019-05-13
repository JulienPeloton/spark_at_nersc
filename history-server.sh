#!/bin/bash

module load spark/spark-history

cd /global/cscratch1/sd/${USER}/spark/event_logs

run_history_server.sh 

cd -
