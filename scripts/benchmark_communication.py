# Copyright 2019 Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from pyspark.sql import SparkSession

from pyspark3d.repartitioning import prePartition
from pyspark3d.repartitioning import repartitionByCol

import argparse
from time import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import pandas as pd

def quiet_logs(sc, log_level="ERROR"):
    """
    Set the level of log in Spark.

    Parameters
    ----------
    sc : SparkContext
        The SparkContext for the session
    log_level : String [optional]
        Level of log wanted: INFO, WARN, ERROR, OFF, etc.

    """
    ## Get the logger
    logger = sc._jvm.org.apache.log4j

    ## Set the level
    level = getattr(logger.Level, log_level, "INFO")

    logger.LogManager.getLogger("org"). setLevel(level)
    logger.LogManager.getLogger("akka").setLevel(level)

def addargs(parser):
    """ Parse command line arguments for benchmark_communication """

    ## Arguments
    parser.add_argument(
        '-inputpath', dest='inputpath',
        required=True,
        help='Path to a FITS file or a directory containing FITS files')

    parser.add_argument(
        '-nloops', dest='nloops',
        required=True, type=int,
        help='Number of times to run the benchmark.')

    parser.add_argument(
        '-log_level', dest='log_level',
        default="ERROR",
        help='Level of log for Spark. Default is ERROR.')


if __name__ == "__main__":
    """
    Test communication performances by re-partitioning data.
    """
    parser = argparse.ArgumentParser(
        description="""
        Test communication performances by re-partitioning data.""")
    addargs(parser)
    args = parser.parse_args(None)

    spark = SparkSession\
        .builder\
        .getOrCreate()

    ## Set logs to be quiet
    quiet_logs(spark.sparkContext, log_level=args.log_level)

    ## FITS
    df = spark.read.format("fits")\
        .option("hdu", 1)\
        .load(args.inputpath)

    options = {
        "geometry": "points",
        "colnames": "Z_COSMO,RA,DEC",
        "coordSys": "spherical",
        "gridtype": "onion"
    }

    # Add a column containing the future partition ID.
    # Note that no shuffle has been done yet.
    df_colid = prePartition(df, options, numPartitions=64)

    ## Burning time
    for loop in range(2):
        df_repart = repartitionByCol(
            df_colid, "partition_id", preLabeled=True, numPartitions=64)
        df_repart.count()

    ## Record times
    outputs = {"Repartitioning": []}
    for loop in range(args.nloops):
        print(loop)
        t0 = time()
        df_repart = repartitionByCol(
            df_colid, "partition_id", preLabeled=True, numPartitions=8)
        df_repart.count()
        outputs["Repartitioning"].append(time() - t0)

    pdf = pd.DataFrame(outputs)

    ## Plot the results
    fig, ax = plt.subplots(figsize=(7.5, 5.5))
    pdf.plot.hist(ax=ax, bins=20, alpha=0.5)
    plt.xlabel("Time (s)")
    plt.ylabel("count")
    plt.title("{} loops".format(args.nloops))
    plt.savefig("benchmark_communication.png")
    print("File saved: benchmark_communication.png")
