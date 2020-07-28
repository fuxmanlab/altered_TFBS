#  Convert the .bed files and the tfinfo.csv file to
#  Spark Parquet format files.
#
#  Must run via Spark.
#
#  6/6/2019 - Brian Gregor, Research Computing Services, Boston University
#
#


import time
import os
import sys
import argparse

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Script to read in the tfbs.pq file and re-save it with new partitions.
# Aiming for ~1 GB file sizes.  
#
# The data set has ~16e9 rows. Partition the output file by the pwmid
# which should be pretty optimal as that's where the query joins happen.

#  This should be a big improvement
# over the original 19000+ partitions for I/O with the dataset.  
#
# Run on Hadoop with the original file tfbs.pq as input, which has ~19000 files.

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-infile', required=True, help="Filename of incoming tfbs.pq.")
    parser.add_argument('-infofile', required=True, help="Filename of incoming tf_info.pq.")
    parser.add_argument('-outfile', required=True, help='Output filename for repartitioned tfbs.pq')
    args = parser.parse_args()    
    
    # Fire up Spark using most available cores.  Using a few less helps balance the
    # Python/Java CPU contention.  
    #  Use this if this were to be run on a non-hadoop node
    #spark = SparkSession.builder.master('local[%s]' % get_n_cores(reserve=4)).appName('tfbs-pq').getOrCreate()
    spark = SparkSession.builder.appName('tfbs-pq').getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("Info")
    infile=args.infile
    outfile=args.outfile
    infofile=args.infofile
    print('Input file: %s' % infile)
    print('Output file after partitioning: %s' % outfile)

    # Get the number of rows in the tf_info
    tf_info = spark.read.parquet(infofile)
    nrows = tf_info.count()

    # Set the number of shuffle partitions to match
    spark.conf.set("spark.sql.shuffle.partitions", str(nrows))

    # Read tfbs.pq to a dataframe and repartition to NCHUNKS chunks
    tfbs = spark.read.parquet(infile)
    # Write it out
    tfbs.repartition('pwmid').write.partitionBy('pwmid').option("compression","snappy").parquet(outfile)
