import sys
import os
import time
import multiprocessing
import itertools
import argparse

import useful
import intersect_bed 
import intersect_vcf
import intersect_ez

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
 
#### RUN: spark-submit --conf "spark.local.dir=$TMPDIR"  --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$TMPDIR"  --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$TMPDIR"



# Set aside some cores to run Java.
# Actually...not needed for the intersect as it doesn't do much
# in the way of launching Python sub-processes
JAVA_RES_CORES=0

# These could go into a common file to be used when creating the files too.
TFBS_PQ='tfbs.pq'
TF_INFO_PQ='tf_info.pq'

def get_n_cores():
    ''' Get a reasonable number of cores to run with'''
    if 'NSLOTS' in os.environ:
        ncores = int(os.environ['NSLOTS'])
    else:
        ncores = multiprocessing.cpu_count()
    # Reserve some cores for Java
    ncores = ncores - JAVA_RES_CORES
    if ncores <= 0:
        return 1
    return ncores


def load_dataframes(spark, tfbs_file, tf_info_file):
    ''' Load the data from the disk into Spark DataFrames '''
    tfbs = spark.read.parquet(tfbs_file)
    tf_info = spark.read.parquet(tf_info_file)
    tfbs.registerTempTable('tfbs')
    tf_info.registerTempTable('tf_info')
    return tfbs, tf_info


@useful.timeit
def show_df(df):
    print(df.show())

    
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-pq_dir', help="Parquet files directory")
    parser.add_argument('-sample_file', help="File to use for sampling")
    parser.add_argument('-output_csv', help='Output CSV file')
    args = parser.parse_args()
    
    pq_dir = args.pq_dir
    sample_file = args.sample_file
    output_csv = args.output_csv
    # If the input file isn't here then just quit.
    # Spark will stop on its own if the parquet files aren't found.
    if not os.path.exists(sample_file):
        sys.stderr.write('File not found! %s' % sample_file)
        exit(-99)
    # Fire up Spark and read in the bed file.    
    spark = SparkSession.builder.master('local[%s]' % get_n_cores()).appName('tfbs-pq').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # Load the raw data frames and define them as Spark SQL tables.
    tfbs, tf_info = load_dataframes(spark,os.path.join(pq_dir,TFBS_PQ),
                                    os.path.join(pq_dir,TF_INFO_PQ))
    # Is this a .bed or a .vcf file?
    name, ext = os.path.splitext(sample_file)
    ext = ext.lower()
    if ext == '.bed':
        intersect = intersect_bed.IntersectBed(sample_file,spark)
    if ext == '.vcf':
        intersect = intersect_vcf.IntersectVCF(sample_file,spark)
    if ext == '.ez':
        intersect = intersect_ez.IntersectEz(sample_file,spark)

    # The loaded sample file has also been defined as a Spark SQL table
    # in its initializer.
    intersect.intersection_query(spark)
    # Write out the results.
    intersect.write_df(output_csv)
 
