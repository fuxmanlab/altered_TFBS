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


#### This is not called on Hadoop!
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
    # Avoid the default partitioning of the dataframe into 200 partitions.
    tf_info = spark.read.parquet(tf_info_file)
    nrows = tf_info.count()
    # Re-partition
    tf_info = tf_info.repartition(nrows)
    # Keep the whole thing in RAM.
    tf_info.cache()

    # Open the tfbs parquet file.
    # This is already partitioned on disk by pwmid.
    tfbs = spark.read.parquet(tfbs_file)

    # Adjust the number of partitions in Spark.SQL
    spark.conf.set("spark.sql.shuffle.partitions", str(nrows))
                                                          
    tf_info.registerTempTable('tf_info')
    tfbs.registerTempTable('tfbs')
    return tfbs, tf_info


@useful.timeit
def show_df(df):
    print(df.show())

    
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-pq_dir', help="Parquet files directory")
    parser.add_argument('-output_csv', help='Output CSV file')
    args = parser.parse_args()
    
    pq_dir = args.pq_dir
    output_csv = args.output_csv
     
    # Fire up Spark and read in the bed file.    
    spark = SparkSession.builder.appName('tfbs-pq').getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    # Print out the amount of parallelism
    #print('****   Spark parallelism: %s' % spark.sparkContext.defaultParallelism)
    # Print number of executors
    #print('****   Executors: %s' % spark.sparkContext._conf.get('spark.executor.instances'))

    # Load the raw data frames and define them as Spark SQL tables.
    tfbs, tf_info = load_dataframes(spark,os.path.join(pq_dir,TFBS_PQ),
                                    os.path.join(pq_dir,TF_INFO_PQ))
    # Is this a .bed or a .vcf file?
 
    #intersect = intersect_bed.IntersectBed(sample_file,spark,hadoop=True)
    query='''SELECT *
                FROM tfbs
                WHERE tfbs.chromosome=-2'''
    intersect_df = spark.sql(query)
    intersect_df.show()
    # The loaded sample file has also been defined as a Spark SQL table
    print("location_123")
    # in its initializer.
    print("location_456",intersect_df)
    # Write out the results.
    #intersect.write_df(output_csv, \
    #      npartitions=int(spark.sparkContext._conf.get('spark.executor.instances')))

 
