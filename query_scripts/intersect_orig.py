import sys
import os
import time
import multiprocessing
import itertools
import argparse

import useful


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

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


def bed_to_df(spark, sample_file):
    ''' Read a bed file into a Spark dataframe '''
    bFile = spark.sparkContext.textFile(sample_file) 
    def bedFilter(line):
        ''' just need chromosome position, start, and stop '''
        vals = line.split()
        chromosome_pos = vals[0]
        start = int(vals[1])
        stop = int(vals[2])
        return tuple(zip(itertools.repeat(chromosome_pos),range(start,stop+1)))
    labels = ('chromosome','mutation_pos')
    df = bFile.map(bedFilter).flatMap(lambda x:x).toDF(labels)
    return df
   
   
def load_dataframes(spark, tfbs_file, tf_info_file, sample_file):
    ''' Load the data from the disk into Spark DataFrames '''
    tfbs = spark.read.parquet(tfbs_file)
    tf_info = spark.read.parquet(tf_info_file)
    bed = bed_to_df(spark,sample_file) 
    tfbs.registerTempTable('tfbs')
    tf_info.registerTempTable('tf_info')
    bed.registerTempTable('bed')
    return tfbs, tf_info, bed


@useful.timeit
def mutation_range(bed):
    vals = spark.sql("SELECT MIN(start) as start_pos, MAX(stop) as stop_pos FROM bed").collect()[0].asDict()
    start_pos = vals['start_pos']
    stop_pos = vals['stop_pos']
    return start_pos, stop_pos

def intersection_query(tfbs,tf_info,bed):
    ''' Intersect the query bedfile with the tfbs data'''
    # Get a data range
    #start_pos, stop_pos = mutation_range(bed)
    intersect = spark.sql('SELECT tfbs.chromosome,pwmid,wt_max_score FROM tfbs JOIN bed ON tfbs.chromosome=bed.chromosome WHERE tfbs.mutation_pos=bed.mutation_pos')
    return intersect  

@useful.timeit
def show_df(df):
    print(df.show())
    
@useful.timeit
def write_df(df, output_csv):
    ''' Convert to a Pandas dataframe and then write to the output csv file.'''  
    #Recommended: write to $TMPDIR then copy to destination.
    pd_df = df.toPandas()
    print('Pandas dataframe shape: (%s,%s)' % pd_df.shape)
    print('Writing to %s' % output_csv)
    pd_df.to_csv(output_csv)
    
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
    # Set the number of cores to the allowed number minus JAVA_RES_CORES
    spark = SparkSession.builder.master('local[%s]' % get_n_cores()).appName('tfbs-pq').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    tfbs, tf_info, bed = load_dataframes(spark,os.path.join(pq_dir,TFBS_PQ),os.path.join(pq_dir,TF_INFO_PQ),sample_file)
    query_res=intersection_query(tfbs,tf_info,bed)
    write_df(query_res, output_csv)
    
