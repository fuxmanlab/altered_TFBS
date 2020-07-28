import sys
import argparse
import os

 

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
 
 
def load_rdds(spark, tfbs_file, tf_info_file):
    ''' Load the data from the disk into Spark RDDs '''
    tfbs = spark.read.parquet(tfbs_file)
    tf_info = spark.read.parquet(tf_info_file)
    return tfbs, tf_info
    
    
TFBS_PQ='tfbs.pq'
TF_INFO_PQ='tf_info.pq'
    
    
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-pq_dir', help="Parquet files directory")

    args = parser.parse_args()
   
    pq_dir = args.pq_dir
    spark = SparkSession.builder.master('local[*]').appName('tfbs-pq').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # Load the raw data frames and define them as Spark SQL tables.
    tfbs, tf_info = load_rdds(spark,os.path.join(pq_dir,TFBS_PQ),
                                    os.path.join(pq_dir,TF_INFO_PQ))
                                    
    # Return the number of rows in each.
    sys.stdout.write('Counting rows in tf_info: ') ; sys.stdout.flush()
    sys.stdout.write('%s\n' % tf_info.count())
    sys.stdout.flush()
    sys.stdout.write('Counting rows in tfbs: ') ; sys.stdout.flush()
    sys.stdout.write('%s\n' % tfbs.count())
    sys.stdout.flush()    
    # Done!
