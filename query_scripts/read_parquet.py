import pyspark
from pyspark import SparkContext
import time
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql import functions as F
 

 
 

if __name__=='__main__':
    spark = SparkSession.builder.master('local[22]').appName('tfbs-pq').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)
    tfbs = sqlContext.read.parquet('parquet/tfbs.pq')
    #tfbs.cache()
    s=time.time() ; z =tfbs.count()  ; e=time.time() ; print('count %s in %s sec' % (z,e-s))
    s=time.time() ; total = tfbs.agg(F.sum("wt_max_score")).collect()[0][0]  ; e=time.time() ; print('count %s in %s sec' % (total,e-s))
    
    
    
    
