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

from useful import get_n_cores

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType

 
# RUN:
# time spark-submit --driver-memory 128G \
#    --conf "spark.local.dir=$TMPDIR"  \
#    --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$TMPDIR"  \
#    --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$TMPDIR"  \
#    create_parquet.py  -csv tfinfo_csv -indir bed_files_input_dir -outdir parquet_output_dir
#
# STRONGLY RECOMMENDED: Put the output_dir on the local scratch disk.

def no_bad_lines(line):
    ''' Filter functon that removes header lines '''
    if line.startswith('Motif_ID'):
        # header line!
        return False
    return True
    

def make_tf_info_tpls(line):
    line = line.split(',')
    line[10] = int(line[10])
    line[12] = float(line[12])
    return tuple(line)

def create_tf_info(sc,csv_file, out_file):
    distFile = sc.textFile(csv_file) 
    noBadLines = distFile.filter(no_bad_lines)
    tf_info_data = noBadLines.map(make_tf_info_tpls)
    labels=('motif_id', 
    'name', 
    'species', 
    'status', 
    'family_name', 
    'dbd', 
    'motif_type', 
    'msource_identifier', 
    'msource_type', 
    'msource_author', 
    'pmid',
    'file',
    'score_threshold',
    'row_id')
    row_labels= Row(labels)
    # Labeled dataframe
    df = tf_info_data.toDF(labels[:-1])
    # Add the row_id column. 
    # Found at: https://stackoverflow.com/questions/43406887/spark-dataframe-how-to-add-a-index-column-aka-distributed-data-index
    new_schema = StructType(df.schema.fields[:] + [StructField("row_id", LongType(), False)])
    zipped_rdd = df.rdd.zipWithIndex()
    # Add 1 to the row index as we go.
    indexed_df = (zipped_rdd.map(lambda ri: row_labels(*list(ri[0]) + [ri[1]+1])).toDF(new_schema))
    indexed_df.show()
    # Get the number of rows
    nrows = indexed_df.count()
    # Compact to 1 partition and save.
    indexed_df.coalesce(1).write.option("compression","snappy").parquet(out_file)
    return nrows

def create_binding_event(b_event):
    ''' Read in the binding event value as a string.
        Convert to an integer reading the string as a 
        string of bits.  Then use testBit on all 4 
        bit positions to see which of the 4 nucleotides
        are 'on'.  Return a tuple of 4 booleans.
        
        tuple order: W A C T G  
        ex:  b_event = 100 --> (F,F,F,T,F)
             b_event = 1101 --> (F, T, F, T, T)
    '''
    event_val = int(b_event,2)
    if event_val == 0:
        return True,False,False,False,False
    A = True if event_val & 1 else False 
    C = True if event_val & 2 else False 
    G = True if event_val & 4 else False 
    T = True if event_val & 8 else False 
    return False,A,C,T,G
    
def get_chromosome(filename):
    ''' Get the X or Y or Number chromosome label
        from the filename. 
        Ex.: Splitting apart: chr10_005.bed to get 10. '''
    name = os.path.basename(filename)
    return name.split('_')[0].split('r')[1]
    
def make_tfbs_tpls(line):
    line = line.split()
    db_row=14 * [None]
    # Set the X or Y chromosome value
    # if needed from the filename
    if line[0].isdigit() and int(line[0]) != 0:
        db_row[0] = int(line[0])
    elif line[0]=='X':
        db_row[0]= -1
    elif line[0]=='Y':
        db_row[0]= -2
    db_row[1] = int(line[1])   
    db_row[2] = int(line[3])   
    db_row[3] = int(line[4])   
    # Lookup the binding event and split into several 0/1 values
    db_row[4:9] = create_binding_event( line[5] )
    # Max of the WT forward/reverse scores
    db_row[9]=max( (float(x) for x in line[6:8]) )
    # And the same for the a/c/g/t values
    db_row[10]=max( (float(x) for x in line[8:10]) )
    db_row[11]=max( (float(x) for x in line[10:12]) )
    db_row[12]=max( (float(x) for x in line[12:14]) )
    db_row[13]=max( (float(x) for x in line[14:16]) )
    return tuple(db_row)


def create_tfbs(sc,bed_files, out_file,nrows):
    distFile = sc.textFile(bed_files) 
    tfbs_data = distFile.map(make_tfbs_tpls)
    labels =   ('chromosome',  
                'mutation_pos',  
                'chromosome_chunk',
                'pwmid',  
                'use_w', 
                'use_a',  
                'use_c',  
                'use_g', 
                'use_t',  
                'wt_max_score',  
                'a_mutation_max_score',  
                'c_mutation_max_score',  
                'g_mutation_max_score',  
                't_mutation_max_score', )
    # The dataframe is now partitioned by the pwmid column to be the
    # same number of partitions as there are unique pwmid values as given
    # by the nrows argument.  Then write out a partition for each pwmid.
    df = tfbs_data.toDF(labels).repartition('pwmid')
    df.write.option("compression","snappy").partitionBy('pwmid').parquet(out_file)
 

def make_all(sc, csvfile, indir, outdir):
    # Read the tf_info CSV and write out a Parquet file for it. 
    print('Creating tf_info parquet file')  
    s=time.time() 
    nrows = create_tf_info(sc,csvfile, os.path.join(outdir,'tf_info.pq'))
    e=time.time()
    print('tf_info complete: %s sec' % (e-s))

    # Set the number of possible partitions to be equal to the
    # number of rows.
    spark.conf.set("spark.sql.shuffle.partitions", str(nrows))

    # And now do the same for tfbs bed files
    print('Creating tfbs parquet file')   
    s=time.time() 
    create_tfbs(sc,indir,os.path.join(outdir,'tfbs.pq'),nrows)
    e=time.time()
    print('tfbs complete: %s sec' % (e-s))


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-csv', required=True, help="tfinfo CSV file.")
    parser.add_argument('-indir', required=True, help="Directory of .bed files.")
    parser.add_argument('-outdir', required=True, help='Output directory for tf_info.pq and tfbs.pq')
    args = parser.parse_args()    
    
    # Fire up Spark using most available cores.  Using a few less helps balance the
    # Python/Java CPU contention.  
    spark = SparkSession.builder.master('local[%s]' % get_n_cores(reserve=4)).appName('tfbs-pq').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    indir=sys.argv[1]
    outdir=sys.argv[2]
    make_all(sc, args.csv, args.indir, args.outdir)
