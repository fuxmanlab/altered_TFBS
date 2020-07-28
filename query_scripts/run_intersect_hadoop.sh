#!/bin/bash -l


module load python2/2.7.16
module load spark/2.3.0
module load java/1.8.0_181

# Arguments:  run_intersect.sh parquet_dir bed_file output.csv
parquet_dir=$1
samp_file=$2 
output_csv=$3

# This is the HADOOP top level directory
TOP_DIR=/project/cancergrp/tfbs

# On HDFS make a random directory for the input bed files and output csv
RAN_DIR=$TOP_DIR/$RANDOM
IN_DIR=$RAN_DIR/input
OUT_DIR=$RAN_DIR/output
hdfs dfs -mkdir $RAN_DIR
hdfs dfs -mkdir $IN_DIR
# Don't create the output directory - Spark will do that automatically.

# Copy over the .bed or .vcf sampling file
hdfs dfs -put $samp_file $IN_DIR
IN_FILE=$IN_DIR/$(basename $samp_file)




# Run a VCF or BED file through spark

# Tips on tuning Spark: https://spoddutur.github.io/spark-notes/distribution_of_executors_cores_and_memory_for_spark_application.html

# Config (June 2019): 20 16-core nodes with min 100 GB/node.
#    Use 3 executors per node with 5 cores --> 15 cores per node is used.
#    This leaves 1 core per node for Yarn overhead.
#    20 nodes * 3 executors = 60 executors.  Subtract 1 so that one is
#    used by Spark as the Application Manager so that's 59 executors.
#    24GB per executor uses ~72 GB per node.  Don't set that too high, it'll
#    have excess garbage collections times - better to use more executors.
#    The driver is run on scc-hadoop, so tell it to use multiple cores too if needed.



# Pack up the Python files to be sent to the cluster
rm -f py_files.zip
zip py_files.zip *.py

# All the python files go into py_files.zip 
time spark-submit --master yarn --py-files py_files.zip \
    --num-executors 59 --executor-cores 5 --executor-memory 24G \
    --driver-cores 4 --driver-memory 8G \
    intersect_hadoop.py -sample $IN_FILE -pq_dir $parquet_dir -output_csv $OUT_DIR


echo Merging CSV files...
time hadoop fs -getmerge $OUT_DIR $output_csv

# NEEDED:  Python cleanup to remove duplicate headers


# Remove the hadoop temporary directory
hadoop fs -rm -r -skipTrash $RAN_DIR

