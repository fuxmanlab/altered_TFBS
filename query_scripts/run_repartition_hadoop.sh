#!/bin/bash -l


module load python2/2.7.16
module load spark/2.3.0
module load java/1.8.0_181

# Arguments:  run_intersect.sh parquet_dir bed_file output.csv
parquet_dir=$1

# Pack up the Python files to be sent to the cluster
rm -f py_files.zip
zip py_files.zip *.py

# All the python files go into py_files.zip 
time spark-submit --master yarn --py-files py_files.zip \
    --num-executors 59 --executor-cores 5 --executor-memory 24G \
    --driver-cores 4 --driver-memory 8G \
    repartition_parquet.py -infofile $parquet_dir/tf_info.pq -infile $parquet_dir/tfbs.pq -outfile $parquet_dir/tfbs2.pq


