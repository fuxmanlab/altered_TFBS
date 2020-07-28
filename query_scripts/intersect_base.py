# Base class for loading, saving, and querying the .bed and .vcf
# files

import useful
import tempfile
import os
import glob
import shutil


class IntersectBase(object):
    ''' Base class for intersecting .bed and .vcf files
        with the alterome datafiles using Spark-SQL.'''
    def __init__(self, filename, hadoop=False):
        if not hadoop:
            if not os.path.exists(filename):
                raise OSError("File not found: %s" % filename)
        self.filename = filename
        # The loaded dataframe from the file
        self.df = None 
        #The result of the query
        self.intersect_df = None 
        # Are we on hadoop?
        self.hadoop = hadoop

    def to_df(self, spark):
        ''' Convert the file to a Spark DataFrame, stored
            internally in self.df. The df is registered as 
            a Spark SQL temp table. '''
        pass
        
    def intersection_query(self,spark):
        ''' Intersection query for the .bed and .vcf files with the
            alterome in the tfbs_df and tf_info_df dataframes. Stores  
            the result internally in self.intersect_df. '''
        pass
        
    def write_df(self, output_csv, npartitions=None):
        ''' Write in parallel to a set of output CSV files 
            and them consolidate them into 1.'''  
        tmp_name = self.df_to_csv(output_csv, npartitions)
        if not self.hadoop:
            self.consolidate_csv(tmp_name,output_csv)

    @useful.timeit
    def df_to_csv(self,output_csv, npartitions=None):
        # Repartition if asked
        if npartitions:
            self.intersect_df.repartition(npartitions)

        # Get a unique temporary filename using the process id
        if not self.hadoop:
            tmp_name = str(os.getpid())+'_tmp'
            tmp_path = os.path.join(os.environ['TMPDIR'],tmp_name+'.csv')
            if os.path.exists(tmp_path):
                shutil.rmtree(tmp_path)
            self.intersect_df.write.option('header','true').csv(tmp_path)
            return tmp_path
        else:
            self.intersect_df.write.option('header','true').csv(output_csv)

    @useful.timeit
    def consolidate_csv(self, input_dir,output_csv, delete_input=True):
        print("Consolidating parallel CSV files.")
        if os.path.exists(output_csv):
            os.unlink(output_csv)
        # Then write a loop to read them in one-by-one and append to the requested output_csv
        csv_files = glob.glob(os.path.join(input_dir,'*.csv'))
        shutil.copyfile(csv_files.pop(0),output_csv)
        # Now open the output file for appending and add all the
        # others to it.
        with open(output_csv, 'ab') as outfile:
            for fname in  csv_files :
                with open(fname, 'rb') as infile:
                    # Throw away the header  line
                    infile.readline()  
                    # Block copy rest of file from input to output without parsing
                    shutil.copyfileobj(infile, outfile)
        # Finally delete the whole temp directory if requested.
        if delete_input:
            shutil.rmtree(input_dir)        
