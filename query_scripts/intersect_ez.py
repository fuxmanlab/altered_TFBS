# Intersect a .bed file with the alterome

from intersect_base import *
from pyspark.sql import Row

from useful import timeit
 
# This is a simple query.  The ez format:
#
# pwmid, tf_name
#  1,TFAP2B
#  3,TFAP2D
#  5
#  6
#
#  This will match tf_name X vs pwmid 1,2,3,4 and the same for Y.
#  Commas or no commas on the pwmid-only rows.  It doesn't make
#  any difference.

 
class IntersectEz(IntersectBase):
    def __init__(self, filename, spark, hadoop=False):
        super(IntersectEz, self).__init__(filename, hadoop=hadoop)
        # Check the number of columns in the first line and 
        # store that for sanity checking when processing the whole file.
        if not self.hadoop:
            with open(filename,'r') as f:
                line = f.readline()
                self.n_cols =  len(line.split(','))
        # Load the .ez file into the internal dataframe.
        self.to_df(spark)

    def bad_lines_filter(self,line):
        ''' Reject a line if it doesn't have 1 or 2 columns'''
        cols=line.split(',')
        right_cols = len(cols) in [1,2]
        # if the right number of columns, check that the first column
        # is an integer. If not, it's bad or a header line.
        if right_cols:
                # Test if it contains just integers.  Also convert to an ASCII
                # string to avoid Unicode errors.
                return cols[0].encode('ascii', 'ignore').strip().isdigit()
        # Otherwise...return False.
        return False

    def ez_parse(self,lines):
        ''' Split the incoming file into two lists. '''
        pwmids = []
        tf_names = []
        for line in lines:
                cols = line.split(',')
                pwmids.append(int(cols[0]))
                if len(cols)==2 and len(cols[1].strip()) > 0:
                        tf_names.append(cols[1].strip())
        return pwmids, tf_names
 
    @timeit
    def to_df(self, spark):
        ''' Convert the file to TWO Spark DataFrames, stored
            internally in self.df_pwmids and self.df_tf_names. The 
            df's are registered as Spark SQL temp tables. '''
        # Read the file and collect to one node as a Python list.
        ezFile = spark.sparkContext.textFile(self.filename).filter(self.bad_lines_filter).collect()
        pwmids, tf_names =self.ez_parse(ezFile)
        rdd=spark.sparkContext.parallelize(pwmids) 
        rdd = rdd.map(lambda x: Row(x))
        self.df_pwmids = rdd.toDF(['pwmid'])
        rdd=spark.sparkContext.parallelize(tf_names) 
        rdd = rdd.map(lambda x: Row(x))
        self.df_tf_names = rdd.toDF(['tf_name'])
        # Cache these
        self.df_pwmids.cache()
        self.df_tf_names.cache()
        # Register as tables
        self.df_pwmids.registerTempTable('pwmid')
        self.df_tf_names.registerTempTable('tf_name')


    def intersection_query(self, spark):
        ''' Intersect the query bedfile with the tfbs data'''
        query='''SELECT tfbs.chromosome as chromosome,  tfbs.mutation_pos as mutation_pos,  
                use_w, use_a,  use_c,  use_g, use_t,  
                wt_max_score,  a_mutation_max_score,  c_mutation_max_score,  
                g_mutation_max_score,  t_mutation_max_score,
                tf_info.motif_id as motif_id, tf_info.name as name, 
                tf_info.score_threshold as score_threshold 
                FROM tfbs JOIN tf_info ON tfbs.pwmid = tf_info.row_id
                WHERE tf_info.name IN (SELECT * FROM tf_name) 
                AND tf_info.row_id IN (SELECT * FROM pwmid)'''
        self.intersect_df = spark.sql(query)
        
