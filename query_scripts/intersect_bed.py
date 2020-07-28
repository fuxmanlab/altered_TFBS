# Intersect a .bed file with the alterome

from intersect_base import *

import itertools 
# 'chromosome',  
# 'mutation_pos',  
# 'use_w', 
# 'use_a',  
# 'use_c',  
# 'use_g', 
# 'use_t',  
# 'wt_max_score',  
# 'a_mutation_max_score',  
# 'c_mutation_max_score',  
# 'g_mutation_max_score',  
# 't_mutation_max_score',
# 'motif_id', 
# 'name', #(from tf_info)
# 'score_threshold',
# 'bed_name'
# .bed files always have 3 columns (chromosome, start position and end position). A modified
# version can have a 4th column.  That 4th column if it exists becomes bed_name. If there's 
# no 4th column the user needs to select a column (first, second, or third) or it'll default 
# to the first.

class IntersectBed(IntersectBase):
    def __init__(self, filename, spark, bed_col=0, hadoop=False):
        self.bed_col = bed_col
        if self.bed_col < 0 or self.bed_col > 2:
            # Illegal value!
            self.bed_col = 0
        super(IntersectBed, self).__init__(filename, hadoop=hadoop)
        # Check the number of columns in the first line and 
        # store that for sanity checking when processing the whole file.
        if not self.hadoop:
            with open(filename,'r') as f:
                line = f.readline()
                self.n_cols =  len(line.split())
        # Load the .bed file into the internal dataframe.
        self.to_df(spark)

    def bad_lines_filter(self,line):
        ''' Reject a line if it doesn't contain the same number of columns
            as the first line did. '''
        if not self.hadoop:
                return len(line.split()) == self.n_cols
        return True # on hadoop

    def bed_parse(self,line):
        ''' just need chromosome position, start, and stop '''
        vals = line.split()
        chromosome_pos = vals[0]
        start = int(vals[1])
        stop = int(vals[2])
        # Use the user supplied or default column for bed_name
        bed_name = vals[self.bed_col]
        if len(vals) == 4:
            # Optional bed_name column supplied, so use it.
            bed_name = vals[3]
        return tuple(zip(itertools.repeat(chromosome_pos),range(start,stop+1),itertools.repeat(bed_name)))


    def to_df(self, spark):
        ''' Convert the file to a Spark DataFrame, stored
            internally in self.df. The df is registered as 
            a Spark SQL temp table. '''
        # Process the file in parallel but save some cores for Java
        # as the bed_parse creates more data than there are lines in the file.
        ncores = int(spark.sparkContext.defaultParallelism / 2.0)
        if ncores < 1:
            ncores = 1
        bFile = spark.sparkContext.textFile(self.filename).repartition(ncores) 
        good_lines = bFile.filter(self.bad_lines_filter)
        labels = ('chromosome','mutation_pos','bed_name')
        self.df = good_lines.flatMap(self.bed_parse).toDF(labels).repartition('mutation_pos')
        self.df.cache()
        self.df.registerTempTable('bed')


    def intersection_query(self, spark):
        ''' Intersect the query bedfile with the tfbs data'''
        query='''SELECT tfbs.chromosome as chromosome,  tfbs.mutation_pos as mutation_pos,  
                use_w, use_a,  use_c,  use_g, use_t,  
                wt_max_score,  a_mutation_max_score,  c_mutation_max_score,  
                g_mutation_max_score,  t_mutation_max_score,
                tf_info.motif_id as motif_id, tf_info.name as name, 
                tf_info.score_threshold as score_threshold, bed_name
                FROM tfbs JOIN bed ON tfbs.chromosome=bed.chromosome 
                JOIN tf_info ON tfbs.pwmid = tf_info.row_id
                WHERE tfbs.mutation_pos=bed.mutation_pos'''
        self.intersect_df = spark.sql(query)
        
