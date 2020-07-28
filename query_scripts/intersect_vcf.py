# Intersect a .vcf file with the alterome

from intersect_base import *

import itertools 

# VCF files:
# chromosome, position, name, reference, alternative 
#    8 145086052 . C T
#    9 74979169 . C A

#'chromosome',  
#'mutation_pos',
#'reference', #(from vcf)  
#'alt', #(from vcf),
#'name', #(from vcf)
#'wt_max_score',  
#'mutation_max_score',  #(from the mutation corresponding to vcf) use_t, use_a, use_w, use_g from Alt
#'motif_id', 
#'name', #(from tf_info)
#'score_threshold',
#'vcf_name' Currently the vcf name column but nice if we can update with something else.


class IntersectVCF(IntersectBase):
    def __init__(self, filename, spark, vcf_col=0, hadoop=False):
        self.vcf_col = vcf_col
        super(IntersectVCF, self).__init__(filename, hadoop=hadoop)
        # Load the .bed file into the internal dataframe.
        self.to_df(spark)

    def bad_lines_filter(self,line):
        ''' Return false if a line starts with a # '''
        return line[0] != '#'

    def vcf_parse(self,line):
        ''' Split each line on tab characters per the vcf file format. 
            Extract the first few columns.  The "alternative" column
            gets translated into a set of true/false values to match with 
            the use_a,use_c,use_g, and use_t columns. '''
        vals = line.split('\t')
        chromosome = vals[0]
        mutation_pos = int(vals[1])
        name = vals[2]
        reference = vals[3]
        alt = vals[4].strip()
        use_a = False
        use_c = False
        use_g = False
        use_t = False
        u_alt = alt.upper()
        if u_alt == 'A':
                use_a = True 
        if u_alt == 'C':
                use_c = True 
        if u_alt == 'G':
                use_g = True 
        if u_alt == 'T':
                use_t = True 
        # TODO: The last element here should be user-selectable.
        return chromosome, mutation_pos, name, reference, alt, use_a, use_c, use_g, use_t, name


    def to_df(self, spark):
        ''' Convert the file to a Spark DataFrame, stored
            internally in self.df. The df is registered as 
            a Spark SQL temp table. '''
        # Process the file in parallel but save some cores for Java
        ncores = int(spark.sparkContext.defaultParallelism / 4.0)
        if ncores < 1:
            ncores = 1
        vFile = spark.sparkContext.textFile(self.filename,ncores) 
        goodLines = vFile.filter(self.bad_lines_filter)
        labels = ('chromosome', 'mutation_pos', 'name', 'reference', 'alt', 'use_a', 'use_c', 'use_g', 'use_t', 'vcf_name')
        self.df = goodLines.map(self.vcf_parse).toDF(labels)
        self.df.registerTempTable('vcf')

    def intersection_query(self, spark):
        ''' Intersect the query bedfile with the tfbs data'''
        query='''SELECT tfbs.chromosome as chromosome,  tfbs.mutation_pos as mutation_pos,  
                vcf.reference as reference,
                vcf.alt as alt,
                vcf.name as name,
                wt_max_score,  
                CASE
                  WHEN vcf.use_a=1 THEN a_mutation_max_score
                  WHEN vcf.use_c=1 THEN c_mutation_max_score
                  WHEN vcf.use_g=1 THEN g_mutation_max_score
                  WHEN vcf.use_t=1 THEN t_mutation_max_score
                END AS mutation_max_score,
                tf_info.motif_id as motif_id, tf_info.name as tf_info_name, 
                tf_info.score_threshold as score_threshold, vcf_name
                FROM tfbs JOIN vcf ON tfbs.chromosome=vcf.chromosome 
                JOIN tf_info ON tfbs.pwmid = tf_info.row_id
                WHERE tfbs.mutation_pos=vcf.mutation_pos'''
        self.intersect_df = spark.sql(query)
 

  
