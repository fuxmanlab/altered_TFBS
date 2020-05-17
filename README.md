# altered TF binding site database

## Instructions to compile the file:

  module load cuda/9.2
  
  module load gcc/4.8.1
  
  nvcc -gencode arch=compute_35,code=sm_35 -gencode arch=compute_60,code=sm_60 -gencode arch=compute_70,code=sm_70  -Xptxas -O3,-v -o tfbs tfbs_main.cu

## Instructions to run the tfbs file:

  module load gcc/4.8.1 cuda/9.2
  
  ./tfbs $SGE_TASK_ID chromosome_file motif_thresholds  dir_motifs results log
  
 
chromosome_file = file containing the chromosome fragments and their sequence
motif_thresholds = file containing motif_id, significance_threshold
dir_motifs = directory containing the motifs
