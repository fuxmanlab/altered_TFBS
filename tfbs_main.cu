#include <stdio.h>
#include <stdlib.h>

#define PROMOTER_LEN 51594
#define SCORE_LEN 1963
#define PNVEC_MAX 18000

#define Nchunk 5000
#define MAX_LINE_LEN 24000
#define DELIM "\t"


#define gpuErrchk(ans) { gpuAssert((ans), __FILE__, __LINE__); }
inline void gpuAssert(cudaError_t code, const char *file, int line, bool abort=true)
{
   if (code != cudaSuccess) 
   {
      fprintf(stderr,"GPUassert: %s %s %d\n", cudaGetErrorString(code), file, line);
      if (abort) exit(code);
   }
}


__global__ 
void tfbs_kernel(int N,    int pvec_max,
                 char *pvec,   int *pvec_len, 
                 float *pwm,    int pwm_len, 
                 float tf_threshold,
                 float *bg,

                 float *output) 

{

   int id = blockIdx.x * blockDim.x + threadIdx.x;
   int ind1, ind2;
   float wt_score1, wt_score2;
   float wt_score31, wt_score41;
   float wt_score32, wt_score42;
   float wt_score33, wt_score43;
   float wt_score34, wt_score44;
   int ioffset = 29; 


   if (id < N ) {

        int ncol = 4;


      /* loop through positions */ 
      for ( int ipos = ioffset; ipos < pvec_len[id] - ioffset; ipos++ ){



         float fs1 = -99999.;
         float fs2 = -99999.;
         float fs31 = -99999.;
         float fs41 = -99999.;
         float fs32 = -99999.;
         float fs42 = -99999.;
         float fs33 = -99999.;
         float fs43 = -99999.;
         float fs34 = -99999.;
         float fs44 = -99999.;
         
         /* loop through all k-mers for that position */
         for (   int ik = 0; ik < pwm_len; ik ++ ) {
            
          
            wt_score1 = 0; wt_score2 = 0; 
	    wt_score31 = 0; wt_score41=0;
	    wt_score32 = 0; wt_score42=0;
	    wt_score33 = 0; wt_score43=0;
	    wt_score34 = 0; wt_score44=0;

            /* loop through the k-mer */
            for (   int i = 0; i < pwm_len; i++){


               ind1 = pvec[id * pvec_max + ipos + ik - pwm_len + 1 + i];
               if ( ind1 < 1 ) { /* skip kmere if not A,C,T,G */
                                        wt_score1 = 0; wt_score2 = 0;
                                        wt_score31 = 0; wt_score41=0;
                                        wt_score32 = 0; wt_score42=0;
                                        wt_score33 = 0; wt_score43=0;
                                        wt_score34 = 0; wt_score44=0;
                                        break;
                                }

               ind2 = 5 - pvec[id * pvec_max + ipos + ik   - i ];
               if ( ind2 > 4 ) { /* skip kmere if not A,C,T,G */
                                        wt_score1 = 0; wt_score2 = 0;
                                        wt_score31 = 0; wt_score41=0;
                                        wt_score32 = 0; wt_score42=0;
                                        wt_score33 = 0; wt_score43=0;
                                        wt_score34 = 0; wt_score44=0;
                                        break;
                                }


               wt_score1 = wt_score1 + log(pwm[ (i * ncol )  +  (ind1 -1) ] / bg[ ind1 - 1 ] );
               wt_score2 = wt_score2 + log(pwm[ (i * ncol )  +  (ind2 -1) ] / bg[ ind2 - 1 ] );

               if ( i == (pwm_len -ik -1)) ind1 = 1;
               if ( i == ik) ind2 = 5 - 1;
               wt_score31 = wt_score31 + log(pwm[ (i * ncol )  +  (ind1 -1) ] / bg[ ind1 - 1 ] );
               wt_score41 = wt_score41 + log(pwm[ (i * ncol )  +  (ind2 -1) ] / bg[ ind2 - 1 ] );

               if ( i == (pwm_len -ik -1)) ind1 = 2;
               if ( i == ik) ind2 = 5 - 2;
               wt_score32 = wt_score32 + log(pwm[ (i * ncol )  +  (ind1 -1) ] / bg[ ind1 - 1 ] );
               wt_score42 = wt_score42 + log(pwm[ (i * ncol )  +  (ind2 -1) ] / bg[ ind2 - 1 ] );

               if ( i == (pwm_len -ik -1)) ind1 = 3;
               if ( i == ik) ind2 = 5 - 3;
               wt_score33 = wt_score33 + log(pwm[ (i * ncol )  +  (ind1 -1) ] / bg[ ind1 - 1 ] );
               wt_score43 = wt_score43 + log(pwm[ (i * ncol )  +  (ind2 -1) ] / bg[ ind2 - 1 ] );

               if ( i == (pwm_len -ik -1)) ind1 = 4;
               if ( i == ik) ind2 = 5 - 4;
               wt_score34 = wt_score34 + log(pwm[ (i * ncol )  +  (ind1 -1) ] / bg[ ind1 - 1 ] );
               wt_score44 = wt_score44 + log(pwm[ (i * ncol )  +  (ind2 -1) ] / bg[ ind2 - 1 ] );
            }

            /* for all kmers for this position calculate maximum scores */
            fs1 = ( fs1 < wt_score1 ) ? wt_score1 : fs1; 
            fs2 = ( fs2 < wt_score2 ) ? wt_score2 : fs2; 
            fs31 = ( fs31 < wt_score31 ) ? wt_score31 : fs31; 
            fs41 = ( fs41 < wt_score41 ) ? wt_score41 : fs41;
            fs32 = ( fs32 < wt_score32 ) ? wt_score32 : fs32; 
            fs42 = ( fs42 < wt_score42 ) ? wt_score42 : fs42;
            fs33 = ( fs33 < wt_score33 ) ? wt_score33 : fs33; 
            fs43 = ( fs43 < wt_score43 ) ? wt_score43 : fs43;
            fs34 = ( fs34 < wt_score34 ) ? wt_score34 : fs34; 
            fs44 = ( fs44 < wt_score44 ) ? wt_score44 : fs44;

         }

         /* if all the scores are less than threshold do not store them */
         int icol =  id * (pvec_max - ioffset * 2 )  +  (ipos - ioffset );
         int ichunk = N * (pvec_max - ioffset * 2 );
         output[ 1 * ichunk + icol ] = ipos + 1;

         output[ 2 * ichunk + icol ] = fs1;
         output[ 3  * ichunk + icol ] = fs2;


         if ( (fs31 >= tf_threshold | fs41 >= tf_threshold ) & pvec[ id * pvec_max + ipos] != 1 ){
            output[ icol ] = 1;
            output[ 4 * ichunk + icol ] = fs31;
            output[ 5 * ichunk + icol ] = fs41;
         } else if( ( fs1 > tf_threshold | fs2 > tf_threshold) &   pvec[ id * pvec_max + ipos] != 1 ){
            output[ icol ] =0;
            output[ 4 * ichunk + icol ] = fs31;
            output[ 5 * ichunk + icol ] = fs41;

         } else {
            output[ icol ]  = 0;

         }

         if ( ( fs32 >= tf_threshold | fs42 >= tf_threshold) & pvec[ id * pvec_max + ipos] != 2 ){
            output[ icol ] = output[  icol  ] + 10;
            output[ 6 * ichunk + icol ] = fs32;
            output[ 7 * ichunk + icol ] = fs42;
         } else if( ( fs1 > tf_threshold | fs2 > tf_threshold) &   pvec[ id * pvec_max + ipos] != 2 ){
            output[ 6 * ichunk + icol ] = fs32;
            output[ 7 * ichunk + icol ] = fs42;
         } 

         if ( ( fs33 >= tf_threshold | fs43 >= tf_threshold) & pvec[ id * pvec_max + ipos] != 3 ){
            output[ icol ] = output[  icol  ] + 100;
            output[ 8 * ichunk + icol ] = fs33;
            output[ 9 * ichunk + icol ] = fs43;
         } else if( ( fs1 > tf_threshold | fs2 > tf_threshold) &   pvec[ id * pvec_max + ipos] != 3 ){
            output[ 8 * ichunk + icol ] = fs33;
            output[ 9 * ichunk + icol ] = fs43;
         } 

         if ( ( fs34 >= tf_threshold | fs44 >= tf_threshold) & pvec[ id * pvec_max + ipos] != 4 ){
            output[ icol ] = output[ icol ] + 1000;
            output[ 10 * ichunk + icol ] = fs34;
            output[ 11 * ichunk + icol ] = fs44;
         } else if( ( fs1 > tf_threshold | fs2 > tf_threshold) &   pvec[ id * pvec_max + ipos] != 4 ){
            output[ 10 * ichunk + icol ] = fs34;
            output[ 11 * ichunk + icol ] = fs44;
         } 


      }
 

   }

}
 

  void tfbs_cuda(  char *pvec,   // promoter
                   int *pvec_len,
                   int pvec_max,
                   float *pwm, 
                   int pwm_len, 
                   float tf_threshold, 
                   float *bg, 
                   int N, 
                   float *output)  {// output matrix


   
     char *d_pvec;
     int *d_pvec_len;
     float *d_output;
     float *d_pwm;
     float *d_bg;

   

	//printf("Allocating GPU memory for pvec_len\n");
	gpuErrchk( cudaMalloc( (void**)&d_pvec_len, N * sizeof(  int)) );
	//printf("Copying GPU memory for pvec_len\n");
	gpuErrchk( cudaMemcpy( d_pvec_len, pvec_len, N * sizeof(  int), cudaMemcpyHostToDevice ) ); 

	//printf("Allocating GPU memory for pvec\n");
	gpuErrchk( cudaMalloc( (void**) &d_pvec, N * pvec_max * sizeof(  char)) );
	//printf("Copying GPU memory for pvec\n");
        gpuErrchk( cudaMemcpy(     d_pvec, pvec, N * pvec_max * sizeof(  char), cudaMemcpyHostToDevice ) ); 

	//printf("Allocating GPU memory for pwm\n");
 	gpuErrchk( cudaMalloc( (void**)&d_pwm, 4 * pwm_len * sizeof(float)) );
	//printf("Copying GPU memory for pwm\n");
	gpuErrchk( cudaMemcpy(     d_pwm, pwm, 4 * pwm_len * sizeof(float), cudaMemcpyHostToDevice ) ); 


	//printf("Allocating GPU memory for bg\n");
 	gpuErrchk( cudaMalloc( (void**)&d_bg, 4 *  sizeof(float)) );
	//printf("Copying GPU memory for bg\n");
	gpuErrchk( cudaMemcpy(      d_bg, bg, 4 *  sizeof(float), cudaMemcpyHostToDevice ) ); 


	//printf("Allocating GPU memory for result\n");
	gpuErrchk( cudaMalloc( (void**)&d_output, N * 12 * (pvec_max -58 ) * sizeof(float) ) ); 
	//printf("Before kernel\n");
	
	dim3 dimBlock( 32, 1 );
	dim3 dimGrid( N/32 + 1,  1 );
	//printf("Calling CUDA kernel\n");
	tfbs_kernel<<<dimGrid,dimBlock>>>(N, pvec_max,
                                          d_pvec, d_pvec_len, 
                                          d_pwm,  pwm_len, 
                                          tf_threshold, d_bg, 
                                          d_output);
	gpuErrchk( cudaPeekAtLastError() );

	//cudaDeviceSynchronize();
	//printf("After CUDA kernel\n");

	gpuErrchk( cudaMemcpy( output, d_output, N * 12 * (pvec_max -58 ) * sizeof(float), cudaMemcpyDeviceToHost ) ); 
	//printf("Free GPU memory \n");

	cudaFree( d_pvec );
	cudaFree( d_pvec_len );
	cudaFree( d_output );

	cudaFree( d_pwm );
	cudaFree( d_bg );


   return;
  }



int main( int argc, char *argv[] ){

   char filename[1024];
   int taskID;
   float bg[] = {0.25, 0.25, 0.25, 0.25};
   FILE *ifp, *ofp;
   char in_line[MAX_LINE_LEN];
   char *token;
   int chrom[PROMOTER_LEN];
   int istart[PROMOTER_LEN];
   int i, j, jj, i1, i2, j1, j2, icurr;
   float score_threshold[SCORE_LEN];
   char **score_files;
   float pwm[30 * 4];
   int pwm_length;
   int any0;
   float sum;
   char *pnvec;
   int *pnvec_len; 
   float *result;
   int ires;
 

   /* command line processing */
   taskID = atoi(argv[1]); 
   printf("Input file name: %s\n", argv[2]);
   printf("tf.info file name: %s\n", argv[3]);
   printf("pwm directory: %s\n", argv[4]);
   sprintf(filename,"./%s/%s_%03d.txt", argv[5], argv[6], taskID);
   printf("Output file name: %s\n", filename);


   /* allocate memory to hold char arrays */
   //sequence = (char **) malloc( PROMOTER_LEN * sizeof(char *) );
   score_files = (char **) malloc( SCORE_LEN * sizeof(char *) );
   result = (float *) malloc( Nchunk * 12 * ( PNVEC_MAX - 58 ) * sizeof(float) );

   pnvec_len = ( int * ) malloc ( PROMOTER_LEN * sizeof( int ) );
   if ( pnvec_len == 0 ) {
      fprintf( stderr, "ERROR allocating pnvec_len: Out of memory\n");
      exit(1);
   }

   //printf("Allocating memory for pnvec\n");
   pnvec = ( char * ) malloc ( PROMOTER_LEN * PNVEC_MAX * sizeof( char) );
   if ( pnvec == 0 ) {
      fprintf( stderr, "ERROR allocating pnvec: Out of memory\n");
      exit(2);
   }

   printf("Reading Input files\n");

   /* read input file line by line  (only 1st, 2nd and 8th columns) */
   ifp = fopen(argv[2], "r");
   fgets(in_line, MAX_LINE_LEN, ifp); //skip header line
   i=0;
   while( fgets(in_line, MAX_LINE_LEN, ifp )!= NULL ){

      token = strtok( in_line, DELIM);
      sscanf( token,"chr%d", &(chrom[i])); 

      token = strtok( NULL, DELIM);
      sscanf( token,"%d", istart + i ); 

      token = strtok( NULL, DELIM);
      token = strtok( NULL, DELIM);
      token = strtok( NULL, DELIM);
      token = strtok( NULL, DELIM);
      token = strtok( NULL, DELIM);
      token = strtok( NULL, DELIM);

      //if(i > 14655) printf(" start processing letter\n");
      pnvec_len[i] = strlen(token);
      for (j = 0; j < pnvec_len[i] ; j++) {
         //if (i == 14660)printf("%c",token[j]);
         switch( token[j] ){
         case 'A':
            pnvec[ i * PNVEC_MAX + j ]=1;
            break;
         case 'a':
            pnvec[ i * PNVEC_MAX + j ]=1;
            break;
         case 'C':
            pnvec[ i * PNVEC_MAX + j ]=2;
            break;
         case 'c':
            pnvec[ i * PNVEC_MAX + j ]=2;
            break;
         case 'G':
            pnvec[ i * PNVEC_MAX + j ]=3;
            break;
         case 'g':
            pnvec[ i * PNVEC_MAX + j ]=3;
            break;
         case 'T':
            pnvec[ i * PNVEC_MAX + j ]=4;
            break;
         case 't':
            pnvec[ i * PNVEC_MAX + j ]=4;
            break;
         default:
            pnvec[ i * PNVEC_MAX + j ]=0;
            break;
         }
      }

      i++;
      //if ( i > 14600) { printf("i=%d\n",i);}
   }
   fclose(ifp);
   printf(" Read %d lines from the input file\n", i); 


   /* Read tf.info file */
   ifp = fopen(argv[3], "r");
   i=0;
   while( fgets(in_line, MAX_LINE_LEN, ifp ) ){

      token = strtok( in_line, DELIM);
      score_files[i] = (char *) malloc ( (strlen(token) + 1 ) * sizeof(char ));
      strcpy( score_files[i], token );

      token = strtok( NULL, DELIM);
      score_threshold[i] = atof(token);

      i++;


   }

   fclose(ifp);
   printf(" Read %d lines from %s file\n", i, argv[3]); 


   /* process chunks */
   i1 = (taskID - 1) * 100 + 1;  // was 10 originally
   i2 = taskID * 100;
   if ( i2 > SCORE_LEN ) i2 = SCORE_LEN;

   /* open output file */
   ofp = fopen(filename,"w");

   if (ofp == NULL) {
      fprintf( stderr, " Can't open output file\n");
      exit(3);
   }


   for ( icurr = i1; icurr <= i2; icurr++){
      printf(" icurr =%d\n", icurr);

      sprintf( filename, "./%s/%s\0", argv[4], score_files[icurr-1] );
      ifp = fopen( filename , "r");
      fgets(in_line, MAX_LINE_LEN, ifp ); // skip first line
      i = 0;
      any0 = 0;
      while( fgets(in_line, MAX_LINE_LEN, ifp ) ){

         token = strtok( in_line, DELIM); //skip first value

         token = strtok( NULL, DELIM);
         pwm[i*4 + 0] = atof(token); 
         if ( !strcmp(token, "0.0") ) any0=1;

         token = strtok( NULL, DELIM);
         pwm[i*4 + 1] = atof(token); 
         if ( !strcmp(token, "0.0") ) any0=1;

         token = strtok( NULL, DELIM);
         pwm[i*4 + 2] = atof(token); 
         if ( !strcmp(token, "0.0") ) any0=1;

         token = strtok( NULL, DELIM);
         pwm[i*4 + 3] = atof(token); 
         if ( !strcmp(token, "0.0\n") ) any0=1;
         
         i++;
      }

      fclose(ifp);
      pwm_length = i;
      printf(" Read %d lines from %s file\n", i, score_files[icurr-1]); 

      /* part of create_pwm function */
      if ( any0 ) {
         for ( j = 0; j < i; j++ ){
           sum = pwm[ j*4 + 0] + pwm[ j*4 + 1] + pwm[ j*4 + 2] + pwm[ j*4 + 3] + 0.001 * 4;
           pwm[ j*4 + 0] = (pwm[ j*4 + 0] + 0.001)/sum;
           pwm[ j*4 + 1] = (pwm[ j*4 + 1] + 0.001)/sum;
           pwm[ j*4 + 2] = (pwm[ j*4 + 2] + 0.001)/sum;
           pwm[ j*4 + 3] = (pwm[ j*4 + 3] + 0.001)/sum;

         }
      }


      /* inner loop in R*/
      for ( j = 1; j < 12; j++ ){

         j1 = (j - 1) * Nchunk + 1;
         j2 = j * Nchunk;
         if ( j2 > PROMOTER_LEN )j2 = PROMOTER_LEN;
         int n = j2 - j1 + 1;

         printf(" j = %d through %d; threshold = %f\n", j1, j2, score_threshold[icurr - 1]);
         tfbs_cuda (pnvec + (j1 -1) * PNVEC_MAX, 
                    pnvec_len + j1 -1, 
                    PNVEC_MAX, 
                    pwm, 
                    pwm_length, 
                    score_threshold[icurr - 1], 
                    bg, 
                    n,  
                    result);

         fflush(stdout);

         /* save result in the output file */
         for (i = 0; i < n * ( PNVEC_MAX - 58 ); i++){
            ires = (int) result [ i ];
            int in = i/( PNVEC_MAX - 58 );
            //printf("%d ",i);
            if (ires > 0 || (score_threshold[ icurr - 1] < result[ i + ( PNVEC_MAX - 58 ) * 2 * n] )|| (score_threshold[icurr - 1] < result[i + (PNVEC_MAX - 58 ) * 3 * n])) {
               fprintf(ofp,"%d ", chrom [ in ]);
               //unsigned int ipos = result [ i +  ( PNVEC_MAX - 58 ) * n * 1] + istart[ j1 - 1 + i/( PNVEC_MAX - 58 ) ]; 
               unsigned int ipos = 30 + i%( PNVEC_MAX - 58 ) + istart[ j1 - 1 + in ]; 
               fprintf(ofp,"%d %d ", ipos, ipos + 1);
               fprintf(ofp,"%d %d ", j1 + in,  icurr);
               fprintf(ofp,"%d ", ires );
               // use %f.3 for printing results with 3 digits
               for ( jj = 2; jj < 11; jj++ )fprintf(ofp,"%.3f ", result [ i +  ( PNVEC_MAX - 58 ) * jj * n] );
               fprintf(ofp,"%.3f\n", result [ i + ( PNVEC_MAX - 58 ) * 11 * n] );
            }
         } 

      } // end of j loop


   } // end of icurr loop
 
   fclose(ofp);

   exit(0);
}
