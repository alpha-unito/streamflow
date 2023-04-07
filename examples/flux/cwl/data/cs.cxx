// Author: Marco Aldinucci
// Date: 13 May 2010
// Ex. 1-2, for PDS-physics class 2010

#include "mpi.h"
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

enum messages {msg_tag,eos_tag};


static inline const double diffmsec(const struct timeval & a, 
                                    const struct timeval & b) {
    long sec  = (a.tv_sec  - b.tv_sec);
    long usec = (a.tv_usec - b.tv_usec);
    
    if(usec < 0) {
        --sec;
        usec += 1000000;
    }
    return ((double)(sec*1000)+ (double)usec/1000.0);
}

int main( int argc, char **argv )
{
  int myid,numprocs,namelen;
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  double t0,t1;
  struct timeval wt1,wt0;
  // MPI_Wtime cannot be called here
  gettimeofday(&wt0,NULL);
  MPI_Init(&argc,&argv );
  t0 = MPI_Wtime();
  //gettimeofday(&wt0,NULL); 
  MPI_Comm_size(MPI_COMM_WORLD,&numprocs); 
  MPI_Comm_rank(MPI_COMM_WORLD,&myid); 
  MPI_Get_processor_name(processor_name,&namelen);
  srand(time(NULL));


  // This is the server code
  // Note - I don't understand how this example works - with the previous
  // it hung forever, so I reduced to just printing the server id and exiting.
  int n_eos = 0;
  std::cout << "Hello I'm the server with id " << myid << " on " << processor_name 
			  << " out of " << numprocs << " I'm the server\n";

  MPI_Barrier(MPI_COMM_WORLD);
  t1 = MPI_Wtime();
  //gettimeofday(&wt1,NULL); 
  MPI_Finalize();
  gettimeofday(&wt1,NULL);  
  std::cout << "Total time (MPI) " << myid << " is " << t1-t0 << "\n";
  std::cout << "Total time (gtd) " << myid << " is " << 
	diffmsec(wt1,wt0)/1000 << "\n";
  return 0;
}
