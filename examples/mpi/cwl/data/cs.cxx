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
  if (myid == 0) {
	// This is the server code
	int n_eos = 0;
	std::cout << "Hello I'm the server with id " << myid << " on " << processor_name 
			  << " out of " << numprocs << " I'm the server\n";
	while (true) {
	  MPI_Status status;
	  int target;
	  
	  MPI_Recv(&target,1, MPI_INT, MPI_ANY_SOURCE,MPI_ANY_TAG,
			   MPI_COMM_WORLD, &status);
	  if (status.MPI_TAG==eos_tag) {
		std::cout << "EOS from " << status.MPI_SOURCE << " received\n";
		if (++n_eos>=(numprocs-1)) break;
	  } else {
	    std::cout << "[server] Request from " << status.MPI_SOURCE << " : " 
		      << target << " --> " << target*target << "\n";
	    target *=target;
	    MPI_Send(&target,1, MPI_INT,status.MPI_SOURCE,msg_tag,MPI_COMM_WORLD);
	  }
	}
  } else {
	// This is the client code
	int request;
	int rep=0;
	int noreq = random()&11;
	std::cerr << "N. req " << noreq << "\n";
	MPI_Status status;
	std::cout << "Hello I'm " << myid << " on " << processor_name 
			  << " out of " << numprocs << " I'm a client\n";
	while (rep<noreq) {
	  request = myid+(random()&11);
	  MPI_Send(&request,1, MPI_INT,0,msg_tag,MPI_COMM_WORLD);
	  MPI_Recv(&request,1, MPI_INT,0,MPI_ANY_TAG,MPI_COMM_WORLD, &status);
	  std::cout << "[client " << myid << "] <- " << request << "\n"; 
	  usleep(random()/4000);
	  ++rep;
	}
	std::cout << "** Proc. " << myid << " received " << noreq << " answers\n";
	// Now sending termination message
	MPI_Send(&request,1, MPI_INT,0,eos_tag,MPI_COMM_WORLD);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  t1 = MPI_Wtime();
  //gettimeofday(&wt1,NULL); 
  MPI_Finalize();
  gettimeofday(&wt1,NULL);  
  std::cout << "Total time (MPI) " << myid << " is " << t1-t0 << "\n";
  std::cout << "Total time (gtd) " << myid << " is " << 
	diffmsec(wt1,wt0)/1000 << "\n";
  if ((((diffmsec(wt1,wt0)/1000)-t1+t0)) > ((diffmsec(wt1,wt0)/1000)/50))
	std::cout << "Why the two measurements are sensibly different?\n";
  return 0;
}
