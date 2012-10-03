// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "MyService.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <listener.h>


#define INTRO_PORT 8000 //Randomly chosen port for introducer


using boost::shared_ptr;

using namespace  ::mp2;


int main(int argc, char* argv[]){


    if (argc<2){
        printf("Please enter value of m\n");
        exit(1);
    }


    pid_t pid = fork();			//creating introducer/listener

    if(pid==0)
    {
        //Execute listener
        printf("Executing listener...\n");
        listener(m);
      
    }
    else
    {
        //Execute introducer
        printf("Executing introducer...\n");
        introducer(argv[1]); 
    }
}
