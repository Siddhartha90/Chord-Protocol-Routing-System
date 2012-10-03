#include "MyService.h"

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

#include <vector>
#include <list>
#include <set>
#include <string>
#include <iostream>
#include <math.h>
#include <sstream>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

// the name space specified in the thrift file
using namespace mp2;
using namespace std;

/********************************************************/

/* Main function of your Chord-node file
   Interacts with the user who specifies his arguments, subsequently 
   forks and executes introducer process (in node.cpp)
   - Siddhartha gupta
*/


#include <getopt.h>
#include <stdarg.h>
#include <assert.h>
#include <stdlib.h>

int main(int argc, char **argv) {
	//  INIT_LOCAL_LOGGER();
	int opt;
	int long_index;

	vector<int> usedports;

	int m, id, port, introducerPort, stabilizeInterval, fixInterval, seed;

	const char *logconffile = NULL;

	if (argc < 7) {
		cout << "You missed either of m, ID or port, please execute again\n";
		exit(0);
	}

	struct option long_options[] = {
	/* mandatory args */

	{ "m", required_argument, 0, 1000 },

	/* id of this node: 0 for introducer */
	{ "id", required_argument, 0, 1001 },

	/* port THIS node will listen on, at least for the
	 * Chord-related API/service
	 */
	{ "port", required_argument, 0, 1002 },

	/* optional args */

	/* if not introducer (id != 0), then this is required: port
	 * the introducer is listening on.
	 */
	{ "introducerPort", required_argument, 0, 1003 },

	/* path to the log configuration file */
	{ "logConf", required_argument, 0, 1004 },

	/* intervals (seconds) for runs of the stabilization and
	 * fixfinger algorithms */
	{ "stabilizeInterval", required_argument, 0, 1005 }, { "fixInterval",
			required_argument, 0, 1006 },

	{ "seed", required_argument, 0, 1007 },

	{ 0, 0, 0, 0 }, };
	while ((opt = getopt_long(argc, argv, "", long_options, &long_index)) != -1) {
		switch (opt) {
		case 0:
			if (long_options[long_index].flag != 0) {
				break;
			}
			printf("option %s ", long_options[long_index].name);
			if (optarg) {
				printf("with arg %s\n", optarg);
			}
			printf("\n");
			break;

		case 1000:
			m = strtol(optarg, NULL, 10);
			assert((m >= 3) && (m <= 10));
			break;

		case 1001:
			id = strtol(optarg, NULL, 10);
			assert(id >= 0);
			break;

		case 1002:
			port = strtol(optarg, NULL, 10);
			assert(port > 0);
			break;

		case 1003:
			introducerPort = strtol(optarg, NULL, 10);
			assert(introducerPort > 0);
			break;

		case 1004:
			logconffile = optarg;
			break;

		case 1005:
			stabilizeInterval = strtol(optarg, NULL, 10);
			assert(stabilizeInterval > 0);
			break;

		case 1006:
			fixInterval = strtol(optarg, NULL, 10);
			assert(fixInterval > 0);
			break;

		case 1007:
			seed = strtol(optarg, NULL, 10);
			break;

		default:
			exit(1);
		}
	}

	/* if you want to use the log4cxx, uncomment this */
	// configureLogging(logconffile);
	assert (port > 0);
	usedports.push_back(port);

	srand(time(NULL)); //generates a random number for port generation

	//Fork and Execute introducer

	//cout << " Forking..." << endl;

	introducerPort = port;

	pid_t pid = fork();

	if (pid < 0) {
		perror("Couldn't create introducer node due to fork failure\n"); 
		return 0;
	}

	if (pid == 0) { 
        // Execute introducer
        
		char **args = (char **) malloc(6 * sizeof(char *));
		for (int i = 0; i <= 4; i++) {
			args[i] = (char *) malloc(50 * sizeof(char));
		}

		strcpy(args[0], "./node");
		sprintf(args[1], "%d", m);
		sprintf(args[2], "%d", 0);
		sprintf(args[3], "%d", port);
		sprintf(args[4], "%d", introducerPort); //Pass in introducerPort as port of Introducer


		int executed;
//		cout << " time to fork \n";
		executed = execv("./node", args);
		cout << "Did not execute with return value " << executed << endl;
		return 0;

	} else { //Parent process

	//	cout << "After forking...in listener\n";
		/* server is listening on port "port" */

		/* these next three lines are standard */
		//so parent listener trying to contact introducer

		boost::shared_ptr < TSocket > socket(new TSocket("localhost", port));
		boost::shared_ptr < TTransport > transport(
				new TBufferedTransport(socket));
		boost::shared_ptr < TProtocol
				> protocol(new TBinaryProtocol(transport));

		string command;

		do {
			string chord_cmd, argument;

	//		cout << endl << "Enter what you want to do: \n";
			getline(cin, command);
			stringstream stream(command);
			getline(stream, chord_cmd, ' ');

			if (chord_cmd.compare("ADD_NODE") == 0) {
                vector<int> args;
				while (getline(stream, argument, ' ')) {
					args.push_back(atoi(argument.c_str()));
                }
                
                for (int i=0; i<args.size(); i++) {
                    int new_id = args[i];
					int random_port = rand() % 7999 + 2000; //generate a random port for the new Node
					while (1) {
                        vector<int>::iterator it = find(usedports.begin(), usedports.end(), random_port);
						if (it != usedports.end()) {
							random_port++;
						} else {
							break;
						}
					}

			//		cout << "Port generated: " << random_port << endl;
					usedports.push_back(random_port); //Put in the queue

					pid_t pid = fork(); //Fork and execv to create new node

					if (pid < 0) { 
						perror("Unable to fork\n");
					}

					if (pid == 0) { //For the newly created node, child process

						char **args = (char **) malloc(6 * sizeof(char *));

						for (int i = 0; i <= 5; i++) {
							args[i] = (char *) malloc(50 * sizeof(char));
						}

						strcpy(args[0], "./node");
						sprintf(args[1], "%d", m);
						sprintf(args[2], "%d", new_id);
						sprintf(args[3], "%d", random_port);
						sprintf(args[4], "%d", introducerPort);
                        
                        MyServiceClient client(protocol);
				        transport->open();
				        client.ADD_NODE(m, new_id, random_port, introducerPort);
				        transport->close();
					}

				} //end of while() and adding all the nodes
			} else if (chord_cmd.compare("ADD_FILE") == 0) {
				string filename, data, temp;

				getline(stream, filename, ' '); //get the filename

				while (getline(stream, temp, ' ')) { //get the file contents
					data += temp + " ";
				}
				
		//		cout<<"filename is "<<filename<<", data is "<<data<<endl;
				/* I am a MyServiceClient */
				MyServiceClient client(protocol);
				transport->open();
				client.ADD_FILE(filename, data);
				transport->close();

			} else if (chord_cmd.compare("DEL_FILE") == 0) {
			    string filename;
				getline(stream, filename, ' '); //get the filename
				
				/* I am a MyServiceClient */
				MyServiceClient client(protocol);
				transport->open();
				client.DEL_FILE(filename);
				transport->close();

			} else if (chord_cmd.compare("GET_FILE") == 0) {
                string filename;
				getline(stream, filename, ' '); //get the filename
				
				/* I am a MyServiceClient */
				MyServiceClient client(protocol);
				transport->open();
				client.GET_FILE(filename);
				transport->close();

			} else if (chord_cmd.compare("GET_TABLE") == 0) {

				getline(stream, argument, ' '); //get the node ID
                int this_id = atoi(argument.c_str());
                /* I am a MyServiceClient */
				MyServiceClient client(protocol);
				transport->open();
				/* make the call and get the return value */
				client.GET_TABLE(this_id);
				transport->close();
				
			}

		} while (command.compare("QUIT") != 0);

	} //end of else case of fork()


	return 0;
}
