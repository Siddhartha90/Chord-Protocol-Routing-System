#include <stdio.h>
#include <math.h>
#include <string.h>
#ifdef WIN32
#include <io.h>
#endif
#include <fcntl.h>
#include "sha1.h"

#include <vector>
#include <string>
#include <iostream>
#include <map>
#include "hash.h"

#include <assert.h>
#include <stdint.h>
#include <iomanip>
#include <sstream>

#include "MyService.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TSocket.h>					//header for client
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <boost/program_options.hpp>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace ::mp2;
using namespace std;

// the name space specified in the thrift file
using namespace mp2;

/*
   Implementation of various chord functions.
   -Siddhartha gupta
   Based on Chord Paper: Chord: A Scalable Peer to peer
   lookup Service for Internet Applications:
   http://pdos.csail.mit.edu/papers/chord:sigcomm01/
*/
vector<struct node_info *> fingers;

int id;
struct node_info * predecessor;
struct node_info * successor;
int m;
int port;
map<int, struct file_info *> files;   //files of this node mapped according to file names
int introducerPort;                   //stores port of introducer Node
pthread_mutex_t table_mutex;
int stabilizeInterval;
int fixInterval;

struct file_info {
	file_info(string filename, string data) :
		filename(filename), data(data) {
	}
	string filename;
	string data;
};

struct node_info {
	node_info(int id, int port) :
		id(id), port(port) {
	}
	int id;
	int port;
};

void init_finger_table();
struct node_info * find_successor(int value);
struct node_info * get_successor(int node_port);
void introducer_find_successor(MyReturn& _return, int start_val);
int finger_start(int power);
struct node_info* find_predecessor(int value);
struct node_info* closest_preceding_finger(int value);
void notify(int node_id, int node_port);
void update_finger_table(int new_id, int i);
bool check_if_belongs(int result);
bool lies_between(int value, int min, int max, bool left_include, bool right_include);
void get_table(int input_id);
void print_table(int node_port);
void print_table_string();
void print_file_string(int key);
void add_file(string filename, string data);
void get_file(string filename);
void del_file(string filename);

string get_ADD_FILE_result_as_string(const char *fname,
                                         const int32_t key,
                                         const int32_t nodeId);
string get_DEL_FILE_result_as_string(const char *fname,
                                         const int32_t key,
                                         const bool deleted,
                                         const int32_t nodeId);
string get_GET_FILE_result_as_string(const char *fname,
                                         const int32_t key,
                                         const bool found,
                                         const int32_t nodeId,
                                         const char *fdata);
string get_GET_TABLE_result_as_string(
        const vector<struct node_info *>& finger_table,
        const uint32_t m,
        const uint32_t myid,
        const uint32_t idx_of_entry1,
        const std::map<int32_t, struct file_info *>& keys_table);
std::string get_finger_table_as_string(const std::vector<struct node_info *>& table,
                           const uint32_t m,
                           const uint32_t myid,
                           const uint32_t idx_of_entry1);        
std::string
get_keys_table_as_string(const std::map<int32_t, struct file_info *>& table);
    /*
     * example output:
fname= foo.c
key= 3
added to node= 4

    */      
    string get_ADD_FILE_result_as_string(const char *fname,
                                         const int32_t key,
                                         const int32_t nodeId)
    {
        std::stringstream s;
        s << "fname= " << fname << "\n";
        s << "key= " << key << "\n";
        s << "added to node= " << nodeId << "\n";
        return s.str();
    }

    /*
     * example output:
fname= foo.c
key= 3
file not found

     * example output:
fname= bar.h
key= 6
was stored at node= 0
deleted

    */
    string get_DEL_FILE_result_as_string(const char *fname,
                                         const int32_t key,
                                         const bool deleted,
                                         const int32_t nodeId)
    {
        std::stringstream s;
        s << "fname= " << fname << "\n";
        s << "key= " << key << "\n";
        if (deleted) {
            // then nodeId is meaningful
            s << "was stored at node= " << nodeId << "\n";
            s << "deleted\n";
        }
        else {
            // assume that this means file was not found
            s << "file not found\n";
        }
        return s.str();
    }

    /*
     * example output:
fname= foo.c
key= 3
file not found

     * example output:
fname= bar.h
key= 6
stored at node= 0
fdata= this is file bar.h

     */
    string get_GET_FILE_result_as_string(const char *fname,
                                         const int32_t key,
                                         const bool found,
                                         const int32_t nodeId,
                                         const char *fdata)
    {
        std::stringstream s;
        s << "fname= " << fname << "\n";
        s << "key= " << key << "\n";
        if (found) {
            // then nodeId is meaningful
            s << "stored at node= " << nodeId << "\n";
            s << "fdata= " << fdata << "\n";
        }
        else {
            // assume that this means file was not found
            s << "file not found\n";
        }
        return s.str();
    }

    /* example output (node has 2 files):
finger table:
entry: i= 1, interval=[ 5, 6), node= 0
entry: i= 2, interval=[ 6, 0), node= 0
entry: i= 3, interval=[ 0, 4), node= 0
keys table:
entry: k= 1, fname= 123.doc, fdata= this is file 123.doc data
entry: k= 3, fname= 123.txt, fdata= this is file 123.txt data

     * example output (node has no file):
finger table:
entry: i= 1, interval=[ 1, 2), node= 4
entry: i= 2, interval=[ 2, 4), node= 4
entry: i= 3, interval=[ 4, 0), node= 4
keys table:

    *
    */
    string get_GET_TABLE_result_as_string(
        const vector<struct node_info *>& finger_table,
        const uint32_t m,
        const uint32_t myid,
        const uint32_t idx_of_entry1,
        const std::map<int32_t, struct file_info *>& keys_table)
    {
        return get_finger_table_as_string(
            finger_table, m, myid, idx_of_entry1) \
            + \
            get_keys_table_as_string(keys_table);
    }



/*
 * use this get_finger_table_as_string() function. when asked for its
 * finger table, a node should respond with the string returned by
 * this function.
 */


/* "..." is some struct/class that contains a member "id" as the id of
 * the node pointed to by that entry.
 *
 * myid is the id of the node
 * calling this function.
 */

std::string get_finger_table_as_string(const std::vector<struct node_info *>& table,
                           const uint32_t m,
                           const uint32_t myid,
                           const uint32_t idx_of_entry1)
{
    std::stringstream s;
    assert(table.size() == (idx_of_entry1 + m));
    s << "finger table:\n";
    for (size_t i = 1; (i - 1 + idx_of_entry1) < table.size(); ++i) {
        using std::setw;
        struct node_info * info = table.at(i - 1 + idx_of_entry1);
        int id = info->id;
        s << "entry: i= " << setw(2) << i << ", interval=["
          << setw(4) << (myid + (int)pow(2, i-1)) % ((int)pow(2, m))
          << ",   "
          << setw(4) << (myid + (int)pow(2, i)) % ((int)pow(2, m))
          << "),   node= "
          << setw(4) << id
          << "\n";
    }
    return s.str();
}

/********************************************************/

/*
 * use this get_keys_table_as_string() function. when asked for its
 * keys table, a node should respond with the string returned by this
 * function.
 */


/* "..." is some struct/class that contains members "name" and "data"
 * as the name and data of the file.
 */

std::string
get_keys_table_as_string(const std::map<int32_t, struct file_info *>& table)
{
    std::stringstream s;
    std::map<int32_t, struct file_info *>::const_iterator it = table.begin();
    /* std::map keeps the keys sorted, so our iteration will be in
     * ascending order of the keys
     */
    s << "keys table:\n";
    for (; it != table.end(); ++it) {
        using std::setw;
        /* assuming file names are <= 10 chars long */
        s << "entry: k= " << setw(4) << it->first
          << ",  fname= " << setw(10) << (it->second)->filename
          << ",  fdata= " << (it->second)->data
          << "\n";
          
    }
    return s.str();
}

/* 
 returns if successfully forks for a new node
 */

class MyServiceHandler: virtual public MyServiceIf {
public:
	MyServiceHandler() {
		// Your initialization goes here
	}

	void FIND_SUCCESSOR(MyReturn& _return, const int32_t value) {
        struct node_info * info = find_successor(value);
        _return.id = info->id;
        _return.port = info->port;
        free(info);
    }

	void GET_SUCCESSOR(MyReturn& _return) {
		_return.id = successor->id;
		_return.port = successor->port;
	}

    void CLOSEST_PRECEDING_FINGER(MyReturn& _return, const int32_t value) {
        struct node_info * info = closest_preceding_finger(value);
        _return.id = info->id;
        _return.port = info->port;
        free(info);
    }

    void GET_PREDECESSOR(MyReturn& _return) {
        _return.id = predecessor->id;
        _return.port = predecessor->port;
    }
	
	void NOTIFY(const int32_t id, const int32_t port) {
        notify(id, port);
    }

    void GET_TABLE(const int32_t id) {
        get_table(id);
    }
    
    void PRINT_TABLE() {
        print_table_string();
    }
	  
    void ADD_FILE(const std::string& filename, const std::string& data) {
        add_file(filename, data);
    }
    
    void ADD_FILE_TO(const int32_t key, const std::string& filename, const std::string& data) {
		files[key] = new struct file_info(filename, data);
		cout << get_ADD_FILE_result_as_string(filename.c_str(), key, id);
		cout<<"node= "<<id<<": added file: k= "<<key<<endl;
    }
    
    void ADD_FILE_TO_UPDATE(const int32_t key, const std::string& filename, const std::string& data) {
		files[key] = new struct file_info(filename, data);
    }
    
    void DEL_FILE(const std::string& filename) {
        del_file(filename);
    }
    
    void DEL_FILE_TO(const int32_t key, const std::string& filename) {
        if (files[key]) {
			files.erase(key);
			cout << get_DEL_FILE_result_as_string(filename.c_str(), key, true, id);
			cout<<"node= "<<id<<": deleted file: k= "<<key<<endl;
		} else {
			cout << get_DEL_FILE_result_as_string(filename.c_str(), key, false, id);
			cout<<"node= "<<id<<": no such file: k= "<<key<<" to delete"<<endl;
		}  
    }
    
    void GET_FILE(const std::string& filename) {
        get_file(filename);
    }

    void GET_FILE_TO(const int32_t key, const std::string& filename) {
        if (files[key]) {
			cout << get_GET_FILE_result_as_string(filename.c_str(), key, true, id, (files[key]->data).c_str());
			cout<<"node= "<<id<<": served file: k= "<<key<<endl;
		} else {
			cout << get_GET_FILE_result_as_string(filename.c_str(), key, false, id, NULL);
			cout<<"node= "<<id<<": no such file: k= "<<key<<" to serve"<<endl;
		}
    }
};

void print_file_string(int key) {
    if (files[key]) {
        cout << get_GET_FILE_result_as_string((files[key]->filename).c_str(), key, true, id, (files[key]->data).c_str());
    } else {
        cout << get_GET_FILE_result_as_string((files[key]->filename).c_str(), key, false, id, (files[key]->data).c_str());
    }
}

void print_table_string() {
    string result = get_GET_TABLE_result_as_string(fingers, m, id, 0, files);
    cout<<result;
}

void print_table(int node_port) {
    if (node_port == port) {
        print_table_string();
        return;
    }

    boost::shared_ptr < TSocket > socket(
            new TSocket("localhost", node_port));
    boost::shared_ptr < TTransport > transport(
            new TBufferedTransport(socket));
    boost::shared_ptr < TProtocol > protocol(
            new TBinaryProtocol(transport));
    /* I am a MyServiceClient */
    MyServiceClient client(protocol);
    transport->open();
    client.PRINT_TABLE();
    transport->close();

}

void get_table (int value) {
    int temp = value+1;
    if (temp >= pow(2,m)) {
        temp -= pow(2, m);
    }
    struct node_info * info = find_predecessor(temp);
    if (info->id != value) {
        return;
    }
    print_table(info->port);
}

struct node_info * find_successor(int value) {
    struct node_info * info = find_predecessor(value);
    int node_port = info->port;
    free(info);
    return get_successor(node_port);
}

// get successor of the node with "node_port"
struct node_info * get_successor(int node_port) {
    if (node_port == port) {
        return new struct node_info(successor->id, successor->port);
    }

    boost::shared_ptr < TSocket > socket(
            new TSocket("localhost", node_port));
    boost::shared_ptr < TTransport > transport(
            new TBufferedTransport(socket));
    boost::shared_ptr < TProtocol > protocol(
            new TBinaryProtocol(transport));
    /* I am a MyServiceClient */
    MyServiceClient client(protocol);
    transport->open();
    MyReturn result;
    client.GET_SUCCESSOR(result);
    transport->close();

    
    return new struct node_info (result.id, result.port);
}

// get predecessor of the node with "node_port"
struct node_info * get_predecessor(int node_port) {
    if (node_port == port) {
        return new struct node_info(predecessor->id, predecessor->port);
    }

    boost::shared_ptr < TSocket > socket(
            new TSocket("localhost", node_port));
    boost::shared_ptr < TTransport > transport(
            new TBufferedTransport(socket));
    boost::shared_ptr < TProtocol > protocol(
            new TBinaryProtocol(transport));
    /* I am a MyServiceClient */
    MyServiceClient client(protocol);
    transport->open();
    MyReturn result;
    client.GET_PREDECESSOR(result);
    transport->close();

    
    return new struct node_info (result.id, result.port);
}

/*	
 * Function to retrieve the predecessor of a value passed in, done through
 * an RPC to any existing node (maybe the introducer)
 * Arguments - The value passed in
 */
struct node_info* find_predecessor(int value) {
	struct node_info *n = new node_info(id, port);
	while (!lies_between(value, n->id, (get_successor(n->port))->id, false, true)) {
	    if (n->id == id) {
	        n = closest_preceding_finger(value);
	    } else {

			boost::shared_ptr < TSocket > socket(
					new TSocket("localhost", n->port));
			boost::shared_ptr < TTransport > transport(
					new TBufferedTransport(socket));
			boost::shared_ptr < TProtocol > protocol(
					new TBinaryProtocol(transport));
			/* I am a MyServiceClient */
			MyServiceClient client(protocol);
			transport->open();
			MyReturn result;
			client.CLOSEST_PRECEDING_FINGER(result, value);
			transport->close();
			
			n->id = result.id;
			n->port = result.port;
		}
	}
	return n;
}

struct node_info* closest_preceding_finger(int value) {
	for (int i=m-1; i>=0; i--) {
		if (lies_between(fingers[i]->id, id, value, false, false)) {
			return new struct node_info(fingers[i]->id, fingers[i]->port);
		}
	}
	return new struct node_info(id, port);
}

/*
 Function to check if a value belongs to a certain node interval
 */

bool lies_between(int value, int min, int max, bool left_include, bool right_include) {
	if (max <= min) //edge case
		max = max + pow(2, m);
    
    bool left = left_include ? (value >= min) : (value > min);
    bool right = right_include ? (value <= max) : (value < max);
	
	return left && right;
}


/*
 To calculate the finger[i].start as described in the paper.
 Argument - finger table entry index number

 */
int finger_start(int power) {
	int temp = id + pow(2, power);
	int temp2 = pow(2, m); // id + 2^power mod #entries
	return temp % temp2;
}

/*
 * Assuming the listener gave the add file command to the introducer with id = 0
 */
void add_file(string filename, string data) {
	char *name = (char*) malloc(50);
	strcpy(name, filename.c_str());

	int key = hash_it(name, m); //result contains the hash value

	if (!key) {
		return;
	}
	
    struct node_info * info = find_successor(key);
    if (info->port == port) {
		files[key] = new struct file_info(filename, data);
		cout << get_ADD_FILE_result_as_string(filename.c_str(), key, id);
		cout<<"node= "<<id<<": added file: k= "<<key<<endl;
	} else {
        boost::shared_ptr < TSocket > socket(
                new TSocket("localhost", info->port));
        boost::shared_ptr < TTransport > transport(
                new TBufferedTransport(socket));
        boost::shared_ptr < TProtocol > protocol(
                new TBinaryProtocol(transport));
        /* I am a MyServiceClient */
        MyServiceClient client(protocol);
        transport->open();
        client.ADD_FILE_TO(key, filename, data);
        transport->close();
	}
}

/*
 * Function to retrieve a file and its contents by hashing its contents
 * to the appropriate key and mapping that to the correct node, 
 * and then retrieving its contents by using the 'files' map entry.
 *
 * Arguments - Filename as passed in by user
 */
void get_file(string filename) {
	char *name = (char*) malloc(50);
	strcpy(name, filename.c_str());

	int key = hash_it(name, m); //result contains the hash value

	if (!key) {
	    perror("key cannot be generated\n");
		return;
	}

    struct node_info * info = find_successor(key);
    if (info->port == port) {
		if (files[key]) {
			cout << get_GET_FILE_result_as_string(filename.c_str(), key, true, id, (files[key]->data).c_str());
			cout<<"node= "<<id<<": served file: k= "<<key<<endl;
		} else {
			cout << get_GET_FILE_result_as_string(filename.c_str(), key, false, id, NULL);
			cout<<"node= "<<id<<": no such file: k= "<<key<<" to serve"<<endl;
		}

	} else {
        boost::shared_ptr < TSocket > socket(
                new TSocket("localhost", info->port));
        boost::shared_ptr < TTransport > transport(
                new TBufferedTransport(socket));
        boost::shared_ptr < TProtocol > protocol(
                new TBinaryProtocol(transport));
        /* I am a MyServiceClient */
        MyServiceClient client(protocol);
        transport->open();
        client.GET_FILE_TO(key, filename);
        transport->close();
	}
}

/*
 * Function to delete a file and its contents by hashing its 
 * contents to the appropriate key and mapping that to the correct node,
 * and then removing the 'files' map entry from that node.
 * Arguments - Filename as passed in by user
 */
void del_file(string filename) {
	char *name = (char*) malloc(50);
	strcpy(name, filename.c_str());

	int key = hash_it(name, m); //result contains the hash value

	if (!key) {
		return;
	}
    struct node_info * info = find_successor(key);
    if (info->port == port) {
		if (files[key]) {
			files.erase(key);
			cout << get_DEL_FILE_result_as_string(filename.c_str(), key, true, id);
			cout<<"node= "<<id<<": deleted file: k= "<<key<<endl;
		} else {
			cout << get_DEL_FILE_result_as_string(filename.c_str(), key, false, id);
			cout<<"node= "<<id<<": no such file: k= "<<key<<" to delete"<<endl;
		}  
	} else {
        boost::shared_ptr < TSocket > socket(
                new TSocket("localhost", info->port));
        boost::shared_ptr < TTransport > transport(
                new TBufferedTransport(socket));
        boost::shared_ptr < TProtocol > protocol(
                new TBinaryProtocol(transport));
        /* I am a MyServiceClient */
        MyServiceClient client(protocol);
        transport->open();
        client.DEL_FILE_TO(key, filename);
        transport->close();
	}
}


void update_files() {
    map<int,struct file_info *>::iterator it;
    
	vector<int> keys_to_delete;
    for (it=files.begin(); it != files.end(); it++) {
        if ((*it).first <= predecessor->id) {
            // move the file to its predecessor
            boost::shared_ptr < TSocket > socket(
                new TSocket("localhost", predecessor->port));
            boost::shared_ptr < TTransport > transport(
                new TBufferedTransport(socket));
            boost::shared_ptr < TProtocol > protocol(
                new TBinaryProtocol(transport));
            MyServiceClient client(protocol);
	        transport->open();
			client.ADD_FILE_TO_UPDATE((*it).first, (*it).second->filename, 
			    (*it).second->data);
			transport->close();
			keys_to_delete.push_back((*it).first);
        }
    }
    
    for (int i=0; i<keys_to_delete.size(); i++) {
        files.erase(keys_to_delete[i]);
    }
}

void notify(int node_id, int node_port) {
    if (predecessor->id==-1 || lies_between(node_id, predecessor->id,id, false, false)) {
        predecessor->id = node_id;						
        predecessor->port = node_port;
        if (predecessor->id != id) {
	    cout<<"node= "<<id<<": updated predecessor= "<<predecessor->id<<endl;		//logging
            update_files();
        }
    }
}

void *stabilize_thread_main(void *discard) {
    while (true) {
        struct node_info * info = get_predecessor(successor->port);
        if (lies_between(info->id, id, successor->id, false, false)) {
            pthread_mutex_lock(&table_mutex);
            successor->id = info->id;
            successor->port = info->port;
            fingers[0]->id = successor->id;
            fingers[0]->port = successor->port;
            pthread_mutex_unlock(&table_mutex);
        }
        free (info);
        
        if (successor->id != id) {
            boost::shared_ptr < TSocket > socket(
					new TSocket("localhost", successor->port));
			boost::shared_ptr < TTransport > transport(
					new TBufferedTransport(socket));
			boost::shared_ptr < TProtocol > protocol(
					new TBinaryProtocol(transport));
			/* I am a MyServiceClient */
			MyServiceClient client(protocol);
			transport->open();
			client.NOTIFY(id, port);
			transport->close();
        }
        
        sleep(stabilizeInterval);
    }
}

void *fixfingers_thread_main(void *discard) {
    while (true) {
        srand ( time(NULL) );
        int i = rand() % (m-1) + 1;
        
        pthread_mutex_lock(&table_mutex);
	struct node_info * info = find_successor(finger_start(i));
	if (fingers[i]->id!=info->id) {	
        	fingers[i] = info;
		cout<<"node= "<<id<<": updated finger entry: "<<"i= "<<i<<", pointer= "<<info->id<<endl;
	}
        pthread_mutex_unlock(&table_mutex);
        
        sleep(fixInterval);
    }
}

int main(int argc, char **argv) {
	
	int retval = pthread_mutex_init(&table_mutex, NULL);
    if (retval != 0)
    {
        perror("pthread_mutex_init.");
        exit(1);
    }
	
	id = strtol(argv[2], NULL, 10);
	m = strtol(argv[1], NULL, 10);
	port = strtol(argv[3], NULL, 10);
	introducerPort = strtol(argv[4], NULL, 10);
	stabilizeInterval = strtol(argv[5], NULL, 10);
	if (stabilizeInterval==-1) {
	    stabilizeInterval = 1;
	}
    fixInterval = strtol(argv[6], NULL, 10);
    if (fixInterval==-1) {
        fixInterval = 1;
    }
	
	predecessor = new struct node_info(-1, -1);
	if (port == introducerPort) {
	    successor = new struct node_info (id, port);
	   cout<<"node= "<<id<<": initial successor= "<<id<<endl;				//logging
	} else {
	    boost::shared_ptr < TSocket > socket(
                new TSocket("localhost", introducerPort));
        boost::shared_ptr < TTransport > transport(
                new TBufferedTransport(socket));
        boost::shared_ptr < TProtocol > protocol(
                new TBinaryProtocol(transport));
        
        /* I am a MyServiceClient */
        MyServiceClient client(protocol);
        transport->open();
        MyReturn result;
        client.FIND_SUCCESSOR(result, id);
	    transport->close();
	    
        successor = new struct node_info (result.id, result.port);
	cout<<"node= "<<id<<": initial successor= "<<result.id<<endl;				//logging
    }
    
    pthread_t stabilize_thread;
	pthread_t fixfingers_thread;

	if (pthread_create(&stabilize_thread, NULL, &stabilize_thread_main, NULL) != 0) {
		fprintf(stderr, "Error in pthread_create\n");
		exit(1);
	}

	if (pthread_create(&fixfingers_thread, NULL, &fixfingers_thread_main, NULL) != 0) {
		fprintf(stderr, "Error in pthread_create\n");
		exit(1);
	}

    pthread_mutex_lock(&table_mutex);
    for (int i=0; i<m; i++) {
        fingers.push_back(new struct node_info(-1, -1));
    }
    fingers[0]->id = successor->id;
    fingers[0]->port = successor->port;
    pthread_mutex_unlock(&table_mutex);

    /*
	 Start a server conection for listener to connect to
	 */
	
	shared_ptr<MyServiceHandler> handler(new MyServiceHandler());
    shared_ptr<TProcessor> processor(new MyServiceProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();

	return 0;
}
