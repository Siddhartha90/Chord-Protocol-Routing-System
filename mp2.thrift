namespace cpp mp2

struct MyObject {
    1: list <i32> IDS;
}

struct MyReturn {
    1: i32 id;
	2: i32 port;
}


/* declare the RPC interface of your network service */
service MyService {
    /* this function takes 3 arguments */
	void ADD_NODE(1: i32 m, 2: i32 new_id, 3: i32 random_port, 4: i32 introducerPort)
	MyReturn FIND_SUCCESSOR(1: i32 value);
	MyReturn GET_SUCCESSOR();
	MyReturn CLOSEST_PRECEDING_FINGER(1: i32 value);
	MyReturn GET_PREDECESSOR();
	void NOTIFY (1: i32 id, 2: i32 port);	
	void GET_TABLE (1: i32 id);
	void PRINT_TABLE();
	void ADD_FILE (1:string filename, 2: string data);
	void ADD_FILE_TO(1: i32 key, 2: string filename, 3: string data);
	void DEL_FILE (1:string filename);
	void DEL_FILE_TO(1: i32 key, 2: string filename);
	void GET_FILE (1:string filename);
	void GET_FILE_TO(1: i32 key, 2: string filename);
}
