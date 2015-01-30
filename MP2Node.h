/**********************************
 * FILE NAME: MP2Node.h
 * AUTHOR: Manish Kumar Pandey
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */

// Transaction class that stores the details of the transactions. It stores the transaction id, transaction time, reply count, type of msg, key, val and validity. Class also provide its setter and getter methods.
class Transaction {
private :
	int trId;
	int trTime;
	int numReplies;
    int type_of_msg;
    string trKey;
    string trVal;
	int valid;

public :
	Transaction(int trId, int type_of_msg, string trKey, string trVal, int trTime) {
		this->trId = trId;
		this->type_of_msg = type_of_msg;
		this->trKey = trKey;
		this->trVal = trVal;
		this->trTime = trTime;
		this->valid = 1;
		this->numReplies = 0;
	}
	Transaction(const Transaction &trObj){
    }

	Transaction & operator=(const Transaction &trObj) {
		cout << "Copy constructor allocating ptr." << endl;
		this->trId = trObj.trId;
		this->trTime = trObj.trTime;
		this->numReplies = trObj.numReplies;
		this->type_of_msg = trObj.type_of_msg;
		this->trKey = trObj.trKey;
		this->trVal = trObj.trVal;
		this->valid = trObj.valid;
		return *this;
	}


	int getNumReplies(){
		return this->numReplies;
	}

	void setNumReplies(int numReplies){
		this->numReplies = numReplies;
	}

	int getTransactionTime(){
		return this->trTime;
	}

	int getTypeOfMessage(){
		return this->type_of_msg;
	}

	string getTrKey(){
		return this->trKey;
	}

	string getTrValue(){
		return this->trVal;
	}

	bool isValid(){
		return this->valid == 1;
	}

	void setInactive(){
		this->valid = 0;
	}

	void setTrValue(string value){
		this->trVal = value;
	}
};

class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	// Map from trans id to transaction information
    map<int, Transaction*> trInfo;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

    ~MP2Node();
};

#endif /* MP2NODE_H_ */
