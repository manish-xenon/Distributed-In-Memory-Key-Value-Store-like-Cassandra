/**********************************
 * FILE NAME: MP2Node.cpp
 * AUTHOR: Manish Kumar Pandey
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"
/**
 * Constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
    // Check if the ring is Empty
    if (ring.empty()){
        // Assigned the current Member List to the Ring and also assigned the hasMyReplicas and haveReplicasOf
        ring = curMemList;
        for (int j = 0; j < ring.size(); j++) {
        	// Find the position of this node in the ring
            if (memcmp(ring[j].getAddress()->addr, &getMemberNode()->addr, sizeof(Address)) == 0) { 
                switch(j){
                    case 0 : {
                    	// If this node is the starting node at location 0 then it has replicas of last 2 nodes in the ring
                        Node *temp_node = new Node(ring[ring.size() - 1].nodeAddress);
                        haveReplicasOf.push_back(*temp_node);
                        temp_node = new Node(ring[ring.size() - 2].nodeAddress);
                        haveReplicasOf.push_back(*temp_node);
                        break;
                    }
                    case 1 : {
                    	// If this node is at location 1, then assign first and last node in the ring to its haveReplicasOf
                        Node *temp_node = new Node(ring[j-1].nodeAddress);
                        haveReplicasOf.push_back(*temp_node);
                        temp_node = new Node(ring[ring.size() - 1].nodeAddress);
                        haveReplicasOf.push_back(*temp_node);
                        break;
                    }
                    default : {
                    	// If node is not at 0 or 1 location then it will have replicas of previous 2 nodes in the ring
                        Node *temp_node = new Node(ring[(j - 1) % ring.size()].nodeAddress);
                        haveReplicasOf.push_back(*temp_node);
                        temp_node = new Node(ring[(j - 2) % ring.size()].nodeAddress);
                        haveReplicasOf.push_back(*temp_node);
                        break;
                    }
                }
                //The replicas of keys which are primary at this node will be the next 2 nodes in the ring
                // Modulo Operator checks for boundary conditions when the node is last or previous to last in the ring.
                Node *temp_node = new Node(ring[(j + 1) % ring.size()].nodeAddress);
                hasMyReplicas.push_back(*temp_node);
                temp_node = new Node(ring[(j + 2) % ring.size()].nodeAddress);
                hasMyReplicas.push_back(*temp_node);
            }
        }
    }

    // If hashtable is not empty and ring has not changed, stablization will be needed
    if (!ht->isEmpty() && ring.size() != curMemList.size()){
        change = true;
    }

    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring

     // If stablization is needed then assign the new member list to the ring and call stabilization protocol
    if (change) {
        ring = curMemList;
        stabilizationProtocol();
        cout << "Manish, Stablization is required" << endl;
    }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {

	// Find the servers where the create message should go
    vector<Node> msg_recipients = findNodes(key);

    // Send create message to primary server as well as its 2 replica
    // Message will contain the address where the message needs to be sent,
    // Message Type as create, key, value and type of replica to which message is being sent.
    Message *messsage;
    
    int trId = g_transID; // Get transaction id from the global transaction id

    messsage = new Message(trId, getMemberNode()->addr, CREATE, key, value, PRIMARY);
    messsage->delimiter = "::";
    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[0].getAddress(), messsage->toString());

    messsage = new Message(trId, getMemberNode()->addr, CREATE, key, value, SECONDARY);
    messsage->delimiter = "::";
    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[1].getAddress(), messsage->toString());

    messsage = new Message(trId, getMemberNode()->addr, CREATE, key, value, TERTIARY);
    messsage->delimiter = "::";
    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[2].getAddress(), messsage->toString());

    // Save the transaction details inside a datastructure so that it can be checked and updated later.
    trInfo[trId] = new Transaction(trId, static_cast<int>(CREATE), key, value, par->getcurrtime());

    g_transID++;// Increment the global transaction id

    cout << "Manish, Create Message received " << endl;

}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){

	// Find the servers where the read message should be sent
    vector<Node> msg_recipients = findNodes(key);

    // Send read message to primary server as well as its 2 replica
    // Message will contain the address where the message needs to be sent,
    // Message Type as read, and key for which message is being sent.
    Message *messsage;

    int trId = g_transID;// Get transaction id from the global transaction id

    messsage = new Message(trId, getMemberNode()->addr, READ, key);
    messsage->delimiter = "::";

    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[0].getAddress(), messsage->toString());

    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[1].getAddress(), messsage->toString());

    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[2].getAddress(), messsage->toString());
	
	// Save the transaction details inside a datastructure so that it can be checked and updated later.
    trInfo[trId] = new Transaction(trId, static_cast<int>(READ), key, "", par->getcurrtime());

    g_transID++;// Increment the global transaction id

    cout << "Manish, READ Message received " << endl;

}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
    
	// Find the servers where the update message should be sent
    vector<Node> msg_recipients = findNodes(key);

    // Send update message to primary server as well as its 2 replica
    // Message will contain the address where the message needs to be sent,
    // Message Type as update, key, new value and type of replica to which message is being sent.
    Message *messsage;

    int trId = g_transID; // Get transaction id from the global transaction id
    
    messsage = new Message(trId, getMemberNode()->addr, UPDATE, key, value, PRIMARY);
    messsage->delimiter = "::";

    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[0].getAddress(), messsage->toString());

    messsage = new Message(trId, getMemberNode()->addr, UPDATE, key, value, SECONDARY);
    messsage->delimiter = "::";
    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[1].getAddress(), messsage->toString());

    messsage = new Message(trId, getMemberNode()->addr, UPDATE, key, value, TERTIARY);
    messsage->delimiter = "::";
    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[2].getAddress(), messsage->toString());

    // Save the transaction details inside a datastructure so that it can be checked and updated later.
    trInfo[trId] = new Transaction(trId, static_cast<int>(UPDATE), key, value, par->getcurrtime());

    g_transID++;// Increment the global transaction id

    cout << "Manish, Update Message received " <<  endl;

}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){

	// find the servers where the delete message should be sent
    vector<Node> msg_recipients = findNodes(key);

    // Send delete message to primary server as well as its 2 replica
    // Message will contain the address where the message needs to be sent,
    // Message Type as delete, and key to be deleted.
    Message *messsage;

    int trId = g_transID; // Get transaction id from the global transaction id

    messsage = new Message(trId, getMemberNode()->addr, DELETE, key);
    messsage->delimiter = "::";

    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[0].getAddress(), messsage->toString());

    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[1].getAddress(), messsage->toString());

    emulNet->ENsend(&getMemberNode()->addr, msg_recipients[2].getAddress(), messsage->toString());

    // Save the transaction details inside a datastructure so that it can be checked and updated later.
    trInfo[trId] = new Transaction(trId, static_cast<int>(DELETE), key, "", par->getcurrtime());

    g_transID++;// Increment the global transaction id

    cout << "Manish, Delete Message " << endl;

}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	
    cout << "Manish server create function" << endl;
    Entry *entry = new Entry(value, par->getcurrtime(), replica); // An entry object that will hold value, time and replica type
    bool create_status = ht->create(key, entry->convertToString()); // Try to insert into the local hashtable of the server
    free(entry); // Free entry object
    return create_status; // Return the status of create operation.
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {

    string valueRead = ht->read(key); // Get the value corresponding to a key
    if (valueRead.compare("") == 0)
        cout << "Manish Read server function " << key << " and value is " << valueRead << endl;
    return valueRead;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {

    cout << "Manish Update server function " << endl;
    Entry *entry = new Entry(value, par->getcurrtime(), replica); // An entry object that will hold the value, current time and replica type
    bool update_status = ht->update(key, entry->convertToString()); // Updated the hashtable to reflect the new value
    free(entry);
    return update_status;

}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {

    cout << "Manish delete server function " << endl;
    bool delete_status = ht->deleteKey(key); // Here deleted the key from Hashtable

    if (key.compare("invalidKey") == 0)
        cout << "Manish Invalid " << delete_status << endl;
    return delete_status;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {

	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// Dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */

		// Retrieve the message in string format
		 // Separate the message by delimiter and form a vector of strings that reflect each part of the message
        string delimiter = "::";
        size_t delim_loc = 0;
        vector<string> message_by_parts;

        while ((delim_loc = message.find(delimiter)) != string::npos) {
            message_by_parts.emplace_back(message.substr(0, delim_loc));
            message.erase(0, delim_loc + delimiter.length());
        } // At each delimiter location add the message part into vector of strings
        message_by_parts.push_back(message);

        MessageType mtype = static_cast<MessageType>(atoi(message_by_parts[2].c_str())); // Get the message type.

        if (mtype == CREATE) { // If message type is create
            cout << "Create message request going to server" << endl;
            bool return_status = createKeyValue(message_by_parts[3], message_by_parts[4], static_cast<ReplicaType>(atoi(message_by_parts[5].c_str()))); // Call server createKeyValue function with key, value and replica type from the message
            int temp_trID = atoi(message_by_parts[0].c_str());
            if (temp_trID != -100){ // If message type is not type reserved for stablization message which doesn't neeed to send the reply.
                Message *message;
                message = new Message(temp_trID, getMemberNode()->addr, REPLY, return_status); // Send a reply message to the coordinator
                message->delimiter = "::";
                Address *rx_address = new Address(message_by_parts[1]); // Get the address of the coordinator

                emulNet->ENsend(&getMemberNode()->addr, rx_address, message->toString()); // Send the reply message through emulnet
                
                // If return status is true then log Create Success otherwise log create failure.
                if (return_status == true){
                    log->logCreateSuccess(&getMemberNode()->addr, false, temp_trID, message_by_parts[3], message_by_parts[4]);
                }
                else{
                    log->logCreateFail(&getMemberNode()->addr, false, temp_trID, message_by_parts[3], message_by_parts[4]);
                }
            }
        } else if (mtype == DELETE) { // If the message type is delete
            cout << "Delete message request going to server" << endl;

            bool return_status = deletekey(message_by_parts[3]); // Call delete server operation with key
            int temp_trID = atoi(message_by_parts[0].c_str()); // Get the transaction id of the message
            Address *rx_address = new Address(message_by_parts[1]); //Get the address of the coordinator

            Message *message;
            message = new Message(temp_trID, getMemberNode()->addr, REPLY, return_status);// Construct a reply message to send to coordinator
            message->delimiter = "::";

            emulNet->ENsend(&getMemberNode()->addr, rx_address, message->toString()); // Send the reply message through emulnet

            
            // If return status is true then log Delete Success otherwise log delete failure.
            if (return_status == true){
                log->logDeleteSuccess(&getMemberNode()->addr, false, temp_trID, message_by_parts[3]);
            }
            else {
                log->logDeleteFail(&getMemberNode()->addr, false, temp_trID, message_by_parts[3]);
            }

        } else if (mtype == READ) { // If the message type is read
            cout<<"Manish read request going to server"<<endl;
            string valueRead = readKey(message_by_parts[3]); // Call read server operation with key
            int temp_trID = atoi(message_by_parts[0].c_str()); // Get the transaction id of the message
            Address *rx_address = new Address(message_by_parts[1]); // Get the address of the coordinator

            Message *message;
            message = new Message(temp_trID, getMemberNode()->addr, valueRead); // Construct a read reply to the coordinator
            message->delimiter = "::";

            emulNet->ENsend(&getMemberNode()->addr, rx_address, message->toString()); // Send the read reply message through emulnet

            if(valueRead.compare("") != 0) // If the value read is not of invalid key then log read success otherwise log failure
            {
                log->logReadSuccess(&getMemberNode()->addr, false, temp_trID, message_by_parts[3], valueRead);
            }
            else
            {
                log->logReadFail(&getMemberNode()->addr, false, temp_trID, message_by_parts[3]);
            }
        } else if (mtype == UPDATE) { // If the message type is update
            bool return_status = updateKeyValue(message_by_parts[3], message_by_parts[4], static_cast<ReplicaType>(atoi(message_by_parts[5].c_str()))); // Call update with key and new value and pass the replica type
            int temp_trID = atoi(message_by_parts[0].c_str());

            if (temp_trID != -100){ // msg type should not be replied back to coordinator reserved for stablization message
                Address *rx_address = new Address(message_by_parts[1]); // Get the address of the coordinator

                Message *message;
                message = new Message(temp_trID, getMemberNode()->addr, REPLY, return_status); // Construct a reply message to send to coordinator
                message->delimiter = "::";

                emulNet->ENsend(&getMemberNode()->addr, rx_address, message->toString()); // Send the reply message throguh the emulnet

                // If the return status is true log update success otherwise log update failure
                if (return_status == true){ 
                    log->logUpdateSuccess(&getMemberNode()->addr, false, temp_trID, message_by_parts[3], message_by_parts[4]);
                } else {
                    log->logUpdateFail(&getMemberNode()->addr, false, temp_trID, message_by_parts[3], message_by_parts[4]);
                }
            }

        } else if (mtype == REPLY) { // If the message type is reply
            Address *sender_address = new Address(message_by_parts[1]); // Get the address of the node which sent the message
            int temp_trId = atoi(message_by_parts[0].c_str()); // Get the transaction id of the message
            int return_status = atoi(message_by_parts[3].c_str()); // Get the return status from the content of the message

            int num_replies = trInfo[temp_trId]->getNumReplies(); // Get the reply count for this message having particular transaction id.
            
            // If the return status is 1, then increase the number of replies otherwise do nothing
            if (return_status == 1){
                trInfo[temp_trId]->setNumReplies(num_replies + 1);
            } else {
                ;
            }
        } else if (mtype == READREPLY) { // If the message type is readreply
            Address *sender_address = new Address(message_by_parts[1]); // Get the address of the node which sent the message
            int temp_trId = atoi(message_by_parts[0].c_str());// Get the transaction id of the message

            string valueRead = message_by_parts[3]; // Get the value read by the main server

            cout << "Manish reply with transaction id = " << temp_trId << " with value = " << valueRead << endl;

            int num_replies = trInfo[temp_trId]->getNumReplies(); // Get the reply count for this message having particular transaction id.

            if(valueRead.compare("") != 0) // If the value read is not empty, then increase the number of replies otherwise do nothing. Also, set the value of the value read in transaction info.
            {
                trInfo[temp_trId]->setNumReplies(num_replies + 1);
                Entry *entry = new Entry(valueRead);
                trInfo[temp_trId]->setTrValue(entry->value);
                free(entry);
            }
            else
            {
                trInfo[temp_trId]->setTrValue(""); // Otherwise set the value of empty.
            }
        }

    }


    int current_system_time = par->getcurrtime(); // Get the current system time

    for (map<int, Transaction*>::iterator iterator=trInfo.begin(); iterator!=trInfo.end(); ++iterator){ // Check for all the transactions which were coordinated by this node
        cout << iterator->first << " => " << iterator->second->getTransactionTime() << '\n';
        if (!iterator->second->isValid()) // Check if the transaction is valid
            continue;
        MessageType messageType = static_cast<MessageType>(iterator->second->getTypeOfMessage());
        if (iterator->second->getNumReplies() < 2){ // If the number of replies is less than 2 and current time is already above timeout of the particular transaction then log the failure of that particular type of message for the coordinator.
            if ((iterator->second->getTransactionTime() + 3) <= current_system_time) {
                if (messageType == CREATE){
                    log->logCreateFail(&getMemberNode()->addr, true, iterator->first, iterator->second->getTrKey(), iterator->second->getTrValue());
                    iterator->second->setInactive();
                } else if (messageType == DELETE) {
                    log->logDeleteFail(&getMemberNode()->addr, true, iterator->first, iterator->second->getTrKey());
                    iterator->second->setInactive();
                } else if (messageType == READ) {
                    log->logReadFail(&getMemberNode()->addr, true, iterator->first, iterator->second->getTrKey());
                    iterator->second->setInactive();
                } else if (messageType == UPDATE) {
                    log->logUpdateFail(&getMemberNode()->addr, true, iterator->first, iterator->second->getTrKey(), iterator->second->getTrValue());
                    iterator->second->setInactive();
                }
                // After logging failure, set this transaction details to be inactive.
            }
        } else {
        	// If the number of replies is greater than 2 for a particular transaction then log success for the coordinator
            if (messageType == CREATE) {
                log->logCreateSuccess(&getMemberNode()->addr, true, iterator->first, iterator->second->getTrKey(), iterator->second->getTrValue());
                iterator->second->setInactive();
            } else if (messageType == DELETE) {
                log->logDeleteSuccess(&getMemberNode()->addr, true, iterator->first, iterator->second->getTrKey());
                iterator->second->setInactive();
            } else if (messageType == READ) {
                if (iterator->second->getTrValue() != ""){ // If the value read is not empty then log success otherwise log failure.
                    log->logReadSuccess(&getMemberNode()->addr, true, iterator->first, iterator->second->getTrKey(), iterator->second->getTrValue());
                } else {
                    log->logReadFail(&getMemberNode()->addr, true, iterator->first, iterator->second->getTrKey());
                }
                iterator->second->setInactive();
            } else if (messageType == UPDATE) {
                log->logUpdateSuccess(&getMemberNode()->addr, true, iterator->first, iterator->second->getTrKey(), iterator->second->getTrValue());
                iterator->second->setInactive();
            }
            // After logging success, set this transaction details to be inactive.

        }
    }


	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// If pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// Go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */

void MP2Node::stabilizationProtocol() {

    cout << "Manish code in stablization function " << endl;

    vector<Node> to_be_predecessor;
    vector<Node> to_be_successor;

    for (int j = 0; j < ring.size(); j++) {
        if (memcmp(ring[j].getAddress()->addr, &getMemberNode()->addr, sizeof(Address)) == 0) { // Find the location of this node and assign new predecessors and successors according to the location in the ring.
            switch(j){
                case 0 : {
                    Node *temp_node = new Node(ring[ring.size() - 1].nodeAddress);
                    to_be_predecessor.push_back(*temp_node);
                    temp_node = new Node(ring[ring.size() - 2].nodeAddress);
                    to_be_predecessor.push_back(*temp_node);
                    break;
                    }
                case 1 : {
                    Node *temp_node = new Node(ring[0].nodeAddress);
                    to_be_predecessor.push_back(*temp_node);
                    temp_node = new Node(ring[ring.size() - 1].nodeAddress);
                    to_be_predecessor.push_back(*temp_node);
                    break;
                }
                default : {
                    Node *temp_node = new Node(ring[(j - 1) % ring.size()].nodeAddress);
                    to_be_predecessor.push_back(*temp_node);
                    temp_node = new Node(ring[(j - 2) % ring.size()].nodeAddress);
                    to_be_predecessor.push_back(*temp_node);
                    break;
                }
            }
            Node *temp_node = new Node(ring[(j + 1) % ring.size()].nodeAddress);
            to_be_successor.push_back(*temp_node);
            temp_node = new Node(ring[(j + 2) % ring.size()].nodeAddress);
            to_be_successor.push_back(*temp_node);
        }
    }


    // Check for change in replicas
    for (int j = 0; j < to_be_predecessor.size(); j++) {
        ReplicaType rt = static_cast<ReplicaType>(j + 1);
        if (memcmp(hasMyReplicas[j].getAddress()->addr, to_be_successor[j].getAddress()->addr, sizeof(Address)) == 0){
            continue; // If this replica is same as before then move forward.
        }
        else {
            int node_found = 0; // If this replica is previously present update the value of keys that are primary to this server otherwise for new replica create them by sending message according to its type.
            for (vector<Node>::iterator itr = hasMyReplicas.begin(); itr != hasMyReplicas.begin(); ++itr){
                if (memcmp(to_be_successor[j].getAddress()->addr, itr->getAddress()->addr, sizeof(Address)) == 0)
                    node_found = 1;
            }
            if (node_found == 1) {
                vector<pair<string, string>> my_primary_keys;
                for (map<string, string>::iterator key_val_itr = ht->hashTable.begin(); key_val_itr != ht->hashTable.end(); key_val_itr++){
                    Entry temp_entry(key_val_itr->second);
                    if (temp_entry.replica == PRIMARY){
                        my_primary_keys.push_back(pair<string, string>(key_val_itr->first, temp_entry.value));
                    }
                }
                Message *message;
                for (int key_id = 0; key_id < my_primary_keys.size(); ++key_id) {
                    message = new Message(-100, getMemberNode()->addr, UPDATE, my_primary_keys[key_id].first, my_primary_keys[key_id].second, rt);
                    emulNet->ENsend(&getMemberNode()->addr, to_be_successor[j].getAddress(), message->toString());
                }
            } else {
                vector<pair<string, string>> my_primary_keys;
                for (map<string, string>::iterator key_val_itr = ht->hashTable.begin(); key_val_itr != ht->hashTable.end(); key_val_itr++){
                    Entry temp_entry(key_val_itr->second);
                    if (temp_entry.replica == PRIMARY){
                        my_primary_keys.push_back(pair<string, string>(key_val_itr->first, temp_entry.value));
                    }
                }
                Message *message;
                for (int key_id = 0; key_id < my_primary_keys.size(); ++key_id) {
                    message = new Message(-100, getMemberNode()->addr, CREATE, my_primary_keys[key_id].first, my_primary_keys[key_id].second, rt);
                    emulNet->ENsend(&getMemberNode()->addr, to_be_successor[j].getAddress(), message->toString());
                }
            }
        }
    }

    // Now update successors and predecessors.
    hasMyReplicas = to_be_successor;
    haveReplicasOf = to_be_predecessor;
}
