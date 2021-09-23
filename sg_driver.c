////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : Hanfei He
//   Last Modified : 12/10/2020
//

// Include Files
#include <string.h>
#include <stdlib.h>

// Project Includes
#include <sg_driver.h>
#include <sg_service.h>

#include <sg_cache.h>

// Defines

//
// Global Data

// Driver file entry

// Global data
int sgDriverInitialized = 0; // The flag indicating the driver initialized
SG_Block_ID sgLocalNodeId;   // The local node identifier
SG_SeqNum sgLocalSeqno;      // The local sequence number

typedef struct rem_info {
    SG_Node_ID remNodeId;
    SG_SeqNum sgRemoteseqno;
    struct rem_info *pNext;
} Rem, *pRem;
pRem myRem;

typedef struct ids_info {
    SG_Block_ID blockIdGot;
    SG_Node_ID remNoteIdGot;
    int track;  // blockCount
    struct ids_info *pNext;
} IdGot, *pIdGot;

typedef struct file_info {
    int filePtr; // file position
    int fileSize;
    int fileStatus;
    const char *filename;
    SgFHandle fileHandle;
    pIdGot myIds;
    struct file_info *pNext;
} File, *pFile;
pFile myFile;

int count = 0; // count files
int remCount = 0; // count remote nodes

// Driver support functions
int sgInitEndpoint( void ); // Initialize the endpoint

int mySgCreateBlock( SgFHandle fh, char *buf, size_t len ); // Create a block
int mySgUpdateBlock( SgFHandle fh, char *buf, size_t len ); // Update a block
int mySgObtainBlock( SgFHandle fh, char *buf ); // Obtain a block

//
// Functions

//
// File system interface implementation

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgopen
// Description  : Open the file for for reading and writing
//
// Inputs       : path - the path/filename of the file to be read
// Outputs      : file handle if successful test, -1 if failure

SgFHandle sgopen(const char *path) {

    // First check to see if we have been initialized
    if (!sgDriverInitialized) {

        initSGCache(SG_MAX_CACHE_ELEMENTS); // Initialize cache

        // Call the endpoint initialization 
        if ( sgInitEndpoint() ) {
            logMessage( LOG_ERROR_LEVEL, "sgopen: Scatter/Gather endpoint initialization failed." );
            return( -1 );
        }

        // Set to initialized
        sgDriverInitialized = 1;
    }

    // If the file exist
    pFile test = myFile;
    for ( int i = 0; i < count; i++ ) {
        while ( test != NULL ) {
            if ( strcmp(test->filename, path) == 0 ) {
                test->filePtr = 0;
                return( test->fileHandle );
            } else {
                test = test->pNext;
            }
        }
    }
    
    // Allocate memory to every new file passed in
    pFile newFile = (pFile) malloc(sizeof(File));

    // Set up filename in my struct
    newFile->filename = strdup(path);
    
    // 2) Assignment a file handle
    newFile->fileHandle = count;
    
    // 3a) Set the file pointer to 0
    newFile->filePtr = 0;

    // 3b) Set the file size to 0
    newFile->fileSize = 0;

    newFile->fileStatus = 1;

    newFile->pNext = myFile;
    myFile = newFile;

    count++;

    // Return the file handle 
    return( myFile->fileHandle );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgread
// Description  : Read data from the file
//
// Inputs       : fh - file handle for the file to read from
//                buf - place to put the data
//                len - the length of the read
// Outputs      : number of bytes read, -1 if failure

int sgread(SgFHandle fh, char *buf, size_t len) {

    // For transitional storage
    char readData[SG_BLOCK_SIZE];

    // 1) Check if file handle to see if assigned before, error if not
    if ( fh < 0 || fh >= count ) {
        return( -1 );
    }

    // Find the corresponding file
    pFile temp = myFile;
    while ( temp != NULL ) {
        if ( temp->fileHandle == fh ) {
            break;
        } else {
            temp = temp->pNext;
        }
    }

    // 2) If file pointer points to end of the file, error reading beyond end of file 
    if ( temp->filePtr >= temp->fileSize ) {
        return( -1 );
    }
    
    // 3) 4) 5) in the function
    mySgObtainBlock(fh, readData);

    if ( temp->filePtr % (SG_BLOCK_SIZE / 2) == 0 ) { // Read first quarter or third quarter of block

        if( temp->filePtr % SG_BLOCK_SIZE == 0 ){ // Read first quarter
            memcpy(buf, readData, len);
        } else {                             // Read third quarter
            memcpy(buf, &readData[len*2], len);
        }

    } else {                                  // Read second half or forth quarter of block

        if( (temp->filePtr - len) % SG_BLOCK_SIZE == 0 ){ // Read second quarter
            memcpy(buf, &readData[len], len);
        } else {                             // Read fourth quarter
            memcpy(buf, &readData[len*3], len);
        }
    }

    // 6) Update the file position by adding len
    temp->filePtr += len;

    // 7) Return the number of bytes read
    // Return the bytes processed
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgwrite
// Description  : write data to the file
//
// Inputs       : fh - file handle for the file to write to
//                buf - pointer to data to write
//                len - the length of the write
// Outputs      : number of bytes written if successful test, -1 if failure

int sgwrite(SgFHandle fh, char *buf, size_t len) {

    // For transitional storage
    char myData[SG_BLOCK_SIZE];

    // 1) Check if file handle to see if assigned before, error if not
    if ( fh < 0 || fh >= count ) {
        return( -1 );
    }

    // Find the corresponding file
    pFile temp = myFile;
    while ( temp != NULL ) {
        if ( temp->fileHandle == fh ) {
            break;
        } else {
            temp = temp->pNext;
        }
    }

    // Count which block is it
    int blockCount = temp->filePtr / SG_BLOCK_SIZE; 

    // 2) If file pointer points to end of the file
    if ( temp->filePtr == temp->fileSize ) {

        if ( temp->filePtr % (SG_BLOCK_SIZE / 2) == 0 ) { // Case1: ptr = 0, size = 0

            if( temp->filePtr % SG_BLOCK_SIZE == 0 ){   // Case1.1: ptr = 0, size = 0 (1st)

                memcpy(myData, buf, len);
                mySgCreateBlock(fh, myData, len);
            } else {                                // Case1.2: ptr = 512, size = 512 (3rd)
            
                mySgObtainBlock(fh, myData);
                memcpy(&myData[len*2], buf, len);
                mySgUpdateBlock(fh, myData, len);

                temp->fileSize += len;
                temp->filePtr = temp->fileSize;
            }
        } else {                                  // Case2: ptr = 512, size = 512
            mySgObtainBlock(fh, myData);

            if( (temp->filePtr - len) % SG_BLOCK_SIZE == 0 ){   // Case2.1: ptr = 512, size = 512 (2nd)

                memcpy(&myData[len], buf, len);
            } else {                                // Case2.2: ptr = 768, size = 768 (4th)
            
                memcpy(&myData[len*3], buf, len);
            }

            mySgUpdateBlock(fh, myData, len);
            temp->fileSize += len;
            temp->filePtr = temp->fileSize;
        }
    } else {    // 3) If file pointer NOT at end of file

        mySgObtainBlock(fh, myData);

        if ( temp->filePtr % (SG_BLOCK_SIZE / 2) == 0 ) { // Case3: ptr = 0, size 1024

            if( temp->filePtr % SG_BLOCK_SIZE == 0 ){   // Case3.1: ptr = 0, size = 1024 (1st)

                memcpy(myData, buf, len);
            } else {                                // Case3.2: ptr = 512, size = 1024 (3rd)
            
                memcpy(&myData[len*2], buf, len);
            }
        } else {                                  // Case4: ptr = 512, size 1024

            if( (temp->filePtr - len) % SG_BLOCK_SIZE == 0 ){   // Case4.1: ptr = 256, size = 1024 (2nd)

                memcpy(&myData[len], buf, len);
            } else {                                // Case4.2: ptr = 768, size = 1024 (4th)
            
                memcpy(&myData[len*3], buf, len);
            }
        }

        mySgUpdateBlock(fh, myData, len);
        temp->filePtr += len;
    }

    // Find the corresponding Ids
    SG_Block_ID blockIdToBePassed;
    SG_Node_ID remNoteIdToBePassed;

    pIdGot findIds = temp->myIds;
    while ( findIds != NULL ) {
        if ( findIds->track == blockCount ) {
            blockIdToBePassed = findIds->blockIdGot;
            remNoteIdToBePassed = findIds->remNoteIdGot;
            break;
        } else {
            findIds = findIds->pNext;
        }
    }

    // Insert block into cache after each write
    putSGDataBlock(remNoteIdToBePassed, 
                blockIdToBePassed, myData);

    // 3) Return number of bytes written
    // Log the write, return bytes written
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgseek
// Description  : Seek to a specific place in the file
//
// Inputs       : fh - the file handle of the file to seek in
//                off - offset within the file to seek to
// Outputs      : new position if successful, -1 if failure

int sgseek(SgFHandle fh, size_t off) {

    // 1) Check if file handle to see if assigned before, error if not
    if ( fh < 0 || fh >= count ) {
        return( -1 );
    }

    // Find the corresponding file
    pFile temp = myFile;
    while ( temp != NULL ) {
        if ( temp->fileHandle == fh ) {
            break;
        } else {
            temp = temp->pNext;
        }
    }

    // 2) Check if seek position <= file size
    if ( temp->filePtr <= temp->fileSize ) {

        // 3) Set file position to the seek position
        temp->filePtr = off;
    }

    // 4) Return the seek position
    // Return new position
    return( off );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgclose
// Description  : Close the file
//
// Inputs       : fh - the file handle of the file to close
// Outputs      : 0 if successful test, -1 if failure

int sgclose(SgFHandle fh) {

    // 1) Check if file handle to see if assigned before, error if not
    if ( fh < 0 || fh >= count ) {
        return( -1 );
    }

    pFile temp = myFile;
    pFile prev = NULL;
    
    // Find the corresponding node and keep tracking previous and current node
    while ( temp != NULL ) {
        if ( temp->fileHandle == fh ) {
            break;
        } else {
            prev = temp;
            temp = temp->pNext; 
        }
    }

    // 2) Clean up data structure, mark file as closed
    if ( temp == myFile) {
        myFile = myFile->pNext;
    } else {
        prev->pNext = temp->pNext;
    }

    temp->fileStatus = 0;

    free(temp->myIds);
    temp->myIds = NULL;

    free(temp);
    temp = NULL;

    // 3) Return 0 (success)
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgshutdown
// Description  : Shut down the filesystem
//
// Inputs       : none
// Outputs      : 0 if successful test, -1 if failure

int sgshutdown(void) {

    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    logMessage( LOG_INFO_LEVEL, "Stopping local endpoint ..." );

    // Setup the packet
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_STOP_ENDPOINT,  // Operation
                                    sgLocalSeqno,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "mySgStopEndpoint: failed serialization of packet [%d].", ret );
        return( -1 );
    }

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "mySgStopEndpoint: failed packet post" );
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "mySgStopEndpoint: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    logMessage( LOG_INFO_LEVEL, "Stopped local node (local node ID %lu)", sgLocalNodeId );
 
    // Print cache statics and free it
    closeSGCache();

    // Clean up rseq structure
    free(myRem);
    myRem = NULL;

    // Log, return successfully
    logMessage( LOG_INFO_LEVEL, "Shut down Scatter/Gather driver." );

    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : serialize_sg_packet
// Description  : Serialize a ScatterGather packet (create packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status serialize_sg_packet(SG_Node_ID loc, SG_Node_ID rem, SG_Block_ID blk,
                                     SG_System_OP op, SG_SeqNum sseq, SG_SeqNum rseq, char *data,
                                     char *packet, size_t *plen) {

    // Check if packet data is bad
    if ( packet == NULL) {
        return SG_PACKT_PDATA_BAD;
    }

    int offset = 0;
    uint32_t magic = SG_MAGIC_VALUE;
    char dataIndicator;
    *plen = 0;
    

    // Determine if data is empty
    if ( data == NULL ){
        dataIndicator = 0;
    } else {
        dataIndicator = 1;
    }


    // Validate each element's status
    if ( loc == 0 ){
        return SG_PACKT_LOCID_BAD;
    }

    if ( rem == 0 ){
        return SG_PACKT_REMID_BAD;
    }

    if ( blk == 0 ){
        return SG_PACKT_BLKID_BAD;
    }

    if ( op < 0 || op > 6 ){
        return SG_PACKT_OPERN_BAD;
    }

    if ( sseq == 0 ){
        return SG_PACKT_SNDSQ_BAD;
    }

    if ( rseq == 0 ){
        return SG_PACKT_RCVSQ_BAD;
    }


    // Copy data into the packet
    memcpy(packet, &magic, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    memcpy(&packet[offset], &loc, sizeof(SG_Node_ID));
    offset += sizeof(SG_Node_ID);
    
    memcpy(&packet[offset], &rem, sizeof(SG_Node_ID));
    offset += sizeof(SG_Node_ID);
    
    memcpy(&packet[offset], &blk, sizeof(SG_Block_ID));
    offset += sizeof(SG_Block_ID);

    memcpy(&packet[offset], &op, sizeof(SG_System_OP));
    offset += sizeof(SG_System_OP);
    
    memcpy(&packet[offset], &sseq, sizeof(SG_SeqNum));
    offset += sizeof(SG_SeqNum);
    
    memcpy(&packet[offset], &rseq, sizeof(SG_SeqNum));
    offset += sizeof(SG_SeqNum);

    memcpy(&packet[offset], &dataIndicator, sizeof(char));
    offset += sizeof(char);

    // Pass data to packet if data is not empty. Skip if it is empty
    if ( data != NULL ){
        memcpy(&packet[offset], data, sizeof(SGDataBlock));
        offset += sizeof(SGDataBlock);
    }

    memcpy(&packet[offset], &magic, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // Set plen equals to packet size
    *plen = offset;

    return( SG_PACKT_OK );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : deserialize_sg_packet
// Description  : De-serialize a ScatterGather packet (unpack packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status deserialize_sg_packet(SG_Node_ID *loc, SG_Node_ID *rem, SG_Block_ID *blk,
                                       SG_System_OP *op, SG_SeqNum *sseq, SG_SeqNum *rseq, char *data,
                                       char *packet, size_t plen) {

    // Check if packet data is bad
    if ( packet == NULL) {
        return SG_PACKT_PDATA_BAD;
    }

    int offset = 0;
    uint32_t magic = SG_MAGIC_VALUE;
    char dataIndicator;
    plen = (size_t)SG_BASE_PACKET_SIZE;
    

    // Get value in the packet first(unpacking)
    memcpy(&magic, packet, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    memcpy(loc, &packet[offset], sizeof(SG_Node_ID));
    offset += sizeof(SG_Node_ID);

    memcpy(rem, &packet[offset], sizeof(SG_Node_ID));
    offset += sizeof(SG_Node_ID);
    
    memcpy(blk, &packet[offset], sizeof(SG_Block_ID));
    offset += sizeof(SG_Block_ID);

    memcpy(op, &packet[offset], sizeof(SG_System_OP));
    offset += sizeof(SG_System_OP);

    memcpy(sseq, &packet[offset], sizeof(SG_SeqNum));
    offset += sizeof(SG_SeqNum);
    
    memcpy(rseq, &packet[offset], sizeof(SG_SeqNum));
    offset += sizeof(SG_SeqNum);

    memcpy(&dataIndicator, &packet[offset], sizeof(char));
    offset += sizeof(char);
    
    // Check if the packet has a data block
    if ( dataIndicator != 0){
        memcpy(data, &packet[offset], sizeof(SGDataBlock));
        offset += sizeof(SGDataBlock);

        // If the packet has a data block, but data pointer(input) is NULL
        if ( data == NULL ){
            return SG_PACKT_BLKDT_BAD;
        }
    }

    memcpy(&magic, &packet[offset], sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // Set plen equals to input packet size
    plen = offset;


    // Validate each element's status
    if ( dataIndicator == 1 && plen != SG_DATA_PACKET_SIZE){
        return SG_PACKT_BLKLN_BAD;              // plen from input parameter doesn't have
    }                                           // the correct value for packet size

    if ( *loc == 0 ){
        return SG_PACKT_LOCID_BAD;
    }

    if ( *rem == 0 ){
        return SG_PACKT_REMID_BAD;
    }

    if ( *blk == 0 ){
        return SG_PACKT_BLKID_BAD;
    }

    if ( *op < 0 || *op > 6 ){
        return SG_PACKT_OPERN_BAD;
    }

    if ( *sseq == 0 ){
        return SG_PACKT_SNDSQ_BAD;
    }

    if ( *rseq == 0 ){
        return SG_PACKT_RCVSQ_BAD;
    }

    return( SG_PACKT_OK );
}

//
// Driver support functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgInitEndpoint
// Description  : Initialize the endpoint
//
// Inputs       : none
// Outputs      : 0 if successfull, -1 if failure

int sgInitEndpoint( void ) {

    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    // Local and do some initial setup
    logMessage( LOG_INFO_LEVEL, "Initializing local endpoint ..." );
    sgLocalSeqno = SG_INITIAL_SEQNO;

    // Setup the packet
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( SG_NODE_UNKNOWN, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_INIT_ENDPOINT,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret );
        return( -1 );
    }


    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    // Sanity check the return value
    if ( loc == SG_NODE_UNKNOWN ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", loc );
        return( -1 );
    }

    // Set the local node ID, log and return successfully
    sgLocalNodeId = loc;
    logMessage( LOG_INFO_LEVEL, "Completed initialization of node (local node ID %lu", sgLocalNodeId );
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : mySgCreateBlock
// Description  : Create a block, used in sgwirte
//
// Inputs       : fh - file handle for the file to write to
//                buf - pointer to data to write
//                len - the length of the write
// Outputs      : 0 if successfull, -1 if failure

int mySgCreateBlock( SgFHandle fh, char *buf, size_t len ) {

    // Find the corresponding file
    pFile temp = myFile;
    while ( temp != NULL ) {
        if ( temp->fileHandle == fh ) {
            break;
        } else {
            temp = temp->pNext;
        }
    }

    // Local variables
    char initPacket[SG_DATA_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    int blockCount = temp->filePtr / SG_BLOCK_SIZE; // Count which block is it

    // 2a) Create a new block containing the contents of the write [SG_CREATE_BLOCK op]
    // Setup the packet
    pktlen = SG_DATA_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( sgLocalNodeId,    // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_CREATE_BLOCK,   // Operation
                                    sgLocalSeqno++,  // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    buf, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "mySgCreateBlock: failed serialization of packet [%d].", ret );
        return( -1 );
    }

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "mySCreateBlock: failed packet post" );
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "mySCreateBlock: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    // Store the corresponding rseq to the remote node ID
    // If previously received the same remote node ID, increment its rseq by 1
    pRem test = myRem;
    int i = 0;
    for ( i = 0; i < remCount; i++ ) {
        while ( test != NULL ) {
            if ( test->remNodeId == rem ) {
                test->sgRemoteseqno++;
                break;
            } else {
                test = test->pNext;
            }
        }
    }

    // Allocate memory to every new rem
    pRem newRem = (pRem) malloc(sizeof(Rem));
    if ( i == remCount ){
        newRem->remNodeId = rem;
        newRem->sgRemoteseqno = srem;
        remCount++;
    }
    newRem->pNext = myRem;
    myRem = newRem;
    

    // Allocate memory to every new Id
    pIdGot newIds = (pIdGot) malloc(sizeof(IdGot));

    // 2b) Save node/block IDs as the current block in the data structure
    // Set the remote node ID and block ID
    newIds->blockIdGot = blkid;
    newIds->remNoteIdGot = rem;
    newIds->track = blockCount;

    newIds->pNext = temp->myIds;
    temp->myIds = newIds;

    // 2c) Increase the file size by length of write
    temp->fileSize += len;

    // 2d) Set file pointer to the end of the file
    temp->filePtr += len;

    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : mySgObtainBlock
// Description  : Obtain a block, used in sgread
//
// Inputs       : fh - file handle for the file to read from
//                buf - place to put the data
// Outputs      : 0 if successfull, -1 if failure

int mySgObtainBlock( SgFHandle fh, char *buf ) {

    // Find the corresponding node
    pFile temp = myFile;
    while ( temp != NULL ) {
        if ( temp->fileHandle == fh ) {
            break;
        } else {
            temp = temp->pNext;
        }
    }

    int blockCount = temp->filePtr / SG_BLOCK_SIZE; // Count which block is it

    // Find the corresponding IDs
    SG_Block_ID blockIdToBePassed;
    SG_Node_ID remNoteIdToBePassed;

    pIdGot findIds = temp->myIds;
    while ( findIds != NULL ) {
        if ( findIds->track == blockCount ) {
            blockIdToBePassed = findIds->blockIdGot;
            remNoteIdToBePassed = findIds->remNoteIdGot;
            break;
        } else {
            findIds = findIds->pNext;
        }
    }

    char *data = getSGDataBlock(remNoteIdToBePassed, 
                    blockIdToBePassed);

    // Check if there is corresponding data in the cache
    if (data != NULL){

        // Pass the corresponding data stored in the cache into buf
        memcpy(buf, data, SG_BLOCK_SIZE);

        // Update block info
        putSGDataBlock(remNoteIdToBePassed, 
                    blockIdToBePassed, buf);
        return( 0 );
    }
    

    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_DATA_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;
    
    // Find the rseq need to be passed
    SG_SeqNum rseqToBePassed = 0;
    pRem remTemp = myRem;
    while ( remTemp != NULL ) {
        if ( remTemp->remNodeId == remNoteIdToBePassed ) {
            remTemp->sgRemoteseqno++;
            rseqToBePassed = remTemp->sgRemoteseqno;
            break;
        } else {
            remTemp = remTemp->pNext;
        }
    }

    //3) Look at data structure and figure out what block to retrieve
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    remNoteIdToBePassed,// Remote ID
                                    blockIdToBePassed,  // Block ID
                                    SG_OBTAIN_BLOCK,  // Operation
                                    sgLocalSeqno++, // Sender sequence number
                                    rseqToBePassed, // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "mySgObtainBlock: failed serialization of packet [%d].", ret );
        return( -1 );
    }

    //4) Retrieve the block [SG_OBTAIN_BLOCK op]
    // Send the packet
    rpktlen = SG_DATA_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "mySgObtainBlock: failed packet post" );
        return( -1 );
    }

    //5) Copy the data from the retrieved block to passed in buf
    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, buf, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "mySgObtainBlock: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    // If there is not corresponding data in the cache, update block info to cache
    putSGDataBlock(remNoteIdToBePassed, 
                    blockIdToBePassed, buf);

    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : mySgUpdateBlock
// Description  : Update a block, used in sgwirte
//
// Inputs       : fh - file handle for the file to write to
//                buf - pointer to data to write
//                len - the length of the write
// Outputs      : 0 if successfull, -1 if failure

int mySgUpdateBlock( SgFHandle fh, char *buf, size_t len ) {

    // Find the corresponding node
    pFile temp = myFile;
    while ( temp != NULL ) {
        if ( temp->fileHandle == fh ) {
            break;
        } else {
            temp = temp->pNext;
        }
    }

    // Local variables
    char initPacket[SG_DATA_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    int blockCount = temp->filePtr / SG_BLOCK_SIZE; // Count which block is it

    // Find the corresponding IDs
    SG_Block_ID blockIdToBePassed;
    SG_Node_ID remNoteIdToBePassed;

    pIdGot findIds = temp->myIds;
    while ( findIds != NULL ) {
        if ( findIds->track == blockCount ) {
            blockIdToBePassed = findIds->blockIdGot;
            remNoteIdToBePassed = findIds->remNoteIdGot;
            break;
        } else {
            findIds = findIds->pNext;
        }
    }

    // Find the rseq need to be passed
    SG_SeqNum rseqToBePassed = 0;
    pRem remTemp = myRem;
    while ( remTemp != NULL ) {
        if ( remTemp->remNodeId == remNoteIdToBePassed ) {
            remTemp->sgRemoteseqno++;
            rseqToBePassed = remTemp->sgRemoteseqno;
            break;
        } else {
            remTemp = remTemp->pNext;
        }
    }

    // Setup the packet
    pktlen = SG_DATA_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( sgLocalNodeId,    // Local ID
                                    remNoteIdToBePassed,// Remote ID
                                    blockIdToBePassed,  // Block ID
                                    SG_UPDATE_BLOCK,   // Operation
                                    sgLocalSeqno++,  // Sender sequence number
                                    rseqToBePassed,  // Receiver sequence number
                                    buf, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "mySgUpdateBlock: failed serialization of packet [%d].", ret );
        return( -1 );
    }

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "mySgUpdateBlock: failed packet post" );
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "mySgUpdateBlock: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    // Query the cache after updates
    getSGDataBlock(remNoteIdToBePassed, 
                    blockIdToBePassed);

    return( 0 );
}