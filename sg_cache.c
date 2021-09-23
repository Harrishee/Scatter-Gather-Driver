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
#include <stdlib.h>
#include <cmpsc311_log.h>
#include <string.h>

// Project Includes
#include <sg_cache.h>

// Defines
typedef struct cache {
    int lastUsed;
    char Data[SG_BLOCK_SIZE];
    SG_Block_ID cacheBlockId;
    SG_Node_ID cacheRemNoteId;
} Cache;

Cache *myCache;

int timeCount = 1;  // Count last used time
int hitCount = 0;   // Count hit
int missCount = 0;  // Count miss
int line = 0;       // Count lines
int getCount = 0;   // Count how many times getBlock is called
int itemCount = 0;  // Count how many items are there
// Functional Prototypes

//
// Functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : initSGCache
// Description  : Initialize the cache of block elements
//
// Inputs       : maxElements - maximum number of elements allowed
// Outputs      : 0 if successful, -1 if failure

int initSGCache( uint16_t maxElements ) {

    myCache = (Cache*) malloc(maxElements * sizeof(Cache));

    // Initialization failed
    if ( myCache == NULL ) {
        return( -1 );
    }

    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : closeSGCache
// Description  : Close the cache of block elements, clean up remaining data
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

int closeSGCache( void ) {

    float hitRate;
    hitRate = (float) hitCount / getCount * 100;

    logMessage(SGDriverLevel, "Closing cache: %d queries, %d hits (%.2f%% hit rate).", 
                                                        getCount, hitCount, hitRate);

    logMessage(LOG_INFO_LEVEL, "Closed cmpsc311 cache, deleting %d items", itemCount);

    free(myCache);
    myCache = NULL;

    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : getSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
// Outputs      : pointer to block or NULL if not found

char * getSGDataBlock( SG_Node_ID nde, SG_Block_ID blk ) {

    getCount++;

    for ( int i = 0; i < line; i++ ){
        
        // If nde and blk match IDs
        if ( nde == myCache[i].cacheRemNoteId && blk == myCache[i].cacheBlockId )
        {
            logMessage(LOG_INFO_LEVEL, "Getting found cache item");
            myCache[i].lastUsed = timeCount;
            hitCount++;
            timeCount++;

            // Return successfully
            return( myCache[i].Data );
        }
    }

    // Did not found correspoding IDs
    logMessage(LOG_INFO_LEVEL, "Getting cache item (not found!)");

    // Not found. Return NULL
    return( NULL );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : putSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
//                block - block to insert into cache
// Outputs      : 0 if successful, -1 if failure

int putSGDataBlock( SG_Node_ID nde, SG_Block_ID blk, char *block ) {

    missCount++;

    // Check if nde or blk are legal
    if ( nde == 0 || blk == 0 ) {
        return( -1 );
    }

    // If nde and blk match, update
    for ( int i = 0; i < line; i++ ) {
        if( nde == myCache[i].cacheRemNoteId && blk == myCache[i].cacheBlockId ){ 
            memcpy(myCache[i].Data, block, SG_BLOCK_SIZE); 
            myCache[i].lastUsed = timeCount;
            timeCount++;
            return ( 0 );
        }
    }

    if ( line < SG_MAX_CACHE_ELEMENTS) {    // Before reaching maximum line

        myCache[line].cacheRemNoteId = nde;
        myCache[line].cacheBlockId = blk;
        memcpy(myCache[line].Data, block, SG_BLOCK_SIZE); 
        myCache[line].lastUsed = timeCount;
        itemCount++;
        line++;

    } else {    // Reach maximum line

        int smallestCount = 0;

        // Find the least recent data block
        for ( int i = 0; i < line; i++ ) {
            if ( myCache[i].lastUsed < myCache[smallestCount].lastUsed ) {
                smallestCount = i;
            }
        }

        // Place least recent(lastUsed) one with the new one
        myCache[smallestCount].cacheRemNoteId = nde;
        myCache[smallestCount].cacheBlockId = blk;
        memcpy(myCache[smallestCount].Data, block, SG_BLOCK_SIZE); 
        myCache[smallestCount].lastUsed = timeCount;
    }

    timeCount++;

    // Return successfully
    return( 0 );
}
