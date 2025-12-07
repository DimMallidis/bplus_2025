#ifndef BP_DATANODE_H
#define BP_DATANODE_H
#include "record.h"
#include "bf.h"

// MAX_RECORDS_LEAF = 5 (Calculated: (512 - 8) / 100)
#define MAX_RECORDS_LEAF 5 

typedef struct {
    int count;                  // Number of active records
    int next_block_id;          // Block ID of the next leaf node (-1 if last)
    Record records[MAX_RECORDS_LEAF]; 
} DataNode;

#endif