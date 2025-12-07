#ifndef BPLUS_DATANODE_H
#define BPLUS_DATANODE_H

#include "record.h"

// Max records in a leaf node.
// Block size 512. Node overhead ~8 bytes (count, next).
// Record size depends on schema, but max is fixed.
// Let's assume max record size is roughly 128 bytes for safety,
// so 512 / 128 = 4.
// A more precise calculation would be:
// sizeof(Record) = 5 * (4 + 4 + 20) = 140 bytes? No, Record is a struct of union.
// Record is FieldValue[5]. FieldValue is union {int, float, char[20]}. max is 20.
// So Record size is 5 * 20 = 100 bytes.
// 512 - 8 = 504. 504 / 100 = 5 records.
// Let's set it to 4 to be safe and leave room.
#define MAX_RECORDS_LEAF 4

typedef struct {
  int count;                      // Number of records currently in the node
  int next_block_id;              // Block ID of the next leaf (-1 if none)
  Record records[MAX_RECORDS_LEAF]; // Array of records
} DataNode;

#endif // BPLUS_DATANODE_H
