#ifndef BPLUS_DATANODE_H
#define BPLUS_DATANODE_H

#include "record.h"

// max records in leaf. block is 512 bytes
// record size is ~~ 100 bytes (5 fields * 20 bytes)
// so 512 / 100 is ~~ 5. we use 4 to be safe 
#define MAX_RECORDS_LEAF 4

typedef struct {
  int count;                      // number of records
  int next_block_id;              // next leaf block id
  Record records[MAX_RECORDS_LEAF]; // records array
} DataNode;

// helper funcs
void datanode_init(DataNode *node);
int datanode_find_insert_pos(const DataNode *node, const TableSchema *schema, int key);
void datanode_insert_at(DataNode *node, int pos, const Record *record);
int datanode_is_full(const DataNode *node);
int datanode_find_key(const DataNode *node, const TableSchema *schema, int key);
int datanode_split(DataNode *node, DataNode *new_node, const Record *record,
                   const TableSchema *schema, int insert_pos, int new_block_id);

#endif // BPLUS_DATANODE_H
