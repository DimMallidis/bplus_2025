/**
 * helper functions for DataNode operations
 */

#include "bplus_datanode.h"
#include <string.h>

// init empty data node
void datanode_init(DataNode *node) {
    node->count = 0;
    node->next_block_id = -1;
}

// find where to insert key in leaf
// returns the index
int datanode_find_insert_pos(const DataNode *node, const TableSchema *schema, int key) {
    int pos = 0;
    while (pos < node->count && record_get_key(schema, &node->records[pos]) < key) {
        pos++;
    }
    return pos;
}

// insert record at pos, shifting others
void datanode_insert_at(DataNode *node, int pos, const Record *record) {
    // shift records
    for (int i = node->count; i > pos; i--) {
        node->records[i] = node->records[i - 1];
    }
    node->records[pos] = *record;
    node->count++;
}

// //
int datanode_is_full(const DataNode *node) {
    return node->count >= MAX_RECORDS_LEAF;
}

// search for key
// returns index or -1 if not found
int datanode_find_key(const DataNode *node, const TableSchema *schema, int key) {
    for (int i = 0; i < node->count; i++) {
        if (record_get_key(schema, &node->records[i]) == key) {
            return i;
        }
    }
    return -1;
}

// splits leaf node
// returns key to promote
int datanode_split(DataNode *node, DataNode *new_node, const Record *record, 
                   const TableSchema *schema, int insert_pos, int new_block_id) {
    // temp array for records
    Record temp[MAX_RECORDS_LEAF + 1];
    int j = 0;
    
    for (int i = 0; i < node->count; i++) {
        if (i == insert_pos) {
            temp[j++] = *record;
        }
        temp[j++] = node->records[i];
    }
    if (insert_pos == node->count) {
        temp[j++] = *record;
    }
    
    // split in middle
    int split = (MAX_RECORDS_LEAF + 1) / 2;
    
    // first half stays
    node->count = split;
    for (int i = 0; i < split; i++) {
        node->records[i] = temp[i];
    }
    
    // second half moves
    new_node->count = (MAX_RECORDS_LEAF + 1) - split;
    for (int i = 0; i < new_node->count; i++) {
        new_node->records[i] = temp[split + i];
    }
    
    // fix pointers
    new_node->next_block_id = node->next_block_id;
    node->next_block_id = new_block_id;
    
    // return first key of new node
    return record_get_key(schema, &new_node->records[0]);
}