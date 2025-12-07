/**
 * Helper functions for DataNode (leaf node) operations.
 * These functions handle record storage and retrieval in B+ tree leaves.
 */

#include "bplus_datanode.h"
#include <string.h>

/**
 * Initializes a new empty data node (leaf).
 */
void datanode_init(DataNode *node) {
    node->count = 0;
    node->next_block_id = -1;
}

/**
 * Finds the sorted insertion position for a key in a leaf node.
 * Returns the index where the record should be inserted.
 */
int datanode_find_insert_pos(const DataNode *node, const TableSchema *schema, int key) {
    int pos = 0;
    while (pos < node->count && record_get_key(schema, &node->records[pos]) < key) {
        pos++;
    }
    return pos;
}

/**
 * Inserts a record at the given position, shifting existing records.
 * Assumes there is room in the node (count < MAX_RECORDS_LEAF).
 */
void datanode_insert_at(DataNode *node, int pos, const Record *record) {
    /* Shift records to make room */
    for (int i = node->count; i > pos; i--) {
        node->records[i] = node->records[i - 1];
    }
    node->records[pos] = *record;
    node->count++;
}

/**
 * Checks if a data node is full.
 */
int datanode_is_full(const DataNode *node) {
    return node->count >= MAX_RECORDS_LEAF;
}

/**
 * Searches for a record with the given key in a leaf node.
 * Returns the index if found, -1 otherwise.
 */
int datanode_find_key(const DataNode *node, const TableSchema *schema, int key) {
    for (int i = 0; i < node->count; i++) {
        if (record_get_key(schema, &node->records[i]) == key) {
            return i;
        }
    }
    return -1;
}

/**
 * Distributes records between two leaf nodes after a split.
 * Original node keeps the first half, new_node gets the second half.
 * The new record is inserted at the appropriate position.
 * Returns the key that should be promoted to the parent.
 */
int datanode_split(DataNode *node, DataNode *new_node, const Record *record, 
                   const TableSchema *schema, int insert_pos, int new_block_id) {
    /* Create temporary array with all records including the new one */
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
    
    /* Split point */
    int split = (MAX_RECORDS_LEAF + 1) / 2;
    
    /* Distribute to original node */
    node->count = split;
    for (int i = 0; i < split; i++) {
        node->records[i] = temp[i];
    }
    
    /* Distribute to new node */
    new_node->count = (MAX_RECORDS_LEAF + 1) - split;
    for (int i = 0; i < new_node->count; i++) {
        new_node->records[i] = temp[split + i];
    }
    
    /* Update linked list pointers */
    new_node->next_block_id = node->next_block_id;
    node->next_block_id = new_block_id;
    
    /* Return the key to promote (first key of new node) */
    return record_get_key(schema, &new_node->records[0]);
}