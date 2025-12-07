/**
 * helper functions for IndexNode
 */

#include "bplus_index_node.h"
#include <string.h>

// init new index node
void indexnode_init(IndexNode *node) {
    node->count = 0;
}

// find child index for key
int indexnode_find_child_index(const IndexNode *node, int key) {
    int pos = 0;
    while (pos < node->count && key >= node->keys[pos]) {
        pos++;
    }
    return pos;
}

// get child block id
int indexnode_get_child(const IndexNode *node, int key) {
    int idx = indexnode_find_child_index(node, key);
    return node->children[idx];
}

// is it full?
int indexnode_is_full(const IndexNode *node) {
    return node->count >= MAX_KEYS_INDEX;
}

// insert key and right child pointer
void indexnode_insert_at(IndexNode *node, int pos, int key, int right_child) {
    // shift everything
    for (int i = node->count; i > pos; i--) {
        node->keys[i] = node->keys[i - 1];
        node->children[i + 1] = node->children[i];
    }
    node->keys[pos] = key;
    node->children[pos + 1] = right_child;
    node->count++;
}

// split index node
// middle key goes up
void indexnode_split(IndexNode *node, IndexNode *new_node, int new_key, 
                     int new_child, int insert_pos, int *promoted_key) {
    int temp_keys[MAX_KEYS_INDEX + 1];
    int temp_children[MAX_KEYS_INDEX + 2];
    
    // copy keys
    int j = 0;
    for (int i = 0; i < node->count; i++) {
        if (i == insert_pos) {
            temp_keys[j++] = new_key;
        }
        temp_keys[j++] = node->keys[i];
    }
    if (insert_pos == node->count) {
        temp_keys[j++] = new_key;
    }
    
    // copy children
    j = 0;
    for (int i = 0; i <= node->count; i++) {
        if (i == insert_pos + 1) {
            temp_children[j++] = new_child;
        }
        temp_children[j++] = node->children[i];
    }
    if (insert_pos + 1 == node->count + 1) {
        temp_children[j++] = new_child;
    }
    
    int total_keys = MAX_KEYS_INDEX + 1;
    int mid = total_keys / 2;
    
    *promoted_key = temp_keys[mid];
    
    // left half
    node->count = mid;
    for (int i = 0; i < mid; i++) {
        node->keys[i] = temp_keys[i];
        node->children[i] = temp_children[i];
    }
    node->children[mid] = temp_children[mid];
    
    // right half
    new_node->count = total_keys - mid - 1;
    for (int i = 0; i < new_node->count; i++) {
        new_node->keys[i] = temp_keys[mid + 1 + i];
        new_node->children[i] = temp_children[mid + 1 + i];
    }
    new_node->children[new_node->count] = temp_children[total_keys];
}