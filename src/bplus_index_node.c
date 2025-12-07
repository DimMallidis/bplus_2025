/**
 * Helper functions for IndexNode (internal node) operations.
 * These functions handle key routing and splitting in B+ tree internal nodes.
 */

#include "bplus_index_node.h"
#include <string.h>

/**
 * Initializes a new empty index node.
 */
void indexnode_init(IndexNode *node) {
    node->count = 0;
}

/**
 * Finds the child pointer to follow for a given key.
 * Returns the index of the child to descend into.
 */
int indexnode_find_child_index(const IndexNode *node, int key) {
    int pos = 0;
    while (pos < node->count && key >= node->keys[pos]) {
        pos++;
    }
    return pos;
}

/**
 * Gets the child block ID for a given key.
 */
int indexnode_get_child(const IndexNode *node, int key) {
    int idx = indexnode_find_child_index(node, key);
    return node->children[idx];
}

/**
 * Checks if an index node is full.
 */
int indexnode_is_full(const IndexNode *node) {
    return node->count >= MAX_KEYS_INDEX;
}

/**
 * Inserts a key and right child pointer at the given position.
 * Assumes there is room in the node (count < MAX_KEYS_INDEX).
 */
void indexnode_insert_at(IndexNode *node, int pos, int key, int right_child) {
    /* Shift keys and children to make room */
    for (int i = node->count; i > pos; i--) {
        node->keys[i] = node->keys[i - 1];
        node->children[i + 1] = node->children[i];
    }
    node->keys[pos] = key;
    node->children[pos + 1] = right_child;
    node->count++;
}

/**
 * Splits an index node when it's full.
 * Original node keeps the first half, new_node gets the second half.
 * The middle key is promoted to the parent.
 * 
 * @param node The original node (will be modified)
 * @param new_node The new node to receive upper half
 * @param new_key The key being inserted that caused the split
 * @param new_child The child pointer for the new key
 * @param insert_pos Position where the new key should be inserted
 * @param promoted_key Output: the key to promote to parent
 */
void indexnode_split(IndexNode *node, IndexNode *new_node, int new_key, 
                     int new_child, int insert_pos, int *promoted_key) {
    /* Create temporary arrays with all keys and children */
    int temp_keys[MAX_KEYS_INDEX + 1];
    int temp_children[MAX_KEYS_INDEX + 2];
    
    /* Copy keys, inserting new key at the right position */
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
    
    /* Copy children, inserting new child at the right position */
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
    
    /* Calculate split point */
    int total_keys = MAX_KEYS_INDEX + 1;
    int mid = total_keys / 2;
    
    /* The middle key is promoted, not kept in either node */
    *promoted_key = temp_keys[mid];
    
    /* Left node gets keys[0..mid-1] and children[0..mid] */
    node->count = mid;
    for (int i = 0; i < mid; i++) {
        node->keys[i] = temp_keys[i];
        node->children[i] = temp_children[i];
    }
    node->children[mid] = temp_children[mid];
    
    /* Right node gets keys[mid+1..end] and children[mid+1..end] */
    new_node->count = total_keys - mid - 1;
    for (int i = 0; i < new_node->count; i++) {
        new_node->keys[i] = temp_keys[mid + 1 + i];
        new_node->children[i] = temp_children[mid + 1 + i];
    }
    new_node->children[new_node->count] = temp_children[total_keys];
}