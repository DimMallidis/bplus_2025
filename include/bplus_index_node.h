#ifndef BPLUS_INDEX_NODE_H
#define BPLUS_INDEX_NODE_H

// max keys for index node
// block 512 bytes. key is 4 bytes, pointer is 4 bytes
// 512 / 8 = 64. minus overhead.. we use 60 to be safe
#define MAX_KEYS_INDEX 60

typedef struct {
  int count;                    // key count
  int keys[MAX_KEYS_INDEX];     // the keys
  int children[MAX_KEYS_INDEX + 1]; // child pointers
} IndexNode;

// helpers
void indexnode_init(IndexNode *node);
int indexnode_find_child_index(const IndexNode *node, int key);
int indexnode_get_child(const IndexNode *node, int key);
int indexnode_is_full(const IndexNode *node);
void indexnode_insert_at(IndexNode *node, int pos, int key, int right_child);
void indexnode_split(IndexNode *node, IndexNode *new_node, int new_key,
                     int new_child, int insert_pos, int *promoted_key);

#endif // BPLUS_INDEX_NODE_H
