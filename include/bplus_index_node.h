#ifndef BPLUS_INDEX_NODE_H
#define BPLUS_INDEX_NODE_H

// Max keys in an index node.
// Block size 512. Overhead ~4 bytes (count).
// Each entry has a key (int 4) and a child pointer (int 4).
// Plus one extra child pointer.
// (512 - 4 - 4) / 8 = 63.
// Let's use a safe number like 60.
#define MAX_KEYS_INDEX 60

typedef struct {
  int count;                    // Number of keys
  int keys[MAX_KEYS_INDEX];     // Keys
  int children[MAX_KEYS_INDEX + 1]; // Child block pointers
} IndexNode;

#endif // BPLUS_INDEX_NODE_H
