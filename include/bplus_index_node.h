#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
#include "bf.h"

// MAX_KEYS_INTERNAL = 63 (Calculated: (512 - 8) / 8)
#define MAX_KEYS_INTERNAL 63 

typedef struct {
    int count;                       // Number of active keys (M)
    int children[MAX_KEYS_INTERNAL + 1]; // Block IDs of children (M+1)
    int keys[MAX_KEYS_INTERNAL];     // Sorted keys (M)
} IndexNode;

#endif