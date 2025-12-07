#ifndef BPLUS_BPLUS_FILE_STRUCTS_H
#define BPLUS_BPLUS_FILE_STRUCTS_H
#include "bf.h"
#include "record.h"

// Define a simple header for the file
typedef struct {
    int magic_number;      // Used to verify file type (e.g., 0xBEEFBEEF)
    int root_block_id;     // Block ID of the current root node
    int height;            // Height of the tree (1 for initial leaf-root)
    int total_blocks;      // The current block counter (useful for allocation tracking)
} BPlusMeta;

#endif //BPLUS_BPLUS_FILE_STRUCTS_H