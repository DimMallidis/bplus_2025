#ifndef BPLUS_FILE_STRUCTS_H
#define BPLUS_FILE_STRUCTS_H

#include "record.h"

// Magic number to identify B+ tree files
#define BPLUS_MAGIC 0xBEEFBEEF

// Metadata structure for the B+ tree (Block 0)
typedef struct {
  int magic_number;    // Identifies the file type
  int root_block_id;   // Block ID of the root node
  int height;          // Height of the tree (1 for only root-leaf)
  int total_blocks;    // Total number of blocks allocated
  TableSchema schema;  // Schema of the records
} BPlusMeta;

#endif // BPLUS_FILE_STRUCTS_H
