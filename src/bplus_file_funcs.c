#include "bplus_file_funcs.h"
#include <stdio.h>

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return -1;              \
    }                         \
  }


int bplus_create_file(const TableSchema *schema, const char *fileName)
{
  // Schema not persisted; leaf records store raw Record
  (void)schema;

  // Create file and open
  CALL_BF(BF_CreateFile(fileName));

  int fd;
  CALL_BF(BF_OpenFile(fileName, &fd));

  // Allocate block 0 (metadata) and block 1 (root leaf)
  BF_Block *b0; BF_Block_Init(&b0);
  BF_Block *b1; BF_Block_Init(&b1);

  CALL_BF(BF_AllocateBlock(fd, b0));
  CALL_BF(BF_AllocateBlock(fd, b1));

  // Initialize metadata
  BPlusMeta meta;
  meta.magic_number = 0xBEEFBEEF;
  meta.root_block_id = 1; // root is a leaf at block 1
  meta.height = 1;        // height 1 => single leaf root
  int blocks_num;
  CALL_BF(BF_GetBlockCounter(fd, &blocks_num));
  meta.total_blocks = blocks_num;

  char *d0 = BF_Block_GetData(b0);
  memcpy(d0, &meta, sizeof(BPlusMeta));
  BF_Block_SetDirty(b0);

  // Initialize empty leaf
  DataNode leaf;
  leaf.count = 0;
  leaf.next_block_id = -1;
  char *d1 = BF_Block_GetData(b1);
  memcpy(d1, &leaf, sizeof(DataNode));
  BF_Block_SetDirty(b1);

  // Unpin & destroy blocks
  CALL_BF(BF_UnpinBlock(b0));
  CALL_BF(BF_UnpinBlock(b1));
  BF_Block_Destroy(&b0);
  BF_Block_Destroy(&b1);

  // Close file
  CALL_BF(BF_CloseFile(fd));
  return 0;
}


int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata)
{
  // Open the BF file
  CALL_BF(BF_OpenFile(fileName, file_desc));

  // Read metadata from block 0
  BF_Block *b; BF_Block_Init(&b);
  CALL_BF(BF_GetBlock(*file_desc, 0, b));
  char *data = BF_Block_GetData(b);

  *metadata = (BPlusMeta*)malloc(sizeof(BPlusMeta));
  if (*metadata == NULL) {
    BF_UnpinBlock(b);
    BF_Block_Destroy(&b);
    return -1;
  }
  memcpy(*metadata, data, sizeof(BPlusMeta));

  CALL_BF(BF_UnpinBlock(b));
  BF_Block_Destroy(&b);
  return 0;
}

int bplus_close_file(const int file_desc, BPlusMeta* metadata)
{
  if (metadata != NULL) {
    // Persist metadata to block 0 before closing
    BF_Block *b; BF_Block_Init(&b);
    CALL_BF(BF_GetBlock(file_desc, 0, b));
    char *data = BF_Block_GetData(b);
    memcpy(data, metadata, sizeof(BPlusMeta));
    BF_Block_SetDirty(b);
    CALL_BF(BF_UnpinBlock(b));
    BF_Block_Destroy(&b);
    free(metadata);
  }
  CALL_BF(BF_CloseFile(file_desc));
  return 0;
}

int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record)
{
  // Traverse from root to leaf
  int current = metadata->root_block_id;
  for (int level = metadata->height; level > 1; --level) {
    BF_Block *b; BF_Block_Init(&b);
    CALL_BF(BF_GetBlock(file_desc, current, b));
    IndexNode idx;
    memcpy(&idx, BF_Block_GetData(b), sizeof(IndexNode));
    int key = record->values[0].int_value;
    int i = 0; while (i < idx.count && key >= idx.keys[i]) i++;
    current = idx.children[i];
    CALL_BF(BF_UnpinBlock(b));
    BF_Block_Destroy(&b);
  }

  // Load leaf
  BF_Block *bl; BF_Block_Init(&bl);
  CALL_BF(BF_GetBlock(file_desc, current, bl));
  DataNode leaf; memcpy(&leaf, BF_Block_GetData(bl), sizeof(DataNode));

  // Insert in sorted order
  int key = record->values[0].int_value;
  int pos = leaf.count;
  while (pos > 0) {
    int kprev = leaf.records[pos-1].values[0].int_value;
    if (kprev > key) { leaf.records[pos] = leaf.records[pos-1]; pos--; } else break;
  }
  leaf.records[pos] = *record;
  leaf.count++;

  if (leaf.count <= MAX_RECORDS_LEAF) {
    // Write back and finish
    char *d = BF_Block_GetData(bl);
    memcpy(d, &leaf, sizeof(DataNode));
    BF_Block_SetDirty(bl);
    CALL_BF(BF_UnpinBlock(bl)); BF_Block_Destroy(&bl);
    return current;
  }

  // Split leaf when overflow
  DataNode right; memset(&right, 0, sizeof(DataNode));
  right.next_block_id = leaf.next_block_id;
  int left_count = (MAX_RECORDS_LEAF + 1) / 2; // ceil split for 6th insert -> 3
  int right_count = leaf.count - left_count;
  right.count = right_count;
  for (int i = 0; i < right_count; ++i) {
    right.records[i] = leaf.records[left_count + i];
  }
  leaf.count = left_count;

  // Allocate new block for right leaf
  BF_Block *br; BF_Block_Init(&br);
  CALL_BF(BF_AllocateBlock(file_desc, br));
  int blocks_num; CALL_BF(BF_GetBlockCounter(file_desc, &blocks_num));
  int right_id = blocks_num - 1;
  leaf.next_block_id = right_id;

  // Write updated left and new right
  char *dl = BF_Block_GetData(bl); memcpy(dl, &leaf, sizeof(DataNode)); BF_Block_SetDirty(bl);
  char *dr = BF_Block_GetData(br); memcpy(dr, &right, sizeof(DataNode)); BF_Block_SetDirty(br);
  CALL_BF(BF_UnpinBlock(bl)); BF_Block_Destroy(&bl);
  CALL_BF(BF_UnpinBlock(br)); BF_Block_Destroy(&br);

  // Promote smallest key of right to parent
  int promote_key = right.records[0].values[0].int_value;

  if (metadata->height == 1) {
    // Create new root index node
    BF_Block *bi; BF_Block_Init(&bi);
    CALL_BF(BF_AllocateBlock(file_desc, bi));
    int blocks_num2; CALL_BF(BF_GetBlockCounter(file_desc, &blocks_num2));
    int new_root = blocks_num2 - 1;

    IndexNode root; memset(&root, 0, sizeof(IndexNode));
    root.count = 1; root.keys[0] = promote_key; root.children[0] = current; root.children[1] = right_id;
    char *di = BF_Block_GetData(bi); memcpy(di, &root, sizeof(IndexNode)); BF_Block_SetDirty(bi);
    CALL_BF(BF_UnpinBlock(bi)); BF_Block_Destroy(&bi);

    metadata->root_block_id = new_root; metadata->height = 2;
    CALL_BF(BF_GetBlockCounter(file_desc, &metadata->total_blocks));
    // persist metadata
    BF_Block *bm; BF_Block_Init(&bm);
    CALL_BF(BF_GetBlock(file_desc, 0, bm));
    char *dm = BF_Block_GetData(bm); memcpy(dm, metadata, sizeof(BPlusMeta)); BF_Block_SetDirty(bm);
    CALL_BF(BF_UnpinBlock(bm)); BF_Block_Destroy(&bm);
    return right_id;
  }

  // For height > 1, minimal parent update is not yet implemented
  // Return right_id to signal split occurred
  return right_id;
}

int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record** out_record)
{  
  int current = metadata->root_block_id;
  // Traverse down to leaf
  for (int level = metadata->height; level > 1; --level) {
    BF_Block *b; BF_Block_Init(&b);
    CALL_BF(BF_GetBlock(file_desc, current, b));
    IndexNode idx; memcpy(&idx, BF_Block_GetData(b), sizeof(IndexNode));
    int i = 0; while (i < idx.count && key >= idx.keys[i]) i++;
    current = idx.children[i];
    CALL_BF(BF_UnpinBlock(b)); BF_Block_Destroy(&b);
  }

  // Search within leaf
  BF_Block *bl; BF_Block_Init(&bl);
  CALL_BF(BF_GetBlock(file_desc, current, bl));
  DataNode leaf; memcpy(&leaf, BF_Block_GetData(bl), sizeof(DataNode));
  for (int i = 0; i < leaf.count; ++i) {
    int k = leaf.records[i].values[0].int_value;
    if (k == key) {
      // Copy out to caller-provided pointer
      if (*out_record != NULL) {
        **out_record = leaf.records[i];
      }
      CALL_BF(BF_UnpinBlock(bl)); BF_Block_Destroy(&bl);
      return 0;
    }
  }
  CALL_BF(BF_UnpinBlock(bl)); BF_Block_Destroy(&bl);
  *out_record = NULL;
  return -1;
}

