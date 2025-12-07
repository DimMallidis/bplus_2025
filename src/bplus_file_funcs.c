#include "bplus_file_funcs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* macro to handle bf errors and avoid leaks */
#define CALL_BF(call) do { \
    BF_ErrorCode code = call; \
    if (code != BF_OK) { \
        BF_PrintError(code); \
        return -1; \
    } \
} while (0)

int bplus_create_file(const TableSchema *schema, const char *fileName) {
    CALL_BF(BF_CreateFile(fileName));
    int fd;
    CALL_BF(BF_OpenFile(fileName, &fd));

    BF_Block *b0;
    BF_Block_Init(&b0);
    BF_Block *b1;
    BF_Block_Init(&b1);

    /* allocate block 0 (metadata) and 1 (root) */
    if (BF_AllocateBlock(fd, b0) != BF_OK) { BF_Block_Destroy(&b0); BF_Block_Destroy(&b1); return -1; }
    if (BF_AllocateBlock(fd, b1) != BF_OK) { BF_Block_Destroy(&b0); BF_Block_Destroy(&b1); return -1; }

    BPlusMeta meta;
    meta.magic_number = BPLUS_MAGIC;
    meta.root_block_id = 1;
    meta.height = 1;
    meta.schema = *schema;
    int blocks;
    if (BF_GetBlockCounter(fd, &blocks) != BF_OK) { BF_Block_Destroy(&b0); BF_Block_Destroy(&b1); return -1; }
    meta.total_blocks = blocks;

    memcpy(BF_Block_GetData(b0), &meta, sizeof(BPlusMeta));
    BF_Block_SetDirty(b0);

    /* Initialize root as empty leaf node using helper */
    DataNode leaf;
    datanode_init(&leaf);
    memcpy(BF_Block_GetData(b1), &leaf, sizeof(DataNode));
    BF_Block_SetDirty(b1);

    BF_UnpinBlock(b0); BF_Block_Destroy(&b0);
    BF_UnpinBlock(b1); BF_Block_Destroy(&b1);
    CALL_BF(BF_CloseFile(fd));
    return 0;
}

int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata) {
    CALL_BF(BF_OpenFile(fileName, file_desc));
    BF_Block *b0;
    BF_Block_Init(&b0);
    if (BF_GetBlock(*file_desc, 0, b0) != BF_OK) {
        BF_Block_Destroy(&b0);
        BF_CloseFile(*file_desc);  /* Fix: close file on error */
        return -1;
    }

    *metadata = malloc(sizeof(BPlusMeta));
    if (*metadata == NULL) {
        BF_UnpinBlock(b0);
        BF_Block_Destroy(&b0);
        BF_CloseFile(*file_desc);  /* Fix: close file on malloc failure */
        return -1;
    }
    memcpy(*metadata, BF_Block_GetData(b0), sizeof(BPlusMeta));

    /* Fix: Validate magic number */
    if ((*metadata)->magic_number != BPLUS_MAGIC) {
        free(*metadata);
        *metadata = NULL;
        BF_UnpinBlock(b0);
        BF_Block_Destroy(&b0);
        BF_CloseFile(*file_desc);
        return -1;  /* Not a valid B+ tree file */
    }

    BF_UnpinBlock(b0);
    BF_Block_Destroy(&b0);
    return 0;
}

int bplus_close_file(int file_desc, BPlusMeta* metadata) {
    if (metadata) {
        BF_Block *b0;
        BF_Block_Init(&b0);
        if (BF_GetBlock(file_desc, 0, b0) != BF_OK) { BF_Block_Destroy(&b0); return -1; }
        memcpy(BF_Block_GetData(b0), metadata, sizeof(BPlusMeta));
        BF_Block_SetDirty(b0);
        BF_UnpinBlock(b0);
        BF_Block_Destroy(&b0);
        free(metadata);
    }
    CALL_BF(BF_CloseFile(file_desc));
    return 0;
}

int bplus_record_find(int file_desc, const BPlusMeta *metadata, int key, Record** out_record) {
    /* Fix: Initialize out_record to NULL for not-found case */
    if (out_record) {
        *out_record = NULL;
    }

    int curr = metadata->root_block_id;
    int height = metadata->height;

    /* traverse index nodes using helper function */
    for (int h = 1; h < height; h++) {
        BF_Block *b;
        BF_Block_Init(&b);
        if (BF_GetBlock(file_desc, curr, b) != BF_OK) { BF_Block_Destroy(&b); return -1; }
        IndexNode *idx = (IndexNode*)BF_Block_GetData(b);

        /* Use helper to find the correct child */
        curr = indexnode_get_child(idx, key);
        BF_UnpinBlock(b);
        BF_Block_Destroy(&b);
    }

    /* search in leaf */
    BF_Block *bl;
    BF_Block_Init(&bl);
    if (BF_GetBlock(file_desc, curr, bl) != BF_OK) { BF_Block_Destroy(&bl); return -1; }
    DataNode *leaf = (DataNode*)BF_Block_GetData(bl);

    /* Use helper to search for key in leaf */
    int found_idx = datanode_find_key(leaf, &metadata->schema, key);
    if (found_idx >= 0) {
        /* Fix: Allocate and copy record when found */
        if (out_record) {
            *out_record = malloc(sizeof(Record));
            if (*out_record) {
                **out_record = leaf->records[found_idx];
            }
        }
        BF_UnpinBlock(bl);
        BF_Block_Destroy(&bl);
        return 0; /* found */
    }

    BF_UnpinBlock(bl);
    BF_Block_Destroy(&bl);
    return -1; /* not found, out_record is already NULL */
}

static int insert_recursive(int file_desc, BPlusMeta *metadata, int curr_block, const Record *record, int *up_key, int *up_right, int height) {
    BF_Block *b;
    BF_Block_Init(&b);
    if (BF_GetBlock(file_desc, curr_block, b) != BF_OK) { BF_Block_Destroy(&b); return -1; }

    int ret_val = -1;

    if (height == 1) { /* leaf node */
        DataNode *leaf = (DataNode*)BF_Block_GetData(b);
        int key = record_get_key(&metadata->schema, record);
        
        /* Use helper to find insert position */
        int pos = datanode_find_insert_pos(leaf, &metadata->schema, key);

        if (!datanode_is_full(leaf)) {
            /* Use helper to insert record */
            datanode_insert_at(leaf, pos, record);
            BF_Block_SetDirty(b);
            *up_right = -1; /* no split */
            ret_val = curr_block;
        } else {
            /* split leaf */
            BF_Block *new_b;
            BF_Block_Init(&new_b);
            if (BF_AllocateBlock(file_desc, new_b) != BF_OK) {
                BF_UnpinBlock(b); BF_Block_Destroy(&b); BF_Block_Destroy(&new_b); return -1;
            }
            int new_id;
            BF_GetBlockCounter(file_desc, &new_id); new_id--;
            metadata->total_blocks = new_id + 1;  /* Fix: update total_blocks */
            DataNode *new_leaf = (DataNode*)BF_Block_GetData(new_b);
            datanode_init(new_leaf);

            /* Use helper to split the leaf node */
            int split = (MAX_RECORDS_LEAF + 1) / 2;
            *up_key = datanode_split(leaf, new_leaf, record, &metadata->schema, pos, new_id);
            *up_right = new_id;

            /* Return id where record landed */
            if (pos < split) ret_val = curr_block;
            else ret_val = new_id;

            BF_Block_SetDirty(b);
            BF_Block_SetDirty(new_b);
            BF_UnpinBlock(new_b); BF_Block_Destroy(&new_b);
        }
    } else { /* index node */
        IndexNode *idx = (IndexNode*)BF_Block_GetData(b);
        int key = record_get_key(&metadata->schema, record);
        
        /* Use helper to find child to descend */
        int pos = indexnode_find_child_index(idx, key);
        int child = idx->children[pos];

        int child_up_key, child_up_right;
        ret_val = insert_recursive(file_desc, metadata, child, record, &child_up_key, &child_up_right, height - 1);

        if (child_up_right != -1) {
            /* child split, insert into this node */
            if (!indexnode_is_full(idx)) {
                /* Use helper to insert key+child */
                indexnode_insert_at(idx, pos, child_up_key, child_up_right);
                BF_Block_SetDirty(b);
                *up_right = -1;
            } else {
                /* split index node */
                BF_Block *new_b;
                BF_Block_Init(&new_b);
                if (BF_AllocateBlock(file_desc, new_b) != BF_OK) {
                    BF_UnpinBlock(b); BF_Block_Destroy(&b); BF_Block_Destroy(&new_b); return -1;
                }
                int new_id;
                BF_GetBlockCounter(file_desc, &new_id); new_id--;
                metadata->total_blocks = new_id + 1;  /* Fix: update total_blocks */
                IndexNode *new_idx = (IndexNode*)BF_Block_GetData(new_b);
                indexnode_init(new_idx);

                /* Use helper to split index node */
                indexnode_split(idx, new_idx, child_up_key, child_up_right, pos, up_key);
                *up_right = new_id;

                BF_Block_SetDirty(b);
                BF_Block_SetDirty(new_b);
                BF_UnpinBlock(new_b); BF_Block_Destroy(&new_b);
            }
        } else {
             *up_right = -1;
        }
    }

    BF_UnpinBlock(b);
    BF_Block_Destroy(&b);
    return ret_val;
}

int bplus_record_insert(int file_desc, BPlusMeta* metadata, const Record *record) {
    int up_key, up_right;
    int ret = insert_recursive(file_desc, metadata, metadata->root_block_id, record, &up_key, &up_right, metadata->height);

    if (up_right != -1) {
        /* root split */
        BF_Block *new_root_b;
        BF_Block_Init(&new_root_b);
        if (BF_AllocateBlock(file_desc, new_root_b) != BF_OK) { BF_Block_Destroy(&new_root_b); return -1; }
        int new_root_id;
        BF_GetBlockCounter(file_desc, &new_root_id); new_root_id--;
        metadata->total_blocks = new_root_id + 1;  /* Fix: update total_blocks */

        IndexNode *root = (IndexNode*)BF_Block_GetData(new_root_b);
        indexnode_init(root);
        root->count = 1;
        root->keys[0] = up_key;
        root->children[0] = metadata->root_block_id;
        root->children[1] = up_right;

        BF_Block_SetDirty(new_root_b);
        BF_UnpinBlock(new_root_b); BF_Block_Destroy(&new_root_b);

        metadata->root_block_id = new_root_id;
        metadata->height++;

        /* update metadata block */
        BF_Block *meta_b;
        BF_Block_Init(&meta_b);
        if (BF_GetBlock(file_desc, 0, meta_b) != BF_OK) { BF_Block_Destroy(&meta_b); return -1; }
        memcpy(BF_Block_GetData(meta_b), metadata, sizeof(BPlusMeta));
        BF_Block_SetDirty(meta_b);
        BF_UnpinBlock(meta_b); BF_Block_Destroy(&meta_b);
    }
    return ret;
}
