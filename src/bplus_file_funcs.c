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

    DataNode leaf;
    leaf.count = 0;
    leaf.next_block_id = -1;
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
    if (BF_GetBlock(*file_desc, 0, b0) != BF_OK) { BF_Block_Destroy(&b0); return -1; }

    *metadata = malloc(sizeof(BPlusMeta));
    memcpy(*metadata, BF_Block_GetData(b0), sizeof(BPlusMeta));

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
    int curr = metadata->root_block_id;
    int height = metadata->height;

    /* traverse index nodes */
    for (int h = 1; h < height; h++) {
        BF_Block *b;
        BF_Block_Init(&b);
        if (BF_GetBlock(file_desc, curr, b) != BF_OK) { BF_Block_Destroy(&b); return -1; }
        IndexNode *idx = (IndexNode*)BF_Block_GetData(b);

        int next_blk = idx->children[idx->count]; /* default to last child */
        for (int i = 0; i < idx->count; i++) {
            if (key < idx->keys[i]) {
                next_blk = idx->children[i];
                break;
            }
        }
        curr = next_blk;
        BF_UnpinBlock(b);
        BF_Block_Destroy(&b);
    }

    /* search in leaf */
    BF_Block *bl;
    BF_Block_Init(&bl);
    if (BF_GetBlock(file_desc, curr, bl) != BF_OK) { BF_Block_Destroy(&bl); return -1; }
    DataNode *leaf = (DataNode*)BF_Block_GetData(bl);

    for (int i = 0; i < leaf->count; i++) {
        if (record_get_key(&metadata->schema, &leaf->records[i]) == key) {
            if (out_record && *out_record) {
                **out_record = leaf->records[i];
            }
            BF_UnpinBlock(bl);
            BF_Block_Destroy(&bl);
            return 0; /* found */
        }
    }

    BF_UnpinBlock(bl);
    BF_Block_Destroy(&bl);
    return -1;
}

static int insert_recursive(int file_desc, int curr_block, const Record *record, int *up_key, int *up_right, int height, const TableSchema *schema) {
    BF_Block *b;
    BF_Block_Init(&b);
    if (BF_GetBlock(file_desc, curr_block, b) != BF_OK) { BF_Block_Destroy(&b); return -1; }

    int ret_val = -1;

    if (height == 1) { /* leaf node */
        DataNode *leaf = (DataNode*)BF_Block_GetData(b);
        /* insert sorted */
        int key = record_get_key(schema, record);
        int pos = 0;
        while (pos < leaf->count && record_get_key(schema, &leaf->records[pos]) < key) pos++;

        if (leaf->count < MAX_RECORDS_LEAF) {
            /* shift */
            for (int i = leaf->count; i > pos; i--) leaf->records[i] = leaf->records[i-1];
            leaf->records[pos] = *record;
            leaf->count++;
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
            DataNode *new_leaf = (DataNode*)BF_Block_GetData(new_b);

            /* distribute */
            Record temp[MAX_RECORDS_LEAF + 1];
            int j=0;
            for(int i=0; i<leaf->count; i++) {
                if(i == pos) temp[j++] = *record;
                temp[j++] = leaf->records[i];
            }
            if(pos == leaf->count) temp[j++] = *record;

            int split = (MAX_RECORDS_LEAF + 1) / 2;

            leaf->count = split;
            for(int i=0; i<split; i++) leaf->records[i] = temp[i];

            new_leaf->count = (MAX_RECORDS_LEAF + 1) - split;
            for(int i=0; i<new_leaf->count; i++) new_leaf->records[i] = temp[split+i];

            new_leaf->next_block_id = leaf->next_block_id;
            leaf->next_block_id = new_id;

            *up_key = record_get_key(schema, &new_leaf->records[0]);
            *up_right = new_id;

            /* fix: return id where record landed */
            if (pos < split) ret_val = curr_block;
            else ret_val = new_id;

            BF_Block_SetDirty(b);
            BF_Block_SetDirty(new_b);
            BF_UnpinBlock(new_b); BF_Block_Destroy(&new_b);
        }
    } else { /* index node */
        IndexNode *idx = (IndexNode*)BF_Block_GetData(b);
        int key = record_get_key(schema, record);
        int pos = 0;
        while(pos < idx->count && key >= idx->keys[pos]) pos++;
        int child = idx->children[pos];

        int child_up_key, child_up_right;
        ret_val = insert_recursive(file_desc, child, record, &child_up_key, &child_up_right, height - 1, schema);

        if (child_up_right != -1) {
            /* child split, insert into this node */
            if (idx->count < MAX_KEYS_INDEX) {
                /* shift */
                for(int i = idx->count; i > pos; i--) {
                    idx->keys[i] = idx->keys[i-1];
                    idx->children[i+1] = idx->children[i];
                }
                idx->keys[pos] = child_up_key;
                idx->children[pos+1] = child_up_right;
                idx->count++;
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
                IndexNode *new_idx = (IndexNode*)BF_Block_GetData(new_b);

                /* temp arrays */
                int temp_keys[MAX_KEYS_INDEX + 1];
                int temp_children[MAX_KEYS_INDEX + 2];

                int j=0;
                for(int i=0; i<idx->count; i++) {
                    if(i==pos) temp_keys[j++] = child_up_key;
                    temp_keys[j++] = idx->keys[i];
                }
                if(pos == idx->count) temp_keys[j++] = child_up_key;

                j=0;
                for(int i=0; i<=idx->count; i++) {
                     if(i==pos+1) temp_children[j++] = child_up_right;
                     temp_children[j++] = idx->children[i];
                }
                if(pos+1 == idx->count+1) temp_children[j++] = child_up_right;

                /* split point */
                int total_keys = MAX_KEYS_INDEX + 1;
                int mid = total_keys / 2;

                *up_key = temp_keys[mid]; /* pivot moves up */
                *up_right = new_id;

                idx->count = mid;
                for(int i=0; i<mid; i++) {
                    idx->keys[i] = temp_keys[i];
                    idx->children[i] = temp_children[i];
                }
                idx->children[mid] = temp_children[mid];

                new_idx->count = total_keys - mid - 1;
                for(int i=0; i<new_idx->count; i++) {
                    new_idx->keys[i] = temp_keys[mid + 1 + i];
                    new_idx->children[i] = temp_children[mid + 1 + i];
                }
                new_idx->children[new_idx->count] = temp_children[total_keys];

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
    int ret = insert_recursive(file_desc, metadata->root_block_id, record, &up_key, &up_right, metadata->height, &metadata->schema);

    if (up_right != -1) {
        /* root split */
        BF_Block *new_root_b;
        BF_Block_Init(&new_root_b);
        if (BF_AllocateBlock(file_desc, new_root_b) != BF_OK) { BF_Block_Destroy(&new_root_b); return -1; }
        int new_root_id;
        BF_GetBlockCounter(file_desc, &new_root_id); new_root_id--;

        IndexNode *root = (IndexNode*)BF_Block_GetData(new_root_b);
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
