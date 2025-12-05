#include "bplus_file_funcs.h"
#include <stdio.h>

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return bplus_ERROR;     \
    }                         \
  }


int bplus_create_file(const TableSchema *schema, const char *fileName)
{
  return -1;
}


int bplus_open_file(const char *fileName, int *file_desc, BPlusMeta **metadata)
{
  return -1;
}

int bplus_close_file(const int file_desc, BPlusMeta* metadata)
{
  return -1;
}

int bplus_record_insert(const int file_desc, BPlusMeta *metadata, const Record *record)
{
  return -1;
}

int bplus_record_find(const int file_desc, const BPlusMeta *metadata, const int key, Record** out_record)
{  
  *out_record=NULL;
  return -1;
}

