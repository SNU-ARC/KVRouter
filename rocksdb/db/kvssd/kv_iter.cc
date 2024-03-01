//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_impl/db_impl.h"
#include <iostream>
#include <stdint.h>
#ifdef OS_SOLARIS
#include <alloca.h>
#endif

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/arena_wrapped_db_iter.h"
#include "db/builder.h"
#include "db/compaction/compaction_job.h"
#include "db/db_info_dumper.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/flush_job.h"
#include "db/forward_iterator.h"
#include "db/import_column_family_job.h"
#include "db/job_context.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/malloc_stats.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/periodic_work_scheduler.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/transaction_log_impl.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "db/write_callback.h"
#include "env/unique_id_gen.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/sst_file_manager_impl.h"
#include "logging/auto_roll_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/in_memory_stats_history.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/persistent_stats_history.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/stats_history.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/version.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/get_context.h"
#include "table/merging_iterator.h"
#include "table/multiget_context.h"
#include "table/sst_file_dumper.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "trace_replay/trace_replay.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "utilities/trace/replayer_impl.h"
#include "db/kv_ssd/kv_ssd.h"
#include "db/kv_ssd/kv_iter.h"

namespace ROCKSDB_NAMESPACE {
//#define SUCCESS (0)
//#define FAILED (1)
class KV_Iter;
int test_cnt = 0;
void complete(kvs_postprocess_context* ioctx) 
{
    if (ioctx->context == KVS_CMD_STORE)
    {
        if (ioctx->result != KVS_SUCCESS)
        {
            fprintf(stdout, "ERROR io_complete: op=%d. result=%d key=%s\n", 
                    ioctx->context, ioctx->result, (char*)ioctx->key->key);
        }
        bool * status = (bool *) ioctx->private1;
        *status = true;
    }
    else if (ioctx->context == KVS_CMD_RETRIEVE)
    {
        if (ioctx->result != KVS_SUCCESS)
        {
            /*
            BUFFER_STAT * buffer_status = (BUFFER_STAT *) ioctx->private2;
            if (buffer_status != nullptr) 
                *buffer_status = BUF_IDLE;
            */

            //fprintf(stdout, "Key is not exist: op=%d. result=%d key=%s\n", 
            //        ioctx->context, ioctx->result, (char*)ioctx->key->key);
            int * status = (int *) ioctx->private1;
            *status = -1;
            printf("CMD RETRIEVE %d %p \n", *status, status);
        }
        else
        {
            // It means that we never use this buffer ! buffer is locked now ! 
            BUFFER_STAT * buffer_status = (BUFFER_STAT *) ioctx->private2;
            if (buffer_status != nullptr) 
                *buffer_status = BUF_DONE;
            

            int * status = (int *) ioctx->private1;
            *status = 1;
            printf("CMD RETRIEVE %d %p \n", *status, status);
        }
    }
    else if (ioctx->context == KVS_CMD_EXIST)
    {
        //uint8_t *exist;
        //exist = (uint8_t*)ioctx->result_buffer.list->result_buffer;
        //fprintf(stdout, "exist? %s :: %p %p \n", *exist == 0? "FALSE":"TRUE", exist, ioctx->private1);
        
        bool * done = (bool * )ioctx->private1;
        *done = true; 
    }
    /*
    switch(ioctx->context) 
    {
        case KVS_CMD_ITER_NEXT:
            //_iterator_complete_handle(ioctx);
            break;
        case KVS_CMD_DELETE:
            //_delete_complete_handle(ioctx);
            break;
        case KVS_CMD_EXIST:
            //_key_exist_complete_handle(ioctx);
            break;
        case KVS_CMD_STORE:
        case KVS_CMD_RETRIEVE:
            q_head = (int *)(ioctx->private1);
            *q_head = (*q_head + 1) % 64; 
            break;
        default:
            fprintf(stdout, "ERROR io_complete unknow op = %d, result=0x%x.\n",
                    ioctx->context, ioctx->result);
            break;
  }
  */
  /*
  common_data *data;
  data = (common_data*)(ioctx->private2);

  data->completed++;
  data->cur_qdepth--;
  */

  /*
  if (data->completed.load() % 1000 == 0)
    fprintf(stdout, "thread [%d] completed io count: %d\n", data->thread_id,
            data->completed.load());
  */
}


KV_SSD::KV_SSD()
{
	strcpy(dev_path, "/dev/nvme2n1");
	strcpy(keyspace_name, "rocksdb_test");
	max_level_ = 0;
	capacity_ = 0;

	if(dev_path == NULL) {
		fprintf(stderr, "Please specify KV SSD device path\n");
		return;
	}
	
	char ks_name[MAX_KEYSPACE_NAME_LEN];
	snprintf(ks_name, MAX_KEYSPACE_NAME_LEN, "%s", "keyspace_test");
	if(env_init() != 0)
	{
		printf("Env init Error ! \n"); 
		while(1) {} 
	}
	//if(env_init(dev_path, &dev, ks_name, &ks_hd) != SUCCESS)
    put_capa_ = 0;   
    printf (" KV SSD open is done ! %p \n", this);
}

KV_SSD::~KV_SSD() 
{
  uint32_t dev_util = 0;
  kvs_get_device_utilization(dev, &dev_util);
  fprintf(stdout, "After: Total used is %d\n", dev_util);  
  kvs_close_key_space(ks_hd);
  kvs_key_space_name ks_name;
  ks_name.name_len = strlen(keyspace_name);
  ks_name.name = keyspace_name;
  kvs_delete_key_space(dev, &ks_name);
  kvs_close_device(dev);
	
  //free(dev_path);
#endif
}

int KV_SSD::env_init() {
  printf("Env Init Start\n");
  fflush(stdout);
  //kvs_result ret = kvs_open_device("/dev/nvme2n1", &dev);
  kvs_result ret = kvs_open_device(dev_path, &dev);
  if(ret != KVS_SUCCESS) {
    fprintf(stderr, "Device open failed 0x%x\n", ret);
    return 1;
  }

  //keyspace list after create "test"
  const uint32_t retrieve_cnt = 2;
  kvs_key_space_name names[retrieve_cnt];
  for(uint8_t idx = 0; idx < retrieve_cnt; idx++) {
    names[idx].name_len = MAX_KEYSPACE_NAME_LEN;
    names[idx].name = (char*)malloc(MAX_KEYSPACE_NAME_LEN);
  }

  uint32_t valid_cnt = 0;
  ret = kvs_list_key_spaces(dev, 1, retrieve_cnt*sizeof(kvs_key_space_name),
    names, &valid_cnt);
  if(ret != KVS_SUCCESS) {
    fprintf(stderr, "List current keyspace failed. error:0x%x.\n", ret);
    kvs_close_device(dev);
    return 1;
  }
  for (uint8_t idx = 0; idx < valid_cnt; idx++) {
    kvs_delete_key_space(dev, &names[idx]);
  }
  printf("Env Init 22 Start\n");
  fflush(stdout);

  //create key spaces
  kvs_key_space_name ks_name;
  kvs_option_key_space option = { KVS_KEY_ORDER_NONE };
  ks_name.name = keyspace_name;
  ks_name.name_len = strlen(keyspace_name);
  //currently size of keyspace is not support specify
  ret = kvs_create_key_space(dev, &ks_name, 0, option);
  if (ret != KVS_SUCCESS) {
    kvs_close_device(dev);
    fprintf(stderr, "Create keyspace failed. error:0x%x.\n", ret);
    return 1;
  }
  printf("Env Init 33 Start\n");
  fflush(stdout);

  ret = kvs_open_key_space(dev, keyspace_name, &ks_hd);
  if(ret != KVS_SUCCESS) {
    fprintf(stderr, "Open keyspace %s failed. error:0x%x.\n", keyspace_name, ret);
    kvs_delete_key_space(dev, &ks_name);
    kvs_close_device(dev);
    return 1;
  }

  kvs_key_space ks_info;
  ks_info.name = (kvs_key_space_name *)malloc(sizeof(kvs_key_space_name));
  if(!ks_info.name) {
    fprintf(stderr, "Malloc resource failed.\n");
    env_exit();
    return 1;
  }
  printf("Env Init 4 Start\n");
  fflush(stdout);
  ks_info.name->name = (char*)malloc(MAX_CONT_PATH_LEN);
  if(!ks_info.name->name) {
    fprintf(stderr, "Malloc resource failed.\n");
    free(ks_info.name);
    env_exit();
    return 1;
  }
  ks_info.name->name_len = MAX_CONT_PATH_LEN;
  ret = kvs_get_key_space_info(ks_hd, &ks_info);
  if(ret != KVS_SUCCESS) {
    fprintf(stderr, "Get info of keyspace failed. error:0x%x.\n", ret);
    free(ks_info.name->name);
    free(ks_info.name);
    return 1;
  }
  fprintf(stdout, "Keyspace information get name: %s\n", ks_info.name->name);
  fprintf(stdout, "open:%d, count:%ld, capacity:%ld, free_size:%ld.\n", 
    ks_info.opened, ks_info.count, ks_info.capacity, ks_info.free_size);
  free(ks_info.name->name);
  free(ks_info.name);

  printf("Env Init 5 Start\n");
  fflush(stdout);
  uint32_t dev_util = 0;
  uint64_t dev_capa = 0;
  kvs_get_device_utilization(dev, &dev_util);
  kvs_get_device_capacity(dev, &dev_capa);
  fprintf(stdout, "Before: Total size is %ld bytes, used is %d\n", dev_capa, dev_util);
  kvs_device *dev_info = (kvs_device*)malloc(sizeof(kvs_device));
  if(dev_info) {
    kvs_get_device_info(dev, dev_info);
    fprintf(stdout, "Total size: %.2f GB\nMax value size: %d\nMax key size: %d\nOptimal value size: %d\n",
    (float)dev_info->capacity/1000/1000/1000, dev_info->max_value_len,
    dev_info->max_key_len, dev_info->optimal_value_len);
    free(dev_info);
  }

  return 0;
}

int KV_SSD::env_exit() 
{
  uint32_t dev_util = 0;
  kvs_get_device_utilization(dev, &dev_util);
  fprintf(stdout, "After: Total used is %d\n", dev_util);  
  kvs_close_key_space(ks_hd);
  kvs_key_space_name ks_name;
  ks_name.name_len = strlen(keyspace_name);
  ks_name.name = keyspace_name;
  kvs_delete_key_space(dev, &ks_name);
  kvs_result ret = kvs_close_device(dev);
  return ret;
}

bool KV_SSD::kv_put_large(uint64_t rocksdb_total_size, const char * key, size_t key_size, const char * buffer, size_t size)
{
    if (rocksdb_total_size) {} 
    // Periodically Get device Capactiy !
    /*
    uint32_t kv_capa = 0;
    kvs_result ret = kvs_get_device_utilization(dev, &kv_capa);
    uint64_t kv_total_size = kv_capa * 3.5*1024*1024*1024*1024/100;

    if (ret != KVS_SUCCESS)
    {
        fprintf(stderr, "kvs_get device capa err 0x%x\n", ret);
		while (1)
		{
			printf("Fuck you Error?\n");
			fflush(stdout);
			exit(-1);
		}
    }
  
    if (kv_total_size * 2 < rocksdb_total_size)
    {
        return false;
    }
    */
    char * new_key = (char *)key;
    char * new_value = (char *)buffer;
    kv_put_buffer(0, new_key, key_size, new_value, size);
   
    return true;
}

void KV_SSD::kv_put_buffer(int level, char * key, size_t key_size, char * buffer, size_t size)
{
    put_capa_ = put_capa_ + (long long int) size;
    if (level == 0) {}
    if (key_size == 0) {}
	char * key_p = key;

	kvs_key kv_key;
	kvs_value kv_value;
	kv_key.key = key_p;
	kv_key.length = (uint16_t) key_size;

    PRINTKV("[DEBUG] %s level %d key %p key len %ld value %p value len %ld used %lld \t", __func__, level, key, key_size, buffer, size, put_capa_);
    /*
    printf("KV_PUT_BUFFER %ld\n", key_size);
    for (size_t i = 0 ; i < key_size ; i ++)
    {
        printf("%02x", *(key_p+i));
    }
    printf("\n");
    */
    // Async 
	kv_value.value = buffer;
	kv_value.length = (uint32_t) size;
	kv_value.actual_value_size = (uint32_t) 0;
	kv_value.offset = 0;

	kvs_option_store option = {KVS_STORE_POST, NULL};
	kvs_result ret = kvs_store_kvp(ks_hd, &kv_key, &kv_value, &option);
	
    if (ret != KVS_SUCCESS) 
	{
		fprintf(stderr, "store tuple failed with err 0x%x\n", ret);
		//_free_kvs_pair(kvskey, kvsvalue, &(pool.keypool), &(pool.valuepool), &lock);
		while (1)
		{
			printf("Fuck you Error?\n");
			fflush(stdout);
			exit(-1);
		}
	}
}

void KV_SSD::kv_put_buffer_sync(int level, char * key, size_t key_size, char * buffer, size_t size)
{
    if (level == 0) {}
    if (key_size == 0) {}
	
    char * key_p = key;

	kvs_key kv_key;
	kvs_value kv_value;
	kv_key.key = key_p;
	kv_key.length = (uint16_t) key_size;

    kv_value.value = buffer;
	kv_value.length = (uint32_t) size;
	kv_value.actual_value_size = (uint32_t) 0;
	kv_value.offset = 0;
   
    //std::vector<async_complete>::iterator ret_it = allocate_async(key);
    kvs_option_store option = {KVS_STORE_POST, NULL};
	kvs_result ret = kvs_store_kvp(ks_hd, &kv_key, &kv_value, &option);
    if (ret != KVS_SUCCESS) 
	{
		fprintf(stderr, "store tuple failed with err 0x%x\n", ret);
        printf("Fuck you Error?\n");
        fflush(stdout);
        exit(-1);
	}
}

void KV_SSD::kv_put_buffer_async(int level, char * key, size_t key_size, char * buffer, size_t size, bool * status)
{
    if (level == 0) {}
    if (key_size == 0) {}
	
    char * key_p = key;

	kvs_key kv_key;
	kvs_value kv_value;
	kv_key.key = key_p;
	kv_key.length = (uint16_t) key_size;

    kv_value.value = buffer;
	kv_value.length = (uint32_t) size;
	kv_value.actual_value_size = (uint32_t) 0;
	kv_value.offset = 0;
   
    //std::vector<async_complete>::iterator ret_it = allocate_async(key);
    kvs_option_store option = {KVS_STORE_POST, NULL};
    kvs_result ret = kvs_store_kvp_async(ks_hd, &kv_key, &kv_value, &option, status, nullptr, complete);	
    if (ret != KVS_SUCCESS) 
	{
		fprintf(stderr, "store tuple failed with err 0x%x\n", ret);
        printf("Fuck you Error?\n");
        fflush(stdout);
        exit(-1);
	}
}

void KV_SSD::kv_put(int level, const Slice & key, const Slice & value)
{
	//if (level != 0) return;
	printf("%s Start.. level %d \n", __func__, level);
    // Key -> Low level Key
	char * key_p = (char * )key.data();
	char * value_p = (char * )value.data();

	kvs_key kv_key;
	kvs_value kv_value;
	kv_key.key = key_p;
	kv_key.length = (uint16_t) key.size();
	
	kv_value.value = value_p;
	kv_value.length = (uint32_t) value.size();
	kv_value.actual_value_size = kv_value.offset = 0;

	kvs_option_store option = {KVS_STORE_POST, NULL};
	kvs_result ret = kvs_store_kvp(ks_hd, &kv_key, &kv_value, &option);

    if (ret != KVS_SUCCESS) 
	{
		fprintf(stderr, "store tuple failed with err 0x%x\n", ret);
		//_free_kvs_pair(kvskey, kvsvalue, &(pool.keypool), &(pool.valuepool), &lock);
		while (1)
		{
			printf("Fuck you Error?\n");
			fflush(stdout);
			exit(-1);
		}
	}
}

void KV_SSD::kv_check_async(int level, char * key, int key_size, uint8_t * status, bool * done)
{
    int ret = KVS_SUCCESS;
    if (level == 0)
    {
        printf("Not implemented yet\n");
        return;
    }
   
    kvs_key kv_key;
    kv_key.key = key;
	kv_key.length = (uint16_t) key_size;
    kvs_exist_list *list = (kvs_exist_list*)malloc(sizeof(kvs_exist_list));
    if (list) {
        list->keys = &kv_key;
        list->length = 1;
        list->num_keys = 1;
        list->result_buffer = status;
    }

    /*
    printf("KV_CHECK_ASYNC key_size %d \n", key_size);
    for (int i = 0 ; i < key_size ; i ++)
    {
        printf("%02x", *(key+i));
    }
    printf("\n");
    */
    ret = kvs_exist_kv_pairs_async(ks_hd, 1, &kv_key, list, done, nullptr, complete);
    if (ret != KVS_SUCCESS) {
        fprintf(stderr, "exit tuple failed with err 0x%x\n", ret);
        while(1) {}
    }
}

bool KV_SSD::kv_check(int level, const char * key, size_t key_size)
{
    printf("[DEBUG] %s level %d key %p key len %ld\n", __func__, level, key, key_size);
    uint8_t exist_status;
    kvs_exist_list list;
    list.result_buffer = &exist_status;
    list.length = 1;
	
	kvs_key kv_key;
	
	char * key_p = (char *)key;
	kv_key.key = key_p;
	kv_key.length = (uint16_t) key_size;
    kvs_result ret = kvs_exist_kv_pairs(ks_hd, 1, &kv_key, &list);
	if (ret != KVS_SUCCESS) 
	{
		fprintf(stderr, "check key failed with err 0x%x\n", ret);
        while (1)
		{
			printf("Fuck You ERR\n");
			fflush(stdout);
		    while(1) {} 
        }
	}
    else
    {
        if (exist_status == 0) return false;
        else return true;
        //fprintf(stderr, "check key %s exist? %s\n", key, exist_status == 0? "FALSE":"TRUE");
    }
    return false;
}

char * KV_SSD::kv_get_buffer(int level, const char * key, size_t key_size, char * buffer, size_t buffer_size)
{
    if (level) {} 
	char * key_p = (char *)key;
	char * value_p = buffer;
	kvs_option_retrieve option = {false};

	kvs_key kv_key;
	kvs_value kv_value;
	
	kv_key.key = key_p;
	kv_key.length = (uint16_t) key_size;
   
    PRINTKV("[DEBUG] %s level %d key %p key len %ld value %p buffer size %ld\n", __func__, level, key, key_size, buffer, buffer_size);
    /*
    for (int i = 0 ; i < 17 ; i ++)
    {
        PRINTKV("%02x", *(key+i)); 
    }
    PRINTKV("\n");
    fflush(stdout);
    */
	kv_value.value = value_p;
    kv_value.length = buffer_size;
    kv_value.actual_value_size = 0;
    kv_value.offset = 0;

	kvs_result ret = kvs_retrieve_kvp(ks_hd, &kv_key, &option, &kv_value);
	if (ret != KVS_SUCCESS) 
	{
		fprintf(stderr, "retrieve tuple failed with err 0x%x\n", ret);
        while (1)
		{
			printf("Fuck You ERR\n");
			fflush(stdout);
			exit(-1);
		}
		//_free_kvs_pair(kvskey, kvsvalue, &(pool.keypool), &(pool.valuepool), &lock);
	}
    return nullptr;
}

void KV_SSD::kv_get_buffer_async(int level, char * key, size_t key_size, char * buffer, size_t buffer_size, bool * status)
{
    KV_SSD_KStruct my_key;
    my_key.level = level;
    memcpy(my_key.key, key, __K_SIZE);
    
    // Key
    kvs_key kv_key;
    kv_key.key = (char *)&my_key;
    kv_key.length = (uint16_t) key_size; // 17 Bytes
  
    // Value
	kvs_value kv_value;
	kv_value.value = (char *)buffer;
    kv_value.length = buffer_size;
    kv_value.actual_value_size = 0;
    kv_value.offset = 0;
    kvs_option_retrieve option = {false};
    kvs_result ret = kvs_retrieve_kvp_async(ks_hd, &kv_key, &option, status, nullptr, &kv_value, complete);
    if (ret != KVS_SUCCESS) 
    {
        fprintf(stderr, "retrieve tuple failed with err 0x%x\n", ret);
        while (1)
        {
            printf("Fuck You ERR\n");
            fflush(stdout);
            exit(-1);
        }
    }
}

bool NextKey(char * buf, size_t size)
{
    size_t index = size - 1 - 4; // offset 4
    while (1)
    {
        if (*(buf + index) == 0x39)
        {
            *(buf + index) = 0x30;
            if (index == 1)
            {
                return false;
            }
            index = index - 1;
        }
        else
        {
            *(buf + index) = *(buf+index) + 1; 
            return true;
        }
    }
}

// left <= right : true
// left > right : false
/*
bool KV_SSD::Compare(char * cur_key, char * last_key, size_t key_size)
{
    int ret = memcmp(left, right, key_size);
    if (ret == 0)
    {
        return false;
    }
    else
    {
        return true;
    }
}
*/
KVIndex * KV_SSD::KVAllocateIndex(int level, char * key, size_t key_size, KVIndex * root)
{
    if (level) {}
    char * buffer = (char *)malloc(2*1024*1024);
    KVIndex * ret = AddIndex(root, buffer, 0, key, key_size, true);
    //KVIndex * ret = AddIndex(root, buffer, 0);
    memcpy(ret->key_, key, key_size);
    ret->key_[12] = 0x30;
    ret->key_[13] = 0x30;
    ret->key_[14] = 0x30;
    ret->key_[15] = 0x30;
    printf("%s :: %s\n", __func__, ret->key_);
    return ret;
}


#define D_MAX_GET_CNT (256)
KVIndex * KV_SSD::KVGetIndexSync(int level, char * smallest, size_t key_size, bool * index_exist)
{
    if (level) {}
    if (key_size) {} 
    printf(" KV Get Index Start smallest %s \n", smallest);

    char first_key[16];
    memcpy(first_key, smallest, 16);
    for (int i = 0 ; i < 4 ; i ++)
    {
        first_key[12+i] = 0x30;
    }

    KVIndex * my_index = nullptr;

    size_t offset_val = 2 * 1024 * 1024; // 2MB
    char * buffer = (char *)malloc(sizeof(char) * offset_val); // 512MB
    
    KV_SSD_KStruct my_key;
    my_key.level = 64 + level;
    memcpy(my_key.key, first_key, 16);
   
    // Key
    kvs_key kv_key;
    kv_key.key = (char *)&my_key;
    kv_key.length = (uint16_t) sizeof(KV_SSD_KStruct); // 17 Bytes

    // Value
    kvs_value kv_value;

    kv_value.value = buffer;
    kv_value.length = offset_val; // 2MB
    kv_value.actual_value_size = 0;
    kv_value.offset = 0;
    kvs_option_retrieve option = {false};
	kvs_result ret = kvs_retrieve_kvp(ks_hd, &kv_key, &option, &kv_value);
    if (ret != KVS_SUCCESS) 
    {
        // Get Fail means there is no key exist in KV SSD 
        *index_exist = false;
        free(buffer);
        return nullptr;
    }

    my_index = AddIndex(nullptr, buffer, 0, first_key, key_size, false);
    PrintIndex(my_index, 0);

    *index_exist = true;
    return my_index;
}

// TODO 
void KV_SSD::ParseIndex(KVIndex * index, int * key_count, size_t * used_buffer)
{
    struct KVInfo * info = (struct KVInfo *) index->buffer_;
    *key_count = info->key_count_;
    *used_buffer = info->used_buffer_;
    
    printf("Parse Index Key %s key count %d used buffer %ld\n", index->key_, info->key_count_, info->used_buffer_);
}

KVIndex * KV_SSD::KVGetIndex(int level, char * smallest, char * largest, size_t key_size, bool * index_exist)
{
    if (level) {}
    printf(" KV Get Index Start smallest %s largest %s \n", smallest, largest);
    int qd = 256;
    int tail = 0;
    int head = 0;
    bool done = false;
    int status[256] = {0};

    bool first_index = false;
    char first_key[16];
    char last_key[16];
    memcpy(first_key, smallest, 16);
    memcpy(last_key, largest, 16);
    for (int i = 0 ; i < 4 ; i ++)
    {
        first_key[12+i] = 0x30;
        last_key[12+i] = 0x30;
    }

    //root = (KVIndex *) malloc(sizeof(KVIndex));
    KVIndex * ret_index = nullptr;
    KVIndex * my_index = nullptr;

    size_t offset_val = 2 * 1024 * 1024; // 2MB
    char * buffer[D_MAX_GET_CNT];
    for (int i = 0 ; i < D_MAX_GET_CNT ; i ++)
    {
        buffer[i] = (char *)malloc(sizeof(char) * offset_val); // 512MB
    }
   
    //BUFFER_STAT buffer_index[D_MAX_GET_CNT] = {BUF_IDLE}; // Max 
    int buffer_done_cnt = 0; 
    while (!done || tail != head)
    {
        // done : Find largest key?
        // tail head : wait for QD
        // buffer_done_cnt : not enough buffer
        if (!done && ((tail + 1) % qd) != head && buffer_done_cnt != D_MAX_GET_CNT)
        {
            KV_SSD_KStruct my_key;
            my_key.level = 64 + level;
            memcpy(my_key.key, first_key, 16);

            // Key
            kvs_key kv_key;
            kv_key.key = (char *)&my_key;
            kv_key.length = (uint16_t) sizeof(KV_SSD_KStruct); // 17 Bytes

            // Value
            kvs_value kv_value;

            kv_value.value = buffer[tail];
            kv_value.length = offset_val; // 2MB
            kv_value.actual_value_size = 0;
            kv_value.offset = 0;
            kvs_option_retrieve option = {false};
            kvs_result ret = kvs_retrieve_kvp_async(ks_hd, &kv_key, &option, &status[tail], nullptr, &kv_value, complete);
            if (ret != KVS_SUCCESS) 
            {
                fprintf(stderr, "retrieve tuple failed with err 0x%x\n", ret);
                while (1)
                {
                    printf("Fuck You ERR\n");
                    fflush(stdout);
                    exit(-1);
                }
            }
            // After
            NextKey(first_key, key_size);
            if (memcmp(first_key, last_key, 16) > 0)
            {
                printf(" %s Done here %s \n", __func__, my_key.key);
                done = true;
            }
            printf("update tail %d\n", tail);
            tail = (tail + 1) % qd;
        }
        else if (head == tail)
        {
            done = true; 
        }

        if (status[head] != 0)
        {
            if (status[head] == 1)
            {
                printf("Get index Success head %d\n", head);
                // TODO : ERRRRRRRRRRRRRRRRRRRRRRRRRRRRrr
                // First key is not true I need to pass status about key information
                my_index = AddIndex(my_index, buffer[head], 0, first_key, key_size, false);
                //my_index = AddIndex(my_index, buffer[head], 0);
                if (first_index == false)
                {
                    ret_index = my_index;
                }
                first_index = true;
                buffer[head] = (char *)malloc(sizeof(char) * offset_val); // 512MB
            }
            printf("update head %d status head %d %p \n", head, status[head], &status[head]);
            status[head] = 0;
            head = (head + 1) % qd;
        }
    }
    PrintIndex(ret_index, 10);


    for (int i = 0 ; i < D_MAX_GET_CNT ; i ++)
    {
        free(buffer[i]);
    }

    if (ret_index == nullptr)
    {
        *index_exist = false;
        return ret_index;
    }
    else
    {
        // Change Smallest To First key (Next Smallest key) 
        memcpy(smallest, first_key, 16);
        *index_exist = true;
        return ret_index;
    }
}

void KV_SSD::kv_get(int level, const Slice & key, const Slice& value)
{
	if (level != 0) return;
	char * key_p = (char *)key.data();
	char * value_p = (char *)value.data();
	
	kvs_option_retrieve option = {false};

	kvs_key kv_key;
	kvs_value kv_value;
	
	kv_key.key = key_p;
	kv_key.length = (uint16_t) key.size();

	kv_value.value = value_p;

	kvs_result ret = kvs_retrieve_kvp(ks_hd, &kv_key, &option, &kv_value);
	//ret = kvs_retrieve_kvp_async(ks_hd, kvskey, &option, &pool, &data, kvsvalue, complete);
	if (ret != KVS_SUCCESS) 
	{
		fprintf(stderr, "retrieve tuple failed with err 0x%x\n", ret);
		while (1)
		{
			printf("Fuck You ERR\n");
			fflush(stdout);
			exit(-1);
		}
		//_free_kvs_pair(kvskey, kvsvalue, &(pool.keypool), &(pool.valuepool), &lock);
	}
}

uint64_t KV_SSD::slice_to_key(const Slice& key)
{
    std::cout << key.ToString(false) << std::endl;
    uint64_t ret_val = 0; 
    const char * buf = (char *) key.data();
    std::cout << key.ToString(false) << std::endl;
    for (int i = 1 ; i < 17; i ++)
    {
        //printf("[%02x:%lld] ", *(buf+i), (long long int) ret_val);
        ret_val = ret_val * 10 + (*(buf+i) - 48);
    }
    //printf(" ret val %lld \n", (long long int)ret_val);
    return ret_val;
}

uint64_t KV_SSD::hash_get(uint64_t key_val)
{
    return hash_.Get(key_val);
}

void KV_SSD::hash_insert(uint64_t key_val, int count)
//void KV_SSD::hash_insert(const Slice& key, int count)
{
    /*
    uint64_t key_val = slice_to_key(key); 
    printf("Hash insert hash %lld count %d \n", (long long int)key_val, count);
    */
    hash_.Insert(key_val, count); 
}

bool KV_SSD::hash_contains(uint64_t key_val)
{
    //uint64_t new_key = slice_to_key(key);
    return hash_.Contains(key_val); 
}

void KV_SSD::hash_delete(const Slice& key)
{
    uint64_t new_key = slice_to_key(key);
    hash_.Delete(new_key); 
}

/*
void KV_SSD::release_async(char * input)
{
    async_lock.lock();
    bool is_find = false;
    for (it = completed_key_.begin() ; it < completed_key_.end() ; it ++)
    {
        if (it->status == 0) continue;
        if (memcmp(input, (char *) &it->encoded_key_, 17) == 0)
        {
            memset((char *)&it->encoded_key_, 0, 17);
            is_find = true;
            break;
        }
    }
    if (is_find == false)
    {
        printf("Fuck You Erorr Why release aysnc fail\n");
        while (1) {} 
    }
    async_lock.unlock();
}

std::vector<struct async_complete>::iterator KV_SSD::allocate_async(char * input)
{
    async_lock.lock();
    std::vector<struct async_complete>::iterator ret_it;    
    struct async_complete new_async;
    new_async.status_ = 1;
    new_async.submit_ = 0;
    new_async.complete_ = 0;
    memcpy((char *) &new_async.encoded_key_, input, 17); 
    completed_key_.push_back(new_async); 
    ret_it = completed_key_.end() - 1;

    async_lock.unlock();
    return ret_it;
}
*/

//  [Index] -> Node -> Node -> Node  
//  [Index] 
//  [Index] 
KVIndex * KV_SSD::AddIndex (KVIndex * prev, char * buffer, size_t offset_val, char * key, size_t key_size, bool init_meta)
{
    KVIndex * my_index = (KVIndex *)malloc(sizeof(KVIndex));
    if (buffer == nullptr)
    {
        printf ("%s Malloc Fail here ! \n", __func__);
        while (1) {}
    }
    memcpy(my_index->key_, key, key_size);
    my_index->entire_buffer_ = buffer;
    my_index->meta_ = (KVInfo *) buffer;
    my_index->buffer_ = buffer + offset_val + sizeof(KVInfo);
    if (prev != nullptr)
    {
        my_index->next_ = prev->next_;
        prev->next_ = my_index;
    }
    else
    {
        my_index->next_ = nullptr;
    }

    // Meta 
    if (init_meta)
    {
        my_index->meta_->bytes_ = 0; 
        my_index->meta_->key_count_ = 0;  
        my_index->meta_->root_offset_ = 0;
        my_index->meta_->used_buffer_ = 0;
    }
    return my_index;
}

void KV_SSD::PrintIndex (KVIndex * prev, int print_cnt)
{
    int cnt = 0;
    printf ("Print Index Start ! %p \n", prev);
    fflush(stdout);
    KVIndex * my_index = prev;
    while (my_index != nullptr)
    {
        printf("%s my_index %p Bytes %ld key count %d print_cnt %d \n", my_index->key_, my_index, my_index->meta_->bytes_, my_index->meta_->key_count_, cnt);
        //PrintNode(my_index->meta_->key_count_, my_index->buffer_, my_index->meta_->root_offset_);
        my_index = my_index->next_;
        if (print_cnt == cnt) break;
        cnt ++;
    }
}

void KV_SSD::PrintNode(int key_count, char * buffer, size_t root_offset)
{
    KVIndexNode * tmp_node = (KVIndexNode *) (buffer + root_offset);
    printf(" Print Node Key Count %d\n", key_count);

    for (int i = 0 ; i < key_count ; i ++)
    {
        printf("%s\t", tmp_node->key_); 
        if (tmp_node->next_offset_ <= 0)
        {
            printf("PrintNode Fail .. next offset is lower than 0\n");
            break;
        }
        tmp_node = (KVIndexNode *) (buffer + tmp_node->next_offset_);
    }
    printf("\n");
}

bool KV_SSD::CheckIndex(KVIndex * prev, char * key, size_t key_size)
{
    KVIndex * my_index = prev;
    /*
    for (size_t i = 0 ; i < key_size - 4 ; i ++)
    {
        printf("CMP %4ld : %02x %02x\n", i, (uint8_t) my_index->key_[i], (uint8_t) key[i]);
    }
    */

    if (memcmp(my_index->key_, key, key_size - 4) == 0)
    {
        return true;
    }
    else
    {
        return false;
    }
}

// SearchIndex ret: 0 [find same key] 1 [find large index] -1 [have to take more Index from NVMe]
KVIndex * KV_SSD::SearchIndex(KVIndex * input_index, char * key, int * search_ret, int * skip_cnt)
{
    // Ret means that I found a large or same KVIndex 
    // False means I have to take more KV Index
    //
    KVIndex * prev = input_index;
    KVIndex * cur = input_index;
    int skip =  0;
    while (cur != nullptr)
    {
        int ret = memcmp(cur->key_, key, 16);
        printf("%s compare result %d\n", __func__, ret);
        if (ret == 0)
        {
            *skip_cnt = skip;
            *search_ret = 0;
            printf("%s return %d %d \n", __func__, *skip_cnt, *search_ret);
            return cur;
        }
        else if (ret > 0)
        {
            *skip_cnt = skip;
            *search_ret = 1;
            printf("%s return %d %d \n", __func__, *skip_cnt, *search_ret);
            return prev;
        }
        prev = cur;
        cur = cur->next_;
        skip += 1;
    }

    // remove all previous nodes !
    *skip_cnt = skip;
    *search_ret = -1; 
    printf("%s return %d %d \n", __func__, *skip_cnt, *search_ret);
    return cur;
}

KVIndexNode * KV_SSD::NextNode(KVIndex * my_index, KVIndexNode * input_node)
{
    if (input_node == nullptr)
    {
        return (KVIndexNode *) (my_index->buffer_ + my_index->meta_->root_offset_);
    }

    // It means last node
    if (input_node->next_offset_ == D_MAX_NEXT_OFFSET)
    {
        return input_node;
    }
    
    KVIndexNode * next_node = (KVIndexNode *) (my_index->buffer_ + input_node->next_offset_);
    return next_node;
}

KVIndexNode * KV_SSD::SearchAndPushNode(KVIndex * my_index, char * key, int64_t * node_offset, size_t node_size)
{
    KVIndexNode * tmp_node = nullptr;
    KVIndexNode * prev_node = nullptr;
    int64_t offset = *node_offset;
    if (offset < 0)
    {
        tmp_node = (KVIndexNode *) (my_index->buffer_ + my_index->meta_->root_offset_);
    }
    else
    {
        tmp_node = (KVIndexNode *) (my_index->buffer_ + offset);
    }
  
    // Empty Node Case ! 
    if (my_index->meta_->used_buffer_ == 0)
    {
        return PushNode(my_index, (KVIndexNode *) nullptr, key, node_size);
    }

    // Search
    int ret = 0;
    while (tmp_node != nullptr)
    {
        ret = memcmp(tmp_node->key_, key, __K_SIZE);
        if (ret == 0) 
        {
            break;
        }
        else if (ret > 0)
        {
            break;
        }
        prev_node = tmp_node;
        tmp_node = NextNode(my_index, tmp_node);
        
        // Empty Nodes ! 
        if (prev_node == tmp_node)
        {
            break;
        }
    }

    // Allocate New KVIndexNode
    KVIndexNode * new_node;
    
    // Overwrite KVIndex already exist
    if (ret == 0)
    {
        new_node = tmp_node;
        return tmp_node;
    }
    // Create New KVIndex
    else
    {
        new_node = PushNode(my_index, tmp_node, key, node_size);
    }

    return new_node; 
}

KVIndexNode * KV_SSD::PushNode(KVIndex * my_index, KVIndexNode * prev_node, char * key, size_t node_size)
{
    // Allocate New KVIndexNode
    KVIndexNode * new_node;
    
    // Create New KVIndex
        // prev_node -> next_node 
        //          | 
        // prev_node -> new_node -> next_node
    new_node = (KVIndexNode *) (my_index->buffer_ + my_index->meta_->used_buffer_); 
    memcpy(new_node->key_, key, 16);
    new_node->node_size_ = node_size;
  

    // Empty Case or I am smallest Key ! need to update root offset
    if (prev_node == nullptr) 
    {
        new_node->next_offset_ = D_MAX_NEXT_OFFSET;
        my_index->meta_->root_offset_ = my_index->meta_->used_buffer_; 
    }
    else
    {
        new_node->next_offset_ = prev_node->next_offset_;
        prev_node->next_offset_ = my_index->meta_->used_buffer_;
    }

    my_index->meta_->key_count_ += 1;
    my_index->meta_->used_buffer_ += sizeof(KVIndexNode);
    printf("Push Node index %p key %s key_count %d \n", my_index, my_index->key_, my_index->meta_->key_count_);
    if (my_index->meta_->used_buffer_ >= 2*1024*1024)
    {
        printf("!PushNode Error Larger Than 2MB Key \n");
        printf("!PushNode Error Larger Than 2MB Key \n");
        while (1) {} 
    }
    
    return new_node; 
}

void KV_SSD::DeleteIndex(KVIndex * start_index, int skip_cnt)
{
    assert(skip_cnt > 0);
    assert(start_index != nullptr);
    int cnt = skip_cnt;
    KVIndex * my_index = start_index;
    KVIndex * prev_index = start_index;
    printf("Start Delte Index..and Print Index\n");
    PrintIndex(my_index, skip_cnt);
    while (my_index != nullptr && cnt != 0)
    {
        printf("Delete Index reverse %d Key %s \n", cnt, my_index->key_);
        free(my_index->entire_buffer_);
        prev_index = my_index;
        my_index = my_index->next_; 
        free(prev_index);
        cnt -= 1;
    }
}


void KV_SSD::DeleteAllIndex(KVIndex * root)
{
    KVIndex * my_index = root;
    while (my_index != nullptr)
    {
        free(my_index->entire_buffer_);
        my_index = my_index->next_; 
    }
}

}  // namespace ROCKSDB_NAMESPACE





