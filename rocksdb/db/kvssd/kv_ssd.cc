//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
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
#include <cassert>
#include <cmath> 
#include <unistd.h>
#include <fstream>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "db/kvssd/kv_ssd.h"
extern int kv_ssd_enable_;
extern bool kvssd_compaction_enable_;
extern int kvssd_algo_value_;
extern uint64_t du_kv_range_;
extern int kvssd_count_;
extern long long int total_rocksdb_size_;
/*
#include "kv_ssd.h"
bool kvssd_compaction_enable_=false;
int kvssd_algo_value_=0;
*/

bool compaction_algo_enable = true;
int threshold_algo_value = 2;

#ifndef KV_CACHE_H
#define KV_CACHE_H
KVCache<std::string, int, Policy::LRU, std::mutex> kv_cache_(16384); 
long long int cache_miss = 0;
long long int cache_miss_lat = 0;
long long int cache_hit = 0;
long long int cache_hit_lat = 0;
std::string change_string(char * key) {
    return std::string(key);
}
int kv_contain(char * key) {
    std::string my_str = change_string(key); 
    if (kv_cache_.contains(my_str)) return kv_cache_[my_str];
    return 0;
} 

void kv_insert(char * key, int value) {
    std::string my_str = change_string(key); 
    kv_cache_.insert(my_str, value); 
}
#endif


namespace ROCKSDB_NAMESPACE {
#define MAX_POP (64)
class KV_SSD;
class KVIndex;
class KVComp;
//#define SUCCESS (0)
//#define FAILED (1)
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
        // status : 2 = submit
        // status : 1 = success
        // status : 0 = Idle
        // status : -1 = error
        int * status = (int *) ioctx->private1;
        if (status == nullptr) {
        } else {
            *status = 1;
        }
        //printf("write success test cnt %d stauts %p ret %d \n", test_cnt++, status, *status);
    }
    else if (ioctx->context == KVS_CMD_RETRIEVE)
    {
        if (ioctx->result != KVS_SUCCESS)
        {
            //fprintf(stdout, "ERROR io_complete: op=%d. result=%d key=%s\n", 
            //      ioctx->context, ioctx->result, (char*)ioctx->key->key);
            int * status = (int *) ioctx->private1;
            if (status == nullptr) {
            } else {
                //if (rand () % 1000 == 0) 
                //    printf("Retrieve Error ! but..proceed\n");
                *status = -1;
            }
        }
        else
        {
            // It means that we never use this buffer ! buffer is locked now ! 
            BUFFER_STAT * buffer_status = (BUFFER_STAT *) ioctx->private2;
            if (buffer_status != nullptr) 
                *buffer_status = BUF_DONE;
            
            int * status = (int *) ioctx->private1;
            
            if (status == nullptr) {
            } else {
                //printf("CMD RETRIEVE %d \n", *status);
                //fflush(stdout);
                *status = 1;
            }
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
}
/*
KV_SSD::KV_SSD(int number) {
    snprintf(dev_path, 32, "/dev/nvme%dn1", number);
    std;:cout << "My dev path "<< dev_path << std::endl;

*/
KV_SSD::KV_SSD(int number) {
    //strcpy(dev_path, "/dev/nvme1n1");
	//strcpy(keyspace_name, "rocksdb_test");
	if (number == 0) {
        printf("Create First KVSSD\n");
        strcpy(dev_path, "/dev/nvme3n1");
        strcpy(keyspace_name, "rocksdb_test");
    } else {
        printf("Create Second KVSSD\n");
        strcpy(dev_path, "/dev/nvme4n1");
        strcpy(keyspace_name, "rocksdb_test");
    }

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
    printf (" KV SSD open is done ! %p \n", this);

    for (int i = 0 ; i < 2 ; i ++) {
        kvstr_cur_.kv_used_[i] = 0;
        kvstr_cur_.rocks_used_[i] = 0;
    }
    kvstr_cur_.border_ = 4096;
    //kvstr_cur_.score_ = 65536; // default
    kvstr_cur_.score_ = (int) 20 * du_kv_range_ / 10000 / 100; // it means 20% of KV Range 
    kvstr_cur_.score_ = 1000; // 100 = 1M 100000 = 100M
    kvstr_cur_.score_ = 0; // 100 = 1M 100000 = 100M

    kvstr_cur_.changed_ = false;
    // 4468 ; 0064 // 44 * 20 = 880 
    kvstr_cur_.threshold_ = 16*1024*1024;//16*1024*1024*1024; 
    kvstr_cur_.last_level_ = 0; 

    kv_ssd_used_ = 0;

    wait_score_ = 0;
    wait_changed_ = false;

    compaction_algo_enable = kvssd_compaction_enable_;
    threshold_algo_value = kvssd_algo_value_;

    for (int i = 0 ; i < 64 ; i ++) {
        kvs_key_p_[i] = (char *) kvs_malloc(64, 4096);
        kvs_value_p_[i] = (char *) kvs_malloc(4096, 4096);
    }
}

KV_SSD::~KV_SSD() 
{
  uint32_t dev_util = 0;
  sleep(10);
  kvs_get_device_utilization(dev, &dev_util);
  fprintf(stdout, "After: Total used is %d\n", dev_util);  
  kvs_close_key_space(ks_hd);
  kvs_key_space_name ks_name;
  ks_name.name_len = strlen(keyspace_name);
  ks_name.name = keyspace_name;
  kvs_delete_key_space(dev, &ks_name);
  kvs_close_device(dev);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
   
    for (int i = 0 ; i < 64 ; i ++) {
        kvs_free(kvs_key_p_[i]);
        kvs_free(kvs_value_p_[i]);
    }
}

void KV_SSD::strPrint() {
    printf("%s Print Region\n", __func__); 
    printf("Border %ld Score %d Threshold %lld \n", kvstr_cur_.border_, kvstr_cur_.score_, kvstr_cur_.threshold_);  

    printf("RocksDB Used %d %d KVSSD Used %d %d\n", 
            kvstr_cur_.rocks_used_[0], kvstr_cur_.rocks_used_[1],
            kvstr_cur_.kv_used_[0], kvstr_cur_.kv_used_[1]);
}

int KV_SSD::env_init() {
  printf("Env Init Start\n");
  fflush(stdout);
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

void KV_SSD::kv_put()
{
    return ; 
}

#include <chrono>
uint64_t micros()
{
    uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::
            now().time_since_epoch()).count();
    return us;
}

void KV_SSD::addRdLatency(uint64_t start, int reader_id) {
#ifdef __LATENCY_PRINT
    if (reader_id < 0) return;
    uint64_t value = micros() - start;
    rd_lat_[reader_id].push_back(value);
#else 
    if (reader_id) {}
    if (start) {}
#endif
}
void KV_SSD::addWrLatency(uint64_t start, int writer_id) {
#ifdef __LATENCY_PRINT
    if (writer_id < 0) return;
    uint64_t value = micros() - start;
    wr_lat_[writer_id].push_back(value);
#else 
    if (writer_id) {}
    if (start) {}
#endif
}

void KV_SSD::printLatency() {
#ifdef __LATENCY_PRINT
    // Accumulate vector
    for (int i = 0 ; i < 64 ; i ++) {
        rd_lat_merge_.insert(rd_lat_merge_.end(), rd_lat_[i].begin(), rd_lat_[i] .end());
    }
    for (int i = 0 ; i < 64 ; i ++) {
        wr_lat_merge_.insert(wr_lat_merge_.end(), wr_lat_[i].begin(), wr_lat_[i] .end());
    }
    
    // Sort latency vector
    sortLatency();

    // Percentile Print
    uint64_t num = (uint64_t) wr_lat_merge_.size();
    uint64_t cnt = 0;
    printf("---------Write latency---------\n");
    if (num > 100)
    {
        cnt = 0.1 * num;
        printf("latency: 10%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.2 * num;
        printf("latency: 20%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.3 * num;
        printf("latency: 30%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.4 * num;
        printf("latency: 40%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.5 * num;
        printf("latency: 50%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.6 * num;
        printf("latency: 60%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.7 * num;
        printf("latency: 70%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.8 * num;
        printf("latency: 80%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.9 * num;
        printf("latency: 90%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.99 * num;
        printf("latency: 99%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.999 * num;
        printf("latency: 99.9%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.9999 * num;
        printf("latency: 99.99%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        cnt = 0.99999 * num;
        printf("latency: 99.999%%th(%lu) = [%lu us]\n", cnt, wr_lat_merge_.at(cnt - 1));
        printf("-------------------------------\n");
    }


    uint64_t num = (uint64_t) rd_lat_merge_.size();
    uint64_t cnt = 0;
    printf("---------read   latency---------\n");
    if (num > 100)
    {
        cnt = 0.1 * num;
        printf("latency: 10%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.2 * num;
        printf("latency: 20%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.3 * num;
        printf("latency: 30%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.4 * num;
        printf("latency: 40%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.5 * num;
        printf("latency: 50%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.6 * num;
        printf("latency: 60%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.7 * num;
        printf("latency: 70%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.8 * num;
        printf("latency: 80%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.9 * num;
        printf("latency: 90%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.99 * num;
        printf("latency: 99%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.999 * num;
        printf("latency: 99.9%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.9999 * num;
        printf("latency: 99.99%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        cnt = 0.99999 * num;
        printf("latency: 99.999%%th(%lu) = [%lu us]\n", cnt, rd_lat_merge_.at(cnt - 1));
        printf("-------------------------------\n");
    }

    cleanLatency();
#endif
}

void KV_SSD::cleanLatency() {
    for (int i = 0 ; i < 64 ; i ++) {
        rd_lat_[i].clear();
        wr_lat_[i].clear();
    }
    rd_lat_merge_.clear();
    wr_lat_merge_.clear();
}   

void KV_SSD::sortLatency() {
#ifdef __LATENCY_PRINT
    sort(rd_lat_merge_.begin(), rd_lat_merge_.end());
    sort(wr_lat_merge_.begin(), wr_lat_merge_.end());
#endif
}



void KV_SSD::kv_put_buffer_sync(const char * key, size_t key_size, const char * buffer, size_t size, int writer_id)
{
#ifdef __LATENCY_PRINT
    uint64_t start_micro = micros();
#else
    if (writer_id) {}
#endif
    //memcpy(kvs_key_p_[writer_id], key, key_size);
    //memcpy(kvs_value_p_[writer_id], buffer, size);

    
	kvs_key kv_key;
	kvs_value kv_value;
	kv_key.key = (char *) key;
	kv_key.length = (uint16_t) key_size;

    kv_value.value = (char *) buffer;
	kv_value.length = (uint32_t) size;
	kv_value.actual_value_size = (uint32_t) 0;
	kv_value.offset = 0;
 
    //printf("%s \t id %d key %s length %d value length %d\n", __func__,
    //        writer_id, key, (int) key_size, (int) size);

    kvs_option_store option = {KVS_STORE_POST, NULL};
	kvs_result ret = kvs_store_kvp(ks_hd, &kv_key, &kv_value, &option);
    if (ret != KVS_SUCCESS) 
	{
		printf("store tuple failed with err 0x%x\n", ret);
        printf("Fuck you Error?\n");
        fflush(stdout);
	    while(1) {}
    }
    else {
        if (rand () % 3000 == 0) {
            //printf("!! rand pass case kv_put_buffer_sync kv_ssd_used %lld\n", kv_ssd_used_);
        }
        kv_ssd_used_ += size;
    }
    /*
    else
    {
		fprintf(stderr, "store tuple %s\n", key);
    }
    */

    // dbg
#if (0) 
    char * dbg_key = (char *) malloc(key_size + 1);
    memcpy(dbg_key, key, key_size); 
    dbg_key[key_size] = '\0';
    printf("%s success ! key %s %ld value_len (%ld) \n", __func__, dbg_key, key_size, (size_t)kv_value.length); 
#endif

#ifdef __LATENCY_PRINT
    addWrLatency(start_micro, writer_id); // Always push to 0  
#endif

}


void KV_SSD::kv_put_buffer_async(const char * key, size_t key_size, const char * buffer, size_t size, volatile int * status)
{
    //char * key_p = key;
	kvs_key kv_key;
	kvs_value kv_value;
	kv_key.key = (char *) key;
	kv_key.length = (uint16_t) key_size;
    if (status != nullptr) *status = 2;

    kv_value.value = (char *) buffer;
	kv_value.length = (uint32_t) size;
	kv_value.actual_value_size = (uint32_t) 0;
	kv_value.offset = 0;
   
    //std::vector<async_complete>::iterator ret_it = allocate_async(key);
    kvs_option_store option = {KVS_STORE_POST, NULL};
    kvs_result ret = kvs_store_kvp_async(ks_hd, &kv_key, &kv_value, &option, (void *) status, nullptr, complete);	
    if (ret != KVS_SUCCESS) 
	{
		fprintf(stderr, "store tuple failed with err 0x%x\n", ret);
        printf("Fuck you Error?\n");
        fflush(stdout);
        //exit(-1);
	} else {
        // TODO maybe need mutex 
        kv_ssd_used_ += size;
    }
}

size_t KV_SSD::kv_get_buffer_sync(const char * key, size_t key_size, char * buffer, size_t buffer_size, int reader_id)
{
#ifdef __LATENCY_PRINT
    uint64_t start_micro = micros();
#else
    if (reader_id) {}
#endif
    
    if (kv_ssd_enable_ == 4) {
        long long int cache_start = (long long int) micros(); 
        if (kv_contain((char *)key) != 0) {
            cache_hit += 1;
            long long int cache_end = (long long int) micros();
            cache_hit_lat += (cache_end - cache_start);
            return 4096;
        } else {
            cache_miss += 1;
            long long int cache_end = (long long int) micros(); 
            cache_miss_lat += (cache_end - cache_start);
        }
    }
    //memcpy(kvs_key_p_[reader_id], key, key_size);
    //memcpy(kvs_value_p_, buffer, buffer_size);
    //printf("test key %p and...%s buffer size %ld \n", kvs_key_p_, key, buffer_size);
    if (buffer && key) {}
    //std::cout << "v " << test_value << "\t" << buffer << std::endl;

    //char * key_p = (char *)key;
	//char * value_p = buffer;
	kvs_option_retrieve option;
    option.kvs_retrieve_delete = false;

	kvs_key kv_key;
	kvs_value kv_value;
	
	//kv_key.key = kvs_key_p_[reader_id];
	kv_key.key = (char *) key;
	kv_key.length = (uint16_t) key_size;
   
	//kv_value.value = kvs_value_p_[reader_id];
	kv_value.value = buffer;
    kv_value.length = buffer_size;
    kv_value.actual_value_size = 0;
    kv_value.offset = 0;

	kvs_result ret = kvs_retrieve_kvp(ks_hd, &kv_key, &option, &kv_value);
    if (ret != KVS_SUCCESS) 
	{
		//printf(" == \t\t == retrieve tuple failed with err 0x%x  %s thread id %d \n", ret, key, reader_id);
        if (rand() % 1000 == 0) printf("Fuck You ERR ret 0x%x key %s key size %ld buffer %p buffer_size %ld \n", ret, key, key_size, buffer, buffer_size);
#if (0)
        fflush(stdout);
        while (1)
		{
        }
#endif
	}
#ifdef __LATENCY_PRINT
    addRdLatency(start_micro, reader_id);
#endif

#if (0)
    printf("%s success ! key %s key_size %ld value_len (%ld => %ld) \n", __func__, key_p, key_size, buffer_size, (size_t)kv_value.length); 
#endif
    if (kv_ssd_enable_ == 4) {
        kv_insert((char *) key, 10);
        if (rand() % 1000 == 0) {
            if ((cache_miss + cache_hit) != 0) { 
                printf("Cache hit %lld miss %lld ratio %f hit lat %lld miss lat %lld \n", cache_hit, cache_miss, (float) cache_hit / (cache_hit + cache_miss), cache_hit_lat, cache_miss_lat);
            }
        }
    }
    return (size_t) kv_value.length;
}

void KV_SSD::kv_get_buffer_async(const char * key, size_t key_size, char * buffer, size_t buffer_size, volatile int * status)
{
    // Key
    kvs_key kv_key;
    kv_key.key = (char *) key;
    kv_key.length = (uint16_t) key_size; // 17 Bytes
    if (status != nullptr) *status = 2;

    // Value
	kvs_value kv_value;
	kv_value.value = (char *)buffer;
    kv_value.length = buffer_size;
    kv_value.actual_value_size = 0;
    kv_value.offset = 0;
    kvs_option_retrieve option = {false};
    kvs_result ret = kvs_retrieve_kvp_async(ks_hd, &kv_key, &option, (void *) status, nullptr, &kv_value, complete);
    //printf("key %s key size %ld buffer size %ld \n", key, key_size, buffer_size);
    if (ret != KVS_SUCCESS) 
    {
        if (rand() % 1000 == 0) 
        {
            fprintf(stderr, "retrieve tuple failed with err 0x%x\n", ret);
            {
                printf("Fuck You ERR\n");
                fflush(stdout);
            }
        }
        *status = -2;
    }
   
    //while (*status != 1) {
    //}
    //printf("It is done here ! kv get buffer async %s %d \n", key, *status );
}

int KV_SSD::kvGetUtilization() {
    uint32_t dev_util = 0;
    kvs_get_device_utilization(dev, &dev_util);
    return dev_util;
}

int KV_SSD::rocksGetTotalFileSize() {
    //system("du -hs /home2/du/work/KV_NVMe/mnt > /home2/du/work/KV_NVMe/1.txt");
    // read File
    int dir_size = 0;
    std::ifstream openFile("/home2/du/work/KV_NVMe/1.txt");
    if( openFile.is_open()){
        std::string line;
        while(getline(openFile, line, 'G')){
            dir_size = stoi(line);
            //std::cout << line << std::endl;
            //printf("%d \n", stoi(line));
            break;
        }
        openFile.close();
    }
    return dir_size; 
}

int KV_SSD::kvUpdateUtilization(uint64_t rocks_used, uint64_t kv_used, uint64_t last_level_size, int last_level) {
    uint32_t dev_util = 0;
    // kvs_get_device_utilization(dev, &dev_util);
    printf ("I need to Check !!!Dev util %d kv used %lld rocks used %lld !!!!! ********************************** \n", dev_util, kv_ssd_used_, (long long int)rocks_used);
    uint32_t kvssd_used = dev_util * 419; // 419 MB =4 * 1024 * 1024 * 1024 * 1024 / 10000;

    kvstr_cur_.kv_used_[0] = kvstr_cur_.kv_used_[1]; 
    kvstr_cur_.rocks_used_[0] = kvstr_cur_.rocks_used_[1]; 

    //kvstr_cur_.kv_used_[0] = (int) dev_util;  
    kvstr_cur_.kv_used_[1] = (int) (kv_used/1024/1024); // kv_ssd_used MB 
    kvstr_cur_.rocks_used_[1] = (int) (rocks_used/1024/1024); // rocksused MB  
    kv_ssd_used_ = 0;

    kv_now_used_ = kvssd_used;
    rocks_now_used_ = rocks_used;
    kvstr_cur_.last_level_ = last_level;
    kvstr_cur_.last_level_size_ = (int) (last_level_size/1024/1024); 

    return (int) dev_util;
}

void KV_SSD::kvstrChange(size_t border, int score) {
    kvstr_cur_.border_ = border;
    kvstr_cur_.score_ = score;
}

// it is called after last level compaction operation
//      => Not time based => becuase high get operation bencharks...it makes system on bad situation
//    => kvstrUpdate is called by put operations...
//    => strategy update does not affect current situdation 
//          => Key pattern problem?  
//          => 
bool KV_SSD::kvstrUpdate() {
#if (1)
    if (rand() % 1 == 0) {
        printf("%s\n", __func__);
        // Gap = 8 GB => 10% 
        // 8GB = 8192
        int total_MB = (kvstr_cur_.kv_used_[1] + kvstr_cur_.rocks_used_[1]);
        int last_level_MB = kvstr_cur_.last_level_size_;
        double range_per_MB = 100000000;
        if (last_level_MB != 0) {
        }
        if (total_MB != 0) {
            range_per_MB = du_kv_range_ / total_MB; 
        }
        int additional_kv = (int) (10000 *kvstr_cur_.score_ / range_per_MB); // (range / (range / MB) = range * MB / range = MB 

        if ((1024 +kvstr_cur_.rocks_used_[1]) < (kvstr_cur_.kv_used_[1] + additional_kv)) {
            if (kvssd_compaction_enable_) {
                kvstr_cur_.score_ = 0; // remove !
                kvstr_cur_.changed_ = false; 
            } else {
                wait_score_ = 0;
                wait_changed_ = false;
                printf("Disable But Now compaction is not enable !!!!!!!!!!!!!! Wait!!!!!!!!\n");
            }
            printf("Change the score %s %d %d Score : %d \n", __func__, kvstr_cur_.rocks_used_[1], kvstr_cur_.kv_used_[1], kvstr_cur_.score_); 

        } else if (kvstr_cur_.rocks_used_[1] > (1024 + additional_kv + kvstr_cur_.kv_used_[1])) {
            int gap = (int) (kvstr_cur_.rocks_used_[1] - kvstr_cur_.kv_used_[1] - additional_kv) / 2;
            int add_range = (int) (gap * range_per_MB / 10000);
            if (kvssd_compaction_enable_ && kvstr_cur_.changed_ != true) {
                kvstr_cur_.score_ += add_range;
                kvstr_cur_.changed_ = true;
            } else {
                wait_score_ = kvstr_cur_.score_ + add_range;
                wait_changed_ = true;
                printf("Change the score But Now compaction is not enable !!!!!!!!!!!!!! Wait!!!!!!!!\n");
            }
            std::cout << __func__ << "\t total MB " <<
                total_MB << "\t last_level_MB " <<
                last_level_MB << "\t range " <<
                du_kv_range_ << "\t rocks used " <<
                kvstr_cur_.rocks_used_[1] << "\t kv used " <<
                kvstr_cur_.kv_used_[1] << "\t additioanl " <<
                additional_kv << "\t gap " <<
                gap << "\t" <<
                range_per_MB << "\t add range " <<
                add_range << "\t Last level " <<
                kvstr_cur_.last_level_ << "\t score " <<
                kvstr_cur_.score_ << "\t" << std::endl;

            printf("Change the score %s %d %d Score : %d \n", __func__, kvstr_cur_.rocks_used_[1], kvstr_cur_.kv_used_[1], kvstr_cur_.score_); 
        } else {
            kvstr_cur_.changed_ = false;
            printf("Nothing in %s %d %d additional KV %d score %d \n", __func__, kvstr_cur_.rocks_used_[1], kvstr_cur_.kv_used_[1], additional_kv,  kvstr_cur_.score_); 
        }
    }
#endif
    return false;
}

bool KV_SSD::sulFlush(std::vector<char *> &input_buf, std::vector<char*> &input_key, int * status, int level) {
    if (input_buf.size() == 0) {}
    if (input_key.size() == 0) {}

    printf("length of buf key %d %d\n", (int) input_key.size(), (int) input_buf.size());
    fflush (stdout);
    int index = 0;
    bool ret = false;
    
    std::vector<char *>::iterator it_buf;
    std::vector<char *>::iterator it_key = input_key.begin();
    for (it_buf = input_buf.begin(); it_buf < input_buf.end() ; ) {
        int meta_size = 2;
        int is_comp = 0;
        if (level != 0) {
            is_comp = 1;
        }
        int key_size = UPPER_CHAR + meta_size + is_comp;
        char key[key_size+1];
        char * buffer = *it_buf;
        char * key_upper = *it_key;
        
        int offset = 0;
        if (is_comp != 0) {
            key[0] = level;
            offset += is_comp;
        }
       
        int sequence_num = 12382;
        key[1] = sequence_num;
        key[2] = sequence_num >> 8;
         
        offset += meta_size;
        memcpy((void *)(key + offset), key_upper, UPPER_CHAR);
        if (status == nullptr) {
            kv_put_buffer_async(key, key_size, buffer, 4096, nullptr);
        } else {
            status[index] = 2;
            kv_put_buffer_async(key, key_size, buffer, 4096, status + index);
        }

        ret = true;
        index ++;
        it_buf++;
        it_key++;
    }
    return ret;
}

bool KV_SSD::sulFlushCheck(volatile int * status, int count) {
    if (status != nullptr) {
        printf("KV_SSD::ul flush count %d\n", count);
        uint64_t ss_micro = micros(); 
        for (int i = 0 ; i < count ; ) {
            uint64_t mid_micro = micros();
            if (*(status + i) == 2) {
                if ((mid_micro - ss_micro) > 1000) {
                    printf("KVSSD::%s Wait time is Too Long value %d\n", __func__, *(status+i));
                    ss_micro = micros();
                }
                continue;
            } else { 
                i ++;
            }
        }
        printf("KV_SSD::I am Done here \n");
        //ul_flush_count_ = 0;
        //free(ul_flush_status_); 
    } else {
        printf("Why...status is nullptr %s\n", __func__); 
    }
    return true;
}




























// KVIndex Start Region

#define BLOCK_SIZE (16*1024)

#define NUM_KEYS (500*1024*1024)

#define SHARD_MOD_BIT (10)
#define SHARD_COUNT (1 << SHARD_MOD_BIT)

KVIndex::KVIndex()
{
    dbg_count_ = 0;

    num_keys_ = NUM_KEYS;
    //num_keys_ = KV_SSD_CAPA / BLOCK_SIZE;
    if (du_kv_range_ >= NUM_KEYS) {
        printf("Too large key value ranges !!!!! %lld %lld %lld \n", 
                (long long int) du_kv_range_,
                (long long int) NUM_KEYS ,
                (long long int) num_hash_entries_);
    }
    
    num_hash_entries_ = num_keys_ / SHARD_COUNT;
    hash_ = (sll *) malloc(sizeof(sll) * num_hash_entries_);
    //printf("Hash entries ! %d\n", num_hash_entries_);
    printf("Hash entries ! %d du_kv_range %lld \n", num_hash_entries_, (long long int)du_kv_range_);

    update_head_ = nullptr;
    update_tail_ = nullptr;

    total_key_count_ = 0;
    total_capacity_ = 0;
    sulCreate(0);

    for (int i = 0 ; i < 20 ; i ++) {
        preManager_[i].write_index_ = 0; 
        preManager_[i].take_ = 0; 
        preManager_[i].hit_ = 0; 
        preManager_[i].miss_ = 0; 
        preManager_[i].skip_ = 0; 
        preManager_[i].data_update_ = 0; 
        preManager_[i].data_fail_ = 0; 
        preManager_[i].data_wait_ = 0; 
        preManager_[i].prefetched_ = 0; 
        for (int j = 0 ; j < PREFETCH_COUNT ; j ++) {
            //preElement_[i*PREFETCH_COUNT+j].value_ = (char *) malloc(16384); 
            preElement_[i*PREFETCH_COUNT+j].status_ = 0;
        }
        iter_list_[i] = nullptr;
    }
}

KVIndex::~KVIndex() 
{
    printf("Hash entries ! %d\n", num_hash_entries_);
    //sllRelease();
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);

    for (int i = 0 ; i < 20 ; i ++) {
        for (int j = 0 ; j < PREFETCH_COUNT ; j ++) {
            //free(preElement_[i*PREFETCH_COUNT+j].value_);
        }
    }
}

void KVIndex::allocateIndex(bool is_l0, int start, int end, int level) {
    if (is_l0) {
        //head_ =  nullptr;
        for (int i = 0 ; i < num_hash_entries_ ; i ++ ) {
            hash_[i].buffer_ = (ll *) malloc(BLOCK_SIZE); 
            memset(hash_[i].buffer_, 0, BLOCK_SIZE);
            uint64_t key_num = i;
            snprintf(hash_[i].key_, sizeof(hash_[i].key_), "%012lu", key_num);
            //snprintf(hash_[i].key_, sizeof(key), "%016lu", key_num);
       
            hash_[i].border_ = 4096;
            hash_[i].buffer_mul_ = 1;
            hash_[i].level_ = 0;
            hash_[i].key_count_ = 0;
            hash_[i].offset_ = 0;
            hash_[i].head_ = 0;
            hash_[i].remain_ = BLOCK_SIZE;
            hash_[i].deleted_key_ = 0;
        }
    } else {
        for (int i = start ; i < end ; i ++) {
            // not allocate yet
            if (hash_[i].buffer_ == nullptr) {
                hash_[i].buffer_ = (ll *) malloc(BLOCK_SIZE); 
                memset(hash_[i].buffer_, 0, BLOCK_SIZE);
            } else {
                continue;
            }
            uint64_t key_num = i;
            snprintf(hash_[i].key_, sizeof(hash_[i].key_), "%012lu", key_num);
            
            hash_[i].buffer_mul_ = 1;
            hash_[i].level_ = level;
            hash_[i].key_count_ = 0;
            hash_[i].offset_ = 0;
            hash_[i].head_ = 0;
            hash_[i].remain_ = BLOCK_SIZE;
            hash_[i].deleted_key_ = 0;
        }
    }
}

// TODO
void KVIndex::allocateSLLBuffer(sll * sll_in) {
    printf("allocateSLL Buffer may be exceed ! buffer_mul %d\n", sll_in->buffer_mul_);
    if (sll_in->deleted_key_ > 1024) {
        gcSLLBuffer(sll_in);
    } else {
        int mul = sll_in->buffer_mul_;
        void * prev_buffer = sll_in->buffer_;
        void * new_buffer = malloc((mul + 1) * BLOCK_SIZE);
        memset(new_buffer, 0, (mul+1)*BLOCK_SIZE); 
        memcpy(new_buffer, prev_buffer, mul * BLOCK_SIZE);
        

        // TODO read or not may be error..
        // After check all of the dependency
        sll_in->remain_ += BLOCK_SIZE;
        sll_in->buffer_ = (ll *) new_buffer;
        sll_in->buffer_mul_ = mul + 1;
        free(prev_buffer);
    }
}

void KVIndex::gcSLLBuffer(sll * sll_in) {
    // Scan full..?  
#if (1)
    printf("%s START !!!!! Deleted key is %d \n", __func__, sll_in->deleted_key_);
    int mul = sll_in->buffer_mul_;
    void * new_buffer = malloc((mul) * BLOCK_SIZE);
    // scan key and push it into new buffer
    llCopy(sll_in, sll_in->buffer_, new_buffer); 
    void * prev_buffer = (void *) sll_in->buffer_;
    sll_in->buffer_ = (ll *) new_buffer;
    sll_in->deleted_key_ = 0;
    free(prev_buffer);
#endif
}

void KVIndex::llCopy(sll * sll_in, ll * ll_in, void * buffer) {
    if (ll_in) {}
    int key_count = 0;
    long offset = 0;
    long head = 0;
    long remain = BLOCK_SIZE * sll_in->buffer_mul_; 

#if (0) 
    // Traverse all of the buffer ! for deleted keys ! 
    ll * ll_tmp = ll_in;
    ll * ll_write = (ll *) buffer;
    ll * ll_prev = nullptr;
    uint64_t traverse = 0;
    while (traverse < remain) {
        if (ll_tmp->enable_) {
                
        } else {
            deleted_keys_.push_back();
        }
    }
#else
    // Traverse only live keys! ignore deleted keys here ! 
    ll * ll_tmp = sllGetHead(sll_in);
    ll * ll_write = (ll *) buffer;
    ll * ll_prev = nullptr;
    while (!llEmpty(ll_tmp)) {
        short short_tmp = ll_tmp->key_num_;
        ll_write->enable_ = true;
        ll_write->key_num_ = short_tmp;
        ll_write->next_ = 0; 
     
        if (ll_prev != nullptr) {
            ll_prev->next_ = 1;
        }             
        ll_prev = ll_write;
        ll_write = (ll_write + 1);
        ll_tmp = llNext(ll_tmp); 
        // Meta
        key_count ++;
        offset += sizeof(ll);
        remain -= sizeof(ll);
    }
#endif
    updateAlgo(sll_in, key_count);

    sll_in->offset_ = offset;
    sll_in->head_ = head;
    sll_in->remain_ = remain;
    sll_in->key_count_ = key_count;
}

void KVIndex::updateAlgo(sll * sll_in, int key_count) {
    if (sll_in) {}
    if (key_count) {}
}

// TODO:: Release erorr... not i do not care this problems..
void KVIndex::sllRelease() {
    // Hash
    for (int i = 0 ; i < num_hash_entries_ ; i ++) {
        printf("hash release %d\n", i);
        fflush(stdout);
        free(hash_[i].buffer_);
    }
    free(hash_);

    /* Linked List
    SLL * cur  = head_;
    while (cur != nullptr) {
        SLL * next = cur->next_;
        LLRelease(cur);
        cur = next;
    }
    */
}

void KVIndex::llRelease(ll * in) {
    ll * cur = in;
    while (llEmpty(cur) != true) {
        ll * next = llNext(cur); 
        free(cur);
        cur = next;
    }
};

void KVIndex::getCapacity(long long * key_count, long long * capacity) {
    *key_count = total_key_count_;
    *capacity = total_capacity_;
}

// keyInsert: keysize 
// WAL + L0: 
// Compaction: 20 B Key size : SUL :
// return current pointer ! 
long KVIndex::keyInsert(char * key, size_t key_size, int seq, size_t value_size, long prev_index, int * ul_value) {
    if (prev_index) {} 
    if (ul_value) {}
    if (key_size == 0) {// TODO
    }
    if (value_size == 0) { // only for capacity calculation
    }
    long ret_index = 0;
    bool dbg = false; 
    int index = (int) CharToShort(key, 0, UPPER_CHAR);
    short key_num = (short) CharToShort(key, UPPER_CHAR, key_size - UPPER_CHAR);
    //short key_num = CharToShort(key, UPPER_CHAR, LOWER_CHAR);
  
    sll * sll_local = &hash_[index];
    
    sll_local->slot_lock_.lock();
    if (false) {
        printf("index %d buffer_size %d level %d key_count %d offset %ld head %ld remain %ld\n",
                index,
                sll_local->buffer_mul_,
                sll_local->level_,
                sll_local->key_count_,
                sll_local->offset_,
                sll_local->head_,
                sll_local->remain_);
    }

    if (sll_local->remain_ < 100) {
        allocateSLLBuffer(sll_local);     
    }
    if (sll_local->remain_ < 32) {
        printf("ERROR Case because of exceed offset %ld \n", sll_local->offset_);
        sll_local->slot_lock_.unlock();
        return 0;
    }
    bool find = false;

    ll * ll_local = (ll *) (sll_local->buffer_ + sll_local->key_count_);
    ll * ll_prev = nullptr;
    if (key_num == 7) {
        printf("QQ\n");
    }
    if (prev_index == 0) {
        ll_prev = llSearch(sllGetHead(sll_local), key_num, &find, false);
    } else {
        ll_prev = llLocate(sll_local, prev_index);
    }

    // Insert to Head 
    short ll_local_offset = llOffset(sll_local, ll_local);
  
    if (sll_local->offset_ == 0) {
        sll_local->head_ = ll_local_offset;
        ll_local->next_ = 0;
    }
    else if (ll_prev == nullptr) { // First 
        ll_local->next_ = sll_local->head_ - ll_local_offset;
        sll_local->head_ = ll_local_offset;
        if (dbg) printf("\t\t ******* Head update here ! key %hd %hd local->next_ %hd \n", key_num, ll_local_offset, ll_local->next_);
    } else if (find == true) {
        // Nothing todo (update case)
    } else if (ll_prev == ll_local) { // Update 
        assert(false);
        if (dbg) printf("\t\t &&&&&& Update Case \n");
        if (ll_prev->next_ == 0) ll_local->next_ = 0;
        else ll_local->next_ = ll_prev->next_ - (long) (ll_local - ll_prev); 
        ll_prev->next_ = ll_local - ll_prev;
        
    } else { // Insert
        if (dbg) printf("\t\t &&&&&& Insert Case \n");
        if (ll_prev->next_ == 0) ll_local->next_ = 0;
        else ll_local->next_ = ll_prev->next_ - (long) (ll_local - ll_prev); 
        ll_prev->next_ = ll_local - ll_prev;
    }

    if (find != true) {
        if (dbg) printf("\t\t Not find Case \n");
        ll_local->enable_ = true;  
        ll_local->key_num_ = key_num;   
       
        sll_local->remain_ = sll_local->remain_ - sizeof(ll);
        sll_local->key_count_ ++;
        sll_local->offset_ = sll_local->offset_ + sizeof(ll);
   
        total_key_count_ ++; // atomic variable
        total_capacity_ += key_size + value_size; // atomic variable
        sll_local->total_capa_ += value_size;
    }
    if (dbg) {
        if (ll_prev == nullptr) 
            printf("Insert done key %d prev is nullptr local (%d : %hd) \n",  key_num, ll_local->key_num_, ll_local->next_ );
        else
            printf("Insert done key %d prev (%d : %hd) local (%d : %hd) \n", key_num, ll_prev->key_num_, ll_prev->next_, ll_local->key_num_, ll_local->next_ );
    }
    ulPush(index, seq);
    sll_local->slot_lock_.unlock();
    ret_index = (long) (ll_local - sllGetHead(sll_local) );
    return ret_index; 
    //return true; 
}


bool KVIndex::keyDelete(char * key, size_t key_size) {
    // Lock
    if (key_size == 0) {// TODO
    }
    bool dbg = false; 
    int index = (int) CharToShort(key, 0, UPPER_CHAR);
    short key_num = (short) CharToShort(key, UPPER_CHAR, key_size - UPPER_CHAR);
 
    if (index >= num_hash_entries_) {
        printf("WTF Error delete key %s index %d entries %d \n", key, index, num_hash_entries_);
        fflush(stdout);
    }

    /*
    if (index % 100 == 0) printf("KeyDelete Index Try is index value is %d\n", index);
    if (index >= 32763) {
        printf("index is not error %d\n", index); 
        fflush(stdout);
    }
    */
    sll * sll_local = &hash_[index];    
    if (sll_local == nullptr) {
        printf("This is reason of err! key %s index %d hash_entry %d\n", key, index, num_hash_entries_);
        fflush(stdout);
    }

    sll_local->slot_lock_.lock();
    if (sll_local->key_count_ == 0) {
        sll_local->slot_lock_.unlock();
        return false;
    }
    bool find = false;
 
    ll * ll_ret[3] = {nullptr};
    ll * ll_head = sllGetHead(sll_local);
    find = llSearchForDelete(ll_head, key_num, ll_ret);
    
    if (dbg)
        printf("Now Delete key %s %hd\n", key, key_num);

    if (find) {
        if (ll_head == ll_ret[0]) {
            sllSetHead(sll_local, ll_ret[2]);
        } else if (ll_ret[0] == nullptr) {
            sllSetHead(sll_local, ll_ret[2]);
        } else { // not...head...
        }

        if (ll_ret[0] == nullptr) {
            printf("WTF? This is real?\n");
        }
        if (ll_ret[1] == nullptr) {
            printf("11 WTF? This is real?\n");
        }
        if (ll_ret[2] == nullptr) {
            printf("22 WTF? This is real?\n");
        }

        ll_ret[1]->enable_ = false;
        ll_ret[1]->key_num_ = 0;
        ll_ret[1]->next_ = 0;
       
        if (sll_local->key_count_ != 0) {
            long long int average = sll_local->total_capa_ / sll_local->key_count_;
            sll_local->total_capa_ -= average;
        }

        if (ll_ret[2] == nullptr) {
            ll_ret[0]->next_ = 0; // means nullptr
        } else {
            ll_ret[0]->next_ = ll_ret[2] - ll_ret[0];
        }
    
        // sll_local->key_count_ --; // Never Reduce key count ! 
        // sll_local->offset_ = sll_local->offset_ - sizeof(ll); // Never reduce offset too
        sll_local->deleted_key_ ++;  
        sll_local->slot_lock_.unlock();
        return true; 
    }
    // Unlock
    sll_local->slot_lock_.unlock();
    return false; 
}

int qwerqwer = 0;

bool KVIndex::checkPrefetch(int thread_id, const char * key, char * value) {
#if (KV_PREFETCH == 1) 
    if (key && value && thread_id) {}
    return false;
#endif
    if (key && value) {} 
    int slot_index = thread_id;
    bool ret = false;
    if (preManager_[slot_index].skip_ > 0) {
        preManager_[slot_index].skip_ --;
        return false;
    }

    // Try to find already prefetched data
    bool data_update = false;
    bool data_wait = false;
    bool data_fail = false;
#if (1)
    int consumed = 0;
    int remain = PREFETCH_COUNT - preManager_[slot_index].prefetched_;  // 4 - 3 (prefetched) = 1  // 4 - 4 = 0;
    int start_index = (preManager_[slot_index].write_index_ + remain) % PREFETCH_COUNT;
    for (int i = start_index ; i < preManager_[slot_index].prefetched_ ; i ++) {
        PreE * search_element = &preElement_[slot_index*PREFETCH_COUNT+(i%PREFETCH_COUNT)];
        int cmp_ret = memcmp(search_element->key_, key, 16);
        // printf("\t\t\t Check Key St %s Add Element index %d key %s write_index %d prefetched %d \n", key, i, search_element->key_, preManager_[slot_index].write_index_, preManager_[slot_index].prefetched_); 
        if (cmp_ret == 0) {
            if (search_element->status_ == 1 || search_element->status_ == 0) {
                data_update = true;
            } else if (search_element->status_ == -1) {
                data_update = true;
                data_fail = true;
            } else if (search_element->status_ == 2) {
                data_update = true;
                data_wait = true;
            }
            ret = true;
            consumed ++;
            break;
            //memcpy(value, search_element->value_, 16384);
            //printf("\t\t Prefetch Hit %s\n", key);
        } else if (cmp_ret > 0) {
            consumed ++;
            break;
        }
        consumed ++;
    }
    preManager_[slot_index].prefetched_ -= consumed;
#else
    for (int i = 0 ; i < PREFETCH_COUNT ; i ++) {
        PreE * search_element = &preElement_[slot_index*PREFETCH_COUNT+i];
        int cmp_ret = memcmp(search_element->key_, key, 16);
        if (cmp_ret == 0) {
            if (search_element->status_ == 1 || search_element->status_ == 0) {
                data_update = true;
            } else if (search_element->status_ == -1) {
                data_fail = true;
            } else if (search_element->status_ == 2) {
                data_wait = true;
            }
            ret = true;
            //memcpy(value, search_element->value_, 16384);
            //printf("\t\t Prefetch Hit %s\n", key);
        }
    }
#endif
    if (preManager_[slot_index].take_ > 0) {
        if (ret) {
            preManager_[slot_index].hit_ ++; 
        } else {
            preManager_[slot_index].miss_ ++; 
        }
        if (data_update) preManager_[slot_index].data_update_ ++;
        if (data_fail) preManager_[slot_index].data_fail_ ++;
        if (data_wait) preManager_[slot_index].data_wait_ ++;

        int hit = preManager_[slot_index].hit_;
        int miss = preManager_[slot_index].miss_;
        //int skip = preManager_[slot_index].skip_;
        double hit_rate = (double)hit / (hit + miss);
        double real_hit_rate = (double)preManager_[slot_index].data_update_ / (hit + miss);
        if (rand() % 1000 == 0) { 
            printf("%s Take %d Miss %d Hit %d Rate %lf skip %d data_update %d real hit %lf \n", __func__, preManager_[slot_index].take_, miss, hit, hit_rate, preManager_[slot_index].skip_, preManager_[slot_index].data_update_, real_hit_rate);
            printf("\t\t [%p:%d] Data Update %d Fail %d Wait %d\n", this, slot_index, preManager_[slot_index].data_update_, preManager_[slot_index].data_fail_, preManager_[slot_index].data_wait_);
        }
        if ((hit+miss) > 10) {
            if (hit_rate < 0.5) {
                preManager_[slot_index].take_ = 0; 
                preManager_[slot_index].hit_ = 0; 
                preManager_[slot_index].miss_ = 0; 
                preManager_[slot_index].skip_ = 2000;
                preManager_[slot_index].data_update_ = 0; 
                preManager_[slot_index].data_fail_ = 0; 
                preManager_[slot_index].data_wait_ = 0; 
                preManager_[slot_index].prefetched_ = 0; 
                printf("Skip Start ! Not prefetch hit %d miss %d hit rate %lf\n", hit, miss, hit_rate);
            }
        }
    }
    return data_update; 
}

PreE * KVIndex::getPrefetchBuffer(int thread_id, bool * prefetch_hit, const char * key, char * value) {
    if (thread_id && prefetch_hit && key && value) {}
   
    int slot_index = thread_id;
    int write_index = preManager_[slot_index].write_index_;
    if (slot_index >= 20) {
        printf("ERROR TOO MUCHTHREAD ID \n");
        return nullptr;
    }

    if (preManager_[slot_index].skip_ > 0) {
        preManager_[slot_index].skip_ --;
        return nullptr;
    }

    if (write_index < 0) {
        printf("qwer why write index error?\n");
    }

    PreE * local_element = &preElement_[slot_index*PREFETCH_COUNT+write_index];
 
    preManager_[slot_index].write_index_ = (write_index + 1) % PREFETCH_COUNT;
    preManager_[slot_index].take_ ++;
    return local_element; 
}

bool KVIndex::keyFind(char * key, size_t key_size, int seq_opt, int thread_id) {
    if (key_size ) {// TODO
    }
    bool dbg = false; 
    int index = (int) CharToShort(key, 0, UPPER_CHAR);
    short key_num = (short) CharToShort(key, UPPER_CHAR, key_size - UPPER_CHAR);

    sll * sll_local = &hash_[index];    
    //sll_local->slot_lock_.lock();
    if (sll_local->key_count_ == 0) {
        //sll_local->slot_lock_.unlock();
        return false;
    }

    ll * ll_ret[3] = {nullptr};
    ll * ll_head = sllGetHead(sll_local);
    bool return_value = llSearchForDelete(ll_head, key_num, ll_ret);
   
    if (seq_opt == 1) {
        if (return_value) {
            int slot_index = thread_id;
            if (ll_ret[2] != nullptr) {
#if (1) 
                int write_index = preManager_[slot_index].write_index_;
                ll * ll_tmp = ll_ret[2];
                int count = 0;
                for (int k = preManager_[slot_index].prefetched_ ; k < PREFETCH_COUNT ; k ++) {
                    PreE * element = &preElement_[slot_index*PREFETCH_COUNT+(write_index+k)%PREFETCH_COUNT];
                    uint64_t upper_key = (uint64_t) index;
                    uint64_t lower_key = (uint64_t) ll_tmp->key_num_;
                    uint64_t my_key = upper_key * 10000 + lower_key;
                    snprintf(element->key_, 17, "%016lu", my_key);
                    if (preManager_[slot_index].take_ > 2000) {
                        preManager_[slot_index].take_ = 0; 
                        preManager_[slot_index].hit_ = 0; 
                        preManager_[slot_index].miss_ = 0; 
                        preManager_[slot_index].skip_ = 0; 
                        preManager_[slot_index].data_update_ = 0; 
                        preManager_[slot_index].data_fail_ = 0; 
                        preManager_[slot_index].data_wait_ = 0; 
                        preManager_[slot_index].prefetched_ = 0; 
                    }
                    count ++;
                    //printf("\t\t find key %s Add Element index %d key %s prefetched %d %d \n", key, k, element->key_, count, preManager_[slot_index].prefetched_); 
                    ll_tmp = llNext(ll_tmp);
                    if (ll_tmp == nullptr) break;
                }
                preManager_[slot_index].prefetched_ += count;
#else
                int write_index = preManager_[slot_index].write_index_;
                PreE * element = &preElement_[slot_index*PREFETCH_COUNT+write_index];
                uint64_t upper_key = (uint64_t) index;
                uint64_t lower_key = (uint64_t) ll_ret[2]->key_num_;
                uint64_t my_key = upper_key * 10000 + lower_key;
                snprintf(element->key_, 17, "%016lu", my_key);
#endif
                if (preManager_[slot_index].take_ > 2000) {
                    preManager_[slot_index].take_ = 0; 
                    preManager_[slot_index].hit_ = 0; 
                    preManager_[slot_index].miss_ = 0; 
                    preManager_[slot_index].skip_ = 0; 
                    preManager_[slot_index].data_update_ = 0; 
                    preManager_[slot_index].data_fail_ = 0; 
                    preManager_[slot_index].data_wait_ = 0; 
                    preManager_[slot_index].prefetched_ = 0; 
                }
            } else {
                preManager_[slot_index].take_ = 0; 
                preManager_[slot_index].hit_ = 0; 
                preManager_[slot_index].miss_ = 0; 
                preManager_[slot_index].skip_ = 0; 
                preManager_[slot_index].data_update_ = 0; 
                preManager_[slot_index].data_fail_ = 0; 
                preManager_[slot_index].data_wait_ = 0; 
                preManager_[slot_index].prefetched_ = 0; 
            }
        }
    }

    if (dbg) {
        if (return_value) {
            if (rand() % 1000 == 0) {
                std::cout << __func__ << "\tKey Find Success\t" <<
                    key << "\t" <<
                    index << "\t" <<
                    key_num << "\t" <<
                    sll_local->key_count_ << "\t" << std::endl;
            }
        }
        else { 
        }
    }

    //sll_local->slot_lock_.unlock();
    return return_value;
}

ll * KVIndex::llLast(ll * ll_in, int count) {
    ll * ll_tmp = ll_in;
    for (int i = 0 ; i < count ; i ++) {    
        printf ("%s enable %d key %d\n", __func__, ll_tmp->enable_, (int) ll_tmp->key_num_);
        ll_tmp = llNext(ll_tmp); 
    }
    return ll_tmp;
}

// Always return small
ll * KVIndex::llSearch(ll * ll_in, short key_num, bool * find, bool get_prev) {
    int dbg = false;
    if (get_prev) dbg = true;
    ll * ll_ret = nullptr;
    ll * ll_tmp = ll_in;
    if (dbg) printf("\t\t key find start %d ll_in %p \n", key_num, ll_in);
    if (key_num == 7) {
        printf("QQ\n");
    }
    while (!llEmpty(ll_tmp)) {
        short short_tmp = ll_tmp->key_num_;
        int ret = llCompare(short_tmp, key_num);
        //if (dbg) printf("\t %hd ret %d \n", short_tmp, ret);
        if (ret > 0) {  // short_tmp < key_num
            break;
        } else if (ret == 0) {  // short_tmp < key_num
            if (ll_tmp->enable_) {
                if (get_prev != true) ll_ret = ll_tmp;
                *find = true;
            } else {
                *find = false;
            }
            break;
        } else {
            if (ll_tmp->enable_) {
                ll_ret = ll_tmp;
            } else {
            }
        }
        
        if (dbg) printf("\t\t\t key %d enable %d next %hd \n", ll_tmp->key_num_, (int) ll_tmp->enable_, ll_tmp->next_);
        ll_tmp = llNext(ll_tmp); 
    }
    // find && ret == nullptr => head
    // ! find && ret == nullptr => empty
    return ll_ret;
}

bool KVIndex::llSearchForDelete(ll * ll_in, short key_num,  ll ** ret_p) {
    int dbg = false;
    ll * ret_prev = ll_in;
    ll * ret_cur = nullptr;
    ll * ret_next = nullptr;
    ll * ll_tmp = ll_in;
    bool find = false;

    if (dbg) printf("\t\t key find start %d ll_in %p \n", key_num, ll_in);
    while (!llEmpty(ll_tmp)) {
        short short_tmp = ll_tmp->key_num_;
        int ret = llCompare(short_tmp, key_num);
        if (ret > 0) {  // short_tmp < key_num
            break;
        } else if (ret == 0) {  // short_tmp < key_num
            if (ll_tmp->enable_) {
                ret_cur = ll_tmp;
                ret_next = llNext(ll_tmp);
                while (ret_next != nullptr && ret_next->enable_ == false) {
                    ret_next = llNext(ll_tmp);
                }
                find = true;
                break;
            } else {
                find = false;
            }
        } else {
            if (ll_tmp->enable_) {
                ret_prev = ll_tmp;
            } else {
                //ret_prev = ll_tmp;
            }
        }
        
        if (dbg) printf("\t\t\t key %d enable %d next %hd \n", ll_tmp->key_num_, (int) ll_tmp->enable_, ll_tmp->next_);
        ll_tmp = llNext(ll_tmp); 
    }
    
    ret_p[0] = ret_prev;
    ret_p[1] = ret_cur;
    ret_p[2] = ret_next;
    return find;
}
ll * KVIndex::sllGetHead(sll * sll_in) {
    ll * ret = (ll *) (sll_in->buffer_ + sll_in->head_); 
    //ll * ret = (ll *) (sll_in->buffer_ + sll_in->head_); 
    return ret;
}

void KVIndex::sllSetHead(sll * sll_in, ll * ll_in) {
    bool dbg = false;
    if (dbg) printf("sll_set head %p %p prev %ld => ", sll_in->buffer_, ll_in, sll_in->head_);
    sll_in->head_ = (long) (ll_in - sll_in->buffer_);
    if (dbg) printf("%ld \n", sll_in->head_);
}

// -1: small, 0: same , 1: large
int KVIndex::llCompare(short key_in, short key_num) {
    if (key_in < key_num) {
        return -1;
    } else if (key_in == key_num) {
        return 0;
    } else {
        return 1;
    }
}

void KVIndex::sulCreate(int seq_start) {  
    //sul_mutex_.lock();
    printf("%s\n", __func__);

    sul * tmp = update_head_;
    //bool added = false;
    if (tmp == nullptr) {
        sul * new_sul = (sul *) malloc(sizeof(sul));
        new_sul->seq_start_ = seq_start;
        new_sul->seq_end_ = seq_start;
        new_sul->buffer_ = nullptr;
        new_sul->next_ = nullptr; 
        update_head_ = new_sul;
        update_tail_ = new_sul;
        //added = true;
        printf("nullcase %p %p \n", update_tail_, update_head_);
    } else {
        while (tmp != nullptr) {
            if (tmp->next_ == nullptr) {
                sul * new_sul = (sul *) malloc(sizeof(sul));
                new_sul->seq_start_ = seq_start;
                new_sul->seq_end_ = seq_start;
                new_sul->buffer_ = nullptr;
                new_sul->next_ = nullptr; 
                tmp->next_ = new_sul;
                update_tail_ = new_sul;
                //added = true;
                printf("\t\t breakcase %p %p \n", update_tail_, update_head_);
                break;
            } else {
                printf("\t\t nullcase %p %p \n", update_tail_, update_head_);
            }
            tmp = tmp->next_;   
        }
    }
    //assert (added == true);
    //sul_mutex_.unlock();
}

void KVIndex::sulDelete(int seq_start, int seq_end) {
    if (seq_start) {}
    if (seq_end) {}
    sul * tmp = update_head_;
    sul * del_sul = tmp;
    while (tmp != nullptr) {
        if (tmp != update_tail_) {
            printf("%s deleteSUL pointer %p seq %d - %d \n", __func__, tmp, tmp->seq_start_, tmp->seq_end_);
            del_sul = tmp;
            tmp = tmp->next_;
            //free(del_sul->buffer_);
            ulClear(del_sul->buffer_);
            free(del_sul); 
            if (tmp == nullptr) {
                update_head_ = update_tail_;
            } else {
                update_head_ = tmp;
            }
        } else {
            break;
        }
    }
}

void KVIndex::sulPrint() {
    int index = 0 ; 
    sul * tmp = update_head_;
    while (tmp != nullptr) {
        printf (" === index %d seq %d -- %d pointer %p \n", index, tmp->seq_start_, tmp->seq_end_, tmp);
        ulPrint(tmp->buffer_);
        index ++;
        tmp = tmp->next_;
    }
}


void KVIndex::allocIter(struct KVIter * iter) {
    for (int i =  0 ; i < MAX_KV_SCAN ; i ++) {
        printf("alloc index %d\n", i);
        iter->iter_element_[i].buffer_ = (char *) malloc(65536);
    }

    printf("alloc doen\n");
}
void KVIndex::freeIter(struct KVIter * iter) {
    for (int i = 0 ; i< MAX_KV_SCAN ; i ++ ) {
        printf("free index %d\n", i);
        if (iter->iter_element_[i].buffer_ != nullptr) {
           free(iter->iter_element_[i].buffer_);
        }
    }
    printf("free doen\n");
}

#if (1)
/***********/ 
/***********/ 
// TEST Region 
/***********/ 
/***********/ 
/***********/ 

// max scan count is 100 
bool KVIndex::makeIter(struct KVIter * iter, char * key, int scan_count, int level, int tid) {
    if (tid) {}
    //printf("Pre Lock %s key %s\n", __func__, key);
    //fflush(stdout);
    // Lock SLL Index
    int index = (int) CharToShort(key, 0, UPPER_CHAR);
    int last_index = (int) CharToShort(key, 0, UPPER_CHAR);
    short key_num = (short) CharToShort(key, UPPER_CHAR, 16 - UPPER_CHAR);
    short last_key_num = key_num + scan_count; //
    if (last_key_num > 10000) {
        /*
        printf("WTF !!!!!!!! DU a error... key %s index %d %d keynum %d %d \n",
                key, index, last_index, (int)key_num, (int)last_key_num);
        last_index = index + 1;
        */
        last_key_num -= 10000;
    } else {
        last_index = index;
    }

    iter->index_ = index;
    iter->key_num_ = key_num;
    iter->last_index_ = last_index;
    iter->last_key_num_ = last_key_num;
    iter->level_ = level;
    iter->wait_thread_ = 0;
    iter->done_ = false;

    for (int i = index ; i <= last_index ; i ++) {
        //printf("%d Slot Lock index %d\n", tid, i);
        //fflush(stdout);
//        hash_[i].slot_lock_.lock();
        //printf("%d Slot Lock Success index %d\n", tid, i);
        //fflush(stdout);
    }
   
    sll * sll_local = &hash_[index];
    ll * ll_ret[3] = {nullptr};
    ll * ll_head = sllGetHead(sll_local);
    bool find = llSearchForDelete(ll_head, key_num, ll_ret);
    if (!find) {
        if (ll_ret[0] == nullptr) {
            printf("It is possible?\n");
            while(1) {}
        }

        ll * ll_tmp = llNext(ll_ret[0]);
        if (ll_tmp == nullptr) {
            if (index > 256 * 1024) {
                printf("I am not sure it is scan bug or not.. too large index?\n");
                fflush(stdout);
            }
            //printf("%d Slot unLock index may be early? %d\n", tid, index);
//            hash_[index].slot_lock_.unlock();
            index ++;
            iter->index_ = index;
            sll_local = &hash_[index];
            iter->last_ = sllGetHead(sll_local);
        } else {
            iter->last_ = ll_tmp;
        }
    } else if (find) {
        iter->last_ = ll_ret[1];
    }

    // There is no more scan key
    if (iter->index_ > iter->last_index_) {
        //printf("Early release %d index %d last %d \n", tid, iter->index_, iter->last_index_);
        releaseIter(iter, tid);
        return false;
    } else {
        //printf("Make iter done !  %d index %d last %d \n", tid, iter->index_, iter->last_index_);
        return true;
    }
}

void KVIndex::nextIter(struct KVIter * iter, char * input_key, size_t * key_size, bool * last) {
    //printf("Start Next %d Last index %d \n", iter->index_, iter->last_index_);
    int last_index = iter->last_index_;
    short last_key_num = iter->last_key_num_;
    
    int index = iter->index_;
    int level = iter->level_;
    
    ll * ll_tmp = iter->last_;
    sll * sll_local = &hash_[index];    
    //printf("next start index %d key_num %d\n", (int) last_index, (int)last_key_num);

    bool last_key = false;
    {
        uint64_t upper_key = (uint64_t) index;
        uint64_t lower_key = (uint64_t) ll_tmp->key_num_;
        uint64_t my_key = upper_key * 10000 + lower_key;
        
        memset(input_key, 0, 17);
        snprintf(input_key, 17, "%016lu", my_key);
   
        if (index == last_index) {
            if ((short) lower_key == last_key_num) {
                *last = true;
                *key_size = 0;
               // printf("next done here ! index %d last_key %d\n", (int)index, (int)lower_key);
                //fflush(stdout);
                return;
            } else if ((short) lower_key > last_key_num) {
                *last = true;
                *key_size = 0;
                //printf("next 11 done here ! index %d last_key %d\n", (int)index, (int)lower_key);
                //fflush(stdout);
                return;
            }
        } else if (index > last_index) {
            *last = true;
            *key_size = 0;
                //printf("next 33 done here ! index %d last_key %d\n", (int)index, (int)lower_key);
                //fflush(stdout);
            return;
        }

        if (level == 0) {
            *key_size = 16;
        } else {
            *key_size = 17;
        }

        ll_tmp = llNext(ll_tmp);
        if (ll_tmp == nullptr) {
            //printf("Last index is not same with indexSlot unLock index %d\n", index);
//            hash_[index].slot_lock_.unlock();
            index += 1;
            sll_local = &hash_[index];   
            ll_tmp = sllGetHead(sll_local);
        }
        iter->last_ = ll_tmp;
        iter->index_ = index;
    }
    *last = last_key;
    //printf("Next Iter Done ! index %d key_num %d \n", (int) last_index, (int)last_key_num); 
}

void KVIndex::releaseIter(struct KVIter * kv_iter, int tid) {
    if (tid) {}
    for (int i = kv_iter->index_ ; i <= kv_iter->last_index_ ; i ++) {
        //printf("%d Slot unLock index %d\n", tid, i);
        //fflush(stdout);
//        hash_[i].slot_lock_.unlock();
        //printf("%d Slot unLock Success %d\n", tid, i);
        //fflush(stdout);
    }
}
#else
// max scan count is 100 
bool KVIndex::makeIter(struct KVIter * iter, char * key, int scan_count, int level, int tid) {
    if (tid) {}
    printf("Pre Lock %s key %s\n", __func__, key);
    fflush(stdout);
    // Lock SLL Index
    
    int index = (int) CharToShort(key, 0, UPPER_CHAR);
    int last_index = (int) CharToShort(key, 0, UPPER_CHAR);
    short key_num = (short) CharToShort(key, UPPER_CHAR, 16 - UPPER_CHAR);
    short last_key_num = key_num + scan_count; //
    if (last_key_num > 10000) {
        last_index = index + 1;
        last_key_num -= 10000;
    } else {
        last_index = index;
    }

    iter->index_ = index;
    iter->key_num_ = key_num;
    iter->last_index_ = last_index;
    iter->last_key_num_ = last_key_num;
    iter->level_ = level;
    iter->wait_thread_ = 0;
    iter->done_ = false;

    iter->element_index_ = 0;
    for (int i =  0 ; i < MAX_KV_SCAN ; i ++) {
        iter->iter_element_[i].buffer_ = (char *) malloc(65536);
        iter->iter_element_[i].status_ = 0;
        if (iter->iter_element_[i].buffer_ == nullptr) {
            printf("Make iter malloc Failed Here !!!!!!!1\n");
            printf("Make iter malloc Failed Here !!!!!!!1\n");
            printf("Make iter malloc Failed Here !!!!!!!1\n");
            printf("Make iter malloc Failed Here !!!!!!!1\n");
            printf("Make iter malloc Failed Here !!!!!!!1\n");
            while (1) {}
        }
        //memset(iter->iter_element_[i].buffer_, 0, 65536);
    }

    bool lock_success = false;
    bool match = false;
    for (int i = index ; i <= last_index ; i ++) {
#if (1) 
        if (hash_[i].slot_lock_.try_lock()) {
            // Lock Success 
            iter_list_[tid] = iter;
            lock_success = true;
        } else {
            // Lock Fail
            if (index == last_index) {
 //            std::unique_lock<std::mutex> lk(iter_mutex_);
               iter_mutex_.lock();
               for (int k = 0 ; k < 16 ; k ++) {
                    if (iter_list_[k] != nullptr) {
                        if (iter_list_[k]->index_ == i) {
                            match = true; 
                            iter_list_[k]->wait_thread_ |= 1 << tid; 
                            printf("Find Match Case here ! index is %d iter index %d wait thread %x \n", iter_list_[k]->index_, k, iter_list_[k]->wait_thread_); 
                            break;
                        }
                    }
                }
                iter_mutex_.unlock();
            }
        }
#else
        hash_[i].slot_lock_.lock();
        lock_success = true;
#endif
    }
   
    if (lock_success == false && match == false) {
        for (int i = index ; i <= last_index ; i ++) {
            hash_[i].slot_lock_.lock();
            iter_list_[tid] = iter;
        }
    } else if (lock_success == false && match == true) {
//        printf("Match Case Done here ! tid %d find_k %d \n", tid, find_k); 
        // I am early Success Here ! wait for  
        return false;
    }   


    sll * sll_local = &hash_[index];
    ll * ll_ret[3] = {nullptr};
    ll * ll_head = sllGetHead(sll_local);
    bool find = llSearchForDelete(ll_head, key_num, ll_ret);
    if (!find) {
        if (ll_ret[0] == nullptr) {
            printf("It is possible?\n");
            while(1) {}
        }

        ll * ll_tmp = llNext(ll_ret[0]);
        if (ll_tmp == nullptr) {
            if (index > 256 * 1024) {
                printf("I am not sure it is scan bug or not.. too large index?\n");
                fflush(stdout);
            }
            //printf("ll_tmp is null..unlock index slot %d key %s key_num %d last_key %d \n", index, key, (int)key_num, (int)last_key_num);
            hash_[index].slot_lock_.unlock();
            //printf("\t Success ll_tmp is null....so index increase and unlock index slot\n");
            index ++;
            iter->index_ = index;
            sll_local = &hash_[index];
            iter->last_ = sllGetHead(sll_local);
        } else {
            iter->last_ = ll_tmp;
        }
    } else if (find) {
        iter->last_ = ll_ret[1];
    }

    // There is no more scan key
    if (iter->index_ > iter->last_index_) {
        //printf("There is no more scan...!!!!!! Too fast release! index %d last %d \n", iter->index_, iter->last_index_);
        releaseIter(iter, tid);
        return false;
    } else {
        return true;
    }
}

void KVIndex::nextIter(struct KVIter * iter) {
    //printf("Start Next %d Last index %d \n", iter->index_, iter->last_index_);
    int last_index = iter->last_index_;
    short last_key_num = iter->last_key_num_;
    
    int index = iter->index_;
    int level = iter->level_;
    int element_index = iter->element_index_;
    
    ll * ll_tmp = iter->last_;
    sll * sll_local = &hash_[index];    

    //char debug_key[MAX_KV_SCAN][17];
    bool last_key = false;
    while (true) {
        uint64_t upper_key = (uint64_t) index;
        uint64_t lower_key = (uint64_t) ll_tmp->key_num_;
        uint64_t my_key = upper_key * 10000 + lower_key;
        char kvkey[17];
        size_t key_size = 0;
        memset(kvkey, 0, 17);
        snprintf(kvkey, 17, "%016lu", my_key);
   
        if (index == last_index) {
            if ((short) lower_key == last_key_num) {
                last_key = true;
            } else if ((short) lower_key > last_key_num) {
                break;
            }
        } else if (index > last_index) {
            break;
        }

        if (level == 0) {
            key_size = 16;
        } else {
            key_size = 17;
        }

        while (iter->iter_element_[element_index].status_ == 2) {}
        
        // DU Debugging Here
        //memcpy(debug_key[element_index], kvkey, 17); 
        //////// DEBUG DONE

        if (key_size) {}
        iter->iter_element_[element_index].status_ = 2;
        kv_ssd_->kv_get_buffer_async(kvkey, key_size, iter->iter_element_[element_index].buffer_, 65536, &iter->iter_element_[element_index].status_);
  
        if (last_key) {
            break; 
        }

        ll_tmp = llNext(ll_tmp);
        if (ll_tmp == nullptr) {
            //printf("Next ll_tmp here ! start\n");
            //fflush(stdout);
            index += 1;
            sll_local = &hash_[index];   
            ll_tmp = sllGetHead(sll_local);
        }
        element_index = (element_index + 1) % MAX_KV_SCAN;
    }
#if (1) 
    for (int i = 0 ; i < MAX_KV_SCAN ; i ++) {
        while (iter->iter_element_[i].status_ == 2) {}
    }
#endif
}

void KVIndex::releaseIter(struct KVIter * kv_iter, int tid) {
    if (tid) {}
    //printf("Now I am on release Wait Thread count %d\n", kv_iter->wait_thread_);
/*
    for (int i = 0 ; i < MAX_KV_SCAN ; i ++) {
        while (kv_iter->iter_element_[i].status_ == 2) {}
    }
*/
    // Release SLL Index
    //printf("%s index %d last %d\n", __func__, kv_iter->index_, kv_iter->last_index_);
    for (int i = kv_iter->index_ ; i <= kv_iter->last_index_ ; i ++) {
        //printf("Try Slot unLock From scan %d\n", i);
        //fflush(stdout);
        hash_[i].slot_lock_.unlock();
        //printf("Success Slot unLock From scan %d\n", i);
    }

    for (int i = 0 ; i < MAX_KV_SCAN ; i ++) {
        if (kv_iter->iter_element_[i].buffer_ != nullptr)
            free(kv_iter->iter_element_[i].buffer_); 
    }
 
    iter_mutex_.lock(); 
    iter_list_[tid] = nullptr; 
    iter_mutex_.unlock(); 
    /*
    std::lock_guard<std::mutex> lk(iter_mutex_);
    iter_list_[tid] = nullptr;
    iter_controller_.notify_all();
    */
}
#endif

void KVIndex::ulPush(int value, int seq) {
    sul_mutex_.lock();
    bool update = false;
    ul * update_list = update_tail_->buffer_;
    // TODO : Always update tail seq 
    update_tail_->seq_end_ = seq; 

    if (update_list == nullptr) {
        printf("WTF??????? why null\n");
    }
    ul * tmp = update_list;
    ul * prev = update_list;
    while (tmp != nullptr) {
        if (tmp->value_ == value) {
            // nothing todo
            update = true;
            break;
        }
        if (tmp->value_ > value) {
            ul * new_ul = (ul *) malloc(sizeof(ul));
            // new_ul < prev == tmp
            if (tmp == prev) { // head
                new_ul->value_ = value;
                new_ul->next_ = tmp;
                update_tail_->buffer_ = new_ul;
                printf("%s update head ! %d %d\n", __func__, value, seq);
            // prev < new_ul < tmp
            } else {
                new_ul->value_ = value;
                new_ul->next_ = tmp;
                prev->next_ = new_ul;
            }
            update = true;
            break;
        }
        prev = tmp;
        tmp = tmp->next_;
    }  
    if (!update) {
        ul * new_ul = (ul *) malloc(sizeof(ul));
        // this ul is empty
        if (update_list == nullptr) {
            new_ul->value_ = value;
            new_ul->next_ = nullptr;
            update_tail_->buffer_ = new_ul;
            printf("%s Allocate new UL head ! %d %d\n", __func__, value, seq);
        // this value is last  
        } else {
            new_ul->value_ = value;
            new_ul->next_ = nullptr;
            prev->next_ = new_ul;
        }
    }
    dbg_count_ ++;
    sul_mutex_.unlock();
}

void KVIndex::ulClear(ul * ul_in) {
    
    ul * tmp = ul_in;
    ul * prev = ul_in;
    while (tmp != nullptr) {
        prev = tmp;
        tmp = tmp->next_;
        free(prev); 
    }
    //update_list_ = nullptr;
}   

void KVIndex::ulPrint(ul * ul_in) {
    ul * tmp = ul_in;
    printf("%s Start\n", __func__);
    while (tmp != nullptr) {
        printf("%d => ", tmp->value_);
        tmp = tmp->next_;
    }
    printf("\n");
}   

void KVIndex::ulPrintAll() {
    sul * tmp_sul = update_tail_;
    ul * tmp_ul = nullptr;
    int index = 0; 
    while (tmp_sul != nullptr) {
        printf("ulPrintAll index %d\n", index);
        tmp_ul = tmp_sul->buffer_;
        while (tmp_ul != nullptr) {
            printf("%d => ", tmp_ul->value_);
            tmp_ul = tmp_ul->next_;
        }
        printf("\n");
        tmp_sul = tmp_sul->next_;
        index++;
    }
}   


void KVIndex::sllPrint(sll * sll_in) {
    if (sll_in == nullptr) {
        printf("%s errors\n", __func__);
        return; 
    }
    ll * ll_local = sllGetHead(sll_in);
    llPrint(ll_local, 1000);
    printf("\n");
}

void KVIndex::sllPrintAll() {
    for (int i = 0 ; i < num_hash_entries_ ; i ++) {
        if (hash_[i].key_count_ != 0) { 
            printf("Print SLL Print index %d\n", i);
            //sllPrint(&hash_[i]);
        }
    }
}

void KVIndex::llPrint(ll * ll_in, int max_count) {
    ll * ll_local = ll_in;
    ll * ll_prev = ll_in;
    int count = 0;
    while (!llEmpty(ll_local)) {    
        if (count % 4 == 0) printf("\n");
        printf("(%4d:%10d:%10hd:%p) ", count++, (int) ll_local->key_num_, ll_local->next_, ll_local);
        fflush(stdout);
        ll_prev = ll_local;
        ll_local = llNext(ll_local);
        if (ll_prev == ll_local) {
            printf("ERrror\n");
            while (1) {}
        }
        if (count >= max_count) break;
    }
    if (count != 0) printf("\n");
}

void KVIndex::llPrintMeta(ll * ll_in) {
    std::cout << "Print LL Meta " << std::endl;
    std::cout << "\t enable " << ll_in->enable_ << std::endl; 
    std::cout << "\t key num " << ll_in->key_num_ << std::endl; 
    std::cout << "\t next " << ll_in->next_ << std::endl; 
}
void KVIndex::sllPrintMeta(sll * sll_in) {
    std::cout << "Print SLL Meta " << std::endl;
    std::cout << "\t Upper " << sll_in->key_ << std::endl; 
    std::cout << "\t Buffer " << sll_in->buffer_ << std::endl; 
    //std::cout << "\t next " << sll_in->next_ << std::endl;
    std::cout << "\t offset " << sll_in->offset_ << std::endl; 
    std::cout << "\t head " << sll_in->head_ << std::endl; 
    std::cout << "\t remain " << sll_in->remain_ << std::endl; 
}








// KVIndex Region But...only exist in here (kvssd.cc) 

// sul :: Seq [ start - end ] -> point ul
// ul  :: hash index -> hash index -> hash index ... (updated hash list) 

// sll :: Upper Key -> point ll ( sll[0], sll[1] hash )
// ll  :: key -> key -> key ... ( lower key )
void KVIndex::sulFlush(int seq_start, int seq_end, std::vector<char *> &input_buf, std::vector<char *> &input_key) {
    sul_mutex_.lock();
    sul * sul_tmp = update_head_;
    if (sul_tmp == nullptr) {
        printf("%s is empty ! seq %d : %d \n", __func__, seq_start, seq_end); 
    }

    std::vector<int> tmp_vec; 

    tmp_vec.clear();
    ul * ul_tmp = nullptr;
    while (sul_tmp != nullptr) {
        /* TODO List */
        // Now always only have 1 sul..so select update_head_ !!!! do not consider seq number
#if (0) 
        int sul_seq_start = sul_tmp->seq_start_;
        if (sul_seq_start == 0) {
            // nothign todo...TODO List
        }
        int sul_seq_end = sul_tmp->seq_end_;
        // This sll will be flushed
        if (sul_seq_end < seq_end) {
#else
        if (true) {
#endif
            ul_tmp = sul_tmp->buffer_; 
            while (ul_tmp != nullptr) {
                tmp_vec.push_back(ul_tmp->value_);
                ul_tmp = ul_tmp->next_; 
            }
        } else {
            break;
        }
        sul_tmp = sul_tmp->next_;
    }
 
    //std::cout << "Previous sort and erase " << tmp_vec << std::endl;
    sort(tmp_vec.begin(), tmp_vec.end());
    tmp_vec.erase(unique(tmp_vec.begin(), tmp_vec.end()), tmp_vec.end());
  
    std::vector<int>::iterator it;
    for (it = tmp_vec.begin() ; it < tmp_vec.end() ; it ++) {
        int index = *it;
        input_buf.push_back((char *)hash_[index].buffer_);
        input_key.push_back((char *)hash_[index].key_);
    }

    printf("ulPush Count %d vector size %d\n", dbg_count_, (int)tmp_vec.size());

    sulCreate(seq_start);
    sul_mutex_.unlock();
    //dbg_count_ = 0;
    //std::cout << "After sort and erase " << tmp_vec << std::endl;
    //std::cout << input_key << std::end;
    /*
    ul_flush_count_ = (int) input.size(); 
    ul_flush_status_ = (int *) malloc(sizeof(int) * ul_flush_count_);
    memset(ul_flush_status_, 2, ul_flush_count_);
    for (int i = 0 ; i < ul_flush_count_ ; i ++) {
        int status_val = *(ul_flush_status_ + i);
        if (status_val != 2) {
            printf("ERror Case !!!!!!!!!!!!!!!!! %s \n", __func__, status_val);        
        }
        int index_val = ul_flush_[i];

        // key: {[2B][Upper_Char]} // index + ?
        int key_size = UPPER_CHAR + 2;
        char key[UPPER_CHAR + 2];
        char * buffer = (char *) hash_[index_val].buffer_;
        // key set
        // buffer set
        // buffer size
        snprintf(key, UPPER_CHAR + 2, "%02lu%012lu", 0, ul_flush_[i]);
        kv_put_buffer_async(key, key_size, buffer, 4096, ul_flush_status_);
    }
    */
}


/*
KVCompaction::KVCompaction(KV_SSD * kv_ssd) {
    kv_ssd_ = kv_ssd;
    file_size_ = 0;
    file_count_ = 0;
    file_list_ = nullptr;

}

KVCompaction::~KVCompaction() {
    kv_ssd_ = nullptr;
    file_size_ = 0;
    file_count_ = 0;
}

Now only consider about last level is compaction!!!!!!!!!! (only write)
char * KVCompaction::compactionNext() {
    size_t * data_index;
    memcpy(data_index, rd_buffer_ + rd_index_, sizeof(size_t));

    if (rd_index_ >= 8192) {
        compactionRead()
    }

    rd_index_ += 32;
    return (char *) (rd_buffer_ + *data_index);
}

// smallest, larest
bool KVCompaction::compactionRead(char * smallest, char * largest, size_t key_size) {
    // scan file list
    std::vector<fl>::iterator it = file_list_.begin();
    for ( ; it < file_liest_.end() ; ) {
        if () {
        } else {
        }
    }
}

bool KVCompaction::compactionWrite(char * key, size_t key_size, char * value, size_t value_size, long seq, int file_num) {
    short key_num = CharToShort(key, UPPER_CHAR, LOWER_CHAR);
    int level = 0 << 32; // upper 4B
    int index = 0; // lower 4B
    int meta = level | index; 

    // buffer : [Meta] [[Key] [Value] [Key] [Value]] 
    if ((wr_index_ + key_size + value_size)  >= COMP_MAX || (meta_index_ + 2 * sizeof(size_t)) >= META_MAX) {
        // 4B : 4B : key_num  => 20Bytes
        char local_key[20];
        snprintf(local_key, "%08lu", meta);
        memcpy(local_key+8, key_num, UPPER_CHAR); 
        
        // Flush to KVSSD // TODO finally I will use async
        kv_ssd_->kv_put_buffer_sync(local_key, 20, buffer_, COMP_MAX);
        memset(buffer_, 0, COMP_MAX);
        meta_index_ = 0;
        wr_index_ = META_MAX;
    }

    // Copy to buffer
    if ( ) {
        memcpy(bufferd_ + meta_index_, &key_size, sizeof(size_t));
        memcpy(bufferd_ + meta_index_ + sizeof(size_t), &value_size, sizeof(size_t));

        memcpy(buffered_ + wr_index_, key, key_size);
        memcpy(buffered_ + wr_index_ + key_size, value, value_size);

        meta_index_ += (2*sizeof(size_t));
        wr_index_ += key_size + value_size;
    }
}



bool KVCompaction::snapshotRead() {
    // Scan File List & Collect file list
    // 
}

*/ 


KVComp::KVComp(KV_SSD * kv_ssd) {
    comp_ssd_ = kv_ssd;
    comp_index_ = new KVIndex();
    smallest_ = 0;
    largest_ = 0;

    max_compaction_ = 64;
    enable_ = false;

    if (kv_comp_opt_ == 2) {
        std::thread(&KVComp::CompDequeue, this);
        wait_list_ = new rigtorp::MPMCQueue<compE>(4096);
        bg_running_ = true;
    } else {
        bg_running_ = false;
    }
}

KVComp::~KVComp() {
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    comp_ssd_ = nullptr;
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    printf (" ********************* %s *************** \n", __func__);
    //del comp_index_
    bg_running_ = false;
}

KVIndex * KVComp::getKVIndex() {
    return comp_index_;
}

void KVComp::setRange(char * smallest, char * largest, size_t key_size) {
    if (key_size) {}
    smallest_ = (int) CharToShort(smallest, 0, UPPER_CHAR);
    largest_ = (int) CharToShort(largest, 0, UPPER_CHAR);
    smallest_ = 0; // now .... smallest is always 0 
}

bool KVComp::checkRange(char * smallest, char * largest, size_t key_size) {
    //printf("check range test enable %d\n", (int)enable_);
    //if (enable_ != true) {return false;} 
    /*
    printf("Start Check Range Here !\n");
    fflush(stdout);
    std::cout << smallest << " \t\t " << largest << std::endl;
    */
    if (key_size) {}
    int small_index = (int) CharToShort(smallest, 0, UPPER_CHAR);
    int large_index = (int) CharToShort(largest, 0, UPPER_CHAR);

    //printf("%s smallest check %d : %d largest check %d : %d\n", __func__, small_index, smallest_, large_index, largest_);

    // < 0 
    // < 641
    
    if (largest_ < small_index) return false;
    //if (large_index < smallest_) return false; 
    //if (small_index < largest_) return false; 
    std::cout << __func__ << "\t Success " <<
        smallest_ << "\t=\t" <<
        largest_ << "\t || " <<
        small_index << "\t=\t" <<
        large_index << std::endl;
        
    //printf ("DU Say Check Range Return True!!!!!!!! \n"); 
    return true;
}

void KVComp::expandRange(char * smallest, char * largest) {
    int smallest_in = (int) CharToShort(smallest, 0, UPPER_CHAR);
    int largest_in = (int) CharToShort(largest, 0, UPPER_CHAR);
    printf("%s Prev smallest %d laresgt %d\t", __func__, smallest_, largest_);
    
    if (smallest_in < smallest_) {
        smallest_ = smallest_in;
    }
    smallest_ = 0; // Always smallest is 0 
    if (largest_in > largest_) {
        largest_ = largest_in;
    }
    printf("Next smallest %d laresgt %d\t", smallest_, largest_);

}

long KVComp::keyInsert(int tail, char * key, size_t key_size, char * value, size_t value_size, int sequence, long start_index) {

#if (1)
    if (start_index) {}
    if (tail) {}
    int meta_size = 1;
    //int level = compact_stat_[tail].level_;
    int my_key_size = key_size + meta_size; 
   
    // level + key
    char my_key[18];
    long ret_index = 0; 
    
    memcpy((void *)(my_key), key, key_size);
    ret_index = comp_index_->keyInsert(my_key, my_key_size, sequence, value_size, start_index); 
    return ret_index; 

#endif

    if (key) {}
    if (key_size) {}
    if (value) {}
    if (value_size) {}
    if (sequence) {}

    return true;
}

int KVComp::compPrepare(int level, char * smallest, char * largest, size_t key_size) {
    int ret_tail = tail_;
#if (1)
    comp_mutex_.lock();
    std::cout << __func__ << "\t" <<
        level << "\t" <<
        smallest << "\t" <<
        largest << std::endl;
    //allocateIndexP(false, smallest, largest, level);
    tail_ = (tail_ + 1) % 64;
    
    compact_stat_[ret_tail].progress_ = true;
    compact_stat_[ret_tail].enqueue_count_ = 0;
    compact_stat_[ret_tail].dequeue_count_ = 0;
    compact_stat_[ret_tail].status_ = 0;

    comp_mutex_.unlock();

#endif
    if (smallest) {}
    if (largest) {}
    if (key_size) {}
    return ret_tail;
}

bool KVComp::compDone(int tail, size_t key_size) {
#if (1)
    comp_mutex_.lock();
    printf("Fucking CompactionDone Here tail %d\n", tail); 
    fflush(stdout);
    if (key_size) {}
    if (compact_stat_[tail].progress_) {
        compact_stat_[tail].progress_ = false;
#if (1)
        if (kv_comp_opt_ == 2) {
            while (compact_stat_[tail].enqueue_count_ != compact_stat_[tail].dequeue_count_) {
            }
        }
#endif 
    } else {
        printf("why compaction does not prepared...but done\n");
        fflush(stdout);
        while (1) {
            for (int i = 0 ; i < 1000 ; i ++) 
                printf(" I am Hanging\n");
            break; 
        }
    }
    std::cout << __func__ << "\t" <<
        tail << "\t" <<
        compact_stat_[tail].enqueue_count_ << "\t" <<
        compact_stat_[tail].dequeue_count_ << "\t" <<
        compact_stat_[tail].status_ << "\t" <<
        compact_stat_[tail].progress_ << std::endl;
    comp_mutex_.unlock();
    comp_index_->ulPrintAll(); 
    //index_update_test();
#endif
    return true;
}

void KVComp::ulPrint() {
    comp_index_->ulPrintAll();
}

void KVComp::sulFlush(int seq_start, int seq_end, int level) {
    if (seq_start && seq_end && level) {}
    printf("KVComp:::sulFlush START HERE !!!!!!!!!!!!!!! level %d \n", level);
    fflush(stdout);
    uint64_t start_micro = micros();  
    
    volatile int * status = nullptr;
    int count = 0 ;
    std::vector<char*> my_buffer; //= kv_ssd_->get_flush_buffer();
    std::vector<char*> my_key; // = kv_ssd_->get_flush_key();
    comp_index_->sulFlush(seq_start, seq_end, my_buffer, my_key);
    printf("KVComp:::sulFlush !!!!!!!count %d\n", (int) my_buffer.size());
    fflush(stdout);
    
    count = (int) my_buffer.size();
    status = (volatile int *) malloc(sizeof(int) * count);
    uint64_t ss_micro = micros(); 
    comp_ssd_->sulFlush(my_buffer, my_key, (int *) nullptr, level);
    while (1) {
        //bool ret = comp_ssd_->sulFlushCheck(status, count);
        bool ret = true;
        uint64_t mid_micro = micros();
        if ((mid_micro - ss_micro) > 1000) {
            printf("KVComp::sul Flush Wait time is Too Long value %d\n", (int)ret);
            ss_micro = micros();
        }
        if (ret) {
            free((void *) status);
            break;
        }
    }
    comp_index_->sulDelete(0,1);
    uint64_t end_micro = micros();  
    printf("KVComp::Compaction Sul Flush is Done in %lld\n", (long long int) (end_micro - start_micro));
}

// not range set..
// Always select smallest SST File from last level - 1 Compaction !!!!!!!! AlWays...do not care score!!!!!
bool KVComp::isRangeSet() { 
    if (largest_ == 0) {
        return false;
    }
    return true;
} 

void KVComp::allocateIndex(bool is_l0, int start, int end, int level) {
    comp_index_->allocateIndex(is_l0, start, end, level);
}

void KVComp::allocateIndexP(bool is_l0, char * smallest, char * largest, int level) {
    int smallest_in = (int) CharToShort(smallest, 0, UPPER_CHAR);
    int largest_in = (int) CharToShort(largest, 0, UPPER_CHAR);
    std::cout << __func__ << "\t" <<
        smallest << "\t" <<
        largest << "\t" <<
        smallest_in << "\t" <<
        largest_in << "\t" <<
        level << "\n" << std::endl;
    allocateIndex(is_l0, smallest_in, largest_in, level);
}

#if (1)
void KVComp::CompEnqueue(int index, char * key, char * value, size_t value_size) {
    compact_stat_[index].enqueue_count_ ++;

    compE item;
    item.index_ = index;
    item.value_ = (char *) malloc(value_size);
    memcpy((void *) item.value_, value, value_size);
    memcpy((void *) item.key_, key, 17);
    item.value_size_ = value_size;
    item.status_ = 0;
    wait_list_->push(item);
}

void KVComp::CompDequeue() {
    int tail = 0;
    int head = 0;
    compE item[MAX_POP];
    while (bg_running_) { 
        if ((tail + 1) % MAX_POP != head) {
            bool found = wait_list_->try_pop(item[tail]);
            if (found) {
         
                item[tail].status_ = 2;
                kv_ssd_->kv_put_buffer_async(item[tail].key_, 17, item[tail].value_, item[tail].value_size_, &item[tail].status_);
              

                int index = item[head].index_; 
                int compare_value = item[head].compare_value_;
                int prev_value = compact_stat_[index].compare_value_;
                long prev_index = compact_stat_[index].prev_index_;
                if (compare_value != prev_value) {
                    prev_index = 0;
                }
                // value is usused...= nullptr
                prev_index = keyInsert(index, item[tail].key_, 16, nullptr, item[tail].value_size_, 0, prev_index); // sequence set to  0
                compact_stat_[index].prev_index_ = prev_index;
                tail = (tail + 1) % MAX_POP;
            } else {

            }
        } else {
            // Queue is full..wtf...
        }

        while (item[head].status_ == 1) {
            item[head].status_ = 0;
            free (item[head].value_);
            head = (head + 1) % MAX_POP;
            
            int index = item[head].index_;
            compact_stat_[index].dequeue_count_ ++;
        }
    }
}
#endif


















#if (1)
#if (KV_MIGRATION == 1)
/*
void WorkerQueue::InsertChange(int id, int device, char * key, char * value, size_t value_size) {
     if (device == 0) {
        struct ChangeElement new_element;
        char * value_p = (char *) malloc(value_size);
        memcpy(new_element.key_, key, 17);
        new_element.value = value_p;
        memcpy(value_p, value, value_size);
        rocks_change_.push_back(new_element); 
     } else {
        struct ChangeElement new_element;
        char * value_p = (char *) malloc(value_size);
        memcpy(new_element.key_, key, 17);
        new_element.value = value_p;
        memcpy(value_p, value, value_size);
        kv_change_.push_back(new_element); 
     }
}

void WorkerQueue::PopChange(int id, int device, char * key, char * value, size_t value_size) {
    if (kv_change_.size() != 0 && rocks_change_.size() != 0) {
        // Pop kv_change 
         
    }
    if (device == 0) {
        struct ChangeElement new_element;
        char * value_p = (char *) malloc(value_size);
        memcpy(new_element.key_, key, 17);
        new_element.value = value_p;
        memcpy(value_p, value, value_size);
        rocks_change_.push_back(new_element); 
     } else {
        struct ChangeElement new_element;
        char * value_p = (char *) malloc(value_size);
        memcpy(new_element.key_, key, 17);
        new_element.value = value_p;
        memcpy(value_p, value, value_size);
        kv_change_.push_back(new_element); 
     }
}
*/

bool WorkerQueue::MigrationOrNot(int direction, float kv_avg, float rocks_avg, float kv_qps, float rocks_qps) {
    if (direction == 1) { // Rocks -> KV
        if (kv_avg == 0 || kv_qps == 0) {
            printf("%s zero division %f %f\n", __func__, kv_avg, kv_qps); 
            return false;
        }
        float latency_ratio = rocks_avg / kv_avg;     
        float qps_ratio = rocks_qps / kv_qps;
        if (latency_ratio >= 1.5) {
            if (qps_ratio >= 1.5) {
                return true;  
            }
        }
    } else if (direction == 2) { // KV -> Rocks ... why?
        // Nothing TODO 
        if (rocks_avg == 0 || rocks_qps == 0) {
            printf("%s zero division %f %f\n", __func__, kv_avg, kv_qps); 
            return false;
        }
        float latency_ratio = kv_avg / rocks_avg;     
        float qps_ratio = kv_qps / rocks_qps;
        if (latency_ratio >= 1.5) {
            if (qps_ratio >= 1.5) { // Slow device processing more requests 
                return true;  
            }
        }
    } else {
        return false;
    }
    return false;
}

bool WorkerQueue::CheckUpdate() {
/*
    long long int gap = 0;
    if (kv_change_size_ > rocks_change_size_) {
        gap = kv_change_size_ - rocks_change_size_;
    } else {
        gap = rocks_change_size_ - kv_change_size_;
    }
    
    if (gap > (long long int) 8192*1024*1024*1024) {
        return false;
    }
*/
    return true;
}

void WorkerQueue::Migration(int direction, size_t value_size) {
    migration_lock_.lock();
    if (direction == 0) {
       // Rocks -> KV
       kv_change_size_ += (long long int)value_size; 
    } else {
       rocks_change_size_ += (long long int)value_size; 
    }
    migration_lock_.unlock();
}

void WorkerQueue::GetLRUKV () {
}

#endif

WorkerQueue::WorkerQueue() {
    // 4K * 4K = 16MB
    wait_list_ = new rigtorp::MPMCQueue<workE>(4096);
    min_seq_num_ = 0;
    max_seq_num_ = 0;

    bg_running_ = true;
    deq_thread_ = std::thread(&WorkerQueue::kvDequeue, this, 10);

    analysis_wait_ = false;
    value_size_anal_.clear();
    wait_period_ = micros();

    kv_comp_size_ = 65536;
    kv_dump_size_ = 4096;

    accumulate_value_size_ = 0;
    kvssd_size_ = 0 ; 
    kv_write_size_ = 0;
    rocks_write_size_ = 0;
    kv_change_size_ = 0;
    rocks_change_size_ = 0;
    migration_stop_ = false;
}

WorkerQueue::~WorkerQueue() {
    bg_running_ = false; 
}

bool WorkerQueue::kvEnqueue (char * key, size_t key_size, int seq, char * value, size_t value_size, uint8_t direction) {
    if (value) {}
    if (key) {}
    if (key_size) {}
    if (value_size) {}
    if (seq) {} 
    //if (value_size >= 4096) {
        workE item;
        item.value_ = (char *) malloc(value_size);
        memcpy((void *) item.value_, value, value_size);
        memcpy((void *) item.key_, key, 16);
        item.key_size_ = key_size;
        item.value_size_ = value_size;
        item.seq_ = seq;
        item.status_ = 0;
        item.direction_ = direction;
        wait_list_->push(item);
    //} 
    return true;
}

void WorkerQueue::kvDequeue (int count) {
    if (count) {}
    int tail = 0;
    int head = 0;
    workE item[MAX_POP];
    while (bg_running_) { 
        if ((tail + 1) % MAX_POP != head) {
            bool found = wait_list_->try_pop(item[tail]);
            if (found) {
                //////////////////////////////////
                //////////////////////////////////
                //////////////////////////////////

                if (analysis_wait_) {
                    uint64_t end_micro = micros(); 
                    uint64_t gap = end_micro - wait_period_;
                    if (gap > 10000000) { // it means 10 seconds 
                        analysis_wait_ = false; 
                    }
                } else if (value_size_anal_.size() > 2000) {
                    std::sort(value_size_anal_.begin(), value_size_anal_.end());
                    printf("Analysis Value Size Distribution\n");
                    std::vector<size_t>::iterator it = value_size_anal_.begin();
                  
                    size_t value_threshold[2] = {0};
                    uint64_t acc_threshold[2];
                    //std::cout << "QQQQQQQQ \t\t" << accumulate_value_size_ << "\t" << kv_ssd_threshold_ratio_ << std::endl;
                    acc_threshold[0] = accumulate_value_size_ / (kv_ssd_threshold_ratio_ + 1); 
                    acc_threshold[1] = acc_threshold[0] * kv_ssd_threshold_ratio_; 
                    //acc_threshold[0] = accumulate_value_size_ / 3; 
                    //acc_threshold[1] = acc_threshold[0] * 2; 
                    uint64_t test_accumulate = 0;
                    uint64_t accumulate = 0;
                    uint64_t medium_accumulate = 0;
                    uint64_t large_accumulate = 0;
                    for (; it < value_size_anal_.end() ; it ++) {
                        test_accumulate += *it;
                        if (*it < 1024) {
                            accumulate += *it;
                        } else if (*it < 4096) {
                            medium_accumulate += *it;
                        } else {
                            large_accumulate += *it;
                        }
                        if (value_threshold[0] == 0 && test_accumulate > acc_threshold[0]) {
                            value_threshold[0] = *it;
                        } 
                        if (value_threshold[1] == 0 && test_accumulate > acc_threshold[1]) {
                            value_threshold[1] = *it;
                        }
                    }
                    std::cout << " ****** " << "\t" <<
                        " count " << value_size_anal_.size() << 
                        " small " << accumulate << 
                        " medium " << medium_accumulate << 
                        " large " << large_accumulate << 
                        " acc_thres[0] " << acc_threshold[0] << 
                        " acc_thres[1] " << acc_threshold[1] << 
                        " value_thres[0] " << value_threshold[0] << 
                        " value_thres[1] " << value_threshold[1] << 
                        std::endl;
                    
                    // TODO Update How Categorisze...
                    int result_index = 0;
                    if (value_threshold[0] >= 4096) { // | 4K | 4K 
                        kv_comp_size_ = 65536;
                        kv_dump_size_ = value_threshold[0];
                        result_index = 1;
                        
                        // RocksDB: 1
                        // KVSSD  : 2
                        // Large KV Case => Positive 
                        rocks_write_size_ = total_rocksdb_size_ / 3;
                        kv_write_size_ = 2 * total_rocksdb_size_ / 3;
                        //remain_gap_size_ = total_rocksdb_size_ / 3;
                    } else if (value_threshold[1] >= 4096) { // | | 4K
                        kv_comp_size_ = 65536;
                        kv_dump_size_ = 4096;
                        result_index = 2;
                        
                        // RocksDB: 1
                        // KVSSD  : 2
                        /*
                        test_accumulate
                        " small " << accumulate << 
                        " medium " << medium_accumulate << 
                        " large " << large_accumulate << 
                        */
                        long long int kv__ = large_accumulate;
                        long long int rocks__ = accumulate + medium_accumulate;
                        float ratio__ = (float) total_rocksdb_size_ / test_accumulate;
                        rocks_write_size_ = rocks__ * ratio__;
                        kv_write_size_ = kv__ * ratio__;
                        //remain_gap_size_ = (long long int) ((kv__ - rocks__) * ratio__);
                    } else if (value_threshold[1] > 1024) {
                        kv_comp_size_ = 65536;
                        kv_dump_size_ = value_threshold[1];
                        result_index = 5;
                        // RocksDB: 2
                        // KVSSD  : 1
                        // Large KV Case => Positive 
                        // remain_gap_size_ = -1 * (total_rocksdb_size_ / 3);
                        rocks_write_size_ = 2 * total_rocksdb_size_ / 3;
                        kv_write_size_ = total_rocksdb_size_ / 3;
                    } else if (value_threshold[1] < 4096) { // 4K | 4K | 
                        // 4096 write to KV SSD
                        // Compaction Enable and value_threshold[1]
                        int64_t remain = (int64_t) acc_threshold[0]; 
                        int64_t prev_remain = 0; 
                        
                        size_t prev_value = 65536; 
                        uint64_t sum_of_value = 0;
                        for (it = value_size_anal_.end() -1 ; it >= value_size_anal_.begin() ; it --) {
                            remain -= *it;
                            if (*it == prev_value) {
                                sum_of_value += (uint64_t) *it;
                            } else {
                                /*
                                printf("prev %lld remain %lld next prev %lld sum_of_value %lld\n",
                                        (long long int) prev_value,
                                        (long long int) remain,
                                        (long long int) *it,
                                        (long long int) sum_of_value);
                                */
                                prev_value = *it;
                                if (remain < 0) {
                                    break;
                                }
                                kv_comp_size_ = *it;
                                sum_of_value = 0;
                                prev_remain = remain;
                            }
                        }
                        
                        std::cout << " remain " << remain <<
                            " prev remain " << prev_remain <<
                            " sum of value " << sum_of_value <<
                            " kv_comp_size " << kv_comp_size_ <<
                            std::endl;

                        int size_ratio = 100;
                        if (prev_remain != 0 && sum_of_value != 0) {
                            size_ratio = 100 * prev_remain / sum_of_value;
                        }

                        printf("Decide Size ratio %d \n", size_ratio);
                        kv_dump_size_ = 4096;                   
                        kv_comp_ratio_ = size_ratio;
                        result_index = 3;
                    } else {
                        printf("%s May be error I don't know ... which value size is threshold\n", __func__);
                        printf("%s May be error I don't know ... which value size is threshold\n", __func__);
                        printf("%s May be error I don't know ... which value size is threshold\n", __func__);
                        printf("%s May be error I don't know ... which value size is threshold\n", __func__);
                        result_index = 4;
                    }
                    printf("Result of kv_comp %d dump %d result index %d Rocks Change %lld KV Change %lld kv_write_size %lld rocks_write_size %lld \n", kv_comp_size_, kv_dump_size_, result_index, rocks_change_size_,kv_change_size_, kv_write_size_, rocks_write_size_);

                    
                    //remain_gap_size_ = total_rocksdb_size_
                    accumulate_value_size_ = 0;
                    value_size_anal_.clear(); 
                    wait_period_ = micros();
                    analysis_wait_ = true;

                } else {
                    if (item[tail].direction_ == 0) {
                        value_size_anal_.push_back(item[tail].value_size_);
                        accumulate_value_size_ += item[tail].value_size_;
                    }
                    //printf("Value size %lld %lld\n", (long long int)accumulate_value_size_, (long long int) item[tail].value_size_); 
                }
                ///////////////////////////////////////////////

                //if (item[tail].value_size_ >= 4096) {
#if (KV_MIGRATION == 1)
                if (item[tail].direction_ == 1) {
                    long long int total_kv__ = kv_write_size_ + kv_change_size_;
                    long long int total_rocks__ = rocks_write_size_ + rocks_change_size_;
                    long long int my_gap = total_rocksdb_size_ / 3; // 200 GB / 3  
                    if ((total_kv__ < total_rocks__) || ((total_kv__ - total_rocks__) < my_gap)) { // It will increase the KV SSD Size 
                        item[tail].status_ = 2;
                        kv_ssd_->kv_put_buffer_async(item[tail].key_, 16, item[tail].value_, item[tail].value_size_, &item[tail].status_);
                        kv_index_->keyInsert(item[tail].key_, 16, item[tail].seq_, item[tail].value_size_);
                        kv_change_size_ += item[tail].value_size_; 
                        rocks_change_size_ -= item[tail].value_size_; 
                    } else {
                        item[tail].status_ = 1;
                        migration_stop_ = true;
                        if (rand() % 1000 == 0) {
                            printf("direction %d total_kv %lld total_rocks %lld my_gap %lld total_gap %lld\n",
                                    item[tail].direction_, total_kv__, total_rocks__,
                                    my_gap, total_kv__ - total_rocks__);
                            printf("kv write %lld kv_change %lld rocks %lld rocks_change %lld total rocks %lld \n",
                                    kv_write_size_, kv_change_size_, rocks_write_size_, rocks_change_size_, total_rocksdb_size_);
                        }
                    }
                } else if (item[tail].direction_ == 2) {
                    long long int total_kv__ = kv_write_size_ + kv_change_size_;
                    long long int total_rocks__ = rocks_write_size_ + rocks_change_size_;
                    long long int my_gap = total_rocksdb_size_ / 3; // 200 GB / 3  
                    item[tail].status_ = 1; // Directly finish async
                    if ((total_rocks__ < total_kv__) || ((total_rocks__ - total_kv__) < my_gap)) {
                        kv_index_->keyDelete(item[tail].key_, 16);
                        rocks_change_size_ += item[tail].value_size_; 
                        kv_change_size_ -= item[tail].value_size_; 
                        migration_stop_ = false;
                        if (rand() % 1000 == 0)
                            printf("MIGRATION STOP DISABLE ! here \n"); 
                    } else {
                        item[tail].status_ = 1;
                        migration_stop_ = false;
                        if (rand() % 1000 == 0) {
                            printf("direction %d total_kv %lld total_rocks %lld my_gap %lld total_gap %lld\n",
                                    item[tail].direction_, total_kv__, total_rocks__,
                                    my_gap, total_kv__ - total_rocks__);
                            printf("kv write %lld kv_change %lld rocks %lld rocks_change %lld total rocks %lld \n",
                                    kv_write_size_, kv_change_size_, rocks_write_size_, rocks_change_size_, total_rocksdb_size_);
                        }
                    }
                } else if ((item[tail].value_size_ >= (size_t) kv_dump_size_)) { // direction == 1 -> Rocks to KVSSD
                    item[tail].status_ = 2;
                    kv_ssd_->kv_put_buffer_async(item[tail].key_, 16, item[tail].value_, item[tail].value_size_, &item[tail].status_);
                    kv_index_->keyInsert(item[tail].key_, 16, item[tail].seq_, item[tail].value_size_);
                } else {
                    item[tail].status_ = 1; // Directly finish async
                    kv_index_->keyDelete(item[tail].key_, 16);
                }
#else
                if (item[tail].value_size_ >= (size_t) kv_dump_size_) {
                    item[tail].status_ = 2;
                    kv_ssd_->kv_put_buffer_async(item[tail].key_, 16, item[tail].value_, item[tail].value_size_, &item[tail].status_);
                    kv_index_->keyInsert(item[tail].key_, 16, item[tail].seq_, item[tail].value_size_);
                } else {
                    item[tail].status_ = 1; // Directly finish async
                    kv_index_->keyDelete(item[tail].key_, 16);
                }
#endif
                tail = (tail + 1) % MAX_POP;
            } else {

            }
        } else {
            // Queue is full..wtf...
        }

        while (item[head].status_ == 1) {
            item[head].status_ = 0;
            free (item[head].value_);
            if (max_seq_num_ < item[head].seq_) {
                max_seq_num_ = item[head].seq_;
            }
            head = (head + 1) % MAX_POP;
        }
    }

}


#endif







}  // namespace ROCKSDB_NAMESPACE





