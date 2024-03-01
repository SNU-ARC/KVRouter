//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_DB_KVSSD_H_
#define ROCKSDB_DB_KVSSD_H_

#include "/home2/du/work/S_KV/PDK/core/include/kvs_api.h"
#include "/home2/du/work/S_KV/PDK/core/include/kvs_const.h"
#include "/home2/du/work/S_KV/PDK/core/include/kvs_result.h"
#include "/home2/du/work/S_KV/PDK/core/include/kvs_struct.h"
#include "db/version_set.h"
#include "memory/arena.h"
#include "options/cf_options.h"
#include "rocksdb/sst_partitioner.h"
#include "util/autovector.h"
#include "util/hash_map.h"
#include <iostream>
#include <algorithm>

#include <thread>
#include <mutex>
#include "db/kv_ssd/kv_ssd.h"
#include "db/kv_ssd/kv_iter.h"
namespace ROCKSDB_NAMESPACE {
#define KV_EMUL (0)
#ifdef __KVPRINT
#define COUTKV(str) do { std::cout << str << std::endl; } while( false )
#define PRINTKV(fmt, args...) fprintf(stdout, "[%s:%d:%s()]: " fmt, \
        __FILE__, __LINE__, __func__, ##args)
#else
#define COUTKV(str) do { } while ( false )
#define PRINTKV(fmt, args...)
#endif

class KVIter;
class KV_SSD;

enum KV_OP{
    KV_PUT,
    KV_GET,
    KV_CHECK
};

enum BUFFER_STAT {
    BUF_IDLE,
    BUF_WAIT,
    BUF_DONE
};

struct KVHeader{
    size_t key_size_;
    size_t value_size_;
};

struct KVInfo {
    size_t bytes_;
    int key_count_;
    size_t root_offset_;// TODO
    size_t used_buffer_; // TODO
};

typedef struct KV_SSD_KStruct{
    uint8_t level;
    char key[16];
}KV_SSD_KStruct ;

typedef struct KVIndex {
    KVInfo * meta_;
    char key_[16];
    char * entire_buffer_;
    char * buffer_;
    KVIndex * next_;
}KVIndex;

#define D_MAX_NEXT_OFFSET (1024*1024*1024)
struct KVIndexNode {
    char key_[16];
    size_t kv_size_;
    size_t next_offset_;
};


struct kv_capa {
    long long int put_;
    long long int del_;
};

struct kv_stat {
    struct kv_capa capa_info_;
};

struct async_complete {
    int status_;  // 1: inserted    2: completed   3: Check done..?
    int submit_;
    int complete_;
    struct KV_SSD_KStruct encoded_key_;
};

class KV_SSD {
	public:
		KV_SSD();
		~KV_SSD();
        bool kv_put_large(uint64_t rocksdb_capa, const char * key, size_t key_size, const char * buffer, size_t size);
		void kv_put(int level, const Slice& key, const Slice& value);
		//void kv_put_buffer(int level, const Slice& key, char * buf, size_t size);
		void kv_put_buffer(int level, char * key, size_t key_size, char * buf, size_t size);
        void kv_put_buffer_sync(int level, char * key, size_t key_size, char * buffer, size_t size);
        void kv_put_buffer_async(int level, char * key, size_t key_size, char * buffer, size_t size, bool * status);
		void kv_get(int level, const Slice& key, const Slice& value);
        char * kv_get_buffer(int level, const char * key, size_t key_size, char * buffer, size_t buffer_size);
        void kv_get_buffer_async(int level, char * key, size_t key_size, char * buffer, size_t size, bool * status);
        bool kv_check(int level, const char * key, size_t key_size);
        void kv_check_async(int level, char * key, int key_size, uint8_t * status, bool * done);
        //void kv_get_buffer(int level, const char * key, size_t key_size, char * buffer, size_t buffer_size);
       
        //kvs_result kvs_exist_kv_pairs(kvs_key_space_handle ks_hd, uint32_t key_cnt, kvs_key *keys, kvs_exist_list *list);
        
        void hash_insert(uint64_t key_val, int count);
        bool hash_contains(uint64_t key_val);
        void hash_delete(const Slice& key);
        uint64_t hash_get(uint64_t key_val);
        std::vector<struct async_complete>::iterator allocate_async(char * input);

        KVIndex * KVAllocateIndex(int level, char * key, size_t key_size, KVIndex * root);
        KVIndex * KVGetIndex(int level, char * smallest, char * largest, size_t key_size, bool * index_exist);
        // Sync is useless but I implement it for debugging
        KVIndex * KVGetIndexSync(int level, char * smallest, size_t key_size, bool * index_exist);
        bool CheckIndex(KVIndex * prev, char * key, size_t key_size);
        KVIndex *  SearchIndex(KVIndex * prev, char * key, int * search_ret, int * skip_cnt);
        KVIndexNode * PushNode(KVIndex * prev, KVIndexNode * prev_node, char * key, size_t node_size);
        KVIndexNode * SearchAndPushNode(KVIndex * my_index, char * key, int64_t * node_offset, size_t node_size);
        KVIndex * AddIndex (KVIndex * prev, char * buffer, size_t offset_val, char * key, size_t key_size, bool init_meta);
        void DeleteIndex(KVIndex * root, int skip_cnt);
        void DeleteAllIndex(KVIndex * root);
        char * GetIndexKey(KVIndex * my_index) 
        {
            return my_index->key_;
        }
        void PrintIndex(KVIndex * prev, int cnt);
        void ParseIndex(KVIndex * index, int * key_count, size_t * used_buffer);
        KVIndexNode * NextNode(KVIndex * my_index, KVIndexNode * input_node);
	private:
        long long int put_capa_;
        /*
        void update_latnecy(KV_OP op, uint64_t val)
        {
            stLatency * target;
            if (op == KV_PUT)
            {
                target = &put_;
            }
            else if (op == KV_GET)
            {
                target = &get_;
            }
            else if (op == KV_CHECK)
            {
                target = &update_;
            }
            else if (op == KV_DEL)
            {
                target = &del_; 
                
            }
            uint64_t max_lat = target->maxLat_;
            uint64_t min_lat = target->minLat_;
            uint64_t avg_lat = target->avgLat_;
            uint64_t total_lat = target->totalLat_;
            uint64_t cnt = target->cnt_;
        
            if (max_lat < val)
            {
            }
            if (min_lat < val)
            {
            }
            total_lat = total_lat + val; 
            cnt = cnt + 1;
            avg_lat = total_lat / cnt
            
            target->maxLat_ = max_lat; 
            target->minLat_ = min_lat; 
            target->avgLat_ = avg_lat; 
            target->totalLat_ = total_lat; 
            target->cnt_ = cnt; 
        }
        */

        uint64_t slice_to_key(const Slice& key);
		int max_level_;
		int max_queue_cnt_;
		uint64_t capacity_;

		// Device init
		char dev_path[32];
		kvs_device_handle dev;
		char keyspace_name[32];
		kvs_key_space_handle ks_hd;

        int q_depth_ = 64;
        int q_tail_;
        int q_head_;
		int submitted_;
		int processed_;
		
		void print_stat();
	
		int env_init();
		int env_exit();

        // Max Thread => 64
        static std::mutex async_lock;
        char thread_map[64][16];
        std::vector<struct async_complete> completed_key_;

        //struct timespect prev_util_time_;

        char emul_key_[1024][64];
        int emul_index_;
        void * kv_emul_buffer[16];
        // Insert(k, v) Delete (k) Get (k) Contains(k)
        HashMap<uint64_t, int, 512> hash_; // = new HashMap<int, int>;

        void PrintNode(int key_count, char * buffer, size_t root_offset);
};


}  // namespace ROCKSDB_NAMESPACE
#endif
