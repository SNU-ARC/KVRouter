//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef KVSSD_H_
#define KVSSD_H_

#include "/home2/du/util/KVSSD/PDK/core/include/kvs_api.h"
#include "/home2/du/util/KVSSD/PDK/core/include/kvs_const.h"
#include "/home2/du/util/KVSSD/PDK/core/include/kvs_result.h"
#include "/home2/du/util/KVSSD/PDK/core/include/kvs_struct.h"
//#include "/home2/du/work/S_KV/PDK/core/include/kvs_api.h"
//#include "/home2/du/work/S_KV/PDK/core/include/kvs_const.h"
//#include "/home2/du/work/S_KV/PDK/core/include/kvs_result.h"
//#include "/home2/du/work/S_KV/PDK/core/include/kvs_struct.h"
#include <iostream>
#include <algorithm>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <vector> 
#include <queue> 
#include <atomic>
#include <thread>
//#include <tbb/concurrent_queue.h>
//#include <concurrent_queue.h>
#include "rocksdb/rocksdb_namespace.h"
#include "/home2/du/util/Cache/include/Cache/Cache.h"
#include "/home2/du/util/Cache/include/Cache/Policy/LRU.h"


/*
class KVCache {
    public:
        KVCache();
        ~KVCache();
        std::string change_string(char * key) {
            return std::string(key);
        }

        int contain(char * key) {
            std::string my_str = change_string(key); 
            if (cache_.contains(my_str)) return cache_[my_str];
        } 
        
        void insert(char * key, int value) {
            std::string my_str = change_string(key); 
            cache_.insert(my_str, value); 
        }
    private: 
    
};
*/

namespace ROCKSDB_NAMESPACE {


// key size
// L0 KV Set : 16 Bytes
// L0 Key Index : UPPER_CHAR + Meta (2) = 12 + 2 => 14 Bytes

// Compaction KV Set : 16 Bytes + Level (1) => 17 Bytes
// Compaction Key Index : UPPER_CHAR + 1 + Meta (2) => 15 Bytes

class KV_SSD;
class KVIndex;
class KVComp;

#if (1)
#include "db/kvssd/MPMCQueue.h"
#define PREFETCH_COUNT (4)
struct ChangeElement {
    char key_[17];
    char * value_; 
    size_t value_size_;
};

struct WORKERElement {
    char key_[17];
    char * value_;
    size_t key_size_;
    size_t value_size_;
    int seq_;
    int status_;
    uint8_t direction_;
};

typedef struct WORKERElement workE;
// Class Worker Queue
class WorkerQueue {
    public:
        WorkerQueue();    
        ~WorkerQueue();  
        void set_ssd(KV_SSD * kv_ssd) { kv_ssd_ = kv_ssd; }
        void set_index(KVIndex * kv_index) { kv_index_ = kv_index; }
        bool kvEnqueue(char * key, size_t key_size, int seq, char * value, size_t value_size, uint8_t direction = 0);
        void kvDequeue(int count);
        void processUpdate(int number);
        int getMaxSeq() { return max_seq_num_; } 
        int getDumpSize() { return kv_dump_size_; }
        int getCompSize() { return kv_comp_size_; }
        int getCompRatio() { return kv_comp_ratio_; }

        bool MigrationOrNot(int direction, float kv_avg, float rocks_avg, float kv_qps, float rocks_qps) ;
        bool CheckUpdate() ;
        void Migration(int direction, size_t value_size) ;
        void GetLRUKV () ;
        bool GetMigrationStop() { return migration_stop_; }
    private:
        KVIndex * kv_index_;
        KV_SSD * kv_ssd_;
        std::thread deq_thread_;
        // Sequence
        // MemTable Number?
        // To Check ... all of the KV SSD job is done before flush operation?
        rigtorp::MPMCQueue<workE> * wait_list_; 
        // Check sequence number 0,1,2,3,4,5,6,7,8 all of the bit is done ! means update smallest number ok?
        std::set<int> unprocessed_number_;

        std::atomic<int> min_seq_num_;
        int max_seq_num_;
    
        bool bg_running_;

        uint64_t accumulate_value_size_;
        std::vector<size_t> value_size_anal_;
        uint64_t wait_period_;
        bool analysis_wait_;

        int kv_dump_size_; 
        int kv_comp_size_;
        int kv_comp_ratio_;

    
        std::mutex migration_lock_;
        bool migration_stop_;
        long long int kvssd_size_;
        long long int kv_write_size_;
        long long int rocks_write_size_;
        long long int kv_change_size_; 
        long long int rocks_change_size_; 
        //std::vector<struct ChangeElement> rocks_change_[16];
        //std::vector<struct ChangeElement> kv_change_[16];
};



#endif








enum BUFFER_STAT {
    BUF_IDLE,
    BUF_WAIT,
    BUF_DONE
};


// Balancing Strategy
//  1. (Rocks > KV) or (Rocks < KV)  => Threshold Size change  (4K -> 1K or 16K...)
//  2. (Rocks > KV) => Highest Level compaction data push to KV SSD
//     (Rocks < KV) => Threshold Size Change (4K -> 16K) 
struct KVStrategy {
    int kv_used_[2];
    int rocks_used_[2];
    size_t border_; // default 4K
    int score_; // default 4K
    int last_level_;
    int last_level_size_;
    int bias_; // do not care difference default 16 GB 
    bool changed_;
    long long int threshold_;

    // previous
    // int prev_dir_; // [0 : 1x] -- [1 : 2x] -- [-1 : 1/2x] 
    // float prev_per_;

    // timestamp
    // int try_;
};

typedef struct KVStrategy kvstr; 

struct PrefetchManager {
    int write_index_; // added point
    int take_;
    int hit_;
    int miss_;
    int skip_;
    int data_update_;
    int data_fail_;
    int data_wait_;
    int prefetched_;
    std::mutex lock_;
};
typedef struct PrefetchManager PreM;

struct PrefetchElement {
    int id_;
    char key_[17];
    //char value_[65536]; //16384];
    size_t value_size_;
    volatile int status_;
};
typedef struct PrefetchElement PreE;

class KV_SSD {
	public:
		KV_SSD(int number = 0);
		~KV_SSD();
        void kv_put(); // { return ; }
        void kv_put_buffer_sync(const char * key, size_t key_size, const char * buffer, size_t size, int writer_id);
        void kv_put_buffer_async(const char * key, size_t key_size, const char * buffer, size_t size, volatile int * status);
        size_t kv_get_buffer_sync(const char * key, size_t key_size, char * buffer, size_t buffer_size, int reader_id);
        void kv_get_buffer_async(const char * key, size_t key_size, char * buffer, size_t size, volatile int * status);
  
        //std::vector<char*> get_flush_buffer() { return ul_flush_buffer_; }
        //std::vector<char*> get_flush_key() { return ul_flush_key_; }
        
        bool sulFlush(std::vector<char *> &input_buf, std::vector<char*> &input_key, int * status, int level = 0);
        bool sulFlushCheck(volatile int * stauts, int count);


        // startegy!!!!!!!!!!!!!
        int isCompactionEnable() {
            if (kvstr_cur_.score_ == 0) return false;
            else return true;
        }
        int getScore() { return kvstr_cur_.score_; } 
        int getLastLevel() { return kvstr_cur_.last_level_; } 
        size_t getBorder() { return kvstr_cur_.border_; }
        int kvGetUtilization();
        
        // Admin Command only for test 
        void kvstrChange(size_t border, int score);

        void kvFinishFill() {
            kvstr_cur_.score_ = wait_score_;
            kvstr_cur_.changed_ = wait_changed_;
            printf("%s Trace Fill is Done !! Now update score ! %d %d level %d \n",
                    __func__, kvstr_cur_.score_, (int) kvstr_cur_.changed_, kvstr_cur_.last_level_);
        }
        bool kvstrUpdate();
        int kvUpdateUtilization(uint64_t rocks_used, uint64_t kv_used, uint64_t last_level_size, int last_level);
        int rocksGetTotalFileSize();
        void strPrint();
        
        void addRdLatency(uint64_t, int);
        void addWrLatency(uint64_t, int);
        void printLatency();
        void cleanLatency();
        void sortLatency();
	private:
		// Device init
		char dev_path[32];
		kvs_device_handle dev;
		char keyspace_name[32];
		kvs_key_space_handle ks_hd;

		int env_init();
		int env_exit();

        int wait_score_;
        bool wait_changed_;

        //int * ul_flush_status_; // 0: Idle 2: Submit 1: Success -1: Not exist
        //int ul_flush_count_;
   
        long long int kv_ssd_used_; 
        kvstr kvstr_cur_;
 
        uint32_t rocks_now_used_;
        uint32_t kv_now_used_;
            
        char * kvs_key_p_[64];
        char * kvs_value_p_[64];

        // Maximum thread number is 64
        std::atomic<int> rd_lat_index_;
        std::atomic<int> wr_lat_index_;
        std::vector<uint64_t> rd_lat_merge_;
        std::vector<uint64_t> wr_lat_merge_;
        std::vector<uint64_t> wr_lat_[64];
        std::vector<uint64_t> rd_lat_[64];
};















// KVIndex Start Region
// TODO List
// 1. Upper key 가 단순하게 관리됨. hash index와 동일하게 유지되어 flush 시에 얍실하게 관리함. 
#define UPPER_CHAR (12)
// #define UPPER_CHAR (12) // UPPER => key size - LOWER_CHAR
// #define LOWER_CHAR (4)
struct KVStat {
    size_t size_;
    int count_;
};

// Linked List (ll) 
struct LinkedList {
    bool enable_;
    short key_num_; // TODO..>?
    short next_; // offset index!!!!! (1,2,3,4,)
};

typedef struct LinkedList ll; 

// In-Memory     
struct ShardLL {
    std::mutex slot_lock_;
    char key_[UPPER_CHAR+1];
    ll * buffer_;
 
    int border_; // default 4KB

    int buffer_mul_; // BLOCK_SIZE * buffer_mul
    int level_;
    int key_count_;
    long offset_;
    long head_;
    long remain_;
    int deleted_key_;

    // TODO: It is tmep method...later it is added to LinkedList Part
    long long int total_capa_;
};

typedef struct ShardLL sll;

struct UpdateList {
    int value_;
    struct UpdateList * next_;
};

typedef struct UpdateList ul; 

struct ShardUL {
    int seq_start_;
    int seq_end_;
    ul * buffer_;
    struct ShardUL * next_;
};

typedef struct ShardUL sul;

// Vector Push and Pop 
// Background Update List (BUL)
struct BUL {
    char key_[17]; // Key size is 16 Bytes 
    int sequence_;
    size_t  value_size_;
};


#define MAX_KV_SCAN (8)
struct IterBuffer {
    char * buffer_;
    volatile int status_;
};

struct KVIter {
    int index_; // sll index
    short key_num_;
    int last_index_; // sll index
    short last_key_num_;
    int level_;

    // For KV API
    int element_index_;
    ll * last_;
    struct IterBuffer iter_element_[MAX_KV_SCAN];

    // Other Thread
    int wait_thread_;
    bool done_;
    
    std::mutex iter_mutex_; 
    std::condition_variable iter_controller_;
};

class KVIndex;
class KVIndex {
    int  CharToShort(char * in, int start, int count) {
    //short  CharToShort(char * in, int start, int count) {
        char new_key[count+1]; 
        for (int i = start ; i < start+count ; i ++)
        {
            if (i > 16) {
                printf("%s Error Case start %d count %d i %d \n", __func__, start, count, i); 
            }
            new_key[i-start] = in[i];
        }
        new_key[count] = '\0';
        //printf("%s %d %d\n", new_key, start, count);
        int ret = std::stoi(new_key);
        //printf("start %d %s ret val %i done\n", start, new_key, ret);
        return ret;
        //return (short)ret;
    }


    public:
		KVIndex();
		~KVIndex();
        long keyInsert(char * key, size_t key_size, int seq, size_t value_size, long prev_index = 0, int * ul_value = nullptr);
        bool keyDelete(char * key, size_t key_size);
        bool keyFind(char * key, size_t key_size, int seq_opt = 0, int thread_id = 0);
        sll * hashGet() { return hash_; }
        char * hashBufferGet(int hash_index) { return (char *) hash_[hash_index].buffer_; }  


        char * llBufferGet(char * key) {
            int index = (int) CharToShort(key, 0, UPPER_CHAR);
            sll * sll_local = &hash_[index];
            char * ret = (char *) sllGetHead(sll_local);
            return ret;
        }

        void sllPrint(sll * sll_in);
        void sllPrintAll();
        
        // UL Functions !!!!
        void sulCreate(int seq_start);
        void sulDelete(int seq_start, int seq_end);
        void sulBufferGet();
        void sulPrint();
        
        void ulPush(int value, int seq);
        void ulClear(ul * ul_in);
        void ulPrint(ul * ul_in);
        void ulPrintAll();
        
        // KVIndex Region But...only exist in here (kvssd.h)
        void sulFlush(int seq_start, int seq_end, std::vector<char *> &input_buf, std::vector<char*> &input_key);

        void getCapacity(long long * key_count, long long * capacity);
 
        void allocateIndex(bool is_l0, int start, int end, int level);
        void allocateSLLBuffer(sll * sll_in);
        void gcSLLBuffer(sll * sll_in);

        // Prefetch
        void startPrefetch(int thread_id);
        PreE * getPrefetchBuffer(int thread_id, bool * prefetch_hit, const char * key, char * value);
        bool checkPrefetch(int thread_id, const char * key, char * value);
        int getPrefetchNum(int thread_id) {
            return preManager_[thread_id].prefetched_;
        }

        // Scan
        void allocIter(struct KVIter * iter);
        void freeIter(struct KVIter * iter);
        bool makeIter(struct KVIter * iter, char * key, int scan_count, int level, int tid);
        void nextIter(struct KVIter * iter, char * input_key, size_t * key_size, bool * last);
        //void nextIter(struct KVIter * iter);
        void releaseIter(struct KVIter * kv_iter, int tid) ;

        // Background Update Functions
#if (0)
        void CreateBGWorker();
        void DeleteBGWorker();
        void BGWork();
        void PushBGQueue();
        void PopBGQueue();
#endif
    private:
        //concurrent_queue<struct BUL> bul_;

        //bul * bg_update_;        

        //SLL * head_;
        sll * hash_;
        sul * update_head_;
        sul * update_tail_;
       
        std::vector<int> ul_flush_;
        //int ul_flush_count_;
        //int * ul_flush_status_; // 0: Idle 2: Submit 1: Success -1: Not exist
        //ul * update_list_;

        // Function
        // LL Functions !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1
        ll * llNext(ll * in) {
            if (in->next_ == 0) return nullptr;
            //printf("%s\t input %p offset %ld \n", __func__, in, in->next_); 
            return (in + in->next_);
        }
      
        ll * llLocate(sll * sll_in, long offset) {
            ll * ret = nullptr;
            if (offset == 0) {
                printf("Erorr offset is 0 utilizes llSearch Not llLocal\n");
            } else {
                ll * buffer = sll_in->buffer_;
                ret = (buffer + offset);
                //printf("Debug Here %s lower key is %d  \n",__func__,  (int) ret->key_num_);
            }
            return ret;
        }
      
        short llOffset(sll * sll_in, ll * ll_in) {
            short ret = (short) (ll_in - sll_in->buffer_);
            //printf("ll offset %p %p ret %ld\n", ll_in, (ll *) sll_in->buffer_, ret);
            return ret;
        }

        // return null : head  
        // return ret == ll_in: Tail
        // return else: ..nothing
        void llCopy(sll * sll_in, ll * ll_in, void * buffer);
        void updateAlgo(sll * sll_in, int count);
        ll * llSearch(ll * ll_in , short key_num, bool * find, bool get_prev);
        ll * llLast(ll * ll_in, int count);
        bool llSearchForDelete(ll * ll_in , short key_num, ll ** prev);
        int llCompare(short key_in, short key_num);

        void llRelease(ll * in); 

        bool llEmpty(ll * in) {
            if (in == nullptr) return true;
            if (in->enable_ == false) return true;
            else return false;
            return !(in->enable_);
        }
        void llPrint(ll * ll_in, int count);
       
        // SLL Functions !!!!!!!!!!!!!!!!!!!!!!!!!!
        ll * sllGetHead(sll * sll_in);
        void sllSetHead(sll * sll_in, ll * ll_in);
        bool sllEmpty(sll * sll_in) { 
            if (sll_in->key_count_ == 0) return true;
            else return false;
        }
        void sllRelease();

    
        // Print Function !!!!!!!!
        void sllPrintMeta(sll * sll_in);
        void llPrintMeta(ll * ll_in);
       
        // Statistics
        std::atomic<long long>  total_key_count_;
        std::atomic<long long>  total_capacity_;

        // Variables 
        int num_hash_entries_;
        int num_keys_;
        
        int max_key_count_;
        int dbg_count_;

        std::vector<char *> deleted_keys_;
        std::mutex sul_mutex_; 
  
        // seq optimization
        // 20 is maximum thraead number
        std::mutex prefetch_lock_;
        PreM preManager_[20];
        PreE preElement_[PREFETCH_COUNT*20];

        // Scan
        struct KVIter * iter_list_[32];
        std::mutex iter_mutex_; 
};



#define MAX_SHORT (32767)
#define MAX_LONG (2147483647)
#define META_MAX (8192) // meta size: size_t
#define COMP_MAX (4*1024*1024) // key + value

class KVComp {
    short  CharToShort(char * in, int start, int count) {
        char new_key[count+1]; 
        for (int i = start ; i < start+count ; i ++)
        {
            if (i > 16) {
                printf("%s Error Case start %d count %d i %d \n", __func__, start, count, i); 
            }
            new_key[i-start] = in[i];
        }
        new_key[count] = '\0';
        //printf("%s %d %d\n", new_key, start, count);
        int ret = std::stoi(new_key);
        //printf("start %d %s ret val %i done\n", start, new_key, ret);

        return (short)ret;
    }

    struct compaction_stat {
        bool progress_;
      
        long prev_index_;
        int compare_value_;
        int enqueue_count_;
        int dequeue_count_;
        int status_;
        char * buffer_; // may be ll buffer
    };

    struct COMPElement {
        int index_;
        int compare_value_;
        char key_[17];
        char * value_;
        size_t value_size_;
        int status_;
    };

    typedef struct COMPElement compE;
    public:
        KVComp(KV_SSD * kv_ssd); 
        ~KVComp();
        
        KVIndex * getKVIndex(); 
        bool isRangeSet();
        /*
        bool isRangeSet() { return false; } // TODO
        */
        void setRange(char * smallest, char * largest, size_t key_size) ;
        bool checkRange(char * smallest, char * largest, size_t key_size) ;
        void expandRange(char * smallest, char * largest) ;
        long keyInsert(int tail, char * key, size_t key_size, char * value, size_t value_size, int sequence, long prev_index = 0) ;
        bool keyFind(char * key, size_t key_size, int seq_opt=0, int thread_id = 0) {
            return comp_index_->keyFind(key, key_size, seq_opt, thread_id);
        }
        int compPrepare(int level, char * smallest, char * largest, size_t key_size) ;
        bool compDone(int tail, size_t key_size) ;
        
        void allocateIndex(bool is_l0, int start, int end, int level) ;
        void allocateIndexP(bool is_l0, char * start, char * end, int level) ;
        /*void allocateIndex(bool is_l0, int start, int end, int level) {
            comp_index_->allocateIndex(is_l0, start, end, level);
        }*/

        void ulPrint();
        void sulFlush(int seq_start, int seq_end, int level);
        void CompEnqueue(int index, char * key, char * value, size_t value_size);
        void CompDequeue();
        
        // Prefetch
        PreE * getPrefetchBuffer(int thread_id, bool * prefetch_hit, const char * key, char * value) {
            return comp_index_->getPrefetchBuffer(thread_id, prefetch_hit, key, value);
        }
        bool checkPrefetch(int thread_id, const char * key, char * value) {
            return comp_index_->checkPrefetch(thread_id, key, value);
        }
        int getPrefetchNum(int thread_id) {
            return comp_index_->getPrefetchNum(thread_id);
        }
        
        // Scan
        void allocIter(struct KVIter * iter) {
            comp_index_->allocIter(iter);
        }
        void freeIter(struct KVIter * iter) {
            comp_index_->freeIter(iter);
        }
        bool makeIter(struct KVIter * iter, char * key, int scan_count, int level, int tid) {
            return comp_index_->makeIter(iter, key, scan_count, level, tid);
        }
        void nextIter(struct KVIter * iter, char * input_key, size_t * key_size, bool * last) {
            comp_index_->nextIter(iter, input_key, key_size, last);
        }
        /*void nextIter(struct KVIter * iter) {
            comp_index_->nextIter(iter);
        }*/
        void releaseIter(struct KVIter * iter, int tid) {
            comp_index_->releaseIter(iter, tid);
        }
    private:
        KVIndex * comp_index_;
        KV_SSD * comp_ssd_;
        
        rigtorp::MPMCQueue<compE> * wait_list_; 
       
        bool enable_;
        int level_;
        int tail_ ;
        
        compaction_stat compact_stat_[64];
        int smallest_;
        int largest_;

        int max_compaction_;
        std::mutex comp_mutex_;
        bool bg_running_;
};


}  // namespace ROCKSDB_NAMESPACE
#endif
