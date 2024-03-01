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

#include <iostream>
#include <algorithm>
#include <thread>
#include <mutex>
namespace ROCKSDB_NAMESPACE {

class KV_SSD;
// KVIndex Start Region

#define UPPER_CHAR (12)
#define LOWER_CHAR (4)
// Linked List (ll) 
struct LinkedList {
    bool enable_;
    short key_num_; // TODO..>?
    //char key_[LOWER_CHAR+1];
    short next_; // offset index!!!!! (1,2,3,4,)
    //long prev_; // offset
};

typedef struct LinkedList ll; 

// In-Memory     
struct ShardLL {
    char key_[UPPER_CHAR+1];
    ll * buffer_;
    //char * next_; // real pointer

    int key_count_;
    long offset_;
    long head_;
    long remain_;
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

class KVIndex;
class KVIndex {
	public:
		KVIndex();
		~KVIndex();
        bool keyInsert(char * key, size_t key_size, int seq);
        bool keyDelete(char * key, size_t key_size);
        sll * hashGet() { return hash_; }
           
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
        void sllPrint(sll * sll_in);
        void sllPrintAll();
        
        // UL Functions !!!!
        void sulCreate(int seq_start);
        void sulDelete(int seq_start, int seq_end);
        void sulPrint();
        
        void ulPush(int value, int seq);
        void ulClear(ul * ul_in);
        void ulPrint(ul * ul_in);
    private:
        //SLL * head_;
        sll * hash_;
        sul * update_head_;
        sul * update_tail_;
        //ul * update_list_;

        // Function
        // LL Functions !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1
        ll * llNext(ll * in) {
            if (in->next_ == 0) return nullptr;
            //printf("%s\t input %p offset %ld \n", __func__, in, in->next_); 
            return (in + in->next_);
        }
      
        short llOffset(sll * sll_in, ll * ll_in) {
            short ret = (short) (ll_in - sll_in->buffer_);
            //printf("ll offset %p %p ret %ld\n", ll_in, (ll *) sll_in->buffer_, ret);
            return ret;
        }

        // return null : head  
        // return ret == ll_in: Tail
        // return else: ..nothing
        ll * llSearch(ll * ll_in , short key_num, bool * find, bool get_prev);
        bool llSearchForDelete(ll * ll_in , short key_num, ll ** prev);
        int llCompare(short key_in, short key_num);

        void llRelease(ll * in); 

        bool llEmpty(ll * in) {
            if (in == nullptr) return true;
            return !(in->enable_);
        }
        void llPrint(ll * ll_in);
       
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
       
        


        // Variables 
        int num_hash_entries_;
        int num_keys_;
        
        int max_key_count_;
 

};



}  // namespace ROCKSDB_NAMESPACE
#endif
