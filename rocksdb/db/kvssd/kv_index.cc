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
#include "kv_index.h"

namespace ROCKSDB_NAMESPACE {

#define BLOCK_SIZE (4*1024)

#define NUM_KEYS (1024*1024*1024)

#define SHARD_MOD_BIT (10)
#define SHARD_COUNT (1 << SHARD_MOD_BIT)

KVIndex::KVIndex()
{
    num_keys_ = NUM_KEYS;
    //num_keys_ = KV_SSD_CAPA / BLOCK_SIZE;
    num_hash_entries_ = num_keys_ / SHARD_COUNT;


    //head_ =  nullptr;
    hash_ = (sll *) malloc(sizeof(sll) * num_hash_entries_);
    for (int i = 0 ; i < num_hash_entries_ ; i ++ ) {
        hash_[i].buffer_ = (ll *) malloc(BLOCK_SIZE); 
        //hash_[i].buffer_ = (ll *) malloc(BLOCK_SIZE); 
        uint64_t key_num = i;
        snprintf(hash_[i].key_, sizeof(hash_[i].key_), "%012lu", key_num);
        //snprintf(hash_[i].key_, sizeof(key), "%016lu", key_num);
        

        hash_[i].key_count_ = 0;
        hash_[i].offset_ = 0;
        hash_[i].remain_ = BLOCK_SIZE;
    }
}

KVIndex::~KVIndex() 
{
    printf("Hash entries ! %d\n", num_hash_entries_);
    //sllRelease();
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

bool KVIndex::keyInsert(char * key, size_t key_size, int seq) {
    if (key_size == 0) {// TODO
    }
    bool dbg = false; 
    int index = (int) CharToShort(key, 0, UPPER_CHAR);
    short key_num = CharToShort(key, UPPER_CHAR, LOWER_CHAR);
  
    sll * sll_local = &hash_[index];    
    if (sll_local->offset_ > 4096) {
        //printf("ERROR Case because of exceed offset %ld \n", sll_local->offset_);
        return false;
    }
    bool find = false;
    ll * ll_local = (ll *) (sll_local->buffer_ + sll_local->offset_);
    ll * ll_prev = llSearch(sllGetHead(sll_local), key_num, &find, false);
    
    // Insert to Head 
    short ll_local_offset = llOffset(sll_local, ll_local);
  

    if (dbg)
    printf("Now insert key %s %hd\n", key, key_num);
    //if (key_num < 10 && index == 0) printf("Now insert key %s %hd\n", key, key_num);

    if (sll_local->offset_ == 0) {
        sll_local->head_ = ll_local_offset;
        ll_local->next_ = 0; 
        if (dbg) printf("\t\t ******* Head update here ! key %hd %hd \n", key_num, ll_local_offset);
    }
    else if (ll_prev == nullptr) { // First 
        ll_local->next_ = sll_local->head_ - ll_local_offset;
        sll_local->head_ = ll_local_offset;
        if (dbg) printf("\t\t ******* Head update here ! key %hd %hd local->next_ %hd \n", key_num, ll_local_offset, ll_local->next_);
    } else if (find == true) {
        // Nothing todo (update case)
    } else if (ll_prev == ll_local) { // Update  
        if (ll_prev->next_ == 0) ll_local->next_ = 0;
        else ll_local->next_ = ll_prev->next_ - (long) (ll_local - ll_prev); 
        ll_prev->next_ = ll_local - ll_prev;
    } else { // Insert
        if (ll_prev->next_ == 0) ll_local->next_ = 0;
        else ll_local->next_ = ll_prev->next_ - (long) (ll_local - ll_prev); 
        ll_prev->next_ = ll_local - ll_prev;
    }

    if (find != true) {
        ll_local->enable_ = true;  
        ll_local->key_num_ = key_num;   

        sll_local->key_count_ ++;       
        sll_local->offset_ = sll_local->offset_ + sizeof(ll);
    }
    if (dbg) {
        if (ll_prev == nullptr) 
            printf("Insert done key %d prev is nullptr local (%d : %hd) \n", key_num, ll_local->key_num_, ll_local->next_ );
        else
            printf("Insert done key %d prev (%d : %hd) local (%d : %hd) \n", key_num, ll_prev->key_num_, ll_prev->next_, ll_local->key_num_, ll_local->next_ );
        //long dbg = llOffset(sll_local, ll_prev);
        //long dbg1 = llOffset(sll_local, ll_local);
        //printf("%ld %ld\n", dbg, dbg1);
    }
    ulPush(index, seq);
    return true; 
}



bool KVIndex::keyDelete(char * key, size_t key_size) {
    if (key_size == 0) {// TODO
    }
    bool dbg = false; 
    int index = (int) CharToShort(key, 0, UPPER_CHAR);
    short key_num = CharToShort(key, UPPER_CHAR, LOWER_CHAR);
  
    sll * sll_local = &hash_[index];    
    if (sll_local->key_count_ == 0) {
        return false;
    }
    bool find = false;
 
    ll * ll_ret[3] = {nullptr};
    printf("%s index %d\n", __func__, index);
    ll * ll_head = sllGetHead(sll_local);
    printf("buffer %p head %ld ret %p\n", sll_local->buffer_, sll_local->head_, ll_head);
    find = llSearchForDelete(ll_head, key_num, ll_ret);
    
    printf("llhead %p ll_prev %p cur %p ll_next %p find %d \n", ll_head, ll_ret[0], ll_ret[1], ll_ret[2], (int)find);
    sllPrintMeta(sll_local);
    llPrintMeta(ll_head);

    if (dbg)
        printf("Now Delete key %s %hd\n", key, key_num);

    if (find) {
        // find is head
        if (ll_head == ll_ret[0]) {
            // update head ..?
            // prev == head and me will be deleted...
            // me -> next is head
            sllSetHead(sll_local, ll_ret[2]);
        } else if (ll_ret[0] == nullptr) {
            // me is head 
            // me -> next is head
            sllSetHead(sll_local, ll_ret[2]);
        } else { // not...head...
        }

        ll_ret[1]->enable_ = false;
        ll_ret[1]->key_num_ = 0;
        ll_ret[1]->next_ = 0;
        ll_ret[0]->next_ = llOffset(sll_local, ll_ret[2]);
    
        sll_local->key_count_ --;       
        sll_local->offset_ = sll_local->offset_ - sizeof(ll);
    }

    return true; 
}

// Always return small
ll * KVIndex::llSearch(ll * ll_in, short key_num, bool * find, bool get_prev) {
    int dbg = false;
    if (get_prev) dbg = true;
    ll * ll_ret = nullptr;
    ll * ll_tmp = ll_in;
    if (dbg) printf("\t\t key find start %d ll_in %p \n", key_num, ll_in);
    while (!llEmpty(ll_tmp)) {
        short short_tmp = ll_tmp->key_num_;
        //short short_tmp = CharToShort(ll_tmp->key_, 0, LOWER_CHAR);
        int ret = llCompare(short_tmp, key_num);
        if (dbg) printf("\t %hd ret %d \n", short_tmp, ret);
        if (ret > 0) {  // short_tmp < key_num
            break;
        } else if (ret == 0) {  // short_tmp < key_num
            if (get_prev != true) ll_ret = ll_tmp;
            *find = true;
            break;
        } else {
            ll_ret = ll_tmp;
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
        printf("\t %hd ret %d \n", short_tmp, ret);
        if (ret > 0) {  // short_tmp < key_num
            break;
        } else if (ret == 0) {  // short_tmp < key_num
            ret_cur = ll_tmp;
            ret_next = llNext(ll_tmp);
            find = true;
            break;
        } else {
            ret_prev = ll_tmp;
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
    sul * tmp = update_head_;
    bool added = false;
    if (tmp == nullptr) {
        sul * new_sul = (sul *) malloc(sizeof(sul));
        new_sul->seq_start_ = seq_start;
        new_sul->seq_end_ = seq_start;
        new_sul->buffer_ = nullptr;
        new_sul->next_ = nullptr; 
        update_head_ = new_sul;
        update_tail_ = new_sul;
        added = true;
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
                added = true;
                break;
            }
            tmp = tmp->next_;   
        }
    }
    assert (added == true);
}

void KVIndex::sulDelete(int seq_start, int seq_end) {
    if (seq_start) {}
    if (seq_end) {}
    sul * tmp = update_head_;
    sul * del_sul = tmp;
    while (tmp != nullptr) {
        // TODO
        if (tmp->seq_start_ < seq_end) {
            printf("%s delete seq %d - %d\n", __func__, del_sul->seq_start_, del_sul->seq_end_);
            free(del_sul); 
            del_sul = tmp;
            tmp = tmp->next_;
         } else {
            update_head_ = tmp; 
            if (tmp->next_ == nullptr) {
                update_tail_ = tmp; 
            }
         }
    }
}

void KVIndex::sulPrint() {
    sul * tmp = update_head_;
    while (tmp != nullptr) {
        printf ("index seq %d -- %d\n", tmp->seq_start_, tmp->seq_end_);
        ulPrint(tmp->buffer_);
        tmp = tmp->next_;
    }
}

void KVIndex::ulPush(int value, int seq) {
    bool update = false;
    ul * update_list = update_tail_->buffer_;
    // TODO : Always update tail seq 
    update_tail_->seq_end_ = seq; 

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
        // this value is last  
        } else {
            new_ul->value_ = value;
            new_ul->next_ = nullptr;
            prev->next_ = new_ul;
        }
    }
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



void KVIndex::sllPrint(sll * sll_in) {
    if (sll_in == nullptr) {
        printf("%s errors\n", __func__);
        return; 
    }
    ll * ll_local = sllGetHead(sll_in);
    llPrint(ll_local);
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

void KVIndex::llPrint(ll * ll_in) {
    ll * ll_local = ll_in;
    ll * ll_prev = ll_in;
    int count = 0;
    while (!llEmpty(ll_local)) {    
        if (count % 10 == 0) printf("\n");
        printf("(%4d:%10d:%10hd) ", count++, (int) ll_local->key_num_, ll_local->next_);
        ll_prev = ll_local;
        ll_local = llNext(ll_local);
        if (ll_prev == ll_local) {
            printf("ERrror\n");
            while (1) {}
        }
    }

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


}  // namespace ROCKSDB_NAMESPACE





