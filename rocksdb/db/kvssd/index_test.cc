#include <iostream>
#include <algorithm>
#include <thread>
#include <mutex>
#include <unistd.h>
#include "kv_ssd.h"

#define TEST (2)

void createKey(char * key, int num) {
    snprintf(key, 17, "%016lu", (long unsigned int)num);
    //for (int i = 0 ; i < 16 ; i ++) printf("%c",key[i]);
    //printf("\t\t %d sizoef key %ld \n", num,sizeof(key)); 
}

void testComp() {
    int test_set = 100000;
    for (int i = 0 ; i < test_set ; i ++) {
    
    }
}

#include <chrono>
uint64_t local_micros()
{
    uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::
            now().time_since_epoch()).count();
    return us;
}

void run_func_get(ROCKSDB_NAMESPACE::KV_SSD * kv_ssd, char ** key, int start, int end, char * value) {
    for (int i = start ; i < end ; i ++ ) {
        kv_ssd->kv_get_buffer_sync(key[i], 16, value, 16384, 0); 
        if (i % 2000 == 0) printf("kv get buffer sync %d total %d\n", i, end);
    }
}
void run_func(ROCKSDB_NAMESPACE::KV_SSD * kv_ssd, char ** key, int start, int end, char * value) {
    for (int i = start ; i < end ; i ++ ) {
        kv_ssd->kv_put_buffer_sync(key[i], 16, value, 16384, 0); 
        if (i % 2000 == 0) printf("kv put buffer sync %d total %d\n", i, end);
    }
}



int main(void) {
#if (TEST == 0)
    bool dbg = true;
    ROCKSDB_NAMESPACE::KVIndex kv_index;
    ROCKSDB_NAMESPACE::KV_SSD kv_ssd_; 

    //kv_index.sulCreate(178);

    char ** key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    char ** delete_key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    int total_key = 1024*1024*16;
    int test_key = 1024*20;
    for (int i = 0 ; i < total_key ; i ++) {
        key[i] = (char *) malloc(sizeof(char) * 17); 
        delete_key[i] = (char *) malloc(sizeof(char) * 17); 
        createKey(key[i], i + 1); 
        createKey(delete_key[i], i + 1); 
    }

    kv_index.sulPrint();
    // Shuffle
    /*
    if (false) {
        char * tmp = (char *)malloc(sizeof(char) * 17);
        for (int i = 0 ; i < total_key ; i ++) {
            int index = rand() % total_key;
            if (false) {
                printf("index %d i %d\n", index, i);
                printf("%s %s => ", key[i], key[index]);
                fflush(stdout);
            }
            memcpy((void *)tmp, (void *)key[index], 17); 
            memcpy((void *)key[index], (void *) key[i], 17); 
            memcpy((void *)key[i], (void *) tmp, 17);
            //if (dbg) printf("%s %s %s\n", tmp, key[i], key[index]);
        }
    }
    */

    kv_index.allocateIndex(true, 0, 1678, 4);
    kv_index.sulPrint();
    for (int i = 0 ; i < test_key ; i ++) {
        kv_index.keyInsert(key[i], 16, 192+i);
    }

    kv_index.sulPrint();

    // Flush Here
    std::vector<char*> my_buffer; //= kv_ssd_->get_flush_buffer();
    std::vector<char*> my_key; // = kv_ssd_->get_flush_key();
    my_buffer.clear();
    my_key.clear();
    kv_index.sulFlush(0, 1, my_buffer, my_key);
   
    printf("\n\n\nsulFlush !!!!!!!1\n");
    fflush(stdout);
    kv_ssd_.sulFlush(my_buffer, my_key);
    while (1) {
        bool ret = kv_ssd_.sulFlushCheck();
        if (ret) {
            printf("ret value %d\n", (int)ret);
            break;
        }
    }
    printf("WTF?\n");
    kv_index.sulDelete(0, 1);
    kv_index.sulPrint();

  
    // Flush Here
    my_buffer.clear();
    my_key.clear();
    kv_index.sulFlush(0, 1, my_buffer, my_key);
   
    printf("\n\n\nsulFlush !!!!!!!1\n");
    fflush(stdout);
    kv_ssd_.sulFlush(my_buffer, my_key);
    while (1) {
        bool ret = kv_ssd_.sulFlushCheck();
        if (ret) {
            printf("ret value %d\n", (int)ret);
            break;
        }
    }
    printf("WTF?\n");

    for (int i = 0 ; i < test_key ; i ++) {
        kv_index.keyInsert(key[i], 16, 100000);
    }


    kv_index.sulPrint();



    return 0;
    

    /*
    if (dbg) {
        ROCKSDB_NAMESPACE::sll * hash = kv_index.hashGet();
        for (int i = 0 ; i < 2 ; i ++) {
            printf("index %d \t\t ******* \t\t %p \n", i, &hash[i]);
            kv_index.sllPrint(&hash[i]);
        }
    }
    */

    kv_index.sulPrint();
#elif (TEST == 1) 
    // Phase 1 : Insert key index and key value
    // Phase 2 : Read keyvalue from index & SSD

    bool dbg = true;
    ROCKSDB_NAMESPACE::KVIndex kv_index;
    ROCKSDB_NAMESPACE::KV_SSD kv_ssd_; 

    //kv_index.sulCreate(178);

    char * value = (char *) malloc(16384);
    char ** key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    char ** delete_key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    int total_key = 1024*1024*16;
    int test_key = 1024*20;
    for (int i = 0 ; i < total_key ; i ++) {
        key[i] = (char *) malloc(sizeof(char) * 17); 
        delete_key[i] = (char *) malloc(sizeof(char) * 17); 
        createKey(key[i], i + 1); 
        createKey(delete_key[i], i + 1); 
    }

    kv_index.sulPrint();

    kv_index.allocateIndex(true, 0, 1678, 4);
    kv_index.sulPrint();
    for (int i = 0 ; i < test_key ; i ++) {
        kv_index.keyInsert(key[i], 16, 192+i);
        kv_ssd_.kv_put_buffer_sync(key[i], 16, value, 16384); 
    }

    kv_index.sulPrint();

    // Flush Here
    std::vector<char*> my_buffer; //= kv_ssd_->get_flush_buffer();
    std::vector<char*> my_key; // = kv_ssd_->get_flush_key();
    my_buffer.clear();
    my_key.clear();
    kv_index.sulFlush(0, 1, my_buffer, my_key);
   
    printf("\n\n\nsulFlush !!!!!!!1\n");
    fflush(stdout);
    kv_ssd_.sulFlush(my_buffer, my_key);
    while (1) {
        bool ret = kv_ssd_.sulFlushCheck();
        if (ret) {
            printf("ret value %d\n", (int)ret);
            break;
        }
    }
    printf("WTF?\n");
    kv_index.sulDelete(0, 1);
    kv_index.sulPrint();



    for (int i = 0 ; i < test_key ; i ++ ) {
        kv_ssd_.kv_get_buffer_sync(key[i], 16, value, 16384); 
    }
  



    return 0;
    

    /*
    if (dbg) {
        ROCKSDB_NAMESPACE::sll * hash = kv_index.hashGet();
        for (int i = 0 ; i < 2 ; i ++) {
            printf("index %d \t\t ******* \t\t %p \n", i, &hash[i]);
            kv_index.sllPrint(&hash[i]);
        }
    }
    */

    kv_index.sulPrint();
#elif (TEST == 2)
    bool dbg = true;
    ROCKSDB_NAMESPACE::KVIndex kv_index;
    ROCKSDB_NAMESPACE::KV_SSD kv_ssd_; 

    //kv_index.sulCreate(178);

    char * value = (char *) malloc(16384);
    char ** key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    char ** shuffle_key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    int total_key = 1024*1024*16;
    int test_key = 1024*20;
    for (int i = 0 ; i < total_key ; i ++) {
        key[i] = (char *) malloc(sizeof(char) * 17); 
        shuffle_key[i] = (char *) malloc(sizeof(char) * 17); 
        createKey(key[i], i + 1); 
        createKey(shuffle_key[i], i + 1); 
    }
   
    if (true) {
        char * tmp = (char *)malloc(sizeof(char) * 17);
        for (int i = 0 ; i < test_key ; i ++) {
            int index = rand() % test_key;
            if (false) {
                printf("index %d i %d\n", index, i);
                printf("%s %s => ", shuffle_key[i], shuffle_key[index]);
                fflush(stdout);
            }
            memcpy((void *)tmp, (void *) shuffle_key[index], 17); 
            memcpy((void *)shuffle_key[index], (void *) shuffle_key[i], 17); 
            memcpy((void *)shuffle_key[i], (void *) tmp, 17);
        }
        printf("shuffle done\n"); 
    }


    int util = kv_ssd_.kvGetUtilization();
    printf("Utilization %d\n", util);

    kv_index.sulPrint();
    kv_index.allocateIndex(true, 0, 1678, 4);
    kv_index.sulPrint();
    for (int i = 0 ; i < test_key ; i ++) {
        kv_index.keyInsert(key[i], 16, 192+i);
        kv_ssd_.kv_put_buffer_sync(key[i], 16, value, 16384); 
    }
    printf("Hello !!!!!!!!!!!1\n");
 

    //scanf("%d", &qwer);
    for (int i = 0 ; i < test_key ; i ++) {
        printf("test_key %s \n", shuffle_key[i]);
        kv_index.keyInsert(shuffle_key[i], 16, 192+i);
        kv_ssd_.kv_put_buffer_sync(shuffle_key[i], 16, value, 16384); 
        if (i % 100 == 0) printf("QQQQQQQQ %d\n", i);
    }
    printf("Hello !!!!!!!!!!!1\n");


    /*
    kv_index.sulPrint();
    kv_ssd_.kvUpdateUtilization(0);
    printf (" str update val %d \n", (int) kv_ssd_.kvstrUpdate());

    // Flush Here
    std::vector<char*> my_buffer; //= kv_ssd_->get_flush_buffer();
    std::vector<char*> my_key; // = kv_ssd_->get_flush_key();
    my_buffer.clear();
    my_key.clear();
    kv_index.sulFlush(0, 1, my_buffer, my_key);
   
    printf("\n\n\nsulFlush !!!!!!!1\n");
    fflush(stdout);
    kv_ssd_.sulFlush(my_buffer, my_key);
    while (1) {
        bool ret = kv_ssd_.sulFlushCheck();
        if (ret) {
            printf("ret value %d\n", (int)ret);
            break;
        }
    }
    kv_index.sulDelete(0, 1);
    kv_index.sulPrint();
    
    util = kv_ssd_.kvGetUtilization();
    printf("Utilization %d\n", util);
    */

    return 0;
    
#elif (TEST==3)
    ROCKSDB_NAMESPACE::KVIndex kv_index;
    ROCKSDB_NAMESPACE::KV_SSD kv_ssd_; 


    char * value = (char *) malloc(16384);
    char ** key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    char ** shuffle_key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    int total_key = 1024*1024;
    int test_key = 1024*20;
    for (int i = 0 ; i < total_key ; i ++) {
        key[i] = (char *) malloc(sizeof(char) * 17); 
        shuffle_key[i] = (char *) malloc(sizeof(char) * 17); 
        createKey(key[i], i + 1); 
        createKey(shuffle_key[i], i + 1); 
    }
  
    /*
    if (true) {
        char * tmp = (char *)malloc(sizeof(char) * 17);
        for (int i = 0 ; i < total_key ; i ++) {
            int index = rand() % total_key;
            if (false) {
                printf("index %d i %d\n", index, i);
                printf("%s %s => ", shuffle_key[i], shuffle_key[index]);
                fflush(stdout);
            }
            memcpy((void *)tmp, (void *) shuffle_key[index], 17); 
            memcpy((void *)shuffle_key[index], (void *) shuffle_key[i], 17); 
            memcpy((void *)shuffle_key[i], (void *) tmp, 17);
        }
        printf("shuffle done\n"); 
    }
    */

    // Sync Test
    uint64_t micro_ret[10] = {0};
    int qq = 0;
    micro_ret[9] = local_micros();
    {
        // Not found Checker !!!!!!!!!!
        int gap = total_key / 8;
        std::thread t0(run_func_get, &kv_ssd_, key, 0,       gap, value);
        std::thread t1(run_func_get, &kv_ssd_, key, gap,   2*gap, value);
        std::thread t2(run_func_get, &kv_ssd_, key, 2*gap, 3*gap, value);
        std::thread t3(run_func_get, &kv_ssd_, key, 3*gap, 4*gap, value);
        std::thread t4(run_func_get, &kv_ssd_, key, 4*gap, 5*gap, value);
        std::thread t5(run_func_get, &kv_ssd_, key, 5*gap, 6*gap, value);
        std::thread t6(run_func_get, &kv_ssd_, key, 6*gap, 7*gap, value);
        std::thread t7(run_func_get, &kv_ssd_, key, 7*gap, 8*gap, value);

        t0.join();
        t1.join();
        t2.join();
        t3.join();
        t4.join();
        t5.join();
        t6.join();
        t7.join();
    }
    micro_ret[0] = local_micros();
    if (true) {    
        int gap = total_key / 8;
        std::thread t0(run_func, &kv_ssd_, key, 0,       gap, value);
        std::thread t1(run_func, &kv_ssd_, key, gap,   2*gap, value);
        std::thread t2(run_func, &kv_ssd_, key, 2*gap, 3*gap, value);
        std::thread t3(run_func, &kv_ssd_, key, 3*gap, 4*gap, value);
        std::thread t4(run_func, &kv_ssd_, key, 4*gap, 5*gap, value);
        std::thread t5(run_func, &kv_ssd_, key, 5*gap, 6*gap, value);
        std::thread t6(run_func, &kv_ssd_, key, 6*gap, 7*gap, value);
        std::thread t7(run_func, &kv_ssd_, key, 7*gap, 8*gap, value);

        t0.join();
        t1.join();
        t2.join();
        t3.join();
        t4.join();
        t5.join();
        t6.join();
        t7.join();
    } else {
        for (int i = 0 ; i < total_key ; i ++ ) {
            kv_ssd_.kv_put_buffer_sync(key[i], 16, value, 4096, 0); 
            if (i % 2000 == 0) printf("kv put buffer sync %d total %d\n", i, total_key);
        }
    }
    micro_ret[1] = local_micros();
    if (true) {
        int gap = total_key / 8;
        std::thread t0(run_func_get, &kv_ssd_, key, 0,       gap, value);
        std::thread t1(run_func_get, &kv_ssd_, key, gap,   2*gap, value);
        std::thread t2(run_func_get, &kv_ssd_, key, 2*gap, 3*gap, value);
        std::thread t3(run_func_get, &kv_ssd_, key, 3*gap, 4*gap, value);
        std::thread t4(run_func_get, &kv_ssd_, key, 4*gap, 5*gap, value);
        std::thread t5(run_func_get, &kv_ssd_, key, 5*gap, 6*gap, value);
        std::thread t6(run_func_get, &kv_ssd_, key, 6*gap, 7*gap, value);
        std::thread t7(run_func_get, &kv_ssd_, key, 7*gap, 8*gap, value);

        t0.join();
        t1.join();
        t2.join();
        t3.join();
        t4.join();
        t5.join();
        t6.join();
        t7.join();
    } else {
        for (int i = 0 ; i < total_key ; i ++ ) {
            kv_ssd_.kv_get_buffer_sync(key[i], 16, value, 4096, 0); 
            if (i % 2000 == 0) printf("kv get buffer sync %d total %d\n", i, total_key);
        }
    }
    micro_ret[2] = local_micros();

    // Async Test
    for (int i = 0 ; i < total_key ;) {
        int status[64] = {0};
        int cnt = 0;
        int j = 0 ;
        for (; j < 64 ; j ++) {
            status[j] = 2;
            kv_ssd_.kv_put_buffer_async(key[i], 16, value, 4096, &status[j]); 
            if ((i + cnt) % 2000 == 0) printf("kv put buffer async %d total %d\n", i, total_key);
            cnt ++;
        }
        j = 0;
        while (j < cnt) {
            if (status[j] != 1) {
                continue;
            }
            status[j] = 0; 
            j ++;
        }
        i = i + cnt;
    }
    micro_ret[3] = local_micros();
    for (int i = 0 ; i < total_key; ) {
        int status[64] = {0};
        int cnt = 0;
        int j = 0 ;
        for (; j < 64 ; j ++) {
            status[j] = 2;
            kv_ssd_.kv_get_buffer_async(key[i], 16, value, 4096, &status[j]); 
            if ((i + cnt) % 2000 == 0) printf("kv get buffer async %d total %d\n", i, total_key);
            cnt ++;
        }
        j = 0;
        while (j < cnt) {
            if (status[j] != 1) {
                continue;
            }
            status[j] = 0; 
            j ++;
        }
        i = i + cnt;
    }
    micro_ret[4] = local_micros();
    std::cout << "Final Result " << "\t" << qq << "\t" <<
        micro_ret[1] - micro_ret[0] << "\t" <<
        micro_ret[2] - micro_ret[1] << "\t" <<
        micro_ret[3] - micro_ret[2] << "\t" <<
        micro_ret[4] - micro_ret[3] << "\t" <<
        micro_ret[0] - micro_ret[9] << "\t" << std::endl;
#else 

    ROCKSDB_NAMESPACE::KV_SSD kv_ssd_; 
    ROCKSDB_NAMESPACE::KVComp * kv_comp_;
    kv_comp_ = new ROCKSDB_NAMESPACE::KVComp(&kv_ssd_);
   
    char ** l4_key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    char ** l3_key = (char **) malloc(sizeof(char *) * 1024 * 1024 * 16);
    int l3_range = 1024 * 1024 * 64;
    int l4_range = 1024 * 1024 * 16;
    int l4_count = 1024*1024*4;
    int l3_count = 1024*1024*4;
    int test_key = 1024*20;
    
    for (int i = 0 ; i < l4_count ; i ++) {
        l4_key[i] = (char *) malloc(sizeof(char) * 17); 
        int l4_val = (rand() % l4_range) + 1;
        createKey(l4_key[i], l4_val); 
    }
    printf ("QQQQQQQQQQQq %d\n", l4_range);
    for (int i = 0 ; i < l3_count ; i ++) {
        l3_key[i] = (char *) malloc(sizeof(char) * 17); 
        int l3_val = (rand() % l3_range) + 1;
        createKey(l3_key[i], l3_val); 
    }

    kv_comp_->allocateIndex(false, 0, 1678, 4);
   
    {
        // do not consider smallest and largest
        int level = 4;
        char smallest[17];
        char largest[17];
        snprintf(smallest, 17, "%016lu", 1);
        snprintf(largest, 17, "%016lu", 9999);
        int tail = kv_comp_->compPrepare(level, smallest, largest, 16);
    
        //for (int i = 0 ; i < l4_count ; i ++) {
        for (int i = 0 ; i < 4096 ; i ++) {
            kv_comp_->keyInsert(tail, l4_key[i], (size_t) 16, l4_key[i], (size_t) 16, 12);
        }
        
        kv_comp_->ulPrint(); 
        printf (" \n ====================== \n");
#if (1)
        kv_comp_->sulFlush(0, 1, 4); // seq start - seq end and level
#endif
        printf (" \n ====================== \n");

        kv_comp_->compDone(tail, (size_t) 16);
        printf ("Fill done!!!!!!!\n");
    } 
    /*
    // compaction prepare
    int tail = kv_comp_->compPrepare(4, smallest, largest, 16);

    // compaction run
    

    // comparion done
    kv_comp_->compDone(tail, 16);
    */
#endif
    return 0;

}

