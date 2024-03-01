#include <iostream>
#include <algorithm>
#include <thread>
#include <mutex>
#include <unistd.h>
#include "/home2/du/util/KVSSD/PDK/core/include/kvs_api.h"
#include "/home2/du/util/KVSSD/PDK/core/include/kvs_const.h"
#include "/home2/du/util/KVSSD/PDK/core/include/kvs_result.h"
#include "/home2/du/util/KVSSD/PDK/core/include/kvs_struct.h"

#define TEST (2)

void complete(kvs_postprocess_context* ioctx) 
{
    if (ioctx->context == KVS_CMD_RETRIEVE)
    {
        if (ioctx->result != KVS_SUCCESS)
        {
            int * status = (int *) ioctx->private1;
            if (status == nullptr) {
            } else {
                *status = -1;
            }
        }
        else
        {
            int * status = (int *) ioctx->private1;
            printf("CMD RETRIEVE %p \n", status);
            fflush(stdout);
            
            if (status == nullptr) {
            } else {
                *status = 1;
            }
        }
    }
}
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

char dev_path[32];
kvs_device_handle dev;
char keyspace_name[32];
kvs_key_space_handle ks_hd;
char ks_name[65536];

int env_init() ;
int env_exit() ;
int start() {
    strcpy(dev_path, "/dev/nvme3n1");
    strcpy(keyspace_name, "rocksdb_test");

	if(dev_path == NULL) {
		fprintf(stderr, "Please specify KV SSD device path\n");
		return 0;
	}
	
	snprintf(ks_name, MAX_KEYSPACE_NAME_LEN, "%s", "keyspace_test");
	if(env_init() != 0)
	{
		printf("Env init Error ! \n"); 
		while(1) {} 
	}
    printf (" KV SSD open is done ! \n");

    return 0;
}

int env_init() {
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

int env_exit() 
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

void kv_get_buffer_async(const char * key, size_t key_size, char * buffer, size_t buffer_size, volatile int * status)
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
    if (ret != KVS_SUCCESS) 
    {
        fprintf(stderr, "retrieve tuple failed with err 0x%x\n", ret);
        while (1)
        {
            printf("Fuck You ERR\n");
            fflush(stdout);
        }
    }
    //printf("It is done here ! kv get buffer async %s %d \n", key, *status );
}




int main(int argc, char * argv[]) {

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
  
    for (int i = 0 ; i < total_key; ) {
        int status[64] = {0};
        int cnt = 0;
        int j = 0 ;
        for (; j < 64 ; j ++) {
            status[j] = 2;
            kv_get_buffer_async(key[i], 16, value, 4096, &status[j]); 
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
    return 0;
}

