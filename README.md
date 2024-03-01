# KVRouter

KVRouter: Optimizing Key-Value Stores Performance for Varying Data Characteristics


# Instruction

1. Install MPDK version 1.6.0 for utilizing KVSSD

```shell

```

2. Install prerequisite library

```shell
```

3. Build KVRouter

``` shell
$ cd script
$ ./make.sh
```

4. Specify the model name of KVSSD

KVRouter currently needs to specify KVSSD separately in rocksdb/db/kvssd/kv\_ssd.cc. 

Currently, it is set to use nvme3n1 and nvme4n1.

5. Run the benchmark scripts
``` shell
$ cd script
$ /bin/bash vldb_script/micro_run.sh
```













