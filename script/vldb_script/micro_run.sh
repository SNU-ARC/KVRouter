#!/bin/sh
#1 First Value Size  (ex 256) 
#2 Value Ratio (ex 4 -> 256 , 1024, 4096)
#3 Value Count (ex 4 -> 256, 1024, 4096, 16384)
#4 Distribution (0: Same, 1: proportional 2: Exponential)


# Calculate Value Sum 

./make.sh

dirtarget="result/micro_run"
if [ ! -d $dirtarget ]; then
    echo "Create $dirtarget"
    mkdir $dirtarget
fi


datetime=$(date '+%Y-%m-%d_%H:%M:%S')

dirtarget=$dirtarget/$datetime


echo $datetime
echo $dirtarget
if [ ! -d $dirtarget ]; then
    echo "Create $dirtarget"
    mkdir $dirtarget
fi

threads=8
db_size=200 # default 100 GB 
first_value=256
value_ratio=4
value_count=3
distribution=4



# Test for RocksDB
kv_ssd_enable=0
my_dir=$dirtarget/$kv_ssd_enable"_"$first_value"_"$value_ratio"_"$value_count"_"$distribution"_"$db_size"_"$threads
if [ ! -d $my_dir ]; then
    echo "Create $my_dir"
    mkdir $my_dir
fi
/bin/bash vldb_script/1.sh $threads $kv_ssd_enable $first_value $value_ratio $value_count $distribution $db_size $my_dir 
 
# Test for KVSSD
kv_ssd_enable=1
my_dir=$dirtarget/$kv_ssd_enable"_"$first_value"_"$value_ratio"_"$value_count"_"$distribution"_"$db_size"_"$threads
if [ ! -d $my_dir ]; then
    echo "Create $my_dir"
    mkdir $my_dir
fi
/bin/bash vldb_script/1.sh $threads $kv_ssd_enable $first_value $value_ratio $value_count $distribution $db_size $my_dir 

# Test for LastDB
kv_ssd_enable=4
my_dir=$dirtarget/$kv_ssd_enable"_"$first_value"_"$value_ratio"_"$value_count"_"$distribution"_"$db_size"_"$threads
if [ ! -d $my_dir ]; then
    echo "Create $my_dir"
    mkdir $my_dir
fi
/bin/bash vldb_script/1.sh $threads $kv_ssd_enable $first_value $value_ratio $value_count $distribution $db_size $my_dir 



