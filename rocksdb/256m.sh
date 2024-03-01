#! /bin/sh

value_array=(1024) # 16384 65536)
test_all_size=8600000   #8G


bench_db_path="/home2/du/mnt/db"
wal_dir="/home2/du/mnt/wal"

#bench_db_path="/home/arc-x7/mnt/db"
#wal_dir="/home/arc-x7/mnt/wal"
bench_value="1024"
bench_compression="none" #"snappy,none"

bench_benchmarks="trace_a,stats"
#bench_benchmarks="trace_fill_rw,stats"
#bench_benchmarks="trace_fill,stats,resetstats,trace_wr,waitforcompaction,stats,levelstats,resetstats,trace_a,waitforcompaction,stats,levelstats,resetstats,trace_c,stats,sstables"
bench_num="640000000"
bench_readnum="1000000"
#bench_max_open_files="1000"
max_background_jobs="2"
#max_bytes_for_level_base="`expr 8 \* 1024 \* 1024 \* 1024`" 
max_bytes_for_level_base="`expr 256 \* 1024 \* 1024`" 

num_levels="6"
bloom_locality="1"
bloom_bits="10"
memtablerep="skip_list"
prefix_size="8"
key_size="8"
keys_per_prefix="0"
disable_wal="0"
#report_ops_latency="false"
#benchmark_write_rate_limit="0"
benchmark_write_rate_limit="40971520"
YCSB_uniform_distribution="false"
max_write_buffer_number="2"
histogram="true"
stats_interval_seconds="10"
inplace_update_support="false"
allow_concurrent_memtable_write="true"
write_buffer_size="268435456"

max_bytes_for_level_multiplier="8"

compaction_pri="1" #"kOldestLargestSeqFirst"
#compaction_pri="2" #"kOldestSmallestSeqFirst"
#compaction_pri="3" # "kMinOverlappingRatio"

#threads="1"
threads=$1

level0_file_path="/home/arcsnu/pmem"

#report_write_latency="true"

bench_file_path="$(dirname $PWD )/db_bench"
bench_file_dir="$(dirname $PWD )"

if [ ! -f "${bench_file_path}" ];then
bench_file_path="$PWD/db_bench"
bench_file_dir="$PWD"
fi

if [ ! -f "${bench_file_path}" ];then
echo "Error:${bench_file_path} or $(dirname $PWD )/db_bench not find!"
exit 1
fi

RUN_ONE_TEST() {
    const_params="
    --db=$bench_db_path \
    --num_levels=$num_levels \
    --bloom_locality=$bloom_locality \
    --bloom_bits=$bloom_bits \
    --memtablerep=$memtablerep \
    --prefix_size=$prefix_size \
    --key_size=$key_size \
    --keys_per_prefix=$keys_per_prefix \
    --wal_dir=$wal_dir \
    --threads=$threads \
    --value_size=$bench_value \
    --benchmarks=$bench_benchmarks \
    --num=$bench_num \
    --compression_type=$bench_compression \
    --max_bytes_for_level_base=$max_bytes_for_level_base \
    --open_files=100 \
    --benchmark_write_rate_limit=$benchmark_write_rate_limit \
    --max_background_jobs=$max_background_jobs \
    --YCSB_uniform_distribution=$YCSB_uniform_distribution \
    --disable_wal=$disable_wal \
    --histogram=$histogram \
    --max_write_buffer_number=$max_write_buffer_number \
    --write_buffer_size=$write_buffer_size \
    --inplace_update_support=$inplace_update_support \
    --allow_concurrent_memtable_write=$allow_concurrent_memtable_write \
    --statistics=true \
    --stats_interval_seconds=$stats_interval_seconds \
    --compaction_pri=$compaction_pri \
    --max_bytes_for_level_multiplier=$max_bytes_for_level_multiplier \
    "
    #cmd="sudo cgexec -g cpuset,memory:durocks $bench_file_path $const_params"
    cmd="$bench_file_path $const_params"
    echo $cmd >out.out
    echo $cmd
    eval $cmd
    #gdb --args $cmd
}

CLEAN_CACHE() {
    if [ -n "$bench_db_path" ];then
        rm -f $bench_db_path/*
    fi
    sleep 2
    sync
    echo 3 > /proc/sys/vm/drop_caches
    sleep 2
}

COPY_OUT_FILE(){
    mkdir $bench_file_dir/result > /dev/null 2>&1
    res_dir=$bench_file_dir/result/value-$bench_value
    mkdir $res_dir > /dev/null 2>&1
    \cp -f $bench_file_dir/compaction.csv $res_dir/
    \cp -f $bench_file_dir/OP_DATA $res_dir/
    \cp -f $bench_file_dir/OP_TIME.csv $res_dir/
    \cp -f $bench_file_dir/out.out $res_dir/
    \cp -f $bench_file_dir/Latency.csv $res_dir/
    #\cp -f $bench_file_dir/NVM_LOG $res_dir/
    \cp -f $bench_db_path/OPTIONS-* $res_dir/
    #\cp -f $bench_db_path/LOG $res_dir/
}
RUN_ALL_TEST() {
    for value in ${value_array[@]}; do
        CLEAN_CACHE
        bench_value="$value"
        bench_num="`expr $test_all_size / $bench_value`"

        RUN_ONE_TEST
        if [ $? -ne 0 ];then
            exit 1
        fi
        # DU CHANGE COPY_OUT_FILE
        sleep 5
        echo 'Done Single Workload'
    done
}

RUN_ALL_TEST
