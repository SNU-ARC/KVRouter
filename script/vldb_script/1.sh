#! /bin/sh


bench_db_path="../mnt/db"
wal_dir="../mnt/wal"
bench_compression="none" #"snappy,none"

#bench_benchmarks="trace_fill,stats,resetstats"
#bench_benchmarks=$bench_benchmarks,"trace_sr,stats,resetstats"

#bench_benchmarks="trace_fill,stats,resetstats"
#bench_benchmarks=$bench_benchmarks,"trace_sw,stats,sstables,resetstats"


bench_benchmarks="trace_fill,stats,resetstats"
bench_benchmarks=$bench_benchmarks,"trace_warm,stats,resetstats" 
bench_benchmarks=$bench_benchmarks,"trace_rw,stats,resetstats"
bench_benchmarks=$bench_benchmarks,"trace_sr,stats,resetstats"
bench_benchmarks=$bench_benchmarks,"trace_rr,stats,resetstats"
bench_benchmarks=$bench_benchmarks,"trace_sw,stats,resetstats"


bench_num="1"
max_background_jobs="4"
max_bytes_for_level_base="`expr 256 \* 1024 \* 1024`" 

num_levels="6"
bloom_locality="1"
bloom_bits="10"
memtablerep="skip_list"
prefix_size="8"
key_size="8"
keys_per_prefix="0"
#report_ops_latency="false"
#benchmark_write_rate_limit="0"
benchmark_write_rate_limit="40971520"
YCSB_uniform_distribution="false"
max_write_buffer_number="2"
histogram="true"
stats_interval_seconds="10"
inplace_update_support="false"
allow_concurrent_memtable_write="true"
write_buffer_size="67108864"
max_bytes_for_level_multiplier="8"


threads=$1
kv_ssd_enable=$2
kv_ssd_first_value=$3
kv_ssd_value_ratio=$4
kv_ssd_value_count=$5
kv_ssd_distribution=$6
kv_ssd_value_change="true"
kv_ssd_db_size=$7
result_dir=$8

kv_seq_opt="1"
kv_comp_opt="-1"
kv_ssd_count="1"
disable_wal="0"
if [ $kv_ssd_enable -eq "1" ] 
then
    disable_wal="1"
    kv_ssd_count="2"
fi

#kv_ssd_key_range="60000"
kv_ssd_max_value="66536"
kv_ssd_zipf_dist="0.99"

bench_file_path="$(dirname $PWD )/rocksdb/db_bench"
bench_file_dir="$(dirname $PWD )/rocksdb/"

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
    --max_bytes_for_level_multiplier=$max_bytes_for_level_multiplier \
    --kv_ssd_enable=$kv_ssd_enable \
    --kv_ssd_max_value=$kv_ssd_max_value \
    --kv_ssd_zipf_dist=$kv_ssd_zipf_dist \
    --kv_ssd_value_change=$kv_ssd_value_change \
    --kv_ssd_count=$kv_ssd_count \
    --kv_comp_opt=$kv_comp_opt \
    --kv_seq_opt=$kv_seq_opt \
    --kv_ssd_first_value=$kv_ssd_first_value \
    --kv_ssd_value_ratio=$kv_ssd_value_ratio \
    --kv_ssd_value_count=$kv_ssd_value_count \
    --kv_ssd_distribution=$kv_ssd_distribution \
    --kv_ssd_db_size=$kv_ssd_db_size \
    "
    cmd="sudo cgexec -g cpuset,memory:durocks $bench_file_path $const_params"
    #cmd="$bench_file_path $const_params"
    echo $cmd >> $result_dir/stderr.out
    echo $cmd 1>> $result_dir/stderr.out 2> $result_dir/stdout.out
    eval $cmd 1>> $result_dir/stderr.out 2> $result_dir/stdout.out
    #eval $cmd
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
    CLEAN_CACHE
    RUN_ONE_TEST
    sleep 5
    echo 'Done Single Workload'
}

RUN_ALL_TEST

