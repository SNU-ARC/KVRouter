#!/bin/bash

cd ..
cd rocksdb
make clean
DEBUG_LEVEL=0 make db_bench -j 8 
#DEBUG_LEVEL=2 make db_bench -j 8 
cd ..
cd script


