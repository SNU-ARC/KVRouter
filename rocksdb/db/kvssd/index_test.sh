#!/bin/sh

g++ -o index_test index_test.cc kv_ssd.cc /home2/du/util/KVSSD/PDK/core/build/deps/libkvapi_static.a -L/home2/du/util/KVSSD/PDK/core/build/libkvapi.so -L/home2/du/util/KVSSD/PDK/core/build/libkvapi.so -I/home2/du/util/KVSSD/PDK/core/inclue -           DWITH_KDD -I/home2/du/util/KVSSD/PDK/core/include -I/home2/du/util/KVSSD/PDK/core/src/device_abstract_layer/include -I/home2/du/util/KVSSD/PDK/core/src/device_abstract_layer/kernel_driver_adapter -I/home2/du/util/KVSSD/PDK/core/src/api/include -I/     home2/du/util/KVSSD/PDK/core/src/api/include/private -I/home2/du/util/KVSSD/PDK/core/src/device_abstract_layer/include/private -lpthread -lnuma -g && ./index_test
