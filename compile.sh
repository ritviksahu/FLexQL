#!/bin/sh
set -eu
mkdir -p build

SDK_PATH="$(xcrun --show-sdk-path)"
CXXFLAGS="-std=c++17 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -I. -isysroot $SDK_PATH"
LDFLAGS="-isysroot $SDK_PATH -lpthread"

c++ $CXXFLAGS src/flexql_server.cpp -o build/server $LDFLAGS
c++ $CXXFLAGS src/client.cpp src/flexql.cpp -o build/client $LDFLAGS
c++ $CXXFLAGS -Wno-unused-function benchmark_flexql.cpp src/flexql.cpp -o build/benchmark $LDFLAGS
c++ $CXXFLAGS benchmark_select.cpp src/flexql.cpp -o build/benchmark_select $LDFLAGS
c++ $CXXFLAGS stress_benchmark.cpp src/flexql.cpp -o build/stress_benchmark $LDFLAGS
