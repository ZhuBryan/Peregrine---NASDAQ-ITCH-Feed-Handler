CXX      ?= g++
CXXFLAGS ?= -std=c++20 -O3 -march=native -Wall -Wextra -Iinclude -pthread
HDRS     := $(wildcard include/*.hpp src/*.hpp)

all: build/itchfh build/itchfh_baseline build/moldsend

build/itchfh: src/main.cpp $(HDRS) | build
	$(CXX) $(CXXFLAGS) $< -o $@
build/itchfh_baseline: src/main.cpp $(HDRS) | build
	$(CXX) $(CXXFLAGS) -DBASELINE $< -o $@
build/moldsend: tools/moldsend.cpp $(HDRS) | build
	$(CXX) $(CXXFLAGS) $< -o $@
build/test: tests/test.cpp $(HDRS) | build
	$(CXX) $(CXXFLAGS) $< -o $@
build/test_baseline: tests/test.cpp $(HDRS) | build
	$(CXX) $(CXXFLAGS) -DBASELINE $< -o $@
build:
	mkdir -p build

test: build/test build/test_baseline
	./build/test && ./build/test_baseline

# Loopback MoldUDP64 run with 2% of packets dropped must match a file replay exactly.
udptest: all
	./scripts/udp_test.sh

.PHONY: all test udptest
