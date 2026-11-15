CXX      ?= g++
CXXFLAGS ?= -std=c++20 -O3 -march=native -Wall -Wextra -Iinclude -Isrc -pthread
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

# Tests under AddressSanitizer + UBSan, and the two-thread pipeline under ThreadSanitizer.
sanitize: build/test | build
	$(CXX) -std=c++20 -O1 -g -fsanitize=address,undefined -fno-sanitize-recover=all -Iinclude -Isrc -pthread tests/test.cpp -o build/test_asan
	./build/test_asan
	$(CXX) -std=c++20 -O1 -g -fsanitize=thread -Iinclude -Isrc -pthread src/main.cpp -o build/itchfh_tsan
	./build/test --emit build/synth.itch
	# setarch -R: older TSan runtimes abort under high-entropy ASLR
	setarch -R ./build/itchfh_tsan --queue 64 --conflate build/synth.itch > /dev/null
	setarch -R ./build/itchfh_tsan --queue 64 build/synth.itch > /dev/null

clean:
	rm -rf build

# Loopback MoldUDP64 run with 2% of packets dropped must match a file replay exactly.
udptest: all
	./scripts/udp_test.sh

.PHONY: all test udptest sanitize clean
