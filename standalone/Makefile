SRCS = vroom.cc ../src/delimited_index.cc
OBJS=$(subst .cc,.o,$(SRCS))

CXXFLAGS = -std=c++11 -DVROOM_STANDALONE -I../src/ -I ../src/mio/include
LDFLAGS =

release: CXXFLAGS += -O3
release: vroom

debug: CXXFLAGS += -g -O0
debug: vroom

vroom: $(OBJS)
	$(CXX) $(LDFLAGS) -o vroom $(OBJS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $*.o

clean:
	rm -f $(OBJS) vroom


# Create a 1Gb ram disk, so AFL writing millions of files doesn't destroy your SSD
# http:/www.cipherdyne.org/blog/2014/12/ram-disks-and-saving-your-ssd-from-afl-fuzzing.html
# diskutil erasevolume HFS+ 'ram_disk' `hdiutil attach -nomount ram:/2097152`
FUZZ_DIR=/Volumes/ram_disk/fuzz-testing

# Generate test cases for the fuzzer
$(FUZZ_DIR)/tests:
	mkdir -p "$@"
	./generate_tests.R $@

VROOM_ARGS=--comment '\#' --escape_double --escape_backslash $(VROOM_FWF_ARGS)

fuzz: CXX = afl-clang++
fuzz: CXXFLAGS += -g
fuzz: release $(FUZZ_DIR)/tests
fuzz:
	mkdir -p "$(FUZZ_DIR)/"
	cp vroom "$(FUZZ_DIR)/vroom"
	afl-fuzz -S fuzzer1 -i $(FUZZ_DIR)/tests -o $(FUZZ_DIR)/out -- $(FUZZ_DIR)/vroom $(VROOM_ARGS) @@ > /dev/null &
	afl-fuzz -S fuzzer2 -i $(FUZZ_DIR)/tests -o $(FUZZ_DIR)/out -- $(FUZZ_DIR)/vroom $(VROOM_ARGS) @@ > /dev/null &
	afl-fuzz -S fuzzer3 -i $(FUZZ_DIR)/tests -o $(FUZZ_DIR)/out -- $(FUZZ_DIR)/vroom $(VROOM_ARGS) @@ > /dev/null &
	afl-fuzz -S fuzzer4 -i $(FUZZ_DIR)/tests -o $(FUZZ_DIR)/out -- $(FUZZ_DIR)/vroom $(VROOM_ARGS) @@ > /dev/null &
	afl-fuzz -S fuzzer5 -i $(FUZZ_DIR)/tests -o $(FUZZ_DIR)/out -- $(FUZZ_DIR)/vroom $(VROOM_ARGS) @@ > /dev/null &
	afl-fuzz -M fuzzer0 -i $(FUZZ_DIR)/tests -o $(FUZZ_DIR)/out -- $(FUZZ_DIR)/vroom $(VROOM_ARGS) @@
