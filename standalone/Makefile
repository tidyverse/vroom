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
