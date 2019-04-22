SRCS = vroom.cc ../src/delimited_index.cc
OBJS=$(subst .cc,.o,$(SRCS))

CXXFLAGS = -O3 -std=c++11 -DVROOM_STANDALONE -I../src/ -I ../src/mio/include
LDFLAGS =

vroom_s: $(OBJS)
	$(CXX) $(LDFLAGS) -o vroom $(OBJS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $*.o
