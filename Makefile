all:
	@echo "make: Entering directory '/Users/jhester/p/vroom/src'"
	@Rscript -e 'pkgload::load_all(quiet = FALSE)'
	@echo "make: Leaving directory '/Users/jhester/p/vroom/src'"

test:
	@echo "make: Entering directory '/Users/jhester/p/vroom/tests/testthat'"
	@Rscript -e 'devtools::test()'
	@echo "make: Leaving directory '/Users/jhester/p/vroom/tests/testthat'"

clean:
	@Rscript -e 'devtools::clean_dll()'

SRCS = vroom_standalone.cc src/delimited_index.cc
OBJS=$(subst .cc,.o,$(SRCS))

CXXFLAGS = -O3 -std=c++11 -DVROOM_STANDALONE -I src/ -I src/mio/include
LDFLAGS =

vroom_s: $(OBJS)
	$(CXX) $(LDFLAGS) -o vroom_s $(OBJS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $*.o
