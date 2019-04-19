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
OBJS = vroom_standalone.o src/delimited_index.o
CXXFLAGS = -O3 -std=c++11
vroom_s: $(OBJS)
	g++ -L /Library/Frameworks/R.framework/Libraries/ -lr $^ -o $@

$(OBJS): $(SRCS)
	g++ -c $(CXXFLAGS) -I src/ -I src/mio/include -I ~/Library/R/3.5/library/Rcpp/include -I ~/Library/R/3.5/library/progress/include -I /Library/Frameworks/R.framework/Headers/ $^
