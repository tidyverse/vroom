all:
	@echo "make: Entering directory '/Users/jhester/p/vroom/src'"
	@Rscript -e 'pkgload::load_all(quiet = FALSE)'
	@echo "make: Leaving directory '/Users/jhester/p/vroom/src'"

test:
	@echo "make: Entering directory '/Users/jhester/p/vroom/tests/testthat'"
	@Rscript -e 'devtools::test()'
	@echo "make: Leaving directory '/Users/jhester/p/vroom/tests/testthat'"

clean:
	@Rscript -e 'pkgbuild::clean_dll()'

BENCH_TAXI := $(wildcard ~/data/taxi_trip_fare*csv)
BENCH_FWF := ~/data/PUMS5_06.TXT
BENCH_ROWS := 1000000
BENCH_COLS := 25
BENCH_SRC := $(wildcard inst/bench/*-benchmark.R)
BENCH_OUT := $(BENCH_SRC:-benchmark.R=-times.tsv)

.NOTPARALLEL: bench

bench: $(BENCH_OUT)

%-times.tsv : %-benchmark.R
	R -q --vanilla -f $< --args $(BENCH_INPUTS)

inst/bench/all_%-times.tsv: inst/bench/all_%-benchmark.R
	R -q --vanilla -f $< --args $(BENCH_ROWS) $(BENCH_COLS)

inst/bench/fwf-times.tsv: inst/bench/fwf-benchmark.R
	R -q --vanilla -f $< --args $(BENCH_FWF)

bench-clean:
	rm -f $(BENCH_OUT)

install:
	R CMD INSTALL .
