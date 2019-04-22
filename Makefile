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
