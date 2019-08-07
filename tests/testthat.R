library(testthat)
library(vroom)

test_check("vroom", reporter = MultiReporter$new(reporters = list(JunitReporter$new(file = "test-results.xml"), CheckReporter$new())))
