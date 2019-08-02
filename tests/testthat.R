library(testthat)
library(vroom)

test_check("vroom", reporter = MultiReporter$new(reporters = list(CheckReporter$new(), JunitReporter$new(file = "test-results.xml"))))
