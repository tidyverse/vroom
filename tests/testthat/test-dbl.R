test_that("Large exponents can parse", {
  res <- vroom("1e-63,1e-64\n", delim = ",", col_types = "dd", col_names = FALSE)
  expect_equal(res[[1]], 1e-63)
  expect_equal(res[[2]], 1e-64)
})
