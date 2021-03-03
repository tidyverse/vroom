test_that("Large exponents can parse", {
  res <- vroom(I("1e-63,1e-64\n"), delim = ",", col_types = "dd", col_names = FALSE)
  expect_equal(res[[1]], 1e-63)
  expect_equal(res[[2]], 1e-64)
})

test_that("Doubles parse correctly with comma as decimal separator", {
  res <- vroom(I("23,4\n"), delim='\t', altrep=FALSE,
               locale=locale(decimal_mark=','),
               col_types='d', col_names=FALSE)
  expect_equal(res[[1]], 23.4)
  res2 <- vroom(I("23,4\n"), delim='\t', altrep=TRUE,
               locale=locale(decimal_mark=','),
               col_types='d', col_names=FALSE)
  expect_equal(res2[[1]], 23.4)
})
