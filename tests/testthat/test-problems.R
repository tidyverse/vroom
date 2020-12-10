test_that("problems works for single files", {
  x <- vroom("x,y\n1,2\n1,1.x\n", col_types = "dd", altrep = FALSE)
  probs <- vroom_problems(x)

  expect_equal(probs$row, 3)
  expect_equal(probs$col, 2)
  expect_equal(probs$expected, "a double")
  expect_equal(probs$actual, "1.x")
})

test_that("problems works for single files", {

  out1 <- file.path(tempdir(), "out1.txt")
  out2 <- file.path(tempdir(), "out2.txt")
  on.exit(unlink(c(out1, out2)))

  writeLines("x,y\n1,2\n1,1.x\n2,2", out1)
  writeLines("x,y\n3.x,4\n1,2\n2,2", out2)

  x <- vroom::vroom(c(out1, out2), delim = ",", col_types = "dd", altrep=F)
  probs <- vroom_problems(x)

  expect_equal(probs$row, c(3, 2))
  expect_equal(probs$col, c(2, 1))
  expect_equal(probs$expected, c("a double", "a double"))
  expect_equal(probs$actual, c("1.x", "3.x"))
  expect_equal(basename(probs$file), basename(c(out1, out2)))
})
