test_that("problems with data parsing works for single files", {
  expect_warning(
    x <- vroom(I("x,y\n1,2\n1,1.x\n"), col_types = "dd", altrep = FALSE)
  )
  probs <- problems(x)

  expect_equal(probs$row, 3)
  expect_equal(probs$col, 2)
  expect_equal(probs$expected, "a double")
  expect_equal(probs$actual, "1.x")
})

test_that("problems works for multiple files", {

  out1 <- file.path(tempdir(), "out1.txt")
  out2 <- file.path(tempdir(), "out2.txt")
  on.exit(unlink(c(out1, out2)))

  writeLines("x,y\n1,2\n1,1.x\n2,2", out1)
  writeLines("x,y\n3.x,4\n1,2\n2,2", out2)

  expect_warning(
    x <- vroom(c(out1, out2), delim = ",", col_types = "dd", altrep=F)
  )
  probs <- problems(x)

  expect_equal(probs$row, c(3, 2))
  expect_equal(probs$col, c(2, 1))
  expect_equal(probs$expected, c("a double", "a double"))
  expect_equal(probs$actual, c("1.x", "3.x"))
  expect_equal(basename(probs$file), basename(c(out1, out2)))
})

test_that("problems with number of columns works for single files", {
  probs3 <- expect_warning(problems(vroom(I("x,y,z\n1,2\n"), col_names = TRUE, col_types = "ddd", altrep = FALSE)))
  expect_equal(probs3$row, 2)
  expect_equal(probs3$col, 2)
  expect_equal(probs3$expected, "3 columns")
  expect_equal(probs3$actual, "2 columns")

  probs3 <- expect_warning(problems(vroom(I("x,y,z\n1,2\n"), col_names = FALSE, col_types = "ddd", altrep = FALSE)))
  expect_equal(probs3$row[[4]], 2)
  expect_equal(probs3$col[[4]], 2)
  expect_equal(probs3$expected[[4]], "3 columns")
  expect_equal(probs3$actual[[4]], "2 columns")

  probs4 <- expect_warning(problems(vroom(I("x,y\n1,2,3,4\n"), col_names = TRUE, col_types = "dd", altrep = FALSE)))
  expect_equal(probs4$row[[2]], 2)
  expect_equal(probs4$col[[2]], 4)
  expect_equal(probs4$expected[[2]], "2 columns")
  expect_equal(probs4$actual[[2]], "4 columns")

  probs2 <- expect_warning(problems(vroom(I("x,y\n1,2,3,4\n"), col_names = FALSE, col_types = "dd", altrep = FALSE)))
  expect_equal(probs2$row[[4]], 2)
  expect_equal(probs2$col[[4]], 4)
  expect_equal(probs2$expected[[4]], "2 columns")
  expect_equal(probs2$actual[[4]], "4 columns")
})

test_that("parsing problems are shown for all datatypes", {
  skip_if(getRversion() < "3.5")

  types <- list(
    "an integer" = col_integer(),
    "a big integer" = col_big_integer(),
    "a double" = col_double(),
    "a number" = col_number(),
    "value in level set" = col_factor(levels = "foo"),
    "date in ISO8601" = col_date(),
    "date in ISO8601" = col_datetime(),
    "time in ISO8601" = col_time()
  )

  for (i in seq_along(types)) {
    type <- types[[i]]
    expected <- names(types)[[i]]

    res <- vroom(I("x\nxyz\n"), delim = ",", col_types = list(type), altrep = TRUE)

    # This calls the type_Elt function
    expect_warning(res[[1]][[1]], "One or more parsing issues")
    expect_equal(problems(res)$expected, expected)

    res <- vroom(I("x\nxyz\n"), delim = ",", col_types = list(type), altrep = TRUE)

    # This calls the read_type function
    expect_warning(vroom_materialize(res, replace = FALSE), "One or more parsing issues")
    expect_equal(problems(res)$expected, expected)
  }


    expect_warning(res <- vroom(I("x\nxyz\n"), delim = ",", col_types = list(col_logical())), "One or more parsing issues")
})

test_that("problems that are generated more than once are not duplicated", {
  # On versions of R without ALTREP the warnings will happen at different times,
  # so we skip this test in those cases
  skip_if(getRversion() < "3.5")

  res <- vroom(I("x\n1\n2\n3\n4\n5\na"), col_types = "i", delim = ",")

  # generate first problem
  expect_warning(res[[1]][[6]], "One or more parsing issues")

  # generate the same problem again
  res[[1]][[6]]

  probs <- problems(res)
  expect_equal(probs$row, 7)
  expect_equal(probs$col, 1)
  expect_equal(probs$expected, "an integer")
})

test_that("problems return the proper row number", {
  expect_warning(
    x <- vroom(I("a,b,c\nx,y,z,,"), altrep = FALSE, col_types = "ccc")
  )
  expect_equal(problems(x)$row, 2)

  expect_warning(
    y <- vroom(I("a,b,c\nx,y,z\nx,y,z,,"), altrep = FALSE, col_types = "ccc")
  )
  expect_equal(problems(y)$row, 3)

  expect_warning(
    z <- vroom(I("a,b,c\nx,y,z,,\nx,y,z,,\n"), altrep = FALSE, col_types = "ccc")
  )
  expect_equal(problems(z)$row, c(2, 3))
})
