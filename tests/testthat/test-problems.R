test_that("problems with data parsing works for single files", {
  expect_snapshot({
    x <- vroom(I("x,y\n1,2\n1,1.x\n"), col_types = "dd", altrep = FALSE)
    problems(x)
  })

})

test_that("problems works for multiple files", {

  out1 <- file.path(tempdir(), "out1.txt")
  out2 <- file.path(tempdir(), "out2.txt")
  on.exit(unlink(c(out1, out2)))

  writeLines("x,y\n1,2\n1,1.x\n2,2", out1)
  writeLines("x,y\n3.x,4\n1,2\n2,2", out2)

  expect_snapshot({
    x <- vroom(c(out1, out2), delim = ",", col_types = "dd", altrep=F)
    problems(x)
  })

})

test_that("problems with number of columns works for single files", {

  expect_snapshot({
    v1 <- vroom(I("x,y,z\n1,2\n"), col_names = TRUE, col_types = "ddd", altrep = FALSE)
    problems(v1)
  })

  expect_snapshot({
    v2 <- vroom(I("x,y,z\n1,2\n"), col_names = FALSE, col_types = "ddd", altrep = FALSE)
    problems(v2)
  })

  expect_snapshot({
    v3 <- vroom(I("x,y\n1,2,3,4\n"), col_names = TRUE, col_types = "dd", altrep = FALSE)
    problems(v3)
  })

 expect_snapshot({
    v4 <- vroom(I("x,y\n1,2,3,4\n"), col_names = FALSE, col_types = "dd", altrep = FALSE)
    problems(v4)
  })
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

    expect_snapshot(vroom(I("x\nxyz\n"), delim = ",", col_types = list(col_logical())))
})

test_that("problems that are generated more than once are not duplicated", {
  # On versions of R without ALTREP the warnings will happen at different times,
  # so we skip this test in those cases
  skip_if(getRversion() < "3.5")

  res <- vroom(I("x\n1\n2\n3\n4\n5\na"), col_types = "i", delim = ",")

  expect_snapshot({
    # generate first problem
    res[[1]][[6]]

    # generate the same problem again
    res[[1]][[6]]

    problems(res)
  })
})

test_that("problems return the proper row number", {
  expect_snapshot({
    x <- vroom(I("a,b,c\nx,y,z,,"), altrep = FALSE, col_types = "ccc")
    problems(x)
  })

  expect_snapshot({
    y <- vroom(I("a,b,c\nx,y,z\nx,y,z,,"), altrep = FALSE, col_types = "ccc")
    problems(y)
  })

  expect_snapshot({
    z <- vroom(I("a,b,c\nx,y,z,,\nx,y,z,,\n"), altrep = FALSE, col_types = "ccc")
    problems(z)
  })
})
