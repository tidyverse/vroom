test_that("col_select works", {
  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = 1, col_types = list())), "model")

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = 1:3, col_types = list())), c("model", "mpg", "cyl"))

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = list(1, 5, 7), col_types = list())), c("model", "hp", "wt"))

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = c("model", "hp", "wt"), col_types = list())), c("model", "hp", "wt"))

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = c(model:cyl), col_types = list())), c("model", "mpg", "cyl"))
})

test_that("col_select with negations works", {
  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = -1, col_types = list())),
    c("mpg", "cyl", "disp", "hp", "drat", "wt", "qsec", "vs", "am", "gear", "carb")
  )

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = -(1:3), col_types = list())),
    c("disp", "hp", "drat", "wt", "qsec", "vs", "am", "gear", "carb")
  )

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = -c(1, 5, 7), col_types = list())),
    c("mpg", "cyl", "disp", "drat", "qsec", "vs", "am", "gear", "carb")
  )

  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = -c(model, hp, wt), col_types = list())),
    c("mpg", "cyl", "disp", "drat", "qsec", "vs", "am", "gear", "carb")
  )
})

test_that("col_select with renaming", {
  expect_equal(colnames(vroom(vroom_example("mtcars.csv"), col_select = list(car = model, everything()), col_types = list())),
    c("car", "mpg", "cyl", "disp", "hp", "drat", "wt", "qsec", "vs", "am", "gear", "carb")
  )
})

test_that("col_select works with vroom_fwf", {

  spec <- fwf_empty(test_path("fwf-trailing.txt"))

  expect_equal(
    colnames(
      vroom_fwf(
        test_path("fwf-trailing.txt"),
        spec,
        col_select = list(foo = X1, X2),
        col_types = list()
      )
    ),
    c("foo", "X2")
  )
})

test_that("col_select can select the id column", {

  expect_named(
    vroom(vroom_example("mtcars.csv"), id = "path", col_select = list(model, mpg, path), col_types = list()),
    c("model", "mpg", "path")
  )

  expect_named(
    vroom(vroom_example("mtcars.csv"), id = "path", col_select = list(path, model, mpg), col_types = list()),
    c("path", "model", "mpg")
  )
})

test_that("col_select works with col_names = FALSE", {
  res <- vroom(I("foo\tbar\n1\t2\n"), col_names = FALSE, col_select = 1, col_types = list())
  expect_equal(res[[1]], c("foo", "1"))

  res2 <- vroom(I("foo\tbar\n1\t2\n"), col_names = FALSE, col_select = c(X2), col_types = list())
  expect_equal(res2[[1]], c("bar", "2"))
})
