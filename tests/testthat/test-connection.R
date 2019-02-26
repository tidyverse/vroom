context("test-connection.R")

test_that("reading from connection is consistent with reading directly from a file", {
  expected <- vroom(vroom_example("mtcars.csv"))

  # This needs to be small enough to have a few blocks in the file, but big
  # enough to fit on the first line (until #47 is fixed)
  withr::with_options(c("vroom.connection_size" = 100), {
    expect_equal(vroom(file(vroom_example("mtcars.csv"), "")), expected)
  })
})
