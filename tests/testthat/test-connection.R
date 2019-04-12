context("test-connection.R")

test_that("reading from connection is consistent with reading directly from a file", {
  expected <- vroom(vroom_example("mtcars.csv"))

  # This needs to be small enough to have a few blocks in the file, but big
  # enough to fit on the first line (until #47 is fixed)
  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 100), {
    actual <- vroom(file(vroom_example("mtcars.csv"), ""))
  })
    expect_equal(actual, expected)
})

test_that("vroom errors when the connection buffer is too small", {
  withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 32), {
    expect_error(vroom(file(vroom_example("mtcars.csv"))), "not large enough")
  })
})
