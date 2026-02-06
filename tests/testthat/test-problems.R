test_that("problems returns a detailed warning message", {
  expect_snapshot(vroom(I("a,b,c\nx,y,z,,"), altrep = FALSE, col_types = "ccc"))
})

test_that("problems with data parsing works for single files", {
  expect_warning(
    x <- vroom(I("x,y\n1,2\n1,1.x\n"), col_types = "dd", altrep = FALSE),
    class = "vroom_parse_issue"
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
    x <- vroom(
      c(out1, out2),
      delim = ",",
      col_types = "dd",
      altrep = F,
    ),
    class = "vroom_parse_issue"
  )
  probs <- problems(x)

  expect_equal(probs$row, c(3, 2))
  expect_equal(probs$col, c(2, 1))
  expect_equal(probs$expected, c("a double", "a double"))
  expect_equal(probs$actual, c("1.x", "3.x"))
  expect_equal(basename(probs$file), basename(c(out1, out2)))
})

test_that("problems with number of columns works for single files", {
  # libvroom reports column-count mismatches with a different format:
  # expected = "Expected N fields, got M", actual = "", col = NA
  expect_warning(
    probs3 <- problems(vroom(
      I("x,y,z\n1,2\n"),
      col_names = TRUE,
      col_types = "ddd",
      altrep = FALSE
    )),
    class = "vroom_parse_issue"
  )
  expect_equal(probs3$row, 2)
  expect_equal(probs3$expected, "Expected 3 fields, got 2")

  # col_names=FALSE: libvroom reports both type-coercion failures for the
  # header row parsed as data AND the column-count mismatch on row 2.
  expect_warning(
    probs3 <- problems(vroom(
      I("x,y,z\n1,2\n"),
      col_names = FALSE,
      col_types = "ddd",
      altrep = FALSE
    )),
    class = "vroom_parse_issue"
  )
  col_count_prob <- probs3[grepl("Expected.*fields", probs3$expected), ]
  expect_equal(col_count_prob$row, 2)
  expect_equal(col_count_prob$expected, "Expected 3 fields, got 2")

  expect_warning(
    probs4 <- problems(vroom(
      I("x,y\n1,2,3,4\n"),
      col_names = TRUE,
      col_types = "dd",
      altrep = FALSE
    )),
    class = "vroom_parse_issue"
  )
  col_count_prob <- probs4[grepl("Expected.*fields", probs4$expected), ]
  expect_equal(col_count_prob$row, 2)
  expect_equal(col_count_prob$expected, "Expected 2 fields, got 4")

  expect_warning(
    probs2 <- problems(vroom(
      I("x,y\n1,2,3,4\n"),
      col_names = FALSE,
      col_types = "dd",
      altrep = FALSE
    )),
    class = "vroom_parse_issue"
  )
  col_count_prob <- probs2[grepl("Expected.*fields", probs2$expected), ]
  expect_equal(col_count_prob$row, 2)
  expect_equal(col_count_prob$expected, "Expected 2 fields, got 4")
})

test_that("parsing problems are shown for all datatypes", {
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

    # libvroom parses eagerly, so warning fires at vroom() time
    expect_warning(
      res <- vroom(
        I("x\nxyz\n"),
        delim = ",",
        col_types = list(type),
        altrep = FALSE
      ),
      class = "vroom_parse_issue"
    )
    expect_equal(problems(res)$expected, expected)
  }

  expect_warning(
    res <- vroom(
      I("x\nxyz\n"),
      delim = ",",
      col_types = list(col_logical()),
    ),
    class = "vroom_parse_issue"
  )
})

test_that("problems that are generated more than once are not duplicated", {
  # libvroom parses eagerly: warning fires at vroom() time
  expect_warning(
    res <- vroom(
      I("x\n1\n2\n3\n4\n5\na"),
      col_types = "i",
      delim = ","
    ),
    class = "vroom_parse_issue"
  )

  probs <- problems(res)
  expect_equal(probs$row, 7)
  expect_equal(probs$col, 1)
  expect_equal(probs$expected, "an integer")
})

test_that("problems return the proper row number", {
  expect_warning(
    x <- vroom(
      I("a,b,c\nx,y,z,,"),
      altrep = FALSE,
      col_types = "ccc",
    ),
    class = "vroom_parse_issue"
  )
  expect_equal(problems(x)$row, 2)

  expect_warning(
    y <- vroom(
      I("a,b,c\nx,y,z\nx,y,z,,"),
      altrep = FALSE,
      col_types = "ccc",
    ),
    class = "vroom_parse_issue"
  )
  expect_equal(problems(y)$row, 3)

  expect_warning(
    z <- vroom(
      I("a,b,c\nx,y,z,,\nx,y,z,,\n"),
      altrep = FALSE,
      col_types = "ccc",
    ),
    class = "vroom_parse_issue"
  )
  expect_equal(problems(z)$row, c(2, 3))
})

# https://github.com/tidyverse/vroom/pull/441#discussion_r883611090
test_that("can promote vroom parse warning to error", {
  # libvroom parses eagerly: warning fires at vroom() time
  make_warning <- function() {
    vroom(
      I("a\nx\n"),
      delim = ",",
      col_types = "d",
      altrep = FALSE
    )
  }

  # Try to throw error after catching the warning
  expect_snapshot(error = TRUE, {
    withCallingHandlers(
      expr = make_warning(),
      vroom_parse_issue = function(cnd) {
        abort("oh no")
      }
    )
  })
})

test_that("emits an error message if provided incorrect input", {
  # user provides something other than a data frame
  a_vector <- c(1, 2, 3)
  expect_snapshot(problems(a_vector), error = TRUE)

  # user provides a data frame from an incorrect source
  a_tibble <- tibble::tibble(x = c(1), y = c(2))
  expect_snapshot(problems(a_tibble), error = TRUE)
})

# https://github.com/tidyverse/vroom/issues/535
test_that("problems are correct even if print is first encounter", {
  # libvroom parses eagerly: warning fires at vroom() time
  expect_warning(
    foo <- vroom(
      I("a\n1\nz\n3\nF\n5"),
      delim = ",",
      col_types = "d",
      show_col_types = FALSE
    ),
    class = "vroom_parse_issue"
  )

  probs <- problems(foo)
  expect_equal(probs$row, c(3, 5))
  expect_equal(probs$actual, c("z", "F"))

  expect_warning(
    foo <- vroom(
      I("1\nz\n3\nF\n5"),
      delim = ",",
      col_names = FALSE,
      col_types = "d",
      show_col_types = FALSE
    ),
    class = "vroom_parse_issue"
  )

  probs <- problems(foo)
  expect_equal(probs$row, c(2, 4))
  expect_equal(probs$actual, c("z", "F"))
})
