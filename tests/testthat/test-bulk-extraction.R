# Tests for bulk extraction API in non-ALTREP mode
# These tests verify that the extract_all() bulk extraction optimization
# works correctly for all column types

test_that("bulk extraction works for integer columns", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      data <- vroom(I("a,b,c\n1,2,3\n4,5,6\n7,8,9\n"), delim = ",")
      expect_equal(data$a, c(1L, 4L, 7L))
      expect_equal(data$b, c(2L, 5L, 8L))
      expect_equal(data$c, c(3L, 6L, 9L))
    }
  )
})

test_that("bulk extraction works for double columns", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      data <- vroom(I("a,b\n1.5,2.5\n3.5,4.5\n"), delim = ",")
      expect_equal(data$a, c(1.5, 3.5))
      expect_equal(data$b, c(2.5, 4.5))
    }
  )
})

test_that("bulk extraction works for character columns", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_CHR" = "false"),
    {
      data <- vroom(I("a,b\nfoo,bar\nbaz,qux\n"), delim = ",")
      expect_equal(data$a, c("foo", "baz"))
      expect_equal(data$b, c("bar", "qux"))
    }
  )
})

test_that("bulk extraction handles NA values correctly", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_CHR" = "false", "VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      # Integer NA
      data <- vroom(I("a\n1\nNA\n3\n"), delim = ",")
      expect_equal(data$a, c(1L, NA_integer_, 3L))

      # Double NA
      data <- vroom(I("a\n1.5\nNA\n3.5\n"), delim = ",")
      expect_equal(data$a, c(1.5, NA_real_, 3.5))

      # Character NA
      data <- vroom(I("a\nfoo\nNA\nbaz\n"), delim = ",")
      expect_equal(data$a, c("foo", NA_character_, "baz"))
    }
  )
})

test_that("bulk extraction works with custom NA values", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_CHR" = "false", "VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      data <- vroom(I("a\n1\n-999\n3\n"), delim = ",", na = c("NA", "-999"))
      expect_equal(data$a, c(1L, NA_integer_, 3L))
    }
  )
})

test_that("bulk extraction works for date columns", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      data <- vroom(
        I("date\n2023-01-15\n2023-06-30\n"),
        delim = ",",
        col_types = cols(date = col_date())
      )
      expect_equal(data$date, as.Date(c("2023-01-15", "2023-06-30")))
    }
  )
})

test_that("bulk extraction works for datetime columns", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      data <- vroom(
        I("dt\n2023-01-15T10:30:00Z\n2023-06-30T14:45:00Z\n"),
        delim = ",",
        col_types = cols(dt = col_datetime())
      )
      expected <- as.POSIXct(
        c("2023-01-15 10:30:00", "2023-06-30 14:45:00"),
        tz = "UTC"
      )
      expect_equal(data$dt, expected)
    }
  )
})

test_that("bulk extraction works for time columns", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      data <- vroom(
        I("time\n10:30:00\n14:45:30\n"),
        delim = ",",
        col_types = cols(time = col_time())
      )
      expect_equal(as.numeric(data$time), c(10 * 3600 + 30 * 60, 14 * 3600 + 45 * 60 + 30))
    }
  )
})

test_that("bulk extraction works for factor columns with explicit levels", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_CHR" = "false"),
    {
      data <- vroom(
        I("x\na\nb\na\nc\n"),
        delim = ",",
        col_types = cols(x = col_factor(levels = c("a", "b", "c")))
      )
      expect_equal(data$x, factor(c("a", "b", "a", "c"), levels = c("a", "b", "c")))
    }
  )
})

test_that("bulk extraction works for factor columns with implicit levels", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_CHR" = "false"),
    {
      data <- vroom(
        I("x\na\nb\na\nc\n"),
        delim = ",",
        col_types = cols(x = col_factor())
      )
      expect_s3_class(data$x, "factor")
      expect_equal(as.character(data$x), c("a", "b", "a", "c"))
    }
  )
})

test_that("bulk extraction works for big integer columns", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      data <- vroom(
        I("big\n9007199254740992\n9007199254740993\n"),
        delim = ",",
        col_types = cols(big = col_big_integer())
      )
      expect_s3_class(data$big, "integer64")
    }
  )
})

test_that("bulk extraction works for number columns with grouping", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      data <- vroom(
        I("x\n1,000\n2,500.50\n"),
        delim = "\t",
        col_types = cols(x = col_number())
      )
      expect_equal(data$x, c(1000, 2500.50))
    }
  )
})

test_that("bulk extraction handles empty strings correctly", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_CHR" = "false"),
    {
      data <- vroom(I("a,b\n,foo\nbar,\n"), delim = ",", na = "")
      expect_equal(data$a, c(NA_character_, "bar"))
      expect_equal(data$b, c("foo", NA_character_))
    }
  )
})

test_that("bulk extraction works with quoted fields", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_CHR" = "false"),
    {
      data <- vroom(I('a,b\n"foo, bar","baz"\n"qux","quux, corge"\n'), delim = ",")
      expect_equal(data$a, c("foo, bar", "qux"))
      expect_equal(data$b, c("baz", "quux, corge"))
    }
  )
})

test_that("bulk extraction handles escape sequences", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_CHR" = "false"),
    {
      data <- vroom(I('a\n"foo ""bar"" baz"\n'), delim = ",")
      expect_equal(data$a, 'foo "bar" baz')
    }
  )
})

test_that("bulk extraction with multi-threaded parsing", {
  withr::with_envvar(
    c("VROOM_USE_ALTREP_CHR" = "false", "VROOM_USE_ALTREP_NUMERICS" = "false"),
    {
      # Generate larger data to trigger multi-threaded parsing
      n <- 1000
      content <- paste0(
        "a,b,c\n",
        paste(seq_len(n), seq_len(n) + 0.5, letters[(seq_len(n) - 1) %% 26 + 1],
          sep = ",", collapse = "\n"
        ),
        "\n"
      )
      data <- vroom(I(content), delim = ",", num_threads = 2)
      expect_equal(nrow(data), n)
      expect_equal(data$a, seq_len(n))
    }
  )
})
