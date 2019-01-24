context("test-vroom.R")

test_that("vroom can read a tsv", {
  test_vroom("a\tb\tc\n1\t2\t3\n",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom can read a csv", {
  test_vroom("a,b,c\n1,2,3\n", delim = ",",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom guesses columns with NAs", {
  test_vroom("a,b,c\nNA,2,3\n4,5,6", delim = ",",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_vroom("a,b,c\nfoo,2,3\n4,5,6", delim = ",", na = "foo",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_vroom("a,b,c\nfoo,2,3\n4.0,5,6", delim = ",", na = "foo",
    equals = tibble::tibble(a = c(NA, 4), b = c(2, 5), c = c(3, 6))
  )

  test_vroom("a,b,c\nfoo,2,3\nbar,5,6", delim = ",", na = "foo",
    equals = tibble::tibble(a = c(NA, "bar"), b = c(2, 5), c = c(3, 6))
  )
})

test_that("vroom can trim whitespace", {
  test_vroom('a,b,c\n foo ,  bar  ,baz', delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  test_vroom('a,b,c\n\tfoo\t,\t\tbar\t\t,baz', delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  # whitespace trimmed before quotes
  test_vroom('a,b,c\n "foo" ,  "bar"  ,"baz"', delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  # whitespace kept inside quotes
  test_vroom('a,b,c\n "foo" ,  " bar"  ,"\tbaz"', delim = ",",
    equals = tibble::tibble(a = "foo", b = " bar", c = "\tbaz")
  )
})

test_that("vroom can read files with quotes", {
  test_vroom('"a","b","c"\n"foo","bar","baz"', delim = ",",
    equals = tibble::tibble(a = "foo", b = "bar", c = "baz")
  )

  test_vroom('"a","b","c"\n",foo","bar","baz"', delim = ",",
    equals = tibble::tibble(a = ",foo", b = "bar", c = "baz")
  )

  test_vroom("'a','b','c'\n',foo','bar','baz'", delim = ",", quote = "'",
    equals = tibble::tibble(a = ",foo", b = "bar", c = "baz")
  )
})

test_that("vroom escapes double quotes", {
  test_vroom('"a","b","c"\n"""fo""o","b""""ar","baz"""', delim = ",",
    equals = tibble::tibble(a = "\"fo\"o", b = "b\"\"ar", c = "baz\"")
  )
})

test_that("vroom escapes backslashes", {
  test_vroom('a,b,c\n\\,foo,\\"ba\\"r,baz\\"', delim = ",", escape_backslash = TRUE,
    equals = tibble::tibble(a = ",foo", b = "\"ba\"r", c = "baz\"")
  )
})

test_that("vroom ignores leading whitespace", {
  test_vroom('\n\n   \t \t\n  \n\na,b,c\n1,2,3"', delim = ",",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom ignores comments", {
  test_vroom('\n\n \t #a,b,c\na,b,c\n1,2,3"', delim = ",", comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})

test_that("vroom respects skip", {
  test_vroom('#a,b,c\na,b,c\n1,2,3"', delim = ",", skip = 1,
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom('#a,b,c\na,b,c\n1,2,3"', delim = ",", skip = 1, comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom('#a,b,c\nasdfasdf\na,b,c\n1,2,3"', delim = ",", skip = 2, comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )

  test_vroom('\n\n#a,b,c\nasdfasdf\na,b,c\n1,2,3"', delim = ",", skip = 4, comment = "#",
    equals = tibble::tibble(a = 1, b = 2, c = 3)
  )
})
