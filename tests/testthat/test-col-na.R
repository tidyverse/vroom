test_that("collector-level na overrides global na", {
  test_vroom(
    "a,b,c\na,foo,REFUSED\nb,REFUSED,NA\nOMITTED,bar,OMITTED\n",
    col_types = cols(
      a = col_character(na = "OMITTED"),
      b = col_character(na = "REFUSED"),
      c = col_character()
    ),
    na = "NA",
    equals = tibble::tibble(
      a = c("a", "b", NA),
      b = c("foo", NA, "bar"),
      c = c("REFUSED", NA, "OMITTED"),
    )
  )
})

test_that("collector-level na works with col_guess", {
  test_vroom(
    "a,b,c\n1,1.1,REFUSED\n2,REFUSED,NA\nOMITTED,2.2,OMITTED\n",
    col_types = cols(
      a = col_guess(na = "OMITTED"),
      b = col_guess(na = "REFUSED"),
      c = col_guess(),
    ),
    na = "NA",
    equals = tibble::tibble(
      a = c(1, 2, NA),
      b = c(1.1, NA, 2.2),
      c = c("REFUSED", NA, "OMITTED"),
    )
  )
})
