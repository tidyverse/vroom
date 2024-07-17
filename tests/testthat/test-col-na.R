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
