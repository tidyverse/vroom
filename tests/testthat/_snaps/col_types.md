# all col_types can be reported with color

    Code
      spec(dat)
    Output
      cols(
        skip = col_skip(),
        guess = [31mcol_character()[39m,
        character = [31mcol_character()[39m,
        factor = [31mcol_factor(levels = NULL, ordered = FALSE, include_na = FALSE)[39m,
        logical = [33mcol_logical()[39m,
        double = [32mcol_double()[39m,
        integer = [32mcol_integer()[39m,
        big_integer = [32mcol_big_integer()[39m,
        number = [32mcol_number()[39m,
        date = [34mcol_date(format = "")[39m,
        datetime = [34mcol_datetime(format = "")[39m,
        .delim = ","
      )

# cols() errors for invalid collector objects

    Code
      cols(a = col_character(), b = 123)
    Condition
      Error in `cols()`:
      ! Column specifications must be created with the `col_*()` functions or their abbreviated character names.
      x Bad specification at position: 2.

---

    Code
      cols(a = col_character(), .default = 123)
    Condition
      Error in `cols()`:
      ! Column specifications must be created with the `col_*()` functions or their abbreviated character names.
      x Bad `.default` specification.

# cols() errors for invalid character specifications

    Code
      cols(X = "z", Y = "i", Z = col_character())
    Condition
      Error in `lapply()`:
      ! Unknown column type specification: "z"

---

    Code
      cols(X = "i", .default = "wut")
    Condition
      Error in `cols()`:
      ! Unknown column type specification: "wut"

# as.col_spec() errors for unhandled input type

    Code
      vroom(I("whatever"), col_types = data.frame())
    Condition
      Error in `vroom()`:
      ! `col_types` must be `NULL`, a `cols()` specification, or a string, not a <data.frame> object.

# as.col_spec() errors for unrecognized single-letter spec

    Code
      vroom(I("whatever"), col_types = "dz")
    Condition
      Error in `lapply()`:
      ! Unknown column type specification: "z"

