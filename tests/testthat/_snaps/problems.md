# problems returns a detailed warning message

    Code
      vroom(I("a,b,c\nx,y,z,,"), altrep = FALSE, col_types = "ccc")
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 3
        a     b     c    
        <chr> <chr> <chr>
      1 x     y     z,,  

# emits an error message if provided incorrect input

    Code
      problems(a_vector)
    Condition
      Error in `problems()`:
      ! The `x` argument of `vroom::problems()` must be a data frame from vroom:
      i You supplied a double.

---

    Code
      problems(a_tibble)
    Condition
      Error in `problems()`:
      ! `x` must have a problems attribute that is from vroom.
      i Is this object from readr?

