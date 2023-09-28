# vroom() informs user to use I() for literal data (or not)

    Code
      x <- vroom("a,b,c,d\n1,2,3,4", show_col_types = FALSE)
    Condition
      Warning:
      The `file` argument of `vroom()` must use `I()` for literal data as of vroom 1.5.0.
        
        # Bad:
        vroom("X,Y\n1.5,2.3\n")
        
        # Good:
        vroom(I("X,Y\n1.5,2.3\n"))

# standardise_path() errors for a mix of connection and not connection

    Code
      f(list(file, conn))
    Condition
      Error in `f()`:
      ! `some_arg_name` cannot be a mix of connection and non-connection inputs

# standardise_path() errors for invalid input

    Code
      f(as.list(files))
    Condition
      Error in `f()`:
      ! `some_arg_name` is not one of the supported inputs:
      * A filepath or character vector of filepaths
      * A connection or list of connections
      * Literal or raw input

