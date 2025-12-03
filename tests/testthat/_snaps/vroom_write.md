# fails to create file in non-existent directory

    Code
      vroom_write(mtcars, file.path(tempdir(), "x", "y"), "\t")
    Condition
      Error:
      ! Cannot open file for writing:
      * '<tempdir>/x/y'

# Can change the escape behavior for quotes

    Code
      vroom_format(df, "\t", escape = "invalid")
    Condition
      Error in `vroom_format()`:
      ! `escape` must be one of "double", "backslash", or "none", not "invalid".

