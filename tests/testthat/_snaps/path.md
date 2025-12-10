# vroom errors if the file does not exist

    Code
      vroom(tf, col_types = list())
    Condition
      Error:
      ! '<tempfile>' does not exist.

---

    Code
      vroom("does-not-exist.csv", col_types = list())
    Condition
      Error:
      ! 'does-not-exist.csv' does not exist in current working directory: '<workdir>'.

# vroom errors via https on non-gz file

    Code
      vroom(url, col_types = list())
    Condition
      Error:
      ! Reading from remote ".bz2" compressed files is not supported.
      i Download the file locally first.

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
      ! `some_arg_name` must be one of the supported input types:
      * A filepath or character vector of filepaths
      * A connection or list of connections
      * Literal or raw input
      x `some_arg_name` is a list

# multiple files with non-ASCII encoding fails informatively

    Code
      vroom(c(input, input), locale = locale(encoding = "UTF-16"))
    Condition
      Error in `vroom()`:
      ! Encoding "UTF-16" is only supported when reading a single input.
      i `file` has length 2.

# writing to .zip without archive package fails informatively

    Code
      vroom_write(mtcars, tempfile(fileext = ".zip"))
    Condition
      Error in `vroom_write()`:
      ! Can only read from, not write to, ".zip" files.
      i Install the archive package to write ".zip" files.

