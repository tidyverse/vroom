# vroom errors informatively when it cannot guess delimiter

    Code
      vroom(I("foo\nbar\nbaz\n"), col_types = list())
    Condition
      Error:
      ! Could not guess the delimiter.
      i Use `vroom(delim =)` to explicitly specify the delimiter.

