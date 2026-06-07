# vroom errors informatively when it cannot guess delimiter

    Code
      vroom(I("foo\nbar\nbaz\n"), col_types = list())
    Condition
      Error:
      ! Could not guess the delimiter.
      i Use `vroom(delim =)` to explicitly specify the delimiter.

# id column name collision with data column produces error (#536)

    Code
      vroom(I("a,hello\n1,2\n"), id = "hello", show_col_types = FALSE)
    Condition
      Error:
      ! The `id` column name ("hello") is the same as one of the data column names.

---

    Code
      vroom(I("a,b\n1,2\n"), id = "a", show_col_types = FALSE)
    Condition
      Error:
      ! The `id` column name ("a") is the same as one of the data column names.

