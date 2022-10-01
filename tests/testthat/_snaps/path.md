# vroom() informs user to use I() for literal data (or not)

    Code
      x <- vroom("a,b,c,d\n1,2,3,4", show_col_types = FALSE)
    Condition
      Warning:
      The `file` argument of `vroom()` must use `I()` for literal data as of vroom 1.5.0.
        
        # Bad:
        vroom("foo\nbar\n")
        
        # Good:
        vroom(I("foo\nbar\n"))

---

    Code
      x <- f("a,b,c,d\n1,2,3,4")
    Condition
      Warning:
      The `file` argument of `vroom()` must use `I()` for literal data as of vroom 1.5.0.
        
        # Bad:
        vroom("foo\nbar\n")
        
        # Good:
        vroom(I("foo\nbar\n"))

