# problems with data parsing works for single files

    Code
      x <- vroom(I("x,y\n1,2\n1,1.x\n"), col_types = "dd", altrep = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

# problems works for multiple files

    Code
      x <- vroom(c(out1, out2), delim = ",", col_types = "dd", altrep = F)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

# problems with number of columns works for single files

    Code
      probs3 <- problems(vroom(I("x,y,z\n1,2\n"), col_names = TRUE, col_types = "ddd",
      altrep = FALSE))
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

---

    Code
      probs3 <- problems(vroom(I("x,y,z\n1,2\n"), col_names = FALSE, col_types = "ddd",
      altrep = FALSE))
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

---

    Code
      probs4 <- problems(vroom(I("x,y\n1,2,3,4\n"), col_names = TRUE, col_types = "dd",
      altrep = FALSE))
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

---

    Code
      probs2 <- problems(vroom(I("x,y\n1,2,3,4\n"), col_names = FALSE, col_types = "dd",
      altrep = FALSE))
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

# parsing problems are shown for all datatypes

    Code
      res[[1]][[1]]
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      [1] NA

---

    Code
      vroom_materialize(res, replace = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 1
            x
        <int>
      1    NA

---

    Code
      res[[1]][[1]]
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      integer64
      [1] <NA>

---

    Code
      vroom_materialize(res, replace = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 1
              x
        <int64>
      1      NA

---

    Code
      res[[1]][[1]]
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      [1] NA

---

    Code
      vroom_materialize(res, replace = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 1
            x
        <dbl>
      1    NA

---

    Code
      res[[1]][[1]]
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      [1] NA

---

    Code
      vroom_materialize(res, replace = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 1
            x
        <dbl>
      1    NA

---

    Code
      res[[1]][[1]]
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      [1] <NA>
      Levels: foo

---

    Code
      vroom_materialize(res, replace = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 1
        x    
        <fct>
      1 <NA> 

---

    Code
      res[[1]][[1]]
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      [1] NA

---

    Code
      vroom_materialize(res, replace = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 1
        x     
        <date>
      1 NA    

---

    Code
      res[[1]][[1]]
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      [1] NA

---

    Code
      vroom_materialize(res, replace = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 1
        x     
        <dttm>
      1 NA    

---

    Code
      res[[1]][[1]]
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      NA

---

    Code
      vroom_materialize(res, replace = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 1
        x     
        <time>
      1    NA 

---

    Code
      res <- vroom(I("x\nxyz\n"), delim = ",", col_types = list(col_logical()))
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

# problems that are generated more than once are not duplicated

    Code
      res[[1]][[6]]
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      [1] NA

# problems return the proper row number

    Code
      x <- vroom(I("a,b,c\nx,y,z,,"), altrep = FALSE, col_types = "ccc")
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

---

    Code
      y <- vroom(I("a,b,c\nx,y,z\nx,y,z,,"), altrep = FALSE, col_types = "ccc")
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

---

    Code
      z <- vroom(I("a,b,c\nx,y,z,,\nx,y,z,,\n"), altrep = FALSE, col_types = "ccc")
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)

