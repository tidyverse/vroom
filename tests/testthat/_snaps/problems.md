# problems with data parsing works for single files

    Code
      x <- vroom(I("x,y\n1,2\n1,1.x\n"), col_types = "dd", altrep = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Code
      problems(x)
    Output
      # A tibble: 1 x 5
          row   col expected actual file                                              
        <int> <int> <chr>    <chr>  <chr>                                             
      1     3     2 a double 1.x    /private/var/folders/4g/9jcx0hbd6r92152m0qrq64yw0~

# problems works for multiple files

    Code
      x <- vroom(c(out1, out2), delim = ",", col_types = "dd", altrep = F)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Code
      problems(x)
    Output
      # A tibble: 2 x 5
          row   col expected actual file                                              
        <int> <int> <chr>    <chr>  <chr>                                             
      1     3     2 a double 1.x    /private/var/folders/4g/9jcx0hbd6r92152m0qrq64yw0~
      2     2     1 a double 3.x    /private/var/folders/4g/9jcx0hbd6r92152m0qrq64yw0~

# problems with number of columns works for single files

    Code
      v1 <- vroom(I("x,y,z\n1,2\n"), col_names = TRUE, col_types = "ddd", altrep = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Code
      problems(v1)
    Output
      # A tibble: 1 x 5
          row   col expected  actual    file                                          
        <int> <int> <chr>     <chr>     <chr>                                         
      1     2     2 3 columns 2 columns /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~

---

    Code
      v2 <- vroom(I("x,y,z\n1,2\n"), col_names = FALSE, col_types = "ddd", altrep = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Code
      problems(v2)
    Output
      # A tibble: 4 x 5
          row   col expected  actual    file                                          
        <int> <int> <chr>     <chr>     <chr>                                         
      1     1     1 a double  x         /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~
      2     1     2 a double  y         /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~
      3     1     3 a double  z         /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~
      4     2     2 3 columns 2 columns /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~

---

    Code
      v3 <- vroom(I("x,y\n1,2,3,4\n"), col_names = TRUE, col_types = "dd", altrep = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Code
      problems(v3)
    Output
      # A tibble: 2 x 5
          row   col expected  actual    file                                          
        <int> <int> <chr>     <chr>     <chr>                                         
      1     2     2 a double  2,3,4     /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~
      2     2     4 2 columns 4 columns /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~

---

    Code
      v4 <- vroom(I("x,y\n1,2,3,4\n"), col_names = FALSE, col_types = "dd", altrep = FALSE)
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Code
      problems(v4)
    Output
      # A tibble: 4 x 5
          row   col expected  actual    file                                          
        <int> <int> <chr>     <chr>     <chr>                                         
      1     1     1 a double  x         /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~
      2     1     2 a double  y         /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~
      3     2     2 a double  2,3,4     /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~
      4     2     4 2 columns 4 columns /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~

# parsing problems are shown for all datatypes

    Code
      vroom(I("x\nxyz\n"), delim = ",", col_types = list(col_logical()))
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Output
      # A tibble: 1 x 1
        x    
        <lgl>
      1 NA   

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
    Code
      res[[1]][[6]]
    Output
      [1] NA
    Code
      problems(res)
    Output
      # A tibble: 1 x 5
          row   col expected   actual file                                            
        <int> <int> <chr>      <chr>  <chr>                                           
      1     7     1 an integer a      /private/var/folders/4g/9jcx0hbd6r92152m0qrq64y~

# problems return the proper row number

    Code
      x <- vroom(I("a,b,c\nx,y,z,,"), altrep = FALSE, col_types = "ccc")
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Code
      problems(x)
    Output
      # A tibble: 1 x 5
          row   col expected  actual    file                                          
        <int> <int> <chr>     <chr>     <chr>                                         
      1     2     5 3 columns 5 columns /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~

---

    Code
      y <- vroom(I("a,b,c\nx,y,z\nx,y,z,,"), altrep = FALSE, col_types = "ccc")
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Code
      problems(y)
    Output
      # A tibble: 1 x 5
          row   col expected  actual    file                                          
        <int> <int> <chr>     <chr>     <chr>                                         
      1     3     5 3 columns 5 columns /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~

---

    Code
      z <- vroom(I("a,b,c\nx,y,z,,\nx,y,z,,\n"), altrep = FALSE, col_types = "ccc")
    Condition
      Warning:
      One or more parsing issues, call `problems()` on your data frame for details, e.g.:
        dat <- vroom(...)
        problems(dat)
    Code
      problems(z)
    Output
      # A tibble: 2 x 5
          row   col expected  actual    file                                          
        <int> <int> <chr>     <chr>     <chr>                                         
      1     2     5 3 columns 5 columns /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~
      2     3     5 3 columns 5 columns /private/var/folders/4g/9jcx0hbd6r92152m0qrq6~

