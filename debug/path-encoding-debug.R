tfile <- withr::local_file("b\u00e9.csv")
writeLines(c("a,b", "A,B"), tfile)
vroom::vroom(tfile, show_col_types = FALSE)

tfile <- withr::local_file("b\u00e9.csv")
writeChar("a,b\nA,B", con = tfile, eos = NULL)
vroom::vroom(tfile, show_col_types = FALSE)
