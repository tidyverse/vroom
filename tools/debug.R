devtools::clean_dll()
devtools::load_all()

# https://github.com/tidyverse/vroom/issues/429
x <- vroom::vroom("investigations/my_file.CSV") # often segfaults here
tail(x[,12:15])

# here's where the problem is, when it occurs
# delimited_index.cc line 416
# has_quote = *begin == quote_;
# EXC_BAD_ACCESS
# in the bad situation, begin == end and both are `\0`
# happens when file is missing a final newline AND is missing the final field
# the pointers to this field's beginning and end are the same and are
# essentially past (at) the end of the file

# the segfault always seems to happen in RStudio

# once I'm in lldb inside VS Code, the bug usually goes away, i.e. dereferencing
# *begin is OK
# but ultimately it is clear which line is the problem (see above)

# if I specify the last col_type, reading (appears to) work
# x <- vroom::vroom(
#   "investigations/my_file.CSV",
#   col_types = list(Average_SOC_Required = "n")
# )
# tail(x[,12:15]) # but now it crashes here

# can I make this happen with a smaller file if I change
# guess_max or the buffer size? nope, I never succeeded to do this

# small example to better understand the index
# writeChar("X,Y\r\na,bbb\r\ne,", "investigations/mini.csv", eos = NULL)
#
# | |       |                          idx_[0]
#             | |           |  |  |  | idx_[1]
# 0 1 2  3  4 5 6 7 8 9 10 11 12 13 14
# X , Y \r \n a , b b b \r \n  e  ,

# withr::with_envvar(c("VROOM_CONNECTION_SIZE" = 10), {
#   x <- vroom(file("investigations/mini.csv"))
# })
