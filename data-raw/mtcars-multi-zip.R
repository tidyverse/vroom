library(vroom)

# this is productive in terms of how the files are named inside the zip archive
# i.e. I prefer just a filename vs. a full path that refers to my machine
withr::local_dir("inst/extdata/")

files <- grep("mtcars-[468].csv", list.files(), value = TRUE)
files

target <- "mtcars-multi-cyl.zip"
# if target exists, nuke it first
if (file.exists(target)) unlink(target)

zip(target, files)

# check that the contents look as expected
unzip(target, list = TRUE)

withr::deferred_run()
