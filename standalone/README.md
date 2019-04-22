## Build a standalone version of vroom

This is a simple example of a standalone C++ version using the vroom library.
It simply returns the total number of fields in a delimited file.

## Run the fuzz tester

Running the following with compile the standalone vroom and run afl-fuzz on it with 8 workers.

If you are running on macOS you should probably use setup a ram disk so the heavy
writing doesn't destroy your SSD, otherwise point `FUZZ_DIR` at the directory
you want to run the fuzzing in.

```shell
make clean
FUZZ_DIR=/Volumns/ram_disk make fuzz
```
