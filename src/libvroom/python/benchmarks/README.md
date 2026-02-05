# vroom-csv Benchmarks

Performance benchmarks comparing vroom-csv against other Python CSV parsers.

## Quick Start

```bash
# Install dependencies
pip install vroom-csv pandas polars pyarrow duckdb

# Run benchmarks
python benchmark_csv.py

# Custom file sizes (in MB)
python benchmark_csv.py --sizes 1,10,100,1000

# Save results to JSON
python benchmark_csv.py --output results.json
```

## Libraries Compared

- **vroom-csv**: SIMD-accelerated parser (this library)
- **pandas**: pandas.read_csv()
- **Polars**: polars.read_csv()
- **PyArrow**: pyarrow.csv.read_csv()
- **DuckDB**: duckdb.read_csv()

## Benchmark Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--sizes` | 1,10,100 | File sizes in MB |
| `--cols` | 10 | Number of columns |
| `--runs` | 5 | Runs per benchmark |
| `--output` | None | JSON output file |

## Example Output

```
CSV Parser Benchmark
============================================================

Installed libraries:
  vroom_csv: OK
  pandas: OK
  polars: OK
  pyarrow: OK
  duckdb: OK

Benchmarking 10.0 MB file (200,000 rows x 10 cols)
------------------------------------------------------------
vroom-csv       0.045s (+/- 0.002s) |    222.2 MB/s
polars          0.052s (+/- 0.003s) |    192.3 MB/s
pyarrow         0.058s (+/- 0.004s) |    172.4 MB/s
duckdb          0.089s (+/- 0.005s) |    112.4 MB/s
pandas          0.245s (+/- 0.012s) |     40.8 MB/s
```

## Notes

- Benchmarks use generated CSV data with mixed types (int, float, bool, string)
- Each library parses the file and accesses row count to ensure complete parsing
- Results may vary based on hardware, file contents, and library versions
