#!/usr/bin/env python3
"""
Profiling script for Arrow export.

Run with py-spy:
  py-spy record -o profile.svg -- python profile_arrow_export.py

This script exercises the key code paths that were optimized:
1. materialize_columns() via Arrow export
2. Table.column() access
3. NullValueConfig.is_null_value() via Arrow type conversion
"""

import random
import string
import tempfile
from pathlib import Path


def generate_csv_file(path: Path, num_rows: int, num_cols: int, null_ratio: float = 0.1) -> None:
    """Generate a CSV file with mixed data types and null values."""
    random.seed(42)
    null_values = ["", "NA", "N/A", "null", "NULL", "None", "NaN"]

    with open(path, "w") as f:
        headers = [f"col_{i}" for i in range(num_cols)]
        f.write(",".join(headers) + "\n")

        for _ in range(num_rows):
            row = []
            for col_idx in range(num_cols):
                if random.random() < null_ratio:
                    row.append(random.choice(null_values))
                else:
                    col_type = col_idx % 4
                    if col_type == 0:
                        row.append(str(random.randint(-1000000, 1000000)))
                    elif col_type == 1:
                        row.append(f"{random.uniform(-1000, 1000):.6f}")
                    elif col_type == 2:
                        row.append(random.choice(["true", "false"]))
                    else:
                        length = random.randint(5, 20)
                        s = "".join(random.choices(string.ascii_letters, k=length))
                        row.append(s)
            f.write(",".join(row) + "\n")


def main():
    import pyarrow
    import vroom_csv

    # Generate test file - use a moderate size for profiling
    num_rows = 100000
    num_cols = 20

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "profile_test.csv"
        print(f"Generating CSV file with {num_rows:,} rows x {num_cols} cols...")
        generate_csv_file(csv_path, num_rows, num_cols)
        print(f"File size: {csv_path.stat().st_size / 1024 / 1024:.1f} MB")

        # Warm up - parse once
        print("Warming up...")
        table = vroom_csv.read_csv(str(csv_path))
        del table

        # Profile: Parse CSV
        print("\n=== Profiling CSV Parsing ===")
        for i in range(3):
            table = vroom_csv.read_csv(str(csv_path))
            _ = table.num_rows

        # Profile: Arrow export (materialize_columns + build_column_array)
        print("\n=== Profiling Arrow Export ===")
        for i in range(10):
            table = vroom_csv.read_csv(str(csv_path))
            arrow_table = pyarrow.table(table)
            _ = arrow_table.num_rows
            del table, arrow_table

        # Profile: Individual column access
        print("\n=== Profiling Column Access ===")
        for i in range(10):
            table = vroom_csv.read_csv(str(csv_path))
            for col in range(table.num_columns):
                _ = table.column(col)
            del table

        # Profile: Batched Arrow export
        print("\n=== Profiling Batched Arrow Export ===")
        for i in range(5):
            total = 0
            for batch in vroom_csv.read_csv_batched(str(csv_path), batch_size=10000):
                arrow_batch = pyarrow.table(batch)
                total += arrow_batch.num_rows
            print(f"  Batch {i+1}: {total} rows")

        print("\nDone!")


if __name__ == "__main__":
    main()
