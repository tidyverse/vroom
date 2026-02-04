#!/usr/bin/env python3
"""Generate test CSV files of various sizes and characteristics for benchmarking."""

import argparse
import csv
import os
import random
import string
from datetime import datetime, timedelta
from pathlib import Path


def random_string(length: int) -> str:
    """Generate a random string of given length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def random_date() -> str:
    """Generate a random ISO8601 date."""
    start = datetime(2000, 1, 1)
    end = datetime(2025, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime('%Y-%m-%d')


def generate_narrow_numeric(output_path: Path, rows: int):
    """Generate a narrow CSV with mostly numeric data (10 columns)."""
    print(f"Generating narrow numeric CSV: {output_path} ({rows:,} rows)")

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)

        # Header
        writer.writerow(['id', 'int_val', 'float_val', 'small_int', 'big_int',
                        'price', 'quantity', 'score', 'rating', 'count'])

        # Data rows
        for i in range(rows):
            writer.writerow([
                i,
                random.randint(-1000000, 1000000),
                round(random.uniform(-1000.0, 1000.0), 4),
                random.randint(0, 100),
                random.randint(-9999999999, 9999999999),
                round(random.uniform(0.01, 9999.99), 2),
                random.randint(1, 10000),
                round(random.uniform(0.0, 100.0), 2),
                round(random.uniform(1.0, 5.0), 1),
                random.randint(0, 1000000),
            ])


def generate_narrow_mixed(output_path: Path, rows: int):
    """Generate a narrow CSV with mixed types (10 columns)."""
    print(f"Generating narrow mixed CSV: {output_path} ({rows:,} rows)")

    categories = ['A', 'B', 'C', 'D', 'E']
    statuses = ['active', 'inactive', 'pending', 'completed']

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)

        # Header
        writer.writerow(['id', 'name', 'value', 'category', 'date',
                        'active', 'score', 'status', 'count', 'notes'])

        # Data rows
        for i in range(rows):
            writer.writerow([
                i,
                f"item_{random_string(8)}",
                round(random.uniform(0.0, 10000.0), 2),
                random.choice(categories),
                random_date(),
                random.choice(['true', 'false']),
                round(random.uniform(0.0, 100.0), 2),
                random.choice(statuses),
                random.randint(0, 1000),
                random_string(random.randint(10, 50)) if random.random() > 0.3 else '',
            ])


def generate_wide_numeric(output_path: Path, rows: int, cols: int = 100):
    """Generate a wide CSV with numeric data."""
    print(f"Generating wide numeric CSV: {output_path} ({rows:,} rows, {cols} columns)")

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)

        # Header
        writer.writerow([f'col_{i}' for i in range(cols)])

        # Data rows
        for _ in range(rows):
            writer.writerow([round(random.uniform(-1000.0, 1000.0), 4) for _ in range(cols)])


def generate_string_heavy(output_path: Path, rows: int):
    """Generate a CSV with mostly string data."""
    print(f"Generating string-heavy CSV: {output_path} ({rows:,} rows)")

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)

        # Header
        writer.writerow(['id', 'first_name', 'last_name', 'email', 'address',
                        'city', 'country', 'phone', 'description', 'notes'])

        # Data rows
        first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']
        countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'Japan']

        for i in range(rows):
            first = random.choice(first_names)
            last = random.choice(last_names)
            writer.writerow([
                i,
                first,
                last,
                f"{first.lower()}.{last.lower()}@example.com",
                f"{random.randint(1, 9999)} {random_string(8)} St",
                random.choice(cities),
                random.choice(countries),
                f"+1-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
                random_string(random.randint(20, 100)),
                random_string(random.randint(50, 200)) if random.random() > 0.5 else '',
            ])


def generate_quoted_fields(output_path: Path, rows: int):
    """Generate a CSV with many quoted fields (embedded commas, newlines)."""
    print(f"Generating quoted fields CSV: {output_path} ({rows:,} rows)")

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)

        # Header
        writer.writerow(['id', 'name', 'description', 'address', 'notes'])

        # Data rows - include commas and newlines in fields
        for i in range(rows):
            writer.writerow([
                i,
                f"Item, Type {random.choice(['A', 'B', 'C'])}",
                f"Description with, commas and\nmultiple lines of text",
                f"{random.randint(1, 999)} Main St, Suite {random.randint(1, 100)}\nCity, State {random.randint(10000, 99999)}",
                f"Note: {random_string(30)}, continued on\nnext line",
            ])


def generate_with_nulls(output_path: Path, rows: int, null_rate: float = 0.1):
    """Generate a CSV with many null values."""
    print(f"Generating CSV with nulls ({null_rate*100:.0f}%): {output_path} ({rows:,} rows)")

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)

        # Header
        writer.writerow(['id', 'value1', 'value2', 'value3', 'value4',
                        'value5', 'value6', 'value7', 'value8', 'value9'])

        # Data rows
        null_values = ['', 'NA', 'null', 'NULL']
        for i in range(rows):
            row = [i]
            for _ in range(9):
                if random.random() < null_rate:
                    row.append(random.choice(null_values))
                else:
                    row.append(round(random.uniform(0.0, 1000.0), 2))
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description='Generate test CSV files for benchmarking')
    parser.add_argument('--output-dir', type=Path, default=Path('bench/data'),
                       help='Output directory for generated files')
    parser.add_argument('--sizes', type=str, default='1000,10000,100000,1000000',
                       help='Comma-separated row counts to generate')
    parser.add_argument('--types', type=str,
                       default='narrow_numeric,narrow_mixed,wide_numeric,string_heavy,quoted,nulls',
                       help='Comma-separated types to generate')
    parser.add_argument('--seed', type=int, default=None,
                       help='Random seed for reproducible data generation')
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    sizes = [int(s) for s in args.sizes.split(',')]
    types = args.types.split(',')

    generators = {
        'narrow_numeric': generate_narrow_numeric,
        'narrow_mixed': generate_narrow_mixed,
        'wide_numeric': lambda p, r: generate_wide_numeric(p, r, cols=100),
        'string_heavy': generate_string_heavy,
        'quoted': generate_quoted_fields,
        'nulls': generate_with_nulls,
    }

    for type_name in types:
        if type_name not in generators:
            print(f"Unknown type: {type_name}")
            continue

        for size in sizes:
            suffix = f"{size // 1000}k" if size >= 1000 else str(size)
            output_path = args.output_dir / f"{type_name}_{suffix}.csv"
            generators[type_name](output_path, size)

    print(f"\nGenerated {len(types) * len(sizes)} test files in {args.output_dir}")

    # Print file sizes
    print("\nFile sizes:")
    for f in sorted(args.output_dir.glob('*.csv')):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name}: {size_mb:.2f} MB")


if __name__ == '__main__':
    main()
