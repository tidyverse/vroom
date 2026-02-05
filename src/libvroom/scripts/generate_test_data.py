#!/usr/bin/env python3
"""
Generate test data files for hypothesis-driven benchmarks.

Standard test files:
- narrow_1M: 1M rows × 10 cols (3 int, 3 double, 4 string) - Primary benchmark
- wide_100K: 100K rows × 100 cols (mixed) - Tests wide data
- narrow_10M: 10M rows × 5 cols (simple) - Large file test
- quoted_heavy: 1M rows × 10 cols (50% quoted fields) - Tests quote handling
- escape_heavy: 1M rows × 10 cols (strings with "") - Tests escape handling

Usage:
    python scripts/generate_test_data.py [output_dir]

If output_dir is not specified, defaults to benchmark/test_data/
"""

import argparse
import csv
import json
import random
import string
import urllib.request
import ssl
from pathlib import Path
from typing import Optional
from urllib.error import URLError, HTTPError


def random_string(min_len: int = 5, max_len: int = 20) -> str:
    """Generate a random alphanumeric string."""
    length = random.randint(min_len, max_len)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def random_quoted_string(min_len: int = 5, max_len: int = 20) -> str:
    """Generate a random string that should be quoted (contains comma or newline)."""
    length = random.randint(min_len, max_len)
    base = ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))
    # Add a comma or space to force quoting
    return f'"{base}, extra"'


def random_escape_string(min_len: int = 5, max_len: int = 15) -> str:
    """Generate a random string with embedded quotes that need escaping."""
    length = random.randint(min_len, max_len)
    base = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    # Add escaped quotes
    return f'"{base}""inside""{random_string(3, 5)}"'


def generate_narrow_1m(output_path: Path) -> None:
    """
    Generate narrow_1M.csv: 1M rows × 10 cols (3 int, 3 double, 4 string).
    Primary benchmark file for most hypothesis tests.
    """
    print(f"Generating narrow_1M.csv...")
    with open(output_path / "narrow_1M.csv", "w") as f:
        # Header
        headers = ["int1", "int2", "int3", "dbl1", "dbl2", "dbl3",
                   "str1", "str2", "str3", "str4"]
        f.write(",".join(headers) + "\n")

        # Data rows
        for i in range(1_000_000):
            row = [
                str(random.randint(-1000000, 1000000)),
                str(random.randint(0, 9999)),
                str(random.randint(-100, 100)),
                f"{random.uniform(-1000, 1000):.6f}",
                f"{random.uniform(0, 1):.10f}",
                f"{random.uniform(-1e10, 1e10):.2e}",
                random_string(),
                random_string(1, 10),
                random_string(10, 30),
                random_string(),
            ]
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'narrow_1M.csv'}")


def generate_wide_100k(output_path: Path) -> None:
    """
    Generate wide_100K.csv: 100K rows × 100 cols (mixed types).
    Tests wide data performance.
    """
    print(f"Generating wide_100K.csv...")
    with open(output_path / "wide_100K.csv", "w") as f:
        # Header: 25 int, 25 double, 50 string
        headers = [f"int{i}" for i in range(25)]
        headers += [f"dbl{i}" for i in range(25)]
        headers += [f"str{i}" for i in range(50)]
        f.write(",".join(headers) + "\n")

        # Data rows
        for i in range(100_000):
            row = []
            # 25 integers
            for _ in range(25):
                row.append(str(random.randint(-1000000, 1000000)))
            # 25 doubles
            for _ in range(25):
                row.append(f"{random.uniform(-1000, 1000):.6f}")
            # 50 strings
            for _ in range(50):
                row.append(random_string(5, 15))
            f.write(",".join(row) + "\n")

            if (i + 1) % 10000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'wide_100K.csv'}")


def generate_narrow_10m(output_path: Path) -> None:
    """
    Generate narrow_10M.csv: 10M rows × 5 cols (simple types).
    Large file test.
    """
    print(f"Generating narrow_10M.csv...")
    with open(output_path / "narrow_10M.csv", "w") as f:
        # Header
        headers = ["id", "value", "amount", "name", "code"]
        f.write(",".join(headers) + "\n")

        # Data rows
        for i in range(10_000_000):
            row = [
                str(i),
                str(random.randint(0, 9999)),
                f"{random.uniform(0, 10000):.2f}",
                random_string(5, 15),
                random_string(3, 8),
            ]
            f.write(",".join(row) + "\n")

            if (i + 1) % 1000000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'narrow_10M.csv'}")


def generate_quoted_heavy(output_path: Path) -> None:
    """
    Generate quoted_heavy.csv: 1M rows × 10 cols with 50% quoted fields.
    Tests quote handling overhead.
    """
    print(f"Generating quoted_heavy.csv...")
    with open(output_path / "quoted_heavy.csv", "w") as f:
        # Header
        headers = [f"col{i}" for i in range(10)]
        f.write(",".join(headers) + "\n")

        # Data rows - 50% of fields are quoted
        for i in range(1_000_000):
            row = []
            for _ in range(10):
                if random.random() < 0.5:
                    row.append(random_quoted_string())
                else:
                    row.append(random_string())
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'quoted_heavy.csv'}")


def generate_escape_heavy(output_path: Path) -> None:
    """
    Generate escape_heavy.csv: 1M rows × 10 cols with strings containing "".
    Tests escape sequence handling.
    """
    print(f"Generating escape_heavy.csv...")
    with open(output_path / "escape_heavy.csv", "w") as f:
        # Header
        headers = [f"col{i}" for i in range(10)]
        f.write(",".join(headers) + "\n")

        # Data rows - most fields have escape sequences
        for i in range(1_000_000):
            row = []
            for _ in range(10):
                if random.random() < 0.7:  # 70% have escape sequences
                    row.append(random_escape_string())
                else:
                    row.append(random_string())
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'escape_heavy.csv'}")


def generate_variable_sizes(output_path: Path) -> None:
    """
    Generate test files for H1 test matrix:
    (rows, cols) ∈ [(10K, 10), (100K, 10), (1M, 10), (100K, 100), (100K, 1000)]
    """
    test_matrix = [
        (10_000, 10, "10k_10c"),
        (100_000, 10, "100k_10c"),
        (1_000_000, 10, "1m_10c"),
        (100_000, 100, "100k_100c"),
        (100_000, 1000, "100k_1000c"),
    ]

    for nrows, ncols, name in test_matrix:
        filename = f"matrix_{name}.csv"
        print(f"Generating {filename}...")

        with open(output_path / filename, "w") as f:
            # Header
            headers = [f"col{i}" for i in range(ncols)]
            f.write(",".join(headers) + "\n")

            # Data rows - simple integers for consistent field sizes
            for i in range(nrows):
                row = [str(random.randint(0, 9999)) for _ in range(ncols)]
                f.write(",".join(row) + "\n")

                if nrows >= 100000 and (i + 1) % 100000 == 0:
                    print(f"  {i + 1:,} rows written...")

        print(f"  Done: {output_path / filename}")


def generate_threading_test(output_path: Path) -> None:
    """
    Generate a ~500MB file for H3 threading benchmark.
    """
    print(f"Generating threading_500mb.csv (~500MB)...")
    target_size = 500 * 1024 * 1024  # 500MB

    with open(output_path / "threading_500mb.csv", "w") as f:
        # Header - 20 columns for reasonable width
        headers = [f"col{i}" for i in range(20)]
        f.write(",".join(headers) + "\n")

        # Estimate: ~20 cols * ~10 chars each = ~200 bytes/row
        # 500MB / 200 = ~2.5M rows
        current_size = 0
        row_count = 0

        while current_size < target_size:
            row = []
            for j in range(20):
                if j < 5:
                    row.append(str(random.randint(-1000000, 1000000)))
                elif j < 10:
                    row.append(f"{random.uniform(-1000, 1000):.4f}")
                else:
                    row.append(random_string(5, 15))

            line = ",".join(row) + "\n"
            f.write(line)
            current_size += len(line)
            row_count += 1

            if row_count % 500000 == 0:
                print(f"  {row_count:,} rows, {current_size / (1024*1024):.1f}MB written...")

    print(f"  Done: {output_path / 'threading_500mb.csv'} ({current_size / (1024*1024):.1f}MB, {row_count:,} rows)")


def generate_nyc_taxi_synthetic(output_path: Path, nrows: int = 1_000_000) -> None:
    """
    Generate synthetic NYC Taxi-like data with realistic column types.

    This synthetic data mimics the NYC Taxi schema with:
    - Integer IDs (VendorID, passenger_count, etc.)
    - Floating point amounts (fare_amount, trip_distance, etc.)
    - Timestamps (pickup/dropoff datetime)
    - Location IDs (PULocationID, DOLocationID)
    - Payment types and rate codes

    Useful for H4 (escape sequences - rare in numeric data) and
    H5 (type widening - decimal amounts that may need int->float).
    """
    print(f"Generating nyc_taxi_synthetic.csv ({nrows:,} rows)...")

    headers = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge"
    ]

    with open(output_path / "nyc_taxi_synthetic.csv", "w") as f:
        f.write(",".join(headers) + "\n")

        base_date = "2024-01-15 "

        for i in range(nrows):
            # Realistic NYC taxi data distributions
            vendor_id = random.choice([1, 2])
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            pickup_time = f"{base_date}{hour:02d}:{minute:02d}:00"
            duration_mins = max(1, int(random.gauss(15, 10)))
            dropoff_hour = (hour + duration_mins // 60) % 24
            dropoff_minute = (minute + duration_mins % 60) % 60
            dropoff_time = f"{base_date}{dropoff_hour:02d}:{dropoff_minute:02d}:00"

            passenger_count = random.choices([1, 2, 3, 4, 5, 6], weights=[70, 15, 8, 4, 2, 1])[0]
            trip_distance = max(0.1, random.gauss(3.0, 2.5))
            ratecode_id = random.choices([1, 2, 3, 4, 5, 6], weights=[90, 5, 2, 1, 1, 1])[0]
            store_and_fwd = random.choice(["N", "Y"]) if random.random() < 0.01 else "N"
            pu_location = random.randint(1, 265)
            do_location = random.randint(1, 265)
            payment_type = random.choices([1, 2, 3, 4], weights=[70, 25, 3, 2])[0]

            # Fare calculations
            base_fare = 3.0
            per_mile = 2.5
            fare_amount = base_fare + (trip_distance * per_mile)
            extra = random.choice([0.0, 0.5, 1.0, 2.5]) if random.random() < 0.3 else 0.0
            mta_tax = 0.5
            tip_amount = round(fare_amount * random.uniform(0, 0.3), 2) if payment_type == 1 else 0.0
            tolls_amount = round(random.uniform(0, 10), 2) if random.random() < 0.1 else 0.0
            improvement_surcharge = 0.3
            congestion_surcharge = 2.5 if pu_location <= 90 else 0.0
            total_amount = fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + congestion_surcharge

            row = [
                str(vendor_id),
                pickup_time,
                dropoff_time,
                str(passenger_count),
                f"{trip_distance:.2f}",
                str(ratecode_id),
                store_and_fwd,
                str(pu_location),
                str(do_location),
                str(payment_type),
                f"{fare_amount:.2f}",
                f"{extra:.2f}",
                f"{mta_tax:.2f}",
                f"{tip_amount:.2f}",
                f"{tolls_amount:.2f}",
                f"{improvement_surcharge:.2f}",
                f"{total_amount:.2f}",
                f"{congestion_surcharge:.2f}"
            ]
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'nyc_taxi_synthetic.csv'}")


def generate_type_widening_test(output_path: Path, nrows: int = 1_000_000) -> None:
    """
    Generate test data for H5: Type widening detection.

    Creates a file where some columns start as integers but contain
    floats later, simulating real-world type inference challenges.

    Column layout:
    - col0: Always integer
    - col1: Integer until row 50K, then floats (widening needed after sample)
    - col2: Integer until row 500K, then floats (late widening)
    - col3: Always float
    - col4: Integer with occasional floats (sparse widening)
    """
    print(f"Generating type_widening_test.csv ({nrows:,} rows)...")

    headers = ["always_int", "early_widen", "late_widen", "always_float", "sparse_widen"]

    with open(output_path / "type_widening_test.csv", "w") as f:
        f.write(",".join(headers) + "\n")

        for i in range(nrows):
            always_int = random.randint(0, 1000000)

            # Early widening: ints for first 50K rows, then floats
            if i < 50000:
                early_widen = str(random.randint(0, 1000))
            else:
                early_widen = f"{random.uniform(0, 1000):.2f}"

            # Late widening: ints for first 500K rows, then floats
            if i < 500000:
                late_widen = str(random.randint(0, 1000))
            else:
                late_widen = f"{random.uniform(0, 1000):.2f}"

            always_float = f"{random.uniform(-1000, 1000):.4f}"

            # Sparse widening: 99% ints, 1% floats randomly distributed
            if random.random() < 0.01:
                sparse_widen = f"{random.uniform(0, 1000):.2f}"
            else:
                sparse_widen = str(random.randint(0, 1000))

            row = [str(always_int), early_widen, late_widen, always_float, sparse_widen]
            f.write(",".join(row) + "\n")

            if (i + 1) % 100000 == 0:
                print(f"  {i + 1:,} rows written...")

    print(f"  Done: {output_path / 'type_widening_test.csv'}")


# Real-world CSV corpus URLs for H4/H5 validation
# Each entry: (name, url, description, source)
CORPUS_URLS = [
    # data.gov sources (publicly accessible CSV files)
    ("nyc_311_service_requests",
     "https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?accessType=DOWNLOAD",
     "NYC 311 Service Requests", "data.gov"),
    ("chicago_crimes",
     "https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD",
     "Chicago Crimes Dataset", "data.gov"),
    ("la_parking_citations",
     "https://data.lacity.org/api/views/wjz9-h9np/rows.csv?accessType=DOWNLOAD",
     "LA Parking Citations", "data.gov"),
    ("austin_311",
     "https://data.austintexas.gov/api/views/xwdj-i9he/rows.csv?accessType=DOWNLOAD",
     "Austin 311 Service Requests", "data.gov"),
    ("sf_fire_incidents",
     "https://data.sfgov.org/api/views/wr8u-xric/rows.csv?accessType=DOWNLOAD",
     "SF Fire Department Incidents", "data.gov"),
    ("seattle_911",
     "https://data.seattle.gov/api/views/kzjm-xkqj/rows.csv?accessType=DOWNLOAD",
     "Seattle 911 Calls", "data.gov"),
    ("boston_311",
     "https://data.boston.gov/api/views/awu8-dc52/rows.csv?accessType=DOWNLOAD",
     "Boston 311 Service Requests", "data.gov"),
    ("denver_crime",
     "https://data.denvergov.org/api/views/sn6e-2k9e/rows.csv?accessType=DOWNLOAD",
     "Denver Crime Statistics", "data.gov"),
    ("philadelphia_crime",
     "https://phl.carto.com/api/v2/sql?q=SELECT+*+FROM+incidents_part1_part2&format=csv",
     "Philadelphia Crime Incidents", "data.gov"),
    ("nyc_taxi_zones",
     "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD",
     "NYC Taxi Zone Lookup", "data.gov"),

    # UCI ML Repository (direct CSV downloads)
    ("adult_income",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data",
     "Adult Income Dataset", "UCI"),
    ("covertype",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/covtype.data.gz",
     "Forest Covertype Dataset", "UCI"),
    ("poker_hand",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/poker/poker-hand-training-true.data",
     "Poker Hand Dataset", "UCI"),
    ("higgs",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/00280/HIGGS.csv.gz",
     "HIGGS Physics Dataset", "UCI"),
    ("kdd_cup",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/kddcup99-mld/kddcup.data.gz",
     "KDD Cup 1999 Dataset", "UCI"),
    ("skin_segmentation",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/00229/Skin_NonSkin.txt",
     "Skin Segmentation Dataset", "UCI"),
    ("shuttle",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/statlog/shuttle/shuttle.trn.Z",
     "Statlog Shuttle Dataset", "UCI"),
    ("letter_recognition",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/letter-recognition/letter-recognition.data",
     "Letter Recognition Dataset", "UCI"),
    ("magic_gamma",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/magic/magic04.data",
     "MAGIC Gamma Telescope Dataset", "UCI"),
    ("susy",
     "https://archive.ics.uci.edu/ml/machine-learning-databases/00279/SUSY.csv.gz",
     "SUSY Physics Dataset", "UCI"),

    # Kaggle / other public datasets (direct download URLs)
    ("global_temperature",
     "https://raw.githubusercontent.com/datasets/global-temp/master/data/monthly.csv",
     "Global Temperature Data", "Kaggle"),
    ("country_codes",
     "https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv",
     "Country Codes Dataset", "Kaggle"),
    ("airport_codes",
     "https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv",
     "Airport Codes Dataset", "Kaggle"),
    ("world_cities",
     "https://raw.githubusercontent.com/datasets/world-cities/master/data/world-cities.csv",
     "World Cities Dataset", "Kaggle"),
    ("s_and_p_500",
     "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv",
     "S&P 500 Companies", "Kaggle"),
    ("population",
     "https://raw.githubusercontent.com/datasets/population/master/data/population.csv",
     "World Population Data", "Kaggle"),
    ("covid_data",
     "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
     "Our World in Data COVID-19", "Kaggle"),
    ("github_languages",
     "https://raw.githubusercontent.com/github/linguist/master/lib/linguist/languages.yml",
     "GitHub Programming Languages", "Kaggle"),
]

# Path to CleverCSV corpus (optional, for additional diverse CSVs)
CLEVERCSV_CORPUS_PATH = Path.home() / "p" / "CSV_Wrangling" / "urls_github.json"


def download_file(url: str, output_path: Path, max_size_mb: int = 100) -> Optional[Path]:
    """
    Download a file from URL with progress reporting.

    Args:
        url: URL to download
        output_path: Path to save the file
        max_size_mb: Maximum file size to download in MB

    Returns:
        Path to downloaded file, or None if failed
    """
    try:
        # Create SSL context that doesn't verify certificates (for some gov sites)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        print(f"  Downloading from {url[:60]}...")

        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})

        with urllib.request.urlopen(req, context=ctx, timeout=60) as response:
            # Check content length
            content_length = response.headers.get('Content-Length')
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                if size_mb > max_size_mb:
                    print(f"  Skipping: file too large ({size_mb:.1f}MB > {max_size_mb}MB)")
                    return None
                print(f"  Size: {size_mb:.1f}MB")

            # Download with progress
            with open(output_path, 'wb') as f:
                downloaded = 0
                block_size = 8192
                while True:
                    buffer = response.read(block_size)
                    if not buffer:
                        break
                    downloaded += len(buffer)
                    f.write(buffer)

                    # Check max size during download
                    if downloaded > max_size_mb * 1024 * 1024:
                        print(f"  Truncating at {max_size_mb}MB")
                        break

        print(f"  Downloaded: {output_path.name} ({downloaded / (1024*1024):.1f}MB)")
        return output_path

    except (URLError, HTTPError) as e:
        print(f"  Failed to download: {e}")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None


def download_clevercsv_sample(output_path: Path, num_files: int = 50) -> list[Path]:
    """
    Download a random sample of CSVs from the CleverCSV corpus.

    These are diverse, real-world CSVs from GitHub repositories.

    Args:
        output_path: Directory to save files
        num_files: Number of files to sample

    Returns:
        List of successfully downloaded file paths
    """
    import json
    import random

    if not CLEVERCSV_CORPUS_PATH.exists():
        print(f"CleverCSV corpus not found at {CLEVERCSV_CORPUS_PATH}")
        return []

    corpus_dir = output_path / "corpus" / "clevercsv"
    corpus_dir.mkdir(parents=True, exist_ok=True)

    print(f"Sampling {num_files} files from CleverCSV corpus...")

    # Load all URLs
    urls = []
    with open(CLEVERCSV_CORPUS_PATH, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                if 'urls' in entry and entry['urls']:
                    urls.append((entry['md5'], entry['urls'][0]))
            except json.JSONDecodeError:
                continue

    print(f"  Found {len(urls)} CSVs in corpus")

    # Random sample
    random.seed(42)  # Reproducible sampling
    sample = random.sample(urls, min(num_files, len(urls)))

    downloaded = []
    for i, (md5, url) in enumerate(sample):
        print(f"  [{i+1}/{len(sample)}] {md5[:8]}...")
        file_path = corpus_dir / f"{md5}.csv"

        if file_path.exists():
            downloaded.append(file_path)
            continue

        result = download_file(url, file_path, max_size_mb=10)  # Smaller limit for GitHub CSVs
        if result:
            downloaded.append(result)

    print(f"Downloaded {len(downloaded)} CleverCSV files")
    return downloaded


def download_corpus(output_path: Path, max_files: int = 30, include_clevercsv: bool = True) -> list[Path]:
    """
    Download real-world CSV corpus for H4/H5 validation.

    Args:
        output_path: Directory to save files
        max_files: Maximum number of files to download from CORPUS_URLS
        include_clevercsv: Also sample from CleverCSV corpus if available

    Returns:
        List of successfully downloaded file paths
    """
    corpus_dir = output_path / "corpus"
    corpus_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading real-world CSV corpus to {corpus_dir}...")
    print(f"Target: up to {max_files} files from data.gov, UCI ML Repository, Kaggle")
    print(f"Available sources: {len(CORPUS_URLS)} URLs")
    print()

    downloaded = []
    for name, url, description, source in CORPUS_URLS[:max_files]:
        print(f"[{source}] {description}")

        # Determine file extension
        if url.endswith('.gz') or url.endswith('.Z'):
            file_path = corpus_dir / f"{name}.csv.gz"
        elif url.endswith('.yml'):
            file_path = corpus_dir / f"{name}.yml"
        else:
            file_path = corpus_dir / f"{name}.csv"

        # Skip if already exists
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"  Already exists: {file_path.name} ({size_mb:.1f}MB)")
            downloaded.append(file_path)
            continue

        result = download_file(url, file_path)
        if result:
            downloaded.append(result)
        print()

    print(f"Downloaded {len(downloaded)} files from main corpus")

    # Optionally add CleverCSV samples
    if include_clevercsv and CLEVERCSV_CORPUS_PATH.exists():
        print()
        clevercsv_files = download_clevercsv_sample(output_path, num_files=50)
        downloaded.extend(clevercsv_files)

    print(f"\nTotal corpus: {len(downloaded)} files")
    return downloaded


def analyze_csv_for_escapes(file_path: Path, sample_rows: int = 100000) -> dict:
    """
    Analyze a CSV file for escape sequences (H4 validation).

    Args:
        file_path: Path to CSV file
        sample_rows: Number of rows to sample

    Returns:
        Dictionary with escape analysis results
    """
    import gzip

    results = {
        'file': file_path.name,
        'total_fields': 0,
        'quoted_fields': 0,
        'escaped_fields': 0,  # Fields with "" escape sequences
        'fields_with_commas': 0,
        'fields_with_newlines': 0,
        'rows_sampled': 0,
    }

    try:
        # Handle gzipped files
        if str(file_path).endswith('.gz'):
            f = gzip.open(file_path, 'rt', encoding='utf-8', errors='replace')
        else:
            f = open(file_path, 'r', encoding='utf-8', errors='replace')

        with f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i >= sample_rows:
                    break

                results['rows_sampled'] = i + 1

                for field in row:
                    results['total_fields'] += 1

                    # Check for patterns requiring quoting/escaping
                    if ',' in field:
                        results['fields_with_commas'] += 1
                    if '\n' in field or '\r' in field:
                        results['fields_with_newlines'] += 1
                    if '"' in field:
                        results['escaped_fields'] += 1

    except Exception as e:
        results['error'] = str(e)

    # Calculate percentages
    if results['total_fields'] > 0:
        results['escape_ratio'] = results['escaped_fields'] / results['total_fields']
        results['comma_ratio'] = results['fields_with_commas'] / results['total_fields']
        results['newline_ratio'] = results['fields_with_newlines'] / results['total_fields']
        results['needs_processing_ratio'] = (
            results['escaped_fields'] + results['fields_with_commas'] +
            results['fields_with_newlines']
        ) / results['total_fields']

    return results


def analyze_csv_for_types(file_path: Path, sample_rows: int = 10000) -> dict:
    """
    Analyze a CSV file for type patterns (H5 validation).

    Args:
        file_path: Path to CSV file
        sample_rows: Number of initial rows to sample for type inference

    Returns:
        Dictionary with type analysis results
    """
    import gzip
    import re

    int_pattern = re.compile(r'^-?\d+$')
    float_pattern = re.compile(r'^-?\d+\.\d+$')

    results = {
        'file': file_path.name,
        'columns': 0,
        'rows_analyzed': 0,
        'type_changes': [],  # Columns that changed type after sample
        'initial_types': {},  # Column -> inferred type from sample
        'final_types': {},  # Column -> final type after full scan
    }

    try:
        if str(file_path).endswith('.gz'):
            f = gzip.open(file_path, 'rt', encoding='utf-8', errors='replace')
        else:
            f = open(file_path, 'r', encoding='utf-8', errors='replace')

        with f:
            reader = csv.reader(f)

            # Skip header
            try:
                header = next(reader)
                results['columns'] = len(header)
            except StopIteration:
                return results

            # Track types per column: 'int', 'float', 'string'
            col_types = ['int'] * results['columns']  # Start assuming int
            col_sample_types = [None] * results['columns']

            for i, row in enumerate(reader):
                results['rows_analyzed'] = i + 1

                for col_idx, field in enumerate(row):
                    if col_idx >= results['columns']:
                        continue

                    field = field.strip()
                    if not field:
                        continue

                    # Determine field type
                    if int_pattern.match(field):
                        field_type = 'int'
                    elif float_pattern.match(field):
                        field_type = 'float'
                    else:
                        field_type = 'string'

                    # Update column type (can only widen: int -> float -> string)
                    current_type = col_types[col_idx]
                    if current_type == 'int' and field_type in ('float', 'string'):
                        col_types[col_idx] = field_type
                    elif current_type == 'float' and field_type == 'string':
                        col_types[col_idx] = 'string'

                # Record types at sample boundary
                if i == sample_rows - 1:
                    col_sample_types = col_types.copy()
                    results['initial_types'] = {
                        f'col_{j}': t for j, t in enumerate(col_sample_types)
                    }

                # Limit full scan for performance
                if i >= sample_rows * 10:
                    break

            results['final_types'] = {f'col_{j}': t for j, t in enumerate(col_types)}

            # Find type changes after sample window
            if col_sample_types[0] is not None:
                for col_idx in range(min(len(col_sample_types), len(col_types))):
                    if col_sample_types[col_idx] != col_types[col_idx]:
                        results['type_changes'].append({
                            'column': col_idx,
                            'initial': col_sample_types[col_idx],
                            'final': col_types[col_idx],
                        })

    except Exception as e:
        results['error'] = str(e)

    return results


def analyze_corpus(corpus_dir: Path) -> dict:
    """
    Analyze all CSV files in corpus directory for H4/H5 validation.

    Args:
        corpus_dir: Directory containing corpus CSV files

    Returns:
        Dictionary with aggregated analysis results
    """
    print(f"\nAnalyzing corpus in {corpus_dir}...")

    results = {
        'h4_escape_analysis': [],
        'h5_type_analysis': [],
        'summary': {
            'total_files': 0,
            'total_fields_analyzed': 0,
            'avg_escape_ratio': 0,
            'files_with_type_changes': 0,
        }
    }

    # Recursively find all CSV files (including in subdirectories like clevercsv/)
    csv_files = list(corpus_dir.glob('**/*.csv')) + list(corpus_dir.glob('**/*.csv.gz'))

    for file_path in csv_files:
        print(f"\n  Analyzing {file_path.name}...")

        # H4 analysis
        h4_result = analyze_csv_for_escapes(file_path)
        results['h4_escape_analysis'].append(h4_result)

        # H5 analysis
        h5_result = analyze_csv_for_types(file_path)
        results['h5_type_analysis'].append(h5_result)

        results['summary']['total_files'] += 1
        results['summary']['total_fields_analyzed'] += h4_result.get('total_fields', 0)

        if h5_result.get('type_changes'):
            results['summary']['files_with_type_changes'] += 1

        # Print per-file summary
        escape_pct = h4_result.get('escape_ratio', 0) * 100
        needs_proc_pct = h4_result.get('needs_processing_ratio', 0) * 100
        type_changes = len(h5_result.get('type_changes', []))

        print(f"    H4: {escape_pct:.2f}% escaped, {needs_proc_pct:.2f}% need processing")
        print(f"    H5: {type_changes} type changes after sample window")

    # Calculate summary statistics
    if results['h4_escape_analysis']:
        total_escape_ratio = sum(
            r.get('escape_ratio', 0) for r in results['h4_escape_analysis']
        ) / len(results['h4_escape_analysis'])
        results['summary']['avg_escape_ratio'] = total_escape_ratio

    return results


def print_corpus_report(results: dict) -> None:
    """Print formatted report of corpus analysis results."""
    print("\n" + "=" * 70)
    print("CORPUS ANALYSIS REPORT - H4/H5 Validation")
    print("=" * 70)

    summary = results['summary']

    print(f"\nFiles analyzed: {summary['total_files']}")
    print(f"Total fields analyzed: {summary['total_fields_analyzed']:,}")

    print("\n--- H4: Zero-copy String Extraction Viability ---")
    print("\nPer-file escape sequence analysis:")
    print(f"{'File':<35} {'Rows':<10} {'Fields':<12} {'Escape%':<10} {'NeedProc%':<10}")
    print("-" * 77)

    for r in results['h4_escape_analysis']:
        name = r['file'][:32] + '...' if len(r['file']) > 35 else r['file']
        rows = r.get('rows_sampled', 0)
        fields = r.get('total_fields', 0)
        escape_pct = r.get('escape_ratio', 0) * 100
        need_proc_pct = r.get('needs_processing_ratio', 0) * 100
        print(f"{name:<35} {rows:<10,} {fields:<12,} {escape_pct:<10.2f} {need_proc_pct:<10.2f}")

    avg_escape = summary['avg_escape_ratio'] * 100
    print(f"\nAverage escape ratio: {avg_escape:.2f}%")
    print(f"H4 Hypothesis: >80% fields need no escape processing")
    print(f"Result: {100 - avg_escape:.1f}% fields are escape-free")
    if avg_escape < 20:
        print(">>> H4 CONFIRMED: Zero-copy viable for majority of fields")
    else:
        print(">>> H4 REFUTED: Significant escape processing required")

    print("\n--- H5: Type Widening Frequency ---")
    print(f"\nFiles with type changes after sample: {summary['files_with_type_changes']}/{summary['total_files']}")

    type_change_pct = summary['files_with_type_changes'] / max(summary['total_files'], 1) * 100
    print(f"Type change rate: {type_change_pct:.1f}%")
    print(f"H5 Hypothesis: <5% of files require type widening")
    if type_change_pct < 5:
        print(">>> H5 CONFIRMED: Type widening is rare")
    elif type_change_pct < 20:
        print(">>> H5 PARTIALLY CONFIRMED: Type widening is uncommon")
    else:
        print(">>> H5 REFUTED: Type widening is common")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Generate test data files for hypothesis-driven benchmarks"
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="benchmark/test_data",
        help="Output directory for test files (default: benchmark/test_data/)"
    )
    parser.add_argument(
        "--small",
        action="store_true",
        help="Generate only small test files (skip 10M and 500MB files)"
    )
    parser.add_argument(
        "--matrix-only",
        action="store_true",
        help="Generate only the H1 test matrix files"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    parser.add_argument(
        "--taxi",
        action="store_true",
        help="Generate NYC Taxi-like synthetic data for H4/H5 testing"
    )
    parser.add_argument(
        "--h5",
        action="store_true",
        help="Generate type widening test data for H5 testing"
    )
    parser.add_argument(
        "--all-h4h5",
        action="store_true",
        help="Generate all H4/H5 test data (taxi + type widening)"
    )
    parser.add_argument(
        "--corpus",
        action="store_true",
        help="Download real-world CSV corpus for H4/H5 validation"
    )
    parser.add_argument(
        "--analyze-corpus",
        type=str,
        metavar="DIR",
        help="Analyze existing CSV corpus directory for H4/H5"
    )
    parser.add_argument(
        "--corpus-max-files",
        type=int,
        default=30,
        help="Maximum number of corpus files to download from main sources (default: 30)"
    )
    parser.add_argument(
        "--no-clevercsv",
        action="store_true",
        help="Skip CleverCSV corpus sampling (faster, but fewer diverse files)"
    )

    args = parser.parse_args()

    # Set random seed for reproducibility
    random.seed(args.seed)

    # Create output directory
    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Output directory: {output_path.absolute()}")
    print(f"Random seed: {args.seed}")
    print()

    if args.matrix_only:
        generate_variable_sizes(output_path)
        return

    # Handle corpus download and analysis
    if args.analyze_corpus:
        corpus_dir = Path(args.analyze_corpus)
        if not corpus_dir.exists():
            print(f"Error: Corpus directory not found: {corpus_dir}")
            return
        results = analyze_corpus(corpus_dir)
        print_corpus_report(results)
        # Save results to JSON
        results_file = output_path / "corpus_analysis.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to: {results_file}")
        return

    if args.corpus:
        downloaded = download_corpus(
            output_path,
            max_files=args.corpus_max_files,
            include_clevercsv=not args.no_clevercsv
        )
        if downloaded:
            corpus_dir = output_path / "corpus"
            results = analyze_corpus(corpus_dir)
            print_corpus_report(results)
            # Save results to JSON
            results_file = output_path / "corpus_analysis.json"
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\nResults saved to: {results_file}")
        return

    if args.taxi or args.all_h4h5:
        generate_nyc_taxi_synthetic(output_path)
        print()

    if args.h5 or args.all_h4h5:
        generate_type_widening_test(output_path)
        print()

    if args.taxi or args.h5 or args.all_h4h5:
        # If only generating H4/H5 data, return early
        if not (args.small or args.matrix_only):
            return

    # Generate standard test files
    generate_narrow_1m(output_path)
    print()

    generate_wide_100k(output_path)
    print()

    if not args.small:
        generate_narrow_10m(output_path)
        print()

    generate_quoted_heavy(output_path)
    print()

    generate_escape_heavy(output_path)
    print()

    # Generate H1 test matrix files
    generate_variable_sizes(output_path)
    print()

    if not args.small:
        # Generate large file for threading tests
        generate_threading_test(output_path)
        print()

    print("All test files generated successfully!")

    # Print summary
    print("\nGenerated files:")
    for f in sorted(output_path.glob("*.csv")):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name}: {size_mb:.1f}MB")


if __name__ == "__main__":
    main()
