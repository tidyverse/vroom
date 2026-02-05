"""Tests for read_csv_batched() and BatchedReader."""

import tempfile

import pytest


@pytest.fixture
def simple_csv():
    """Create a simple CSV file for testing."""
    content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def larger_csv():
    """Create a larger CSV file for batch testing."""
    lines = ["id,value,category"]
    for i in range(100):
        lines.append(f"{i},{i * 10},cat_{i % 5}")
    content = "\n".join(lines) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def csv_with_nulls():
    """Create a CSV file with null values."""
    content = "name,value,status\nAlice,100,active\nBob,NA,inactive\nCharlie,,pending\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def tsv_file():
    """Create a TSV file for delimiter testing."""
    content = "name\tvalue\nAlice\t100\nBob\t200\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def no_header_csv():
    """Create a CSV file without header."""
    content = "Alice,30,New York\nBob,25,Los Angeles\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestBatchedReaderBasic:
    """Basic tests for read_csv_batched() functionality."""

    def test_read_csv_batched_returns_iterator(self, simple_csv):
        """Test that read_csv_batched returns an iterator."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        assert hasattr(reader, "__iter__")
        assert hasattr(reader, "__next__")

    def test_read_csv_batched_yields_record_batches(self, simple_csv):
        """Test that iterating yields RecordBatch objects."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)
        assert isinstance(batch, vroom_csv.RecordBatch)

    def test_batch_has_correct_structure(self, simple_csv):
        """Test that batches have the correct columns and data."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        assert batch.num_columns == 3
        assert batch.column_names == ["name", "age", "city"]
        assert batch.num_rows == 3

    def test_batch_column_access_by_index(self, simple_csv):
        """Test accessing columns by index."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        names = batch.column(0)
        assert names == ["Alice", "Bob", "Charlie"]

    def test_batch_column_access_by_name(self, simple_csv):
        """Test accessing columns by name."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        names = batch.column("name")
        assert names == ["Alice", "Bob", "Charlie"]

    def test_batch_row_access(self, simple_csv):
        """Test accessing rows by index."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        row = batch.row(0)
        assert row == ["Alice", "30", "New York"]

    def test_batch_len(self, simple_csv):
        """Test that len() returns num_rows."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        assert len(batch) == 3

    def test_batch_repr(self, simple_csv):
        """Test the string representation of RecordBatch."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        repr_str = repr(batch)
        assert "RecordBatch" in repr_str
        assert "3 rows" in repr_str
        assert "3 columns" in repr_str


class TestBatchedReaderIterator:
    """Tests for iterator behavior."""

    def test_iteration_stops_at_end(self, simple_csv):
        """Test that iteration raises StopIteration at end."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv, batch_size=10)
        batches = list(reader)

        # Small file should fit in one batch
        assert len(batches) == 1

    def test_multiple_batches(self, larger_csv):
        """Test that large files are split into multiple batches."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(larger_csv, batch_size=25)
        batches = list(reader)

        # 100 rows / 25 per batch = 4 batches
        assert len(batches) == 4

        # Verify row counts
        assert batches[0].num_rows == 25
        assert batches[1].num_rows == 25
        assert batches[2].num_rows == 25
        assert batches[3].num_rows == 25

    def test_last_batch_smaller(self, larger_csv):
        """Test that last batch can be smaller than batch_size."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(larger_csv, batch_size=30)
        batches = list(reader)

        # 100 rows / 30 per batch = 4 batches (30, 30, 30, 10)
        assert len(batches) == 4
        assert batches[0].num_rows == 30
        assert batches[1].num_rows == 30
        assert batches[2].num_rows == 30
        assert batches[3].num_rows == 10

    def test_for_loop_iteration(self, larger_csv):
        """Test that standard for loop works."""
        import vroom_csv

        total_rows = 0
        for batch in vroom_csv.read_csv_batched(larger_csv, batch_size=25):
            total_rows += batch.num_rows

        assert total_rows == 100

    def test_early_termination(self, larger_csv):
        """Test that early termination works correctly."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(larger_csv, batch_size=25)
        batch = next(reader)

        assert batch.num_rows == 25
        # Early termination - reader is dropped, should be safe


class TestBatchedReaderOptions:
    """Tests for read_csv_batched options."""

    def test_custom_batch_size(self, larger_csv):
        """Test custom batch_size parameter."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(larger_csv, batch_size=50)
        batches = list(reader)

        assert len(batches) == 2
        assert batches[0].num_rows == 50
        assert batches[1].num_rows == 50

    def test_delimiter_option(self, tsv_file):
        """Test delimiter parameter for TSV files."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(tsv_file, delimiter="\t")
        batch = next(reader)

        assert batch.column_names == ["name", "value"]
        assert batch.column("name") == ["Alice", "Bob"]

    def test_no_header_option(self, no_header_csv):
        """Test has_header=False generates column names."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(no_header_csv, has_header=False)
        batch = next(reader)

        assert batch.column_names == ["column_0", "column_1", "column_2"]
        assert batch.num_rows == 2

    def test_reader_properties(self, simple_csv):
        """Test BatchedReader properties."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv, batch_size=100)

        assert reader.batch_size == 100
        assert simple_csv in reader.path
        # Note: column_names is empty until first batch is read
        # (header is parsed lazily during first next_row call)
        assert reader.column_names == []

        # After reading first batch, column names become available
        batch = next(reader)
        assert reader.column_names == ["name", "age", "city"]

    def test_reader_repr(self, simple_csv):
        """Test the string representation of BatchedReader."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv, batch_size=100)

        repr_str = repr(reader)
        assert "BatchedReader" in repr_str
        assert "batch_size=100" in repr_str


class TestBatchedReaderErrors:
    """Tests for error handling."""

    def test_nonexistent_file_raises(self):
        """Test that reading a nonexistent file raises ValueError."""
        import vroom_csv

        with pytest.raises(ValueError, match="Failed to open"):
            vroom_csv.read_csv_batched("/nonexistent/path/file.csv")

    def test_invalid_delimiter_raises(self, simple_csv):
        """Test that multi-character delimiter raises ValueError."""
        import vroom_csv

        with pytest.raises(ValueError, match="single character"):
            vroom_csv.read_csv_batched(simple_csv, delimiter=";;")

    def test_invalid_quote_char_raises(self, simple_csv):
        """Test that multi-character quote_char raises ValueError."""
        import vroom_csv

        with pytest.raises(ValueError, match="single character"):
            vroom_csv.read_csv_batched(simple_csv, quote_char="''")

    def test_column_index_out_of_range(self, simple_csv):
        """Test that invalid column index raises IndexError."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        with pytest.raises(IndexError):
            batch.column(100)

    def test_column_name_not_found(self, simple_csv):
        """Test that invalid column name raises KeyError."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        with pytest.raises(KeyError):
            batch.column("nonexistent")

    def test_row_index_out_of_range(self, simple_csv):
        """Test that invalid row index raises IndexError."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        with pytest.raises(IndexError):
            batch.row(100)


class TestBatchedReaderArrowCapsule:
    """Tests for Arrow PyCapsule interface on RecordBatch."""

    def test_batch_has_arrow_c_schema(self, simple_csv):
        """Test that __arrow_c_schema__ method exists."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        assert hasattr(batch, "__arrow_c_schema__")
        capsule = batch.__arrow_c_schema__()
        assert capsule is not None

    def test_batch_has_arrow_c_stream(self, simple_csv):
        """Test that __arrow_c_stream__ method exists."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)

        assert hasattr(batch, "__arrow_c_stream__")
        capsule = batch.__arrow_c_stream__()
        assert capsule is not None


# Try to import pyarrow, skip tests if not available
try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestBatchedReaderPyArrow:
    """Tests for PyArrow interoperability."""

    def test_batch_to_pyarrow_table(self, simple_csv):
        """Test converting batch to PyArrow Table."""
        import pyarrow as pa

        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)
        arrow_table = pa.table(batch)

        assert arrow_table.num_rows == 3
        assert arrow_table.num_columns == 3
        assert arrow_table.column_names == ["name", "age", "city"]

    def test_batch_data_values(self, simple_csv):
        """Test that data values are correctly transferred."""
        import pyarrow as pa

        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)
        arrow_table = pa.table(batch)

        names = arrow_table.column("name").to_pylist()
        assert names == ["Alice", "Bob", "Charlie"]

        # age should be auto-inferred as int64
        ages = arrow_table.column("age").to_pylist()
        assert ages == [30, 25, 35]

    def test_batch_type_inference(self, larger_csv):
        """Test automatic type inference in batches."""
        import pyarrow as pa

        import vroom_csv

        reader = vroom_csv.read_csv_batched(larger_csv, batch_size=25)
        batch = next(reader)
        arrow_table = pa.table(batch)

        # id and value should be int64 (numeric)
        assert pa.types.is_int64(arrow_table.column("id").type)
        assert pa.types.is_int64(arrow_table.column("value").type)
        # category should be string
        assert pa.types.is_string(arrow_table.column("category").type)

    def test_iterate_and_convert_all_batches(self, larger_csv):
        """Test iterating and converting all batches to PyArrow."""
        import pyarrow as pa

        import vroom_csv

        tables = []
        for batch in vroom_csv.read_csv_batched(larger_csv, batch_size=25):
            tables.append(pa.table(batch))

        assert len(tables) == 4

        # Combine all tables
        combined = pa.concat_tables(tables)
        assert combined.num_rows == 100


# Try to import polars, skip tests if not available
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
class TestBatchedReaderPolars:
    """Tests for Polars interoperability."""

    def test_batch_to_polars_dataframe(self, simple_csv):
        """Test converting batch to Polars DataFrame."""
        import polars as pl

        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)
        df = pl.from_arrow(batch)

        assert df.shape == (3, 3)
        assert df.columns == ["name", "age", "city"]

    def test_batch_data_values_polars(self, simple_csv):
        """Test that data values are correctly transferred to Polars."""
        import polars as pl

        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv)
        batch = next(reader)
        df = pl.from_arrow(batch)

        names = df["name"].to_list()
        assert names == ["Alice", "Bob", "Charlie"]

        # age should be auto-inferred as int64
        ages = df["age"].to_list()
        assert ages == [30, 25, 35]

    def test_process_large_file_in_batches(self, larger_csv):
        """Test processing large file in batches with Polars."""
        import polars as pl

        import vroom_csv

        total_value = 0
        for batch in vroom_csv.read_csv_batched(larger_csv, batch_size=25):
            df = pl.from_arrow(batch)
            total_value += df["value"].sum()

        # Sum of 0*10, 10*10, 20*10, ... 990*10 = 10 * (0 + 10 + 20 + ... + 990)
        # = 10 * 10 * (0 + 1 + 2 + ... + 99) = 100 * (99 * 100 / 2) = 100 * 4950 = 495000
        expected = sum(i * 10 for i in range(100))
        assert total_value == expected


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestBatchedReaderNullHandling:
    """Tests for null value handling in batched reading."""

    def test_default_null_values(self, csv_with_nulls):
        """Test that default null values are recognized."""
        import pyarrow as pa

        import vroom_csv

        reader = vroom_csv.read_csv_batched(csv_with_nulls)
        batch = next(reader)
        arrow_table = pa.table(batch)

        # Note: "100" is inferred as int64 due to type inference
        values = arrow_table.column("value").to_pylist()
        assert values[0] == 100   # valid value (inferred as int64)
        assert values[1] is None  # NA
        assert values[2] is None  # empty

    def test_custom_null_values(self, csv_with_nulls):
        """Test custom null_values parameter."""
        import pyarrow as pa

        import vroom_csv

        reader = vroom_csv.read_csv_batched(csv_with_nulls, null_values=["NA", "inactive"])
        batch = next(reader)
        arrow_table = pa.table(batch)

        # "NA" should be null
        values = arrow_table.column("value").to_pylist()
        assert values[1] is None

        # "inactive" should be null
        status = arrow_table.column("status").to_pylist()
        assert status[1] is None


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestBatchedReaderDtype:
    """Tests for dtype parameter in batched reading."""

    def test_dtype_override(self, larger_csv):
        """Test that dtype parameter overrides type inference."""
        import pyarrow as pa

        import vroom_csv

        reader = vroom_csv.read_csv_batched(larger_csv, dtype={"id": "string"})
        batch = next(reader)
        arrow_table = pa.table(batch)

        # id should be string (overridden), not int64
        assert pa.types.is_string(arrow_table.column("id").type)
        ids = arrow_table.column("id").to_pylist()
        assert ids[0] == "0"

    def test_dtype_int64(self, simple_csv):
        """Test int64 dtype conversion."""
        import pyarrow as pa

        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv, dtype={"age": "int64"})
        batch = next(reader)
        arrow_table = pa.table(batch)

        assert pa.types.is_int64(arrow_table.column("age").type)
        ages = arrow_table.column("age").to_pylist()
        assert ages == [30, 25, 35]

    def test_unknown_dtype_raises(self, simple_csv):
        """Test that unknown dtype raises ValueError."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv, dtype={"age": "unknown_type"})

        with pytest.raises(ValueError, match="Unknown dtype"):
            next(reader)

    def test_unknown_column_for_dtype_raises(self, simple_csv):
        """Test that dtype for unknown column raises ValueError."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv, dtype={"nonexistent": "int64"})

        with pytest.raises(ValueError, match="Column not found"):
            next(reader)


class TestBatchedReaderDataIntegrity:
    """Tests for data integrity across batches."""

    def test_all_rows_read(self, larger_csv):
        """Test that all rows are read across batches."""
        import vroom_csv

        all_ids = []
        for batch in vroom_csv.read_csv_batched(larger_csv, batch_size=25):
            # Get ids as strings and convert to int
            ids = batch.column("id")
            all_ids.extend(int(id_) for id_ in ids)

        assert len(all_ids) == 100
        assert all_ids == list(range(100))

    def test_row_order_preserved(self, larger_csv):
        """Test that row order is preserved across batches."""
        import vroom_csv

        all_values = []
        for batch in vroom_csv.read_csv_batched(larger_csv, batch_size=30):
            values = batch.column("value")
            all_values.extend(int(v) for v in values)

        expected = [i * 10 for i in range(100)]
        assert all_values == expected

    def test_column_consistency(self, larger_csv):
        """Test that column names are consistent across batches."""
        import vroom_csv

        expected_columns = ["id", "value", "category"]
        for batch in vroom_csv.read_csv_batched(larger_csv, batch_size=25):
            assert batch.column_names == expected_columns


class TestBatchedReaderEdgeCases:
    """Tests for edge cases."""

    def test_empty_file(self):
        """Test reading an empty file."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("")
            empty_path = f.name

        reader = vroom_csv.read_csv_batched(empty_path)
        batches = list(reader)
        assert len(batches) == 0

    def test_header_only_file(self):
        """Test reading a file with only header."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("a,b,c\n")
            path = f.name

        reader = vroom_csv.read_csv_batched(path)
        batches = list(reader)
        assert len(batches) == 0

    def test_single_row_file(self):
        """Test reading a file with a single data row."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("a,b,c\n1,2,3\n")
            path = f.name

        reader = vroom_csv.read_csv_batched(path)
        batches = list(reader)
        assert len(batches) == 1
        assert batches[0].num_rows == 1

    def test_batch_size_equals_rows(self, larger_csv):
        """Test when batch_size equals total rows."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(larger_csv, batch_size=100)
        batches = list(reader)
        assert len(batches) == 1
        assert batches[0].num_rows == 100

    def test_batch_size_larger_than_rows(self, simple_csv):
        """Test when batch_size is larger than total rows."""
        import vroom_csv

        reader = vroom_csv.read_csv_batched(simple_csv, batch_size=10000)
        batches = list(reader)
        assert len(batches) == 1
        assert batches[0].num_rows == 3

    def test_quoted_fields(self):
        """Test handling of quoted fields with special characters."""
        import vroom_csv

        content = 'name,value\n"Alice, Jr.",100\n"Bob ""The Builder""",200\n'
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            path = f.name

        reader = vroom_csv.read_csv_batched(path)
        batch = next(reader)

        names = batch.column("name")
        assert names[0] == "Alice, Jr."
        assert names[1] == 'Bob "The Builder"'


class TestBatchedReaderProgress:
    """Tests for progress callback support in read_csv_batched."""

    @pytest.fixture
    def larger_csv(self):
        """Create a larger CSV file for progress testing."""
        lines = ["id,value,description"]
        for i in range(5000):
            lines.append(f"{i},{i * 10},description for row {i}")
        content = "\n".join(lines) + "\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            yield f.name

    def test_progress_callback_is_called(self, larger_csv):
        """Test that progress callback is invoked during batch reading."""
        import vroom_csv

        call_count = [0]
        progress_values = []

        def progress_callback(bytes_read, total_bytes):
            call_count[0] += 1
            progress_values.append((bytes_read, total_bytes))

        batches = list(
            vroom_csv.read_csv_batched(larger_csv, batch_size=1000, progress=progress_callback)
        )

        # Should have read 5000 rows in ~5 batches
        assert len(batches) == 5
        # Callback should have been called at least once per batch
        assert call_count[0] >= 5
        # All progress values should have consistent total_bytes
        if progress_values:
            total = progress_values[0][1]
            for bytes_read, total_bytes in progress_values:
                assert total_bytes == total

    def test_progress_callback_total_bytes_matches_file_size(self, larger_csv):
        """Test that total_bytes in callback matches actual file size."""
        import os

        import vroom_csv

        file_size = os.path.getsize(larger_csv)
        reported_total = [None]

        def progress_callback(bytes_read, total_bytes):
            reported_total[0] = total_bytes

        list(vroom_csv.read_csv_batched(larger_csv, progress=progress_callback))

        assert reported_total[0] == file_size

    def test_progress_callback_reaches_100_percent(self, larger_csv):
        """Test that progress callback reaches 100% at the end."""
        import vroom_csv

        final_progress = [None]

        def progress_callback(bytes_read, total_bytes):
            final_progress[0] = (bytes_read, total_bytes)

        list(vroom_csv.read_csv_batched(larger_csv, progress=progress_callback))

        # Final call should report bytes_read == total_bytes (100%)
        assert final_progress[0] is not None
        bytes_read, total_bytes = final_progress[0]
        assert bytes_read == total_bytes

    def test_progress_callback_monotonically_increasing(self, larger_csv):
        """Test that bytes_read values are monotonically non-decreasing."""
        import vroom_csv

        bytes_values = []

        def progress_callback(bytes_read, total_bytes):
            bytes_values.append(bytes_read)

        list(vroom_csv.read_csv_batched(larger_csv, batch_size=500, progress=progress_callback))

        # Values should be non-decreasing
        for i in range(1, len(bytes_values)):
            assert bytes_values[i] >= bytes_values[i - 1]

    def test_progress_callback_with_none(self, larger_csv):
        """Test that None progress callback works (no callback)."""
        import vroom_csv

        # Should not raise any errors
        batches = list(vroom_csv.read_csv_batched(larger_csv, progress=None))
        assert len(batches) > 0

    def test_progress_callback_exception_handling(self, larger_csv):
        """Test that exceptions in progress callback are handled properly."""
        import vroom_csv

        def bad_callback(bytes_read, total_bytes):
            raise ValueError("Callback error")

        # Exception in callback should propagate to caller
        with pytest.raises(ValueError, match="Callback error"):
            list(vroom_csv.read_csv_batched(larger_csv, progress=bad_callback))

    def test_progress_callback_preserves_data_integrity(self, larger_csv):
        """Test that using progress callback doesn't affect parsed data."""
        import vroom_csv

        # Parse without callback
        batches_without = list(vroom_csv.read_csv_batched(larger_csv, batch_size=1000))

        # Parse with callback
        def progress_callback(bytes_read, total_bytes):
            pass

        batches_with = list(
            vroom_csv.read_csv_batched(larger_csv, batch_size=1000, progress=progress_callback)
        )

        # Data should be identical
        assert len(batches_without) == len(batches_with)
        for batch_a, batch_b in zip(batches_without, batches_with):
            assert batch_a.num_rows == batch_b.num_rows
            assert batch_a.column_names == batch_b.column_names

    def test_default_progress_with_batched(self, larger_csv):
        """Test that default_progress can be used with read_csv_batched."""
        import io
        import sys

        import vroom_csv

        # Capture stderr
        old_stderr = sys.stderr
        sys.stderr = io.StringIO()

        try:
            list(vroom_csv.read_csv_batched(larger_csv, progress=vroom_csv.default_progress))
            output = sys.stderr.getvalue()
        finally:
            sys.stderr = old_stderr

        # Progress bar should have been printed
        assert "%" in output
