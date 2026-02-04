"""Basic tests for vroom-csv Python bindings."""

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
def tsv_file():
    """Create a TSV file for testing."""
    content = "name\tage\tcity\nAlice\t30\tNew York\nBob\t25\tLos Angeles\nCharlie\t35\tChicago\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def no_header_csv():
    """Create a CSV file without header."""
    content = "Alice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestReadCsv:
    """Tests for read_csv function."""

    def test_read_simple_csv(self, simple_csv):
        """Test reading a simple CSV file."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_read_with_explicit_delimiter(self, tsv_file):
        """Test reading with explicit tab delimiter."""
        import vroom_csv

        table = vroom_csv.read_csv(tsv_file, delimiter="\t")

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_read_without_header(self, no_header_csv):
        """Test reading a file without header."""
        import vroom_csv

        table = vroom_csv.read_csv(no_header_csv, has_header=False)

        assert table.num_rows == 3
        assert table.num_columns == 3
        # Should have auto-generated column names
        assert table.column_names == ["column_0", "column_1", "column_2"]

    def test_read_nonexistent_file(self):
        """Test error handling for non-existent file."""
        import vroom_csv

        with pytest.raises((ValueError, vroom_csv.VroomError)):
            vroom_csv.read_csv("/nonexistent/path/to/file.csv")

    def test_invalid_delimiter(self, simple_csv):
        """Test error for invalid delimiter."""
        import vroom_csv

        with pytest.raises(ValueError, match="single character"):
            vroom_csv.read_csv(simple_csv, delimiter=",,")

    def test_multi_threaded_parsing(self, simple_csv):
        """Test multi-threaded parsing works."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, num_threads=4)

        assert table.num_rows == 3
        assert table.num_columns == 3


class TestSkipRowsAndNRows:
    """Tests for skip_rows and n_rows parameters."""

    @pytest.fixture
    def larger_csv(self):
        """Create a larger CSV file for skip/limit testing."""
        lines = ["id,value"]
        for i in range(10):
            lines.append(f"{i},{i * 10}")
        content = "\n".join(lines) + "\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            return f.name

    def test_skip_rows_basic(self, larger_csv):
        """Test skipping first N rows."""
        import vroom_csv

        table = vroom_csv.read_csv(larger_csv, skip_rows=3)

        assert table.num_rows == 7
        assert table.row(0) == ["3", "30"]
        assert table.column("id") == ["3", "4", "5", "6", "7", "8", "9"]

    def test_n_rows_basic(self, larger_csv):
        """Test reading only first N rows."""
        import vroom_csv

        table = vroom_csv.read_csv(larger_csv, n_rows=3)

        assert table.num_rows == 3
        assert table.row(0) == ["0", "0"]
        assert table.row(2) == ["2", "20"]
        assert table.column("id") == ["0", "1", "2"]

    def test_skip_rows_and_n_rows_combined(self, larger_csv):
        """Test using skip_rows and n_rows together."""
        import vroom_csv

        table = vroom_csv.read_csv(larger_csv, skip_rows=2, n_rows=4)

        assert table.num_rows == 4
        assert table.row(0) == ["2", "20"]
        assert table.row(3) == ["5", "50"]
        assert table.column("id") == ["2", "3", "4", "5"]

    def test_skip_rows_exceeds_total(self, larger_csv):
        """Test skip_rows larger than total rows returns empty table."""
        import vroom_csv

        table = vroom_csv.read_csv(larger_csv, skip_rows=100)

        assert table.num_rows == 0

    def test_skip_rows_with_no_header(self):
        """Test skip_rows works correctly without header."""
        content = "0,a\n1,b\n2,c\n3,d\n4,e\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            path = f.name

        import vroom_csv

        table = vroom_csv.read_csv(path, has_header=False, skip_rows=2)

        assert table.num_rows == 3
        assert table.row(0) == ["2", "c"]

    def test_repr_shows_filtered_count(self, larger_csv):
        """Test __repr__ shows filtered row count."""
        import vroom_csv

        table = vroom_csv.read_csv(larger_csv, skip_rows=5, n_rows=3)

        assert "3 rows" in repr(table)


class TestTable:
    """Tests for Table class."""

    def test_column_by_index(self, simple_csv):
        """Test getting column by index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        names = table.column(0)
        assert names == ["Alice", "Bob", "Charlie"]

        ages = table.column(1)
        assert ages == ["30", "25", "35"]

    def test_column_by_name(self, simple_csv):
        """Test getting column by name."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        names = table.column("name")
        assert names == ["Alice", "Bob", "Charlie"]

        cities = table.column("city")
        assert cities == ["New York", "Los Angeles", "Chicago"]

    def test_column_not_found(self, simple_csv):
        """Test error for non-existent column."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        with pytest.raises(KeyError, match="nonexistent"):
            table.column("nonexistent")

    def test_column_index_out_of_range(self, simple_csv):
        """Test error for out-of-range column index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        with pytest.raises(IndexError):
            table.column(100)

    def test_row(self, simple_csv):
        """Test getting row by index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        row0 = table.row(0)
        assert row0 == ["Alice", "30", "New York"]

        row2 = table.row(2)
        assert row2 == ["Charlie", "35", "Chicago"]

    def test_row_index_out_of_range(self, simple_csv):
        """Test error for out-of-range row index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        with pytest.raises(IndexError):
            table.row(100)

    def test_len(self, simple_csv):
        """Test __len__ returns num_rows."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        assert len(table) == 3

    def test_repr(self, simple_csv):
        """Test string representation."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        assert "3 rows" in repr(table)
        assert "3 columns" in repr(table)


class TestVersion:
    """Tests for version information."""

    def test_version_exists(self):
        """Test that version is exposed."""
        import vroom_csv

        assert hasattr(vroom_csv, "__version__")
        assert isinstance(vroom_csv.__version__, str)

    def test_libvroom_version_exists(self):
        """Test that libvroom version is exposed."""
        import vroom_csv

        assert hasattr(vroom_csv, "LIBVROOM_VERSION")
        assert isinstance(vroom_csv.LIBVROOM_VERSION, str)


class TestProgressCallback:
    """Tests for progress callback functionality."""

    @pytest.fixture
    def simple_csv(self):
        """Create a simple CSV file for testing."""
        content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            return f.name

    def test_progress_callback_is_called(self, simple_csv):
        """Test that progress callback is invoked during parsing."""
        import vroom_csv

        call_count = [0]
        progress_values = []

        def progress_callback(bytes_read, total_bytes):
            call_count[0] += 1
            progress_values.append((bytes_read, total_bytes))

        table = vroom_csv.read_csv(simple_csv, progress=progress_callback)

        assert table.num_rows == 3
        # Callback should have been called at least once
        assert call_count[0] >= 1
        # All progress values should have consistent total_bytes
        if progress_values:
            total = progress_values[0][1]
            for bytes_read, total_bytes in progress_values:
                assert total_bytes == total
                assert bytes_read >= 0
                assert bytes_read <= total_bytes

    def test_progress_callback_total_bytes_matches_file_size(self, simple_csv):
        """Test that total_bytes in callback matches actual file size."""
        import os

        import vroom_csv

        file_size = os.path.getsize(simple_csv)
        reported_total = [None]

        def progress_callback(bytes_read, total_bytes):
            reported_total[0] = total_bytes

        vroom_csv.read_csv(simple_csv, progress=progress_callback)

        assert reported_total[0] == file_size

    def test_progress_callback_reaches_100_percent(self, simple_csv):
        """Test that progress callback reaches 100% at the end."""
        import vroom_csv

        final_progress = [None]

        def progress_callback(bytes_read, total_bytes):
            final_progress[0] = (bytes_read, total_bytes)

        table = vroom_csv.read_csv(simple_csv, progress=progress_callback)

        # Final call should report bytes_read == total_bytes (100%)
        assert final_progress[0] is not None
        bytes_read, total_bytes = final_progress[0]
        assert bytes_read == total_bytes

    def test_progress_callback_monotonically_increasing(self, simple_csv):
        """Test that bytes_read values are monotonically non-decreasing."""
        import vroom_csv

        bytes_values = []

        def progress_callback(bytes_read, total_bytes):
            bytes_values.append(bytes_read)

        vroom_csv.read_csv(simple_csv, progress=progress_callback)

        # Values should be non-decreasing
        for i in range(1, len(bytes_values)):
            assert bytes_values[i] >= bytes_values[i - 1]

    def test_progress_callback_with_none(self, simple_csv):
        """Test that None progress callback works (no callback)."""
        import vroom_csv

        # Should not raise any errors
        table = vroom_csv.read_csv(simple_csv, progress=None)
        assert table.num_rows == 3

    def test_progress_callback_with_multi_threading(self, simple_csv):
        """Test that progress callback works with multi-threaded parsing."""
        import vroom_csv

        call_count = [0]

        def progress_callback(bytes_read, total_bytes):
            call_count[0] += 1

        table = vroom_csv.read_csv(simple_csv, num_threads=4, progress=progress_callback)

        assert table.num_rows == 3
        assert call_count[0] >= 1

    def test_progress_callback_exception_handling(self, simple_csv):
        """Test that exceptions in progress callback are handled properly."""
        import vroom_csv

        def bad_callback(bytes_read, total_bytes):
            raise ValueError("Callback error")

        # Exception in callback should propagate to caller
        with pytest.raises(ValueError, match="Callback error"):
            vroom_csv.read_csv(simple_csv, progress=bad_callback)

    @pytest.fixture
    def larger_csv(self):
        """Create a larger CSV file for progress testing."""
        lines = ["id,value,description"]
        # Create ~100KB of data to ensure multiple progress callbacks
        for i in range(5000):
            lines.append(f"{i},{i * 10},description for row {i}")
        content = "\n".join(lines) + "\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            return f.name

    def test_progress_callback_with_larger_file(self, larger_csv):
        """Test progress callback with a larger file."""
        import vroom_csv

        progress_values = []

        def progress_callback(bytes_read, total_bytes):
            progress_values.append((bytes_read, total_bytes))

        table = vroom_csv.read_csv(larger_csv, progress=progress_callback)

        assert table.num_rows == 5000
        assert len(progress_values) >= 1
        # First should start at 0 or low value, last should reach total
        assert progress_values[-1][0] == progress_values[-1][1]

    def test_progress_callback_preserves_data_integrity(self, simple_csv):
        """Test that using progress callback doesn't affect parsed data."""
        import vroom_csv

        # Parse without callback
        table_without = vroom_csv.read_csv(simple_csv)

        # Parse with callback
        def progress_callback(bytes_read, total_bytes):
            pass

        table_with = vroom_csv.read_csv(simple_csv, progress=progress_callback)

        # Data should be identical
        assert table_without.num_rows == table_with.num_rows
        assert table_without.num_columns == table_with.num_columns
        assert table_without.column_names == table_with.column_names
        for i in range(table_without.num_rows):
            assert table_without.row(i) == table_with.row(i)

    def test_default_progress_is_exported(self):
        """Test that default_progress is exported from vroom_csv."""
        import vroom_csv

        assert hasattr(vroom_csv, "default_progress")
        assert callable(vroom_csv.default_progress)

    def test_default_progress_with_read_csv(self, simple_csv):
        """Test that default_progress can be used with read_csv."""
        import io
        import sys

        import vroom_csv

        # Capture stderr
        old_stderr = sys.stderr
        sys.stderr = io.StringIO()

        try:
            table = vroom_csv.read_csv(simple_csv, progress=vroom_csv.default_progress)
            output = sys.stderr.getvalue()
        finally:
            sys.stderr = old_stderr

        assert table.num_rows == 3
        # Progress bar should have been printed
        assert "%" in output
        # Should show completion
        assert "100" in output

    def test_default_progress_format(self):
        """Test default_progress output format."""
        import io
        import sys

        import vroom_csv

        old_stderr = sys.stderr
        sys.stderr = io.StringIO()

        try:
            # Simulate 50% progress
            vroom_csv.default_progress(512, 1024)
            output = sys.stderr.getvalue()
        finally:
            sys.stderr = old_stderr

        # Should contain progress bar, percentage, and bytes
        assert "[" in output
        assert "]" in output
        assert "50" in output
        assert "%" in output

    def test_default_progress_zero_total(self):
        """Test default_progress handles zero total gracefully."""
        import io
        import sys

        import vroom_csv

        old_stderr = sys.stderr
        sys.stderr = io.StringIO()

        try:
            # Should not crash with zero total
            vroom_csv.default_progress(0, 0)
            output = sys.stderr.getvalue()
        finally:
            sys.stderr = old_stderr

        # Should produce no output (early return)
        assert output == ""


class TestExceptions:
    """Tests for exception types."""

    def test_vroom_error_exists(self):
        """Test VroomError exception exists."""
        import vroom_csv

        assert hasattr(vroom_csv, "VroomError")

    def test_parse_error_exists(self):
        """Test ParseError exception exists."""
        import vroom_csv

        assert hasattr(vroom_csv, "ParseError")

    def test_io_error_exists(self):
        """Test IOError exception exists."""
        import vroom_csv

        assert hasattr(vroom_csv, "IOError")


class TestMemoryMap:
    """Tests for memory_map parameter."""

    def test_memory_map_true_works(self, simple_csv):
        """Test reading with memory_map=True."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, memory_map=True)

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]
        assert table.column("name") == ["Alice", "Bob", "Charlie"]

    def test_memory_map_false_works(self, simple_csv):
        """Test reading with memory_map=False (traditional loading)."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, memory_map=False)

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_memory_map_none_default(self, simple_csv):
        """Test that memory_map=None (default) auto-detects based on file size."""
        import vroom_csv

        # Small file should not use mmap automatically
        table = vroom_csv.read_csv(simple_csv, memory_map=None)

        assert table.num_rows == 3
        assert table.num_columns == 3

    def test_memory_map_with_other_options(self, simple_csv):
        """Test memory_map works with other parameters."""
        import vroom_csv

        table = vroom_csv.read_csv(
            simple_csv,
            memory_map=True,
            skip_rows=1,
            n_rows=1,
        )

        assert table.num_rows == 1
        # After skipping first data row, second row is Bob
        assert table.row(0) == ["Bob", "25", "Los Angeles"]

    def test_memory_map_with_usecols(self, simple_csv):
        """Test memory_map works with column selection."""
        import vroom_csv

        table = vroom_csv.read_csv(
            simple_csv,
            memory_map=True,
            usecols=["name", "city"],
        )

        assert table.num_columns == 2
        assert table.column_names == ["name", "city"]
        assert table.column("name") == ["Alice", "Bob", "Charlie"]
        assert table.column("city") == ["New York", "Los Angeles", "Chicago"]

    def test_memory_map_nonexistent_file(self):
        """Test error handling for non-existent file with memory_map=True."""
        import vroom_csv

        with pytest.raises((ValueError, vroom_csv.VroomError)):
            vroom_csv.read_csv("/nonexistent/path/to/file.csv", memory_map=True)

    def test_memory_map_with_no_header(self, no_header_csv):
        """Test memory_map works with has_header=False."""
        import vroom_csv

        table = vroom_csv.read_csv(no_header_csv, memory_map=True, has_header=False)

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["column_0", "column_1", "column_2"]

    def test_memory_map_with_delimiter(self, tsv_file):
        """Test memory_map works with explicit delimiter."""
        import vroom_csv

        table = vroom_csv.read_csv(tsv_file, memory_map=True, delimiter="\t")

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_memory_map_preserves_table_interface(self, simple_csv):
        """Test that Table interface is consistent with memory_map enabled."""
        import vroom_csv

        table_mmap = vroom_csv.read_csv(simple_csv, memory_map=True)
        table_regular = vroom_csv.read_csv(simple_csv, memory_map=False)

        # Both should have same data
        assert table_mmap.num_rows == table_regular.num_rows
        assert table_mmap.num_columns == table_regular.num_columns
        assert table_mmap.column_names == table_regular.column_names

        for col in table_mmap.column_names:
            assert table_mmap.column(col) == table_regular.column(col)

        for row_idx in range(table_mmap.num_rows):
            assert table_mmap.row(row_idx) == table_regular.row(row_idx)
