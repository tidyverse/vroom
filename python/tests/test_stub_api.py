"""Tests for the current stub API after libvroom2 migration.

These tests verify only the functionality that's actually implemented
in the stub bindings.
"""

import tempfile
import os

import pytest


@pytest.fixture
def simple_csv():
    """Create a simple CSV file for testing."""
    content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    f.write(content)
    f.close()
    yield f.name
    os.unlink(f.name)


@pytest.fixture
def tsv_file():
    """Create a TSV file for testing."""
    content = "name\tage\tcity\nAlice\t30\tNew York\nBob\t25\tLos Angeles\nCharlie\t35\tChicago\n"
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False)
    f.write(content)
    f.close()
    yield f.name
    os.unlink(f.name)


@pytest.fixture
def no_header_csv():
    """Create a CSV file without header."""
    content = "Alice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    f.write(content)
    f.close()
    yield f.name
    os.unlink(f.name)


class TestReadCsv:
    """Tests for read_csv function."""

    def test_read_simple_csv(self, simple_csv):
        """Test reading a simple CSV file."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_read_with_separator(self, tsv_file):
        """Test reading with explicit separator parameter."""
        import vroom_csv

        table = vroom_csv.read_csv(tsv_file, separator="\t")

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_read_without_header(self, no_header_csv):
        """Test reading a file without header."""
        import vroom_csv

        table = vroom_csv.read_csv(no_header_csv, has_header=False)

        assert table.num_rows == 3
        assert table.num_columns == 3

    def test_read_nonexistent_file(self):
        """Test error handling for non-existent file."""
        import vroom_csv

        with pytest.raises(RuntimeError):
            vroom_csv.read_csv("/nonexistent/path/to/file.csv")


class TestToParquet:
    """Tests for to_parquet function."""

    def test_basic_conversion(self, simple_csv):
        """Test basic CSV to Parquet conversion."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path)
            assert os.path.exists(output_path)
            assert os.path.getsize(output_path) > 0
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_compression_zstd(self, simple_csv):
        """Test conversion with ZSTD compression."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path, compression="zstd")
            assert os.path.exists(output_path)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_compression_snappy(self, simple_csv):
        """Test conversion with Snappy compression."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path, compression="snappy")
            assert os.path.exists(output_path)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_compression_none(self, simple_csv):
        """Test conversion with no compression."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path, compression="none")
            assert os.path.exists(output_path)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_nonexistent_input(self):
        """Test error for non-existent input file."""
        import vroom_csv

        with pytest.raises(RuntimeError):
            vroom_csv.to_parquet("/nonexistent/file.csv", "/tmp/output.parquet")


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


class TestVersion:
    """Tests for version information."""

    def test_version_exists(self):
        """Test that version is exposed."""
        import vroom_csv

        assert hasattr(vroom_csv, "__version__")
        assert isinstance(vroom_csv.__version__, str)


class TestErrorHandling:
    """Tests for error handling functionality."""

    @pytest.fixture
    def empty_header_csv(self):
        """Create a CSV file with empty header."""
        content = "\n1,2,3\n4,5,6\n"
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write(content)
        f.close()
        yield f.name
        os.unlink(f.name)

    @pytest.fixture
    def duplicate_column_csv(self):
        """Create a CSV file with duplicate column names."""
        content = "A,B,A,C,B\n1,2,3,4,5\n6,7,8,9,10\n"
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write(content)
        f.close()
        yield f.name
        os.unlink(f.name)

    def test_error_mode_disabled_default(self, simple_csv):
        """Test that error_mode defaults to disabled (no errors collected)."""
        import vroom_csv

        # Default mode should work without issues
        table = vroom_csv.read_csv(simple_csv)
        assert table.num_rows == 3

    def test_error_mode_permissive_with_warnings(self, duplicate_column_csv):
        """Test permissive mode with duplicate column warnings."""
        import vroom_csv

        # Permissive mode should succeed even with warnings
        table = vroom_csv.read_csv(duplicate_column_csv, error_mode="permissive")
        assert table.num_rows == 2
        assert table.num_columns == 5

    def test_error_mode_strict_with_warnings(self, duplicate_column_csv):
        """Test that strict mode catches warnings."""
        import vroom_csv

        # Strict mode should NOT fail on warnings (only errors)
        # Duplicate column names are warnings, not errors
        table = vroom_csv.read_csv(duplicate_column_csv, error_mode="strict")
        assert table.num_rows == 2

    def test_error_mode_fail_fast_alias(self, simple_csv):
        """Test that 'fail_fast' works as error_mode."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, error_mode="fail_fast")
        assert table.num_rows == 3

    def test_error_mode_best_effort(self, simple_csv):
        """Test best_effort error mode."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, error_mode="best_effort")
        assert table.num_rows == 3

    def test_invalid_error_mode(self, simple_csv):
        """Test that invalid error_mode raises error."""
        import vroom_csv

        with pytest.raises(RuntimeError, match="Unknown error_mode"):
            vroom_csv.read_csv(simple_csv, error_mode="invalid_mode")

    def test_max_errors_enables_collection(self, simple_csv):
        """Test that setting max_errors enables error collection."""
        import vroom_csv

        # max_errors should auto-enable permissive mode
        table = vroom_csv.read_csv(simple_csv, max_errors=100)
        assert table.num_rows == 3

    def test_empty_header_fails(self, empty_header_csv):
        """Test that empty header causes failure."""
        import vroom_csv

        with pytest.raises(RuntimeError, match="empty"):
            vroom_csv.read_csv(empty_header_csv, separator=",", error_mode="permissive")

    def test_error_details_in_exception(self, empty_header_csv):
        """Test that error details are included in exception message."""
        import vroom_csv

        try:
            vroom_csv.read_csv(empty_header_csv, separator=",", error_mode="permissive")
            assert False, "Should have raised RuntimeError"
        except RuntimeError as e:
            error_msg = str(e)
            # Should contain some indication of the error
            assert "empty" in error_msg.lower() or "header" in error_msg.lower()


class TestToParquetErrorHandling:
    """Tests for error handling in to_parquet function."""

    @pytest.fixture
    def simple_csv(self):
        """Create a simple CSV file for testing."""
        content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\n"
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write(content)
        f.close()
        yield f.name
        os.unlink(f.name)

    @pytest.fixture
    def duplicate_column_csv(self):
        """Create a CSV file with duplicate column names."""
        content = "A,B,A\n1,2,3\n4,5,6\n"
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write(content)
        f.close()
        yield f.name
        os.unlink(f.name)

    def test_to_parquet_error_mode_permissive(self, duplicate_column_csv):
        """Test to_parquet with permissive error mode."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            # Should succeed with warnings (duplicate column names)
            vroom_csv.to_parquet(
                duplicate_column_csv, output_path, error_mode="permissive"
            )
            assert os.path.exists(output_path)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_to_parquet_error_mode_strict(self, simple_csv):
        """Test to_parquet with strict error mode."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path, error_mode="strict")
            assert os.path.exists(output_path)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_to_parquet_max_errors(self, simple_csv):
        """Test to_parquet with max_errors parameter."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path, max_errors=50)
            assert os.path.exists(output_path)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_to_parquet_invalid_error_mode(self, simple_csv):
        """Test that invalid error_mode raises error."""
        import vroom_csv

        with pytest.raises(RuntimeError, match="Unknown error_mode"):
            vroom_csv.to_parquet(simple_csv, "/tmp/out.parquet", error_mode="bad")
