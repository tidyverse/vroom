"""Tests for the full Phase 2 API: detect_dialect() and extended read_csv() options."""

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
def semicolon_csv():
    """Create a semicolon-delimited CSV file."""
    content = "name;age;city\nAlice;30;New York\nBob;25;Los Angeles\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def tsv_file():
    """Create a TSV file for testing."""
    content = "name\tage\tcity\nAlice\t30\tNew York\nBob\t25\tLos Angeles\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def quoted_csv():
    """Create a CSV with quoted fields."""
    content = 'name,description,value\n"Alice","Has a ""nickname""",100\n"Bob","Simple",200\n'
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def null_values_csv():
    """Create a CSV with null/NA values."""
    content = "name,age,city\nAlice,30,New York\nBob,NA,\nCharlie,N/A,Chicago\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def single_quote_csv():
    """Create a CSV with single quotes."""
    content = "name,description,value\n'Alice','Has a name',100\n'Bob','Simple',200\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestDetectDialect:
    """Tests for detect_dialect function."""

    def test_detect_csv_dialect(self, simple_csv):
        """Test detecting standard CSV dialect."""
        import vroom_csv

        dialect = vroom_csv.detect_dialect(simple_csv)

        assert dialect.delimiter == ","
        assert dialect.quote_char == '"'
        assert dialect.has_header is True
        assert dialect.confidence > 0.5

    def test_detect_semicolon_dialect(self, semicolon_csv):
        """Test detecting semicolon-delimited dialect."""
        import vroom_csv

        dialect = vroom_csv.detect_dialect(semicolon_csv)

        assert dialect.delimiter == ";"
        assert dialect.has_header is True
        assert dialect.confidence > 0.5

    def test_detect_tsv_dialect(self, tsv_file):
        """Test detecting tab-delimited dialect."""
        import vroom_csv

        dialect = vroom_csv.detect_dialect(tsv_file)

        assert dialect.delimiter == "\t"
        assert dialect.has_header is True

    def test_dialect_repr(self, simple_csv):
        """Test Dialect __repr__."""
        import vroom_csv

        dialect = vroom_csv.detect_dialect(simple_csv)
        repr_str = repr(dialect)

        assert "Dialect" in repr_str
        assert "delimiter" in repr_str
        assert "has_header" in repr_str

    def test_detect_nonexistent_file(self):
        """Test error handling for non-existent file."""
        import vroom_csv

        with pytest.raises(ValueError):
            vroom_csv.detect_dialect("/nonexistent/path/to/file.csv")

    def test_dialect_attributes(self, simple_csv):
        """Test all Dialect attributes are accessible."""
        import vroom_csv

        dialect = vroom_csv.detect_dialect(simple_csv)

        # All attributes should be accessible without error
        assert isinstance(dialect.delimiter, str)
        assert isinstance(dialect.quote_char, str)
        assert isinstance(dialect.escape_char, str)
        assert isinstance(dialect.double_quote, bool)
        assert isinstance(dialect.line_ending, str)
        assert isinstance(dialect.has_header, bool)
        assert isinstance(dialect.confidence, float)


class TestReadCsvQuoteChar:
    """Tests for quote_char option."""

    def test_explicit_quote_char(self, quoted_csv):
        """Test reading with explicit quote char."""
        import vroom_csv

        table = vroom_csv.read_csv(quoted_csv, quote_char='"')

        assert table.num_rows == 2
        assert table.num_columns == 3

    def test_single_quote_csv(self, single_quote_csv):
        """Test reading CSV with single quotes."""
        import vroom_csv

        table = vroom_csv.read_csv(single_quote_csv, quote_char="'")

        assert table.num_rows == 2

    def test_invalid_quote_char(self, simple_csv):
        """Test error for invalid quote_char."""
        import vroom_csv

        with pytest.raises(ValueError, match="single character"):
            vroom_csv.read_csv(simple_csv, quote_char="''")


class TestReadCsvUsecols:
    """Tests for usecols option."""

    def test_usecols_by_name(self, simple_csv):
        """Test reading specific columns by name."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, usecols=["name", "city"])

        assert table.num_columns == 2
        assert table.column_names == ["name", "city"]
        assert table.num_rows == 3

    def test_usecols_by_index(self, simple_csv):
        """Test reading specific columns by index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, usecols=[0, 2])

        assert table.num_columns == 2
        assert table.column_names == ["name", "city"]

    def test_usecols_mixed(self, simple_csv):
        """Test reading columns by mixed name and index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, usecols=["name", 1])

        assert table.num_columns == 2
        assert table.column_names == ["name", "age"]

    def test_usecols_invalid_name(self, simple_csv):
        """Test error for invalid column name in usecols."""
        import vroom_csv

        with pytest.raises(KeyError):
            vroom_csv.read_csv(simple_csv, usecols=["nonexistent"])

    def test_usecols_invalid_index(self, simple_csv):
        """Test error for out-of-range column index in usecols."""
        import vroom_csv

        with pytest.raises(IndexError):
            vroom_csv.read_csv(simple_csv, usecols=[100])

    def test_usecols_single_column(self, simple_csv):
        """Test reading a single column."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, usecols=["name"])

        assert table.num_columns == 1
        assert table.column_names == ["name"]


class TestReadCsvNullValues:
    """Tests for null_values and empty_is_null options."""

    def test_null_values_list(self, null_values_csv):
        """Test specifying null values as a list."""
        import vroom_csv

        table = vroom_csv.read_csv(null_values_csv, null_values=["NA", "N/A"])

        # The table should parse successfully
        assert table.num_rows == 3
        assert table.num_columns == 3

    def test_empty_is_null_true(self, null_values_csv):
        """Test empty_is_null=True (default)."""
        import vroom_csv

        table = vroom_csv.read_csv(null_values_csv, empty_is_null=True)

        assert table.num_rows == 3

    def test_empty_is_null_false(self, null_values_csv):
        """Test empty_is_null=False."""
        import vroom_csv

        table = vroom_csv.read_csv(null_values_csv, empty_is_null=False)

        assert table.num_rows == 3


class TestReadCsvEncoding:
    """Tests for encoding option."""

    def test_encoding_parameter_accepted(self, simple_csv):
        """Test that encoding parameter is accepted."""
        import vroom_csv

        # Should not raise an error
        table = vroom_csv.read_csv(simple_csv, encoding="utf-8")

        assert table.num_rows == 3


class TestReadCsvSkipRows:
    """Tests for skip_rows option."""

    def test_skip_rows_parameter_accepted(self, simple_csv):
        """Test that skip_rows parameter is accepted."""
        import vroom_csv

        # Should not raise an error (even though not fully implemented)
        table = vroom_csv.read_csv(simple_csv, skip_rows=0)

        assert table.num_rows == 3


class TestReadCsvNRows:
    """Tests for n_rows option."""

    def test_n_rows_parameter_accepted(self, simple_csv):
        """Test that n_rows parameter is accepted."""
        import vroom_csv

        # Should not raise an error (even though not fully implemented)
        table = vroom_csv.read_csv(simple_csv, n_rows=10)

        assert table.num_rows == 3


class TestReadCsvDtype:
    """Tests for dtype option."""

    def test_dtype_parameter_accepted(self, simple_csv):
        """Test that dtype parameter is accepted."""
        import vroom_csv

        # Should not raise an error (even though not fully implemented)
        table = vroom_csv.read_csv(simple_csv, dtype={"age": "int64"})

        assert table.num_rows == 3


class TestReadCsvCombinedOptions:
    """Tests for combining multiple options."""

    def test_delimiter_and_usecols(self, semicolon_csv):
        """Test combining delimiter with usecols."""
        import vroom_csv

        table = vroom_csv.read_csv(semicolon_csv, delimiter=";", usecols=["name", "age"])

        assert table.num_columns == 2
        assert table.column_names == ["name", "age"]
        assert table.num_rows == 2

    def test_all_options(self, simple_csv):
        """Test combining multiple options."""
        import vroom_csv

        table = vroom_csv.read_csv(
            simple_csv,
            delimiter=",",
            quote_char='"',
            has_header=True,
            usecols=["name", "city"],
            null_values=["NA"],
            empty_is_null=True,
            num_threads=1,
        )

        assert table.num_columns == 2
        assert table.num_rows == 3


class TestDialectWithReadCsv:
    """Tests for using detect_dialect with read_csv."""

    def test_detect_then_read(self, semicolon_csv):
        """Test detecting dialect then using it with read_csv."""
        import vroom_csv

        dialect = vroom_csv.detect_dialect(semicolon_csv)
        table = vroom_csv.read_csv(semicolon_csv, delimiter=dialect.delimiter)

        assert table.num_rows == 2
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]
