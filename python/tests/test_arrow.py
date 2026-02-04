"""Tests for Arrow PyCapsule interface."""

import tempfile

import pytest


@pytest.fixture
def simple_csv():
    """Create a simple CSV file for testing."""
    content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestArrowCapsule:
    """Tests for Arrow PyCapsule interface methods."""

    def test_arrow_c_schema_method_exists(self, simple_csv):
        """Test that __arrow_c_schema__ method exists."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        assert hasattr(table, "__arrow_c_schema__")

    def test_arrow_c_stream_method_exists(self, simple_csv):
        """Test that __arrow_c_stream__ method exists."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        assert hasattr(table, "__arrow_c_stream__")

    def test_arrow_c_schema_returns_capsule(self, simple_csv):
        """Test that __arrow_c_schema__ returns a PyCapsule."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        capsule = table.__arrow_c_schema__()

        # Check it's a PyCapsule (limited checking available in pure Python)
        assert capsule is not None

    def test_arrow_c_stream_returns_capsule(self, simple_csv):
        """Test that __arrow_c_stream__ returns a PyCapsule."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        capsule = table.__arrow_c_stream__()

        assert capsule is not None


# Try to import pyarrow, skip tests if not available
try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestPyArrowInterop:
    """Tests for PyArrow interoperability."""

    def test_convert_to_pyarrow_table(self, simple_csv):
        """Test converting to PyArrow Table."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        assert arrow_table.num_rows == 3
        assert arrow_table.num_columns == 3
        assert arrow_table.column_names == ["name", "age", "city"]

    def test_pyarrow_column_types(self, simple_csv):
        """Test that column types are automatically inferred in PyArrow."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        # name column should be string (detected as string)
        assert pa.types.is_string(arrow_table.column("name").type)
        # age column should be int64 (detected as integer)
        assert pa.types.is_int64(arrow_table.column("age").type)
        # city column should be string (detected as string)
        assert pa.types.is_string(arrow_table.column("city").type)

    def test_pyarrow_data_values(self, simple_csv):
        """Test that data values are correctly transferred with inferred types."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        names = arrow_table.column("name").to_pylist()
        assert names == ["Alice", "Bob", "Charlie"]

        # ages are now integers (auto-inferred)
        ages = arrow_table.column("age").to_pylist()
        assert ages == [30, 25, 35]

        cities = arrow_table.column("city").to_pylist()
        assert cities == ["New York", "Los Angeles", "Chicago"]


# Try to import polars, skip tests if not available
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
class TestPolarsInterop:
    """Tests for Polars interoperability."""

    def test_convert_to_polars_dataframe(self, simple_csv):
        """Test converting to Polars DataFrame."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        df = pl.from_arrow(table)

        assert df.shape == (3, 3)
        assert df.columns == ["name", "age", "city"]

    def test_polars_data_values(self, simple_csv):
        """Test that data values are correctly transferred to Polars with inferred types."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        df = pl.from_arrow(table)

        names = df["name"].to_list()
        assert names == ["Alice", "Bob", "Charlie"]

        # ages are now integers (auto-inferred)
        ages = df["age"].to_list()
        assert ages == [30, 25, 35]


# =============================================================================
# Tests for null value handling in Arrow export
# =============================================================================


@pytest.fixture
def csv_with_nulls():
    """Create a CSV file with various null representations."""
    content = "name,value,status\nAlice,100,active\nBob,NA,inactive\nCharlie,,pending\nDave,N/A,\nEve,null,NULL\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def csv_with_custom_nulls():
    """Create a CSV file with custom null representations."""
    content = "name,value,status\nAlice,100,active\nBob,-999,inactive\nCharlie,missing,pending\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestNullValueHandling:
    """Tests for null value handling in Arrow export."""

    def test_default_null_values(self, csv_with_nulls):
        """Test that default null values (NA, N/A, null, NULL, empty) are recognized."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(csv_with_nulls)
        arrow_table = pa.table(table)

        # Check values column: 100, NA (null), "" (null), N/A (null), null (null)
        # Note: "100" is inferred as int64 due to type inference
        values = arrow_table.column("value").to_pylist()
        assert values[0] == 100    # valid value (inferred as int64)
        assert values[1] is None   # NA -> null
        assert values[2] is None   # empty string -> null (in default null_values)
        assert values[3] is None   # N/A -> null
        assert values[4] is None   # null -> null

    def test_null_count(self, csv_with_nulls):
        """Test that null_count is properly set in Arrow arrays."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(csv_with_nulls)
        arrow_table = pa.table(table)

        # "value" column has 4 nulls: NA, "", N/A, null
        value_array = arrow_table.column("value").chunk(0)
        assert value_array.null_count == 4

        # "status" column has 2 nulls: "" (row 4), NULL (row 5)
        status_array = arrow_table.column("status").chunk(0)
        assert status_array.null_count == 2

    def test_custom_null_values(self, csv_with_custom_nulls):
        """Test that custom null_values parameter works."""
        import pyarrow as pa

        import vroom_csv

        # Use custom null values
        table = vroom_csv.read_csv(csv_with_custom_nulls, null_values=["-999", "missing"])
        arrow_table = pa.table(table)

        values = arrow_table.column("value").to_pylist()
        assert values[0] == "100"  # valid value
        assert values[1] is None   # -999 -> null
        assert values[2] is None   # missing -> null

    def test_empty_is_null(self):
        """Test that empty_is_null=True treats empty strings as null."""
        import pyarrow as pa

        import vroom_csv

        content = "a,b\nfoo,bar\n,baz\nqux,\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        # Without empty_is_null, and with null_values not containing ""
        table = vroom_csv.read_csv(csv_path, null_values=["NA"], empty_is_null=False)
        arrow_table = pa.table(table)
        a_values = arrow_table.column("a").to_pylist()
        assert a_values == ["foo", "", "qux"]  # empty string preserved

        # With empty_is_null=True
        table = vroom_csv.read_csv(csv_path, null_values=["NA"], empty_is_null=True)
        arrow_table = pa.table(table)
        a_values = arrow_table.column("a").to_pylist()
        assert a_values[0] == "foo"
        assert a_values[1] is None  # empty -> null
        assert a_values[2] == "qux"

    def test_no_nulls_no_validity_bitmap(self, simple_csv):
        """Test that when there are no nulls, null_count is 0."""
        import pyarrow as pa

        import vroom_csv

        # Use null_values that don't appear in the data
        table = vroom_csv.read_csv(simple_csv, null_values=["NONEXISTENT"])
        arrow_table = pa.table(table)

        for col in arrow_table.columns:
            chunk = col.chunk(0)
            assert chunk.null_count == 0

    def test_all_nulls(self):
        """Test a column where all values are null."""
        import pyarrow as pa

        import vroom_csv

        content = "a,b\nNA,foo\nNA,bar\nNA,baz\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        table = vroom_csv.read_csv(csv_path, null_values=["NA"])
        arrow_table = pa.table(table)

        a_values = arrow_table.column("a").to_pylist()
        assert a_values == [None, None, None]

        a_chunk = arrow_table.column("a").chunk(0)
        assert a_chunk.null_count == 3

    def test_mixed_null_values(self):
        """Test with a mix of null value representations."""
        import pyarrow as pa

        import vroom_csv

        content = "val\n10\nNA\n20\nN/A\n30\nnull\n40\n\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        table = vroom_csv.read_csv(csv_path)  # using defaults
        arrow_table = pa.table(table)

        # Note: numeric values are inferred as int64 due to type inference
        values = arrow_table.column("val").to_pylist()
        expected = [10, None, 20, None, 30, None, 40, None]
        assert values == expected

    def test_null_handling_with_polars(self):
        """Test null handling when converting to Polars."""
        if not HAS_POLARS:
            pytest.skip("polars not installed")

        import polars as pl

        import vroom_csv

        content = "a,b\nfoo,10\nNA,20\nbar,NA\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        table = vroom_csv.read_csv(csv_path, null_values=["NA"])
        df = pl.from_arrow(table)

        a_values = df["a"].to_list()
        assert a_values[0] == "foo"
        assert a_values[1] is None
        assert a_values[2] == "bar"

        b_values = df["b"].to_list()
        assert b_values[0] == "10"
        assert b_values[1] == "20"
        assert b_values[2] is None


# =============================================================================
# dtype parameter tests
# =============================================================================


@pytest.fixture
def typed_csv():
    """Create a CSV file with various data types for testing."""
    content = "name,age,score,active\nAlice,30,95.5,true\nBob,25,87.3,false\nCharlie,35,92.1,yes\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def csv_with_dtype_nulls():
    """Create a CSV file with missing/null values for dtype tests."""
    content = "name,age,score,active\nAlice,30,95.5,true\nBob,,87.3,\nCharlie,invalid,NA,yes\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestDtypeParameter:
    """Tests for the dtype parameter in read_csv."""

    def test_dtype_int64(self, typed_csv):
        """Test that int64 dtype converts age column correctly."""
        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, dtype={"age": "int64"})

        # Verify table was parsed
        assert table.num_rows == 3
        assert table.num_columns == 4

    def test_dtype_float64(self, typed_csv):
        """Test that float64 dtype converts score column correctly."""
        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, dtype={"score": "float64"})

        # Verify table was parsed
        assert table.num_rows == 3
        assert table.num_columns == 4

    def test_dtype_bool(self, typed_csv):
        """Test that bool dtype converts active column correctly."""
        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, dtype={"active": "bool"})

        # Verify table was parsed
        assert table.num_rows == 3
        assert table.num_columns == 4

    def test_dtype_multiple_columns(self, typed_csv):
        """Test that multiple dtypes can be specified."""
        import vroom_csv

        table = vroom_csv.read_csv(
            typed_csv, dtype={"age": "int64", "score": "float64", "active": "bool"}
        )

        # Verify table was parsed
        assert table.num_rows == 3
        assert table.num_columns == 4

    def test_dtype_unknown_raises(self, typed_csv):
        """Test that unknown dtype raises ValueError."""
        import vroom_csv

        with pytest.raises(ValueError, match="Unknown dtype"):
            vroom_csv.read_csv(typed_csv, dtype={"age": "unknown_type"})

    def test_dtype_unknown_column_raises(self, typed_csv):
        """Test that unknown column name raises ValueError."""
        import vroom_csv

        with pytest.raises(ValueError, match="Column not found"):
            vroom_csv.read_csv(typed_csv, dtype={"nonexistent": "int64"})

    def test_dtype_string_synonyms(self, typed_csv):
        """Test that various string type names are accepted."""
        import vroom_csv

        for dtype_name in ["str", "string", "object"]:
            table = vroom_csv.read_csv(typed_csv, dtype={"name": dtype_name})
            assert table.num_rows == 3

    def test_dtype_int_synonyms(self, typed_csv):
        """Test that various int type names are accepted."""
        import vroom_csv

        for dtype_name in ["int", "int64", "Int64"]:
            table = vroom_csv.read_csv(typed_csv, dtype={"age": dtype_name})
            assert table.num_rows == 3

    def test_dtype_float_synonyms(self, typed_csv):
        """Test that various float type names are accepted."""
        import vroom_csv

        for dtype_name in ["float", "float64", "Float64", "double"]:
            table = vroom_csv.read_csv(typed_csv, dtype={"score": dtype_name})
            assert table.num_rows == 3

    def test_dtype_bool_synonyms(self, typed_csv):
        """Test that various bool type names are accepted."""
        import vroom_csv

        for dtype_name in ["bool", "boolean"]:
            table = vroom_csv.read_csv(typed_csv, dtype={"active": dtype_name})
            assert table.num_rows == 3


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestDtypeWithPyArrow:
    """Tests for dtype parameter with PyArrow conversion."""

    def test_dtype_int64_arrow_type(self, typed_csv):
        """Test that int64 dtype produces correct Arrow type."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, dtype={"age": "int64"})
        arrow_table = pa.table(table)

        assert pa.types.is_int64(arrow_table.column("age").type)

    def test_dtype_float64_arrow_type(self, typed_csv):
        """Test that float64 dtype produces correct Arrow type."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, dtype={"score": "float64"})
        arrow_table = pa.table(table)

        assert pa.types.is_float64(arrow_table.column("score").type)

    def test_dtype_bool_arrow_type(self, typed_csv):
        """Test that bool dtype produces correct Arrow type."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, dtype={"active": "bool"})
        arrow_table = pa.table(table)

        assert pa.types.is_boolean(arrow_table.column("active").type)

    def test_dtype_int64_values(self, typed_csv):
        """Test that int64 values are correctly converted."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, dtype={"age": "int64"})
        arrow_table = pa.table(table)

        ages = arrow_table.column("age").to_pylist()
        assert ages == [30, 25, 35]

    def test_dtype_float64_values(self, typed_csv):
        """Test that float64 values are correctly converted."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, dtype={"score": "float64"})
        arrow_table = pa.table(table)

        scores = arrow_table.column("score").to_pylist()
        assert scores == pytest.approx([95.5, 87.3, 92.1])

    def test_dtype_bool_values(self, typed_csv):
        """Test that bool values are correctly converted."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, dtype={"active": "bool"})
        arrow_table = pa.table(table)

        active = arrow_table.column("active").to_pylist()
        assert active == [True, False, True]  # yes is truthy

    def test_dtype_null_handling_int64(self, csv_with_dtype_nulls):
        """Test that invalid int64 values become null."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(csv_with_dtype_nulls, dtype={"age": "int64"})
        arrow_table = pa.table(table)

        ages = arrow_table.column("age").to_pylist()
        assert ages == [30, None, None]  # empty and "invalid" both become null

    def test_dtype_null_handling_float64(self, csv_with_dtype_nulls):
        """Test that NA values in float column become null."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(csv_with_dtype_nulls, dtype={"score": "float64"})
        arrow_table = pa.table(table)

        scores = arrow_table.column("score").to_pylist()
        assert scores[0] == pytest.approx(95.5)
        assert scores[1] == pytest.approx(87.3)
        assert scores[2] is None  # NA becomes null

    def test_dtype_null_handling_bool(self, csv_with_dtype_nulls):
        """Test that empty bool values become null."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(csv_with_dtype_nulls, dtype={"active": "bool"})
        arrow_table = pa.table(table)

        active = arrow_table.column("active").to_pylist()
        assert active == [True, None, True]  # empty becomes null

    def test_dtype_mixed_types(self, typed_csv):
        """Test that multiple columns with different types work together."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(
            typed_csv, dtype={"age": "int64", "score": "float64", "active": "bool"}
        )
        arrow_table = pa.table(table)

        # Check types
        assert pa.types.is_int64(arrow_table.column("age").type)
        assert pa.types.is_float64(arrow_table.column("score").type)
        assert pa.types.is_boolean(arrow_table.column("active").type)
        assert pa.types.is_string(arrow_table.column("name").type)  # default to string

        # Check values
        assert arrow_table.column("age").to_pylist() == [30, 25, 35]
        assert arrow_table.column("score").to_pylist() == pytest.approx([95.5, 87.3, 92.1])
        assert arrow_table.column("active").to_pylist() == [True, False, True]
        assert arrow_table.column("name").to_pylist() == ["Alice", "Bob", "Charlie"]

    def test_dtype_override_to_string(self, typed_csv):
        """Test that dtype can override inferred types back to string."""
        import pyarrow as pa

        import vroom_csv

        # Force age to string even though it would be inferred as int64
        table = vroom_csv.read_csv(typed_csv, dtype={"age": "string"})
        arrow_table = pa.table(table)

        # age should be string (overridden)
        assert pa.types.is_string(arrow_table.column("age").type)
        ages = arrow_table.column("age").to_pylist()
        assert ages == ["30", "25", "35"]

    def test_auto_inference_types(self, typed_csv):
        """Test that types are automatically inferred without dtype parameter."""
        import pyarrow as pa

        import vroom_csv

        # No dtype - let inference happen
        table = vroom_csv.read_csv(typed_csv)
        arrow_table = pa.table(table)

        # name should be string (inferred)
        assert pa.types.is_string(arrow_table.column("name").type)
        # age should be int64 (inferred from numeric values)
        assert pa.types.is_int64(arrow_table.column("age").type)
        # score should be float64 (inferred from decimal values)
        assert pa.types.is_float64(arrow_table.column("score").type)
        # active should be bool (inferred from true/false/yes values)
        assert pa.types.is_boolean(arrow_table.column("active").type)

        # Check values
        assert arrow_table.column("name").to_pylist() == ["Alice", "Bob", "Charlie"]
        assert arrow_table.column("age").to_pylist() == [30, 25, 35]
        assert arrow_table.column("score").to_pylist() == pytest.approx([95.5, 87.3, 92.1])
        assert arrow_table.column("active").to_pylist() == [True, False, True]


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
class TestDtypeWithPolars:
    """Tests for dtype parameter with Polars conversion."""

    def test_dtype_with_polars(self, typed_csv):
        """Test that dtype works with Polars conversion."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(
            typed_csv, dtype={"age": "int64", "score": "float64", "active": "bool"}
        )
        df = pl.from_arrow(table)

        # Check types
        assert df["age"].dtype == pl.Int64
        assert df["score"].dtype == pl.Float64
        assert df["active"].dtype == pl.Boolean
        assert df["name"].dtype == pl.String

        # Check values
        assert df["age"].to_list() == [30, 25, 35]
        assert df["score"].to_list() == pytest.approx([95.5, 87.3, 92.1])
        assert df["active"].to_list() == [True, False, True]
        assert df["name"].to_list() == ["Alice", "Bob", "Charlie"]


# =============================================================================
# skip_rows and n_rows parameter tests with Arrow
# =============================================================================


@pytest.fixture
def larger_csv_for_arrow():
    """Create a larger CSV file for skip/limit testing with Arrow."""
    lines = ["id,value"]
    for i in range(10):
        lines.append(f"{i},{i * 10}")
    content = "\n".join(lines) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestSkipRowsNRowsWithPyArrow:
    """Tests for skip_rows and n_rows parameters with PyArrow conversion."""

    def test_skip_rows_arrow_table(self, larger_csv_for_arrow):
        """Test skip_rows works correctly when converting to PyArrow."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(larger_csv_for_arrow, skip_rows=3)
        arrow_table = pa.table(table)

        assert arrow_table.num_rows == 7
        # With auto-inference, numeric columns are detected as int64
        ids = arrow_table.column("id").to_pylist()
        assert ids == [3, 4, 5, 6, 7, 8, 9]

    def test_n_rows_arrow_table(self, larger_csv_for_arrow):
        """Test n_rows works correctly when converting to PyArrow."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(larger_csv_for_arrow, n_rows=3)
        arrow_table = pa.table(table)

        assert arrow_table.num_rows == 3
        # With auto-inference, numeric columns are detected as int64
        ids = arrow_table.column("id").to_pylist()
        assert ids == [0, 1, 2]

    def test_skip_rows_and_n_rows_combined_arrow(self, larger_csv_for_arrow):
        """Test skip_rows and n_rows combined with PyArrow."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(larger_csv_for_arrow, skip_rows=2, n_rows=4)
        arrow_table = pa.table(table)

        assert arrow_table.num_rows == 4
        # With auto-inference, numeric columns are detected as int64
        ids = arrow_table.column("id").to_pylist()
        assert ids == [2, 3, 4, 5]
        values = arrow_table.column("value").to_pylist()
        assert values == [20, 30, 40, 50]

    def test_skip_rows_with_dtype_arrow(self, larger_csv_for_arrow):
        """Test skip_rows works with dtype parameter for Arrow."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(
            larger_csv_for_arrow, skip_rows=5, dtype={"id": "int64", "value": "int64"}
        )
        arrow_table = pa.table(table)

        assert arrow_table.num_rows == 5
        assert pa.types.is_int64(arrow_table.column("id").type)
        ids = arrow_table.column("id").to_pylist()
        assert ids == [5, 6, 7, 8, 9]
        values = arrow_table.column("value").to_pylist()
        assert values == [50, 60, 70, 80, 90]

    def test_n_rows_with_dtype_arrow(self, larger_csv_for_arrow):
        """Test n_rows works with dtype parameter for Arrow."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(
            larger_csv_for_arrow, n_rows=3, dtype={"id": "int64", "value": "int64"}
        )
        arrow_table = pa.table(table)

        assert arrow_table.num_rows == 3
        assert pa.types.is_int64(arrow_table.column("id").type)
        ids = arrow_table.column("id").to_pylist()
        assert ids == [0, 1, 2]


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
class TestSkipRowsNRowsWithPolars:
    """Tests for skip_rows and n_rows parameters with Polars conversion."""

    def test_skip_rows_polars_dataframe(self, larger_csv_for_arrow):
        """Test skip_rows works correctly when converting to Polars."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(larger_csv_for_arrow, skip_rows=3)
        df = pl.from_arrow(table)

        assert df.shape[0] == 7
        # With auto-inference, numeric columns are detected as int64
        ids = df["id"].to_list()
        assert ids == [3, 4, 5, 6, 7, 8, 9]

    def test_n_rows_polars_dataframe(self, larger_csv_for_arrow):
        """Test n_rows works correctly when converting to Polars."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(larger_csv_for_arrow, n_rows=3)
        df = pl.from_arrow(table)

        assert df.shape[0] == 3
        # With auto-inference, numeric columns are detected as int64
        ids = df["id"].to_list()
        assert ids == [0, 1, 2]

    def test_skip_rows_and_n_rows_combined_polars(self, larger_csv_for_arrow):
        """Test skip_rows and n_rows combined with Polars."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(larger_csv_for_arrow, skip_rows=2, n_rows=4)
        df = pl.from_arrow(table)

        assert df.shape[0] == 4
        # With auto-inference, numeric columns are detected as int64
        ids = df["id"].to_list()
        assert ids == [2, 3, 4, 5]
