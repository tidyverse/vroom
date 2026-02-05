/**
 * @file bindings.cpp
 * @brief Python bindings for libvroom using pybind11.
 *
 * This module provides Python access to the libvroom high-performance CSV parser.
 * It implements the Arrow PyCapsule interface for zero-copy interoperability with
 * PyArrow, Polars, and DuckDB.
 *
 * Uses libvroom::Table for multi-batch Arrow stream export (Issue #632).
 */

#include "libvroom.h"
#include "libvroom/table.h"

#include <memory>
#include <optional>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace py = pybind11;

// =============================================================================
// Custom Python exceptions
// =============================================================================

static PyObject* VroomError = nullptr;
static PyObject* ParseError = nullptr;
static PyObject* IOError_custom = nullptr;

// =============================================================================
// read_csv function - main entry point
// =============================================================================

std::shared_ptr<libvroom::Table>
read_csv(const std::string& path, std::optional<char> separator = std::nullopt,
         std::optional<char> quote = std::nullopt, bool has_header = true,
         std::optional<size_t> num_threads = std::nullopt,
         std::optional<std::string> error_mode = std::nullopt,
         std::optional<size_t> max_errors = std::nullopt,
         std::optional<std::string> encoding = std::nullopt,
         std::optional<char> comment = std::nullopt, bool skip_empty_rows = true) {
  // Set up options
  libvroom::CsvOptions csv_opts;
  if (separator)
    csv_opts.separator = *separator;
  if (quote)
    csv_opts.quote = *quote;
  csv_opts.has_header = has_header;
  if (num_threads)
    csv_opts.num_threads = *num_threads;

  // Set encoding
  if (encoding) {
    auto enc = libvroom::parse_encoding_name(*encoding);
    if (enc == libvroom::CharEncoding::UNKNOWN) {
      throw std::runtime_error(
          "Unknown encoding: " + *encoding +
          " (use 'utf-8', 'utf-16le', 'utf-16be', 'utf-32le', 'utf-32be', 'latin1', "
          "'windows-1252')");
    }
    csv_opts.encoding = enc;
  }

  // Set comment character
  if (comment)
    csv_opts.comment = *comment;

  // Set skip_empty_rows
  csv_opts.skip_empty_rows = skip_empty_rows;

  // Set error handling options
  if (error_mode) {
    if (*error_mode == "disabled") {
      csv_opts.error_mode = libvroom::ErrorMode::DISABLED;
    } else if (*error_mode == "fail_fast" || *error_mode == "strict") {
      csv_opts.error_mode = libvroom::ErrorMode::FAIL_FAST;
    } else if (*error_mode == "permissive") {
      csv_opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
    } else if (*error_mode == "best_effort") {
      csv_opts.error_mode = libvroom::ErrorMode::BEST_EFFORT;
    } else {
      throw std::runtime_error("Unknown error_mode: " + *error_mode +
                               " (use 'disabled', 'fail_fast', 'permissive', or 'best_effort')");
    }
  }
  if (max_errors) {
    csv_opts.max_errors = *max_errors;
    // Enable error collection if max_errors is specified
    if (csv_opts.error_mode == libvroom::ErrorMode::DISABLED) {
      csv_opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
    }
  }

  // Create reader and open file
  libvroom::CsvReader reader(csv_opts);
  auto open_result = reader.open(path);
  if (!open_result.ok) {
    // Include any collected errors in the exception message
    std::string error_msg = open_result.error;
    if (reader.has_errors()) {
      error_msg += "\n\nParse errors:\n";
      for (const auto& err : reader.errors()) {
        error_msg += "  " + err.to_string() + "\n";
      }
    }
    throw std::runtime_error(error_msg);
  }

  // Read all data
  auto read_result = reader.read_all();
  if (!read_result.ok) {
    // Include any collected errors in the exception message
    std::string error_msg = read_result.error;
    if (reader.has_errors()) {
      error_msg += "\n\nParse errors:\n";
      for (const auto& err : reader.errors()) {
        error_msg += "  " + err.to_string() + "\n";
      }
    }
    throw std::runtime_error(error_msg);
  }

  // Create Table from parsed chunks - O(1), no merge needed
  return libvroom::Table::from_parsed_chunks(reader.schema(), std::move(read_result.value));
}

// =============================================================================
// to_parquet function - CSV to Parquet conversion
// =============================================================================

void to_parquet(const std::string& input_path, const std::string& output_path,
                std::optional<std::string> compression = std::nullopt,
                std::optional<size_t> row_group_size = std::nullopt,
                std::optional<size_t> num_threads = std::nullopt,
                std::optional<std::string> error_mode = std::nullopt,
                std::optional<size_t> max_errors = std::nullopt,
                std::optional<char> comment = std::nullopt, bool skip_empty_rows = true) {
  libvroom::VroomOptions opts;
  opts.input_path = input_path;
  opts.output_path = output_path;

  // Set compression
  if (compression) {
    if (*compression == "zstd") {
#ifdef VROOM_HAVE_ZSTD
      opts.parquet.compression = libvroom::Compression::ZSTD;
#else
      throw std::runtime_error("zstd compression not available (not compiled in)");
#endif
    } else if (*compression == "snappy") {
      opts.parquet.compression = libvroom::Compression::SNAPPY;
    } else if (*compression == "lz4") {
      opts.parquet.compression = libvroom::Compression::LZ4;
    } else if (*compression == "gzip") {
      opts.parquet.compression = libvroom::Compression::GZIP;
    } else if (*compression == "none") {
      opts.parquet.compression = libvroom::Compression::NONE;
    } else {
      throw std::runtime_error("Unknown compression: " + *compression);
    }
  }

  if (row_group_size) {
    opts.parquet.row_group_size = *row_group_size;
  }

  if (num_threads) {
    opts.threads.num_threads = *num_threads;
  }

  // Set comment character
  if (comment)
    opts.csv.comment = *comment;

  // Set skip_empty_rows
  opts.csv.skip_empty_rows = skip_empty_rows;

  // Set error handling options
  if (error_mode) {
    if (*error_mode == "disabled") {
      opts.csv.error_mode = libvroom::ErrorMode::DISABLED;
    } else if (*error_mode == "fail_fast" || *error_mode == "strict") {
      opts.csv.error_mode = libvroom::ErrorMode::FAIL_FAST;
    } else if (*error_mode == "permissive") {
      opts.csv.error_mode = libvroom::ErrorMode::PERMISSIVE;
    } else if (*error_mode == "best_effort") {
      opts.csv.error_mode = libvroom::ErrorMode::BEST_EFFORT;
    } else {
      throw std::runtime_error("Unknown error_mode: " + *error_mode +
                               " (use 'disabled', 'fail_fast', 'permissive', or 'best_effort')");
    }
  }
  if (max_errors) {
    opts.csv.max_errors = *max_errors;
    // Enable error collection if max_errors is specified
    if (opts.csv.error_mode == libvroom::ErrorMode::DISABLED) {
      opts.csv.error_mode = libvroom::ErrorMode::PERMISSIVE;
    }
  }

  auto result = libvroom::convert_csv_to_parquet(opts);
  if (!result.ok()) {
    // Include any collected errors in the exception message
    std::string error_msg = result.error;
    if (result.has_errors()) {
      error_msg += "\n\nParse errors:\n";
      for (const auto& err : result.parse_errors) {
        error_msg += "  " + err.to_string() + "\n";
      }
    }
    throw std::runtime_error(error_msg);
  }
}

// =============================================================================
// to_arrow_ipc function - CSV to Arrow IPC conversion
// =============================================================================

void to_arrow_ipc(const std::string& input_path, const std::string& output_path,
                  std::optional<size_t> batch_size = std::nullopt,
                  std::optional<size_t> num_threads = std::nullopt) {
  libvroom::CsvOptions csv_opts;
  if (num_threads) {
    csv_opts.num_threads = *num_threads;
  }

  libvroom::ArrowIpcOptions ipc_opts;
  if (batch_size) {
    ipc_opts.batch_size = *batch_size;
  }

  auto result = libvroom::convert_csv_to_arrow_ipc(input_path, output_path, csv_opts, ipc_opts);
  if (!result.ok()) {
    throw std::runtime_error(result.error);
  }
}

// =============================================================================
// Python module definition
// =============================================================================

PYBIND11_MODULE(_core, m) {
  m.doc() = R"doc(
        vroom_csv._core - High-performance CSV parser with Arrow interop

        This module provides the core C++ implementation of the vroom CSV parser.
        For the high-level Python API, use vroom_csv directly.
    )doc";

  // Register custom exceptions
  VroomError = PyErr_NewException("vroom_csv.VroomError", PyExc_RuntimeError, nullptr);
  ParseError = PyErr_NewException("vroom_csv.ParseError", VroomError, nullptr);
  IOError_custom = PyErr_NewException("vroom_csv.IOError", VroomError, nullptr);

  m.attr("VroomError") = py::handle(VroomError);
  m.attr("ParseError") = py::handle(ParseError);
  m.attr("IOError") = py::handle(IOError_custom);

  // Table class (using shared_ptr holder for move-only type)
  py::class_<libvroom::Table, std::shared_ptr<libvroom::Table>>(m, "Table", R"doc(
        A table of data read from a CSV file.

        This class implements the Arrow PyCapsule interface (__arrow_c_stream__)
        for zero-copy interoperability with PyArrow, Polars, and DuckDB.

        Each parsed chunk is emitted as a separate RecordBatch in the Arrow
        stream, avoiding expensive chunk merge operations.
    )doc")
      .def_property_readonly("num_rows", &libvroom::Table::num_rows, "Number of rows in the table")
      .def_property_readonly("num_columns", &libvroom::Table::num_columns,
                             "Number of columns in the table")
      .def_property_readonly("column_names", &libvroom::Table::column_names, "List of column names")
      .def_property_readonly("num_chunks", &libvroom::Table::num_chunks,
                             "Number of chunks (RecordBatches) in the table")
      .def(
          "__arrow_c_stream__",
          [](std::shared_ptr<libvroom::Table> self, py::object requested_schema) {
            auto* stream = new libvroom::ArrowArrayStream();
            self->export_to_stream(stream);

            return py::capsule(stream, "arrow_array_stream", [](void* ptr) {
              auto* s = static_cast<libvroom::ArrowArrayStream*>(ptr);
              if (s->release)
                s->release(s);
              delete s;
            });
          },
          py::arg("requested_schema") = py::none(),
          "Export table as Arrow stream via PyCapsule (zero-copy, multi-batch)")
      .def(
          "__arrow_c_schema__",
          [](std::shared_ptr<libvroom::Table> self) {
            // Get schema via a temporary stream
            auto* stream = new libvroom::ArrowArrayStream();
            self->export_to_stream(stream);

            auto* schema = new libvroom::ArrowSchema();
            stream->get_schema(stream, schema);

            stream->release(stream);
            delete stream;

            return py::capsule(schema, "arrow_schema", [](void* ptr) {
              auto* s = static_cast<libvroom::ArrowSchema*>(ptr);
              if (s->release)
                s->release(s);
              delete s;
            });
          },
          "Export table schema as Arrow schema via PyCapsule");

  // read_csv function
  m.def("read_csv", &read_csv, py::arg("path"), py::arg("separator") = py::none(),
        py::arg("quote") = py::none(), py::arg("has_header") = true,
        py::arg("num_threads") = py::none(), py::arg("error_mode") = py::none(),
        py::arg("max_errors") = py::none(), py::arg("encoding") = py::none(),
        py::arg("comment") = py::none(), py::arg("skip_empty_rows") = true,
        R"doc(
        Read a CSV file into a Table.

        Parameters
        ----------
        path : str
            Path to the CSV file to read.
        separator : str, optional
            Field separator character. Default is auto-detect.
        quote : str, optional
            Quote character. Default is '"'.
        has_header : bool, optional
            Whether the file has a header row. Default is True.
        num_threads : int, optional
            Number of threads to use. Default is auto-detect.
        error_mode : str, optional
            Error handling mode:
            - "disabled" (default): No error collection, maximum performance
            - "fail_fast" or "strict": Stop on first error
            - "permissive": Collect all errors, stop on fatal
            - "best_effort": Ignore errors, parse what's possible
        max_errors : int, optional
            Maximum number of errors to collect. Default is 10000.
            Setting this automatically enables "permissive" mode if error_mode is not set.
        encoding : str, optional
            Force input encoding. Default is auto-detect.
            Supported: "utf-8", "utf-16le", "utf-16be", "utf-32le", "utf-32be",
            "latin1", "windows-1252".
        comment : str, optional
            Character that marks comment lines. Lines starting with this
            character are skipped during parsing. Default is None (no comment
            skipping).
        skip_empty_rows : bool, optional
            Whether to skip empty lines in the input. Default is True.

        Returns
        -------
        Table
            A Table object containing the parsed data.

        Raises
        ------
        RuntimeError
            If parsing fails. In permissive mode, collected errors are included
            in the exception message.

        Examples
        --------
        >>> import vroom_csv
        >>> table = vroom_csv.read_csv("data.csv")
        >>> print(table.num_rows, table.num_columns)

        # With error handling
        >>> table = vroom_csv.read_csv("data.csv", error_mode="permissive")

        # With encoding override
        >>> table = vroom_csv.read_csv("data.csv", encoding="latin1")

        # With comment skipping
        >>> table = vroom_csv.read_csv("data.csv", comment="#")
    )doc");

  // to_parquet function
  m.def("to_parquet", &to_parquet, py::arg("input_path"), py::arg("output_path"),
        py::arg("compression") = py::none(), py::arg("row_group_size") = py::none(),
        py::arg("num_threads") = py::none(), py::arg("error_mode") = py::none(),
        py::arg("max_errors") = py::none(), py::arg("comment") = py::none(),
        py::arg("skip_empty_rows") = true,
        R"doc(
        Convert a CSV file to Parquet format.

        Parameters
        ----------
        input_path : str
            Path to the input CSV file.
        output_path : str
            Path to the output Parquet file.
        compression : str, optional
            Compression codec: "zstd", "snappy", "lz4", "gzip", or "none".
            Default is "zstd" if available, otherwise "gzip".
        row_group_size : int, optional
            Number of rows per row group. Default is 1,000,000.
        num_threads : int, optional
            Number of threads to use. Default is auto-detect.
        error_mode : str, optional
            Error handling mode:
            - "disabled" (default): No error collection, maximum performance
            - "fail_fast" or "strict": Stop on first error
            - "permissive": Collect all errors, stop on fatal
            - "best_effort": Ignore errors, parse what's possible
        max_errors : int, optional
            Maximum number of errors to collect. Default is 10000.
            Setting this automatically enables "permissive" mode if error_mode is not set.
        comment : str, optional
            Character that marks comment lines. Lines starting with this
            character are skipped during parsing. Default is None (no comment
            skipping).
        skip_empty_rows : bool, optional
            Whether to skip empty lines in the input. Default is True.

        Raises
        ------
        RuntimeError
            If parsing fails. In permissive mode, collected errors are included
            in the exception message.

        Examples
        --------
        >>> import vroom_csv
        >>> vroom_csv.to_parquet("data.csv", "data.parquet")

        # With error handling
        >>> vroom_csv.to_parquet("data.csv", "data.parquet", error_mode="strict")
    )doc");

  // to_arrow_ipc function
  m.def("to_arrow_ipc", &to_arrow_ipc, py::arg("input_path"), py::arg("output_path"),
        py::arg("batch_size") = py::none(), py::arg("num_threads") = py::none(),
        R"doc(
        Convert a CSV file to Arrow IPC format.

        NOTE: This function is not yet implemented. It will raise an error
        explaining that Arrow IPC output requires FlatBuffers integration.
        Use to_parquet() for columnar output instead.

        Parameters
        ----------
        input_path : str
            Path to the input CSV file.
        output_path : str
            Path to the output Arrow IPC file (.arrow or .feather).
        batch_size : int, optional
            Number of rows per record batch. Default is 65536.
        num_threads : int, optional
            Number of threads to use. Default is auto-detect.

        Raises
        ------
        RuntimeError
            Always raised - Arrow IPC output is not yet implemented.
    )doc");

  // Version info
  m.attr("__version__") = "2.0.0";
}
