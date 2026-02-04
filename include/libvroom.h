/**
 * @file libvroom.h
 * @brief libvroom - High-performance CSV parser and Parquet writer
 * @version 2.0.0
 *
 * This is the main public header for the libvroom library.
 * Migrated from libvroom2 for improved performance.
 */

#ifndef LIBVROOM_H
#define LIBVROOM_H

#define LIBVROOM_VERSION_MAJOR 2
#define LIBVROOM_VERSION_MINOR 0
#define LIBVROOM_VERSION_PATCH 0
#define LIBVROOM_VERSION_STRING "2.0.0"

// Core headers
#include "libvroom/cache.h"
#include "libvroom/common_defs.h"
#include "libvroom/dialect.h"
#include "libvroom/elias_fano.h"
#include "libvroom/error.h"
#include "libvroom/io_util.h"
#include "libvroom/options.h"
#include "libvroom/types.h"
#include "libvroom/vroom.h"

// Column builders
#include "libvroom/arrow_column_builder.h"

// Parsing
#include "libvroom/quote_parity.h"
#include "libvroom/split_fields.h"

// Statistics and dictionary
#include "libvroom/dictionary.h"
#include "libvroom/statistics.h"

// Table (multi-batch Arrow stream export)
#include "libvroom/table.h"

// Streaming parser
#include "libvroom/streaming.h"

// Output formats
#include "libvroom/arrow_ipc_writer.h"

#endif // LIBVROOM_H
