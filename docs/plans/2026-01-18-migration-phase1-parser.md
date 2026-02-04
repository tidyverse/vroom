# Phase 1: Core Parser Migration

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Copy libvroom2's parser, column builder, and Parquet writer source files into libvroom's directory structure.

**Context:** This is part of migrating libvroom2 into libvroom. We're keeping libvroom's project structure but replacing the core parsing engine.

**Prerequisites:** None - this is the first phase.

**After completion:** Phase 2 (Build System Migration) can begin.

---

## Task 1: Create Directory Structure

**Files:**
- Create: `src/parser/`, `src/writer/`, `src/encoding/`, `src/schema/`, `src/columns/`, `src/simd/`, `src/reader/`
- Create: `include/vroom/`

**Step 1: Create all directories**

```bash
cd /home/jimhester/p/libvroom
mkdir -p src/parser src/writer src/encoding src/schema src/columns src/simd src/reader
mkdir -p include/vroom
```

**Step 2: Verify directories created**

```bash
ls -la src/
ls -la include/
```

**Step 3: Commit**

```bash
git add src/ include/
git commit -m "chore: Create directory structure for libvroom2 migration"
```

---

## Task 2: Copy libvroom2 Headers

**Files:**
- Copy: All headers from `~/p/libvroom2/include/vroom/` → `include/vroom/`

**Step 1: Copy all header files**

```bash
cp ~/p/libvroom2/include/vroom/*.h include/vroom/
```

**Step 2: Verify headers copied**

```bash
ls -la include/vroom/
```

Expected files: types.h, options.h, vroom.h, arrow_column_builder.h, split_fields.h, quote_parity.h, statistics.h, dictionary.h, data_chunk.h, arrow_buffer.h, fast_arrow_context.h, fast_column_context.h, simd_atoi.h

**Step 3: Commit**

```bash
git add include/vroom/
git commit -m "feat: Add libvroom2 headers"
```

---

## Task 3: Copy Parser Sources

**Files:**
- Copy: `~/p/libvroom2/src/parser/*.cpp` → `src/parser/`
- Copy: `~/p/libvroom2/src/reader/*.cpp` → `src/reader/`

**Step 1: Copy parser and reader files**

```bash
cp ~/p/libvroom2/src/parser/*.cpp src/parser/
cp ~/p/libvroom2/src/reader/*.cpp src/reader/
```

**Step 2: Verify files copied**

```bash
ls -la src/parser/
ls -la src/reader/
```

Expected parser: chunk_finder.cpp, line_parser.cpp, split_fields.cpp, split_fields_iter.cpp, quote_parity.cpp, simd_chunk_finder.cpp, simd_atoi.cpp
Expected reader: csv_reader.cpp, mmap_source.cpp

**Step 3: Commit**

```bash
git add src/parser/ src/reader/
git commit -m "feat: Add libvroom2 parser sources"
```

---

## Task 4: Copy Column Builder and Schema Sources

**Files:**
- Copy: `~/p/libvroom2/src/columns/*.cpp` → `src/columns/`
- Copy: `~/p/libvroom2/src/schema/*.cpp` → `src/schema/`
- Copy: `~/p/libvroom2/src/simd/*.cpp` → `src/simd/`

**Step 1: Copy files**

```bash
cp ~/p/libvroom2/src/columns/*.cpp src/columns/
cp ~/p/libvroom2/src/schema/*.cpp src/schema/
cp ~/p/libvroom2/src/simd/*.cpp src/simd/
```

**Step 2: Verify files copied**

```bash
ls -la src/columns/
ls -la src/schema/
ls -la src/simd/
```

**Step 3: Commit**

```bash
git add src/columns/ src/schema/ src/simd/
git commit -m "feat: Add libvroom2 column builder and schema sources"
```

---

## Task 5: Copy Parquet Writer and Encoding Sources

**Files:**
- Copy: `~/p/libvroom2/src/writer/*.cpp` → `src/writer/`
- Copy: `~/p/libvroom2/src/encoding/*.cpp` → `src/encoding/`

**Step 1: Copy writer and encoding files**

```bash
cp ~/p/libvroom2/src/writer/*.cpp src/writer/
cp ~/p/libvroom2/src/encoding/*.cpp src/encoding/
```

**Step 2: Verify files copied**

```bash
ls -la src/writer/
ls -la src/encoding/
```

Expected writer: parquet_file.cpp, row_group.cpp, column_writer.cpp, page_writer.cpp, thrift_compact.cpp, parquet_types.cpp, compression.cpp, statistics.cpp, dictionary.cpp
Expected encoding: plain.cpp, rle.cpp, delta_bitpacked.cpp, delta_length.cpp, hybrid_rle.cpp

**Step 3: Commit**

```bash
git add src/writer/ src/encoding/
git commit -m "feat: Add libvroom2 Parquet writer and encoding sources"
```

---

## Completion Checklist

- [ ] Directory structure created
- [ ] All libvroom2 headers copied to include/vroom/
- [ ] Parser sources copied to src/parser/ and src/reader/
- [ ] Column builder sources copied to src/columns/, src/schema/, src/simd/
- [ ] Parquet writer sources copied to src/writer/ and src/encoding/
- [ ] All changes committed

**Next:** Phase 2 - Build System Migration
