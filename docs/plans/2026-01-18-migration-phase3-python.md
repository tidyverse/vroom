# Phase 3: Python Bindings Migration

**Goal:** Rewrite Python bindings to expose the new libvroom2 API: `read_csv()`, `to_parquet()`, and batched reading with Arrow PyCapsule export.

**Prerequisites:** Phase 2 (Build System Migration) complete.

---

## Tasks

### Task 1: Update Python CMakeLists.txt
Update source file list and dependencies for new architecture.

### Task 2: Create Minimal Bindings Stub
Replace existing bindings with minimal version that compiles.

### Task 3: Implement read_csv() Function
Return Table object with column data.

### Task 4: Implement Arrow PyCapsule Export
Add __arrow_c_stream__ for zero-copy interop.

### Task 5: Implement to_parquet() Function
CSV to Parquet conversion.

### Task 6: Add Batched Reading Support
Implement BatchedReader class.

### Task 7: Update Type Stubs
Update _core.pyi for new API.
