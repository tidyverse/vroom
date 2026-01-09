# Vendoring grisu3 from MathGeoLib

The `grisu3.c` and `grisu3.h` files in `src/` are vendored from the [MathGeoLib](https://github.com/juj/MathGeoLib) library. We maintain local modifications documented as patch files.

## Files in maintenance/

- **`grisu3-pull-from-upstream.R`**: Script to download fresh grisu3 files from upstream
- **`grisu3-upstream-info.txt`**: Records the upstream commit SHA and date
- **`patches/grisu3-header.patch`**: Modifications to `grisu3.h`
- **`patches/grisu3-source.patch`**: Modifications to `grisu3.c`

## Workflow to re-vendor grisu3

```r
# 1. Pull fresh upstream files to maintenance/
source("maintenance/01_pull-grisu3-upstream.R")
```

This downloads `grisu3.{c,h}` from upstream to the `maintenance/` directory.

```bash
# 2. Apply patches and copy to src/
cd maintenance
patch < patches/grisu3-header.patch
patch < patches/grisu3-source.patch
cp grisu3.h ../src/grisu3.h
cp grisu3.c ../src/grisu3.c
cd ..
```

## What we modify

The patches transform the upstream grisu3 code for vroom's needs:

**Header simplifications** (`grisu3-header.patch`):
- Add Jukka Jylänki copyright header
- Remove MathGeoLib-specific includes (`#include "../MathBuildConfig.h"`)
- Remove Emscripten support
- Remove unused function declarations (`f32_to_string`, `u32_to_string`, `i32_to_string`, hex functions)
- Simplify C++ string support (remove conditional compilation)

**Source modifications** (`grisu3-source.patch`):
- Add copyright headers (Jukka Jylänki and mikkelfj)
- Use `snprintf()` instead of `sprintf()` for buffer safety
- Remove unused helper functions (`u32_to_string`, `i32_to_string`, `f32_to_string`, hex functions)
- Add simplified `i_to_str()` function for internal use
- Include mikkelfj modifications for better decimal formatting:
  - Handle whole numbers as integers (< 10^15)
  - Fix zero prefix (.1 => 0.1) for JSON export
  - Prefer unscientific notation for short decimals
  - These modifications have been here ever since grisu3.c first appeared in vroom, so Jim Hester must have found them somewhere.
- Remove `#include "grisu3.h"` (not needed in vroom's structure)

## Why vendor grisu3?

vroom uses grisu3 for fast, accurate double-to-string conversion when writing CSV files (see `src/vroom_write.cc:208`). The vendored version is simpler than the full MathGeoLib implementation—we only keep what vroom needs: the `dtoa_grisu3()` function.

## Upstream source

**Repository**: https://github.com/juj/MathGeoLib
**Path**: `src/Math/grisu3.{c,h}`
**Current version**: See `grisu3-upstream-info.txt` for the specific commit SHA

The original grisu3 algorithm is from the research paper:
> "Printing Floating-Point Numbers Quickly And Accurately with Integers"
> by Florian Loitsch
