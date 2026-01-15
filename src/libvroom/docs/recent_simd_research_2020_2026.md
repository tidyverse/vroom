# Recent SIMD CSV/Text Parsing Research (2020-2026)

**Date**: 2026-01-01
**Purpose**: Research recent work to identify approaches better than simdjson (2019)
**Conclusion**: **No paradigm shifts found. simdjson approach remains state-of-the-art.**

---

## Executive Summary

### Key Finding: Stick with simdjson Approach ✅

After comprehensive research of work from 2020-2026, **no superior approach to simdjson's two-stage SIMD parsing has been published**. Instead, the research reveals:

1. **Industry Validation**: Sep (21 GB/s), DuckDB, Arrow all adopted simdjson-style techniques
2. **Incremental Improvements**: AVX-512 (25-40% speedup), ARM SVE (portability), not fundamental breakthroughs
3. **Production Proof**: Sep's 21 GB/s CSV parsing (2025) demonstrates simdjson techniques translate perfectly to CSV

### Papers/Approaches Reviewed

| Paper/Approach | Year | Applicability | Priority | Change Approach? |
|----------------|------|---------------|----------|------------------|
| **Mison** (Li et al.) | 2017 | LOW-MEDIUM | LOW | **NO** |
| **Sparser** (Palkar et al.) | 2018 | LOW (core), MEDIUM (query) | LOW-MEDIUM | **NO** for core |
| **AVX-512 Optimizations** | 2020-2024 | MEDIUM | MEDIUM | **NO**, add as optional |
| **ARM SVE/SVE2** | 2019-2024 | MEDIUM-HIGH (for ARM) | MEDIUM | **NO** for x86, **YES** for ARM (Phase 5) |
| **Sep (.NET CSV)** | 2023-2025 | HIGH (validation) | N/A (competitor) | **NO** (validates our approach) |
| **DuckDB CSV 2.0** | 2024 | HIGH (validation) | N/A (competitor) | **NO** (validates our approach) |
| **zsv** | 2023-2024 | MEDIUM (validation) | N/A (competitor) | **NO** |

---

## 1. Mison - Speculative JSON Parsing (Li et al., VLDB 2017)

### Summary
**Key Contribution**: Level-based speculation predicts field locations in nested JSON to skip irrelevant data

**Performance**: 3.6x faster than Jackson without speculation; 10.2x with speculation

**Core Technique**:
- **Upper level**: Speculates field positions based on schema regularity
- **Lower level**: SIMD structural indexing (predecessor to simdjson)
- **Best for**: Analytics queries accessing few fields in large JSON documents

### Comparison to simdjson

| Aspect | Mison | simdjson |
|--------|-------|----------|
| **Goal** | Selective field access | Full document parsing |
| **Speculation** | Level-based prediction | None (deterministic) |
| **SIMD** | Basic structural indexing | Advanced bit manipulation |
| **Performance** | 10x for sparse queries | 2-5x for full parsing |
| **Robustness** | Requires regular schema | Any valid JSON |

**Key Difference**: Mison optimizes for **sparse access** (few fields), simdjson for **dense access** (full document).

### Applicability to libvroom

**Rating**: LOW-MEDIUM

**Why Low**:
- CSV parsing requires **full row parsing**, not selective field access
- CSV lacks nesting hierarchy Mison exploits
- Most CSV use cases need all fields (data loading, transformation)
- Speculation complexity doesn't justify gains for CSV

**Why Medium (edge case)**:
- **If** parsing only specific columns in very wide CSV (e.g., 5 of 100 columns)
- **If** integrating with vroom's lazy column materialization
- Deferred to future optimization (post-1.0)

### Should we change our approach?

**NO**

**Rationale**:
- Mison's speculation is orthogonal to SIMD structural indexing
- simdjson's deterministic approach better fits CSV (always need full rows)
- Effort better spent on core SIMD optimizations
- If needed later, implement as separate optional layer

### Implementation Priority

**LOW** (defer indefinitely)

---

## 2. Sparser - Filter Before You Parse (Palkar et al., VLDB 2018)

### Summary
**Key Contribution**: SIMD-accelerated raw filters (RFs) pre-filter data before expensive parsing

**Performance**: 22x faster than Mison on selective JSON queries, 9x end-to-end in Spark

**Core Technique**:
- **Raw Filters**: Fast SIMD substring search (never false negatives, occasional false positives)
- **Filter Cascades**: Chain filters by selectivity (most selective first)
- **Example**: Query `WHERE city = "Seattle"` → RF searches raw bytes for `"Seattle"`, parses only matches

### Comparison to simdjson

| Aspect | Sparser | simdjson |
|--------|---------|----------|
| **Approach** | Filter then parse | Always parse (just faster) |
| **SIMD Use** | Substring search | Structural indexing |
| **Best For** | Selective queries (1% of data) | Full document parsing |
| **Speedup** | 22x (selective) | 2-5x (full parsing) |

**Key Difference**: Sparser **avoids parsing** most data; simdjson parses everything faster.

### Applicability to libvroom

**Rating**: LOW for core CSV parsing, MEDIUM-HIGH for future query integration

**Why Low for Core**:
- CSV parsing typically scans all fields
- vroom indexing requires full-file parsing (no filtering benefit)
- Extra filtering pass adds overhead for full scans

**Why Medium-High for Query Integration**:
- **Future use case**: `SELECT * FROM csv WHERE state = 'CA'`
  - RF searches for `"CA"` in raw CSV
  - Parse only matching rows (10x+ speedup)
- **Hybrid approach**: Full scan uses SIMD parser, selective queries use RFs

### Should we change our approach?

**NO for core parser, YES for future query layer**

**Rationale**:
- Core libvroom: Focus on fast full-file parsing (simdjson approach)
- Future enhancement: Add optional RF layer for query workloads (post-1.0)
- Not applicable to Phase 1-3 (vroom integration)

### Implementation Priority

**LOW** for Phase 1-3, **MEDIUM** for future (post-1.0)

### Novel Idea for libvroom

**Structure-Aware Raw Filters**:
- Combine Sparser's filtering with simdjson's structural indexing
- Use structural index to find field boundaries
- Apply RFs only within target columns (eliminate cross-field false positives)
- **Benefit**: Predicate pushdown during indexing (vroom could skip non-matching rows)

---

## 3. Recent AVX-512 Optimizations (2020-2024)

### Summary
**Key Findings**: AVX-512 provides 25-40% speedup but suffers from CPU frequency throttling

**Sources**:
- [simdjson 2.0 AVX-512](https://lemire.me/blog/2022/05/25/parsing-json-faster-with-intel-avx-512/) (2022)
- [Parsing integers with AVX-512](https://lemire.me/blog/2023/09/22/parsing-integers-quickly-with-avx-512/) (2023)
- [Ice Lake AVX-512 Downclocking](https://travisdowns.github.io/blog/2020/08/19/icl-avx512-freq.html) (2020)
- [Cloudflare on frequency scaling dangers](https://blog.cloudflare.com/on-the-dangers-of-intels-frequency-scaling/) (2020)

### Performance Results

**Pros**:
- simdjson 2.0: **25-40% faster** with AVX-512 vs AVX2
- Integer parsing: **2x parallelism** (parse 2 numbers simultaneously)
- Mask registers (k1-k8): Enable branchless conditional operations

**Cons (Critical ⚠️)**:
- **CPU Downclocking**: AVX-512 causes frequency throttling on older Intel CPUs (2018-2021)
  - **Worst case**: CPU runs at **50% base frequency** during AVX-512 execution
  - **Whole-application impact**: Slowdown persists after AVX-512 code exits (ramp-up delay)
  - **Multi-core**: Throttling affects ALL cores, not just one executing AVX-512
- **AMD Zen 4 better**: Implements AVX-512 without severe downclocking (2022+)

### Comparison to simdjson (AVX2)

| Aspect | AVX2 (simdjson baseline) | AVX-512 |
|--------|--------------------------|---------|
| **Speedup** | Baseline | 25-40% faster (if no throttling) |
| **CPU Support** | Universal x86-64 | Intel Ice Lake+, AMD Zen 4+ |
| **Downclocking** | None | Severe on older Intel |
| **Complexity** | Medium | High |
| **Risk** | Low | Medium (must benchmark) |

### Applicability to libvroom

**Rating**: MEDIUM (worthwhile, but not critical)

**Why Medium**:
- **Potential 25-40% gain** (similar to simdjson)
- **AMD Zen 4+** makes it more viable (no throttling)
- **AVX2 already excellent**: >5 GB/s achievable without AVX-512

**Risk**:
- Downclocking on older Intel CPUs could make entire application **slower**
- Implementation complexity (more intrinsics, more testing)

### Should we change our approach?

**NO, but add AVX-512 as optional optimization**

**Mitigation Strategy**:
1. **Implement AVX-512**: But make it **opt-in, not default**
2. **Runtime selection**: Benchmark AVX-512 vs AVX2 on specific CPU at startup
3. **Use faster path**: Fall back to AVX2 if AVX-512 measured slower
4. **Test on target hardware**: Intel Ice Lake, Sapphire Rapids, AMD Zen 4

### Implementation Priority

**MEDIUM** (Phase 4+)

**Recommended Approach**:
- **Phase 2-3**: Perfect AVX2 implementation first (>5 GB/s target)
- **Phase 4**: Add AVX-512 as runtime-selected optimization
- **Testing**: Benchmark on multiple CPUs, use faster path dynamically

### Implementation Roadmap Update

**NO** change to core approach, **YES** add AVX-512 as Phase 4 enhancement

---

## 4. ARM SVE/SVE2 (2019-2024)

### Summary
**Key Innovation**: Variable vector length (128-2048 bits, determined at runtime)

**Sources**:
- [ARM SVE2 Explained](https://www.techno-logs.com/2024/10/03/arm-sve2-explained/) (2024)
- [Introduction to SVE2 (ARM)](https://developer.arm.com/-/media/Arm%20Developer%20Community/PDF/102340_0001_00_en_introduction-to-sve2.pdf) (2021)
- [Critical Look at SVE2](https://gist.github.com/atlury/c9e02bfe7489b5d3b37986b066478154) (2021)

### Key Features

**1. Predication-Centric Design**:
- All instructions use predicate registers (P0-P7) as masks
- Merging (`/m`) vs zeroing (`/z`) modes
- **Benefit**: Loop control without padding/scalar cleanup

**2. Length-Agnostic Algorithms**:
- Write code once, runs on 128-bit (NEON) to 2048-bit SVE
- No hard-coded vector widths
- **Benefit**: Forward compatibility (future CPUs with wider vectors)

**3. SVE2 String Operations**:
- Dedicated string processing acceleration (e.g., `strlen`, parsing)
- Speculative loads (`ldff1b`) for safe vectorization

### Comparison to AVX-512

| Feature | ARM SVE/SVE2 | x86 AVX-512 |
|---------|--------------|-------------|
| **Vector Width** | Variable (128-2048) | Fixed 512 bits |
| **Predication** | Mandatory | Optional |
| **Hardware** | Graviton 3+, Apple M-series, Neoverse V1+ | Intel Ice Lake+, AMD Zen 4+ |
| **Downclocking** | **None reported** | **Severe on older Intel** |

### Applicability to libvroom

**Rating**: MEDIUM-HIGH (for ARM support)

**Why Medium-High**:
- **ARM market growing**: AWS Graviton, Apple Silicon, cloud ARM instances
- **No downclocking penalty**: Unlike AVX-512 on Intel
- **Length-agnostic code**: Single implementation scales across ARM CPUs

**Challenges**:
- **Limited SVE2 hardware**: Still rare (mostly server chips)
- **NEON more common**: Most ARM devices use 128-bit NEON
- **Porting effort**: Requires learning SVE intrinsics

### Should we change our approach?

**NO for Phase 1-3, YES for ARM support (Phase 5)**

**Recommended Strategy**:
- **Phase 1-3**: Focus on x86 AVX2/AVX-512
- **Phase 5**: Port to ARM using **Google Highway**
  - Highway abstracts NEON/SVE, providing portable code
  - If Highway overhead acceptable (<5%), no need for hand-coded SVE
  - Test on: Apple Silicon, AWS Graviton, Raspberry Pi

### Implementation Priority

**MEDIUM** (Phase 5)

**Specific Recommendation**:
- Use **Google Highway** for ARM support (see Production Plan Section 2.2)
- Highway provides single abstraction for NEON/SVE
- Fallback: Direct NEON if SVE2 unavailable

### Implementation Roadmap Update

**NO** change to approach, **YES** validate Highway in Phase 1, deploy ARM in Phase 5

**Research Gap**: No published papers on SVE-based CSV parsing found (2020-2024)

---

## 5. Parallel CSV Parsing (Ge et al., 2021+)

### Summary
**Finding**: Chang et al.'s SIGMOD 2019 paper (already reviewed) remains the most recent academic work

**Search Results**:
- No new major papers found beyond Chang et al. (2019)
- Industry implementations (DuckDB, Sep) use similar speculative approaches
- **Conclusion**: 2019 research still state-of-the-art for parallel CSV parsing

### Applicability to libvroom

**Rating**: MEDIUM (already incorporated)

**Status**:
- Chang et al.'s techniques already reviewed in original literature review
- Speculative parsing at chunk boundaries applicable to vroom integration
- No new breakthroughs since 2019

### Should we change our approach?

**NO**

**Rationale**: Already planning to use Chang et al.'s speculation for chunk boundaries (Phase 3+)

---

## 6. Recent High-Performance CSV Parsers (2020-2025)

### 6.1 Sep (.NET/C#) - 21 GB/s CSV Parser ✅

**Source**: [nietras.com/sep](https://nietras.com/2025/05/09/sep-0-10-0/)

**Performance**: **21 GB/s** on AMD Ryzen 9950X (AVX-512, April 2025)
- **~3x improvement** since initial release (June 2023)
- AVX-512-to-256 parser circumvents .NET mask register inefficiencies

**Key Techniques** (validates simdjson approach):
1. **Quote matching**: Carryless multiply (PCLMULQDQ) for "quote mask" → "quoted regions mask"
   - **Same as simdjson**: Bit manipulation for quote handling
2. **Branchless design**: SIMD asks "where are ALL commas/quotes in 64 bytes?"
   - **Same as simdjson**: Data parallelism over control flow
3. **Two-stage parsing**: Structural indexing then value extraction
   - **Same as simdjson**: Separate structure from content

**Insight**: **Proves simdjson techniques translate directly to CSV with excellent results**

### Applicability to libvroom

**Rating**: HIGH (validation)

**Why High**:
- **Direct validation**: Sep achieves 21 GB/s using exact techniques we're planning
- **Same bit tricks**: PCLMULQDQ for quotes, SIMD comparisons for delimiters
- **Production proof**: Not academic theory, actual deployed code

### Should we change our approach?

**NO** - Sep **validates** our simdjson-based approach

**Takeaway**:
- Our approach is correct (proven by Sep's 21 GB/s)
- Target: **>5 GB/s AVX2**, **>8 GB/s AVX-512** (conservative vs Sep's 21 GB/s)
- **Action**: Study Sep's techniques, apply lessons learned

---

### 6.2 zsv - "World's Fastest CSV Parser"

**Source**: [GitHub - liquidaty/zsv](https://github.com/liquidaty/zsv)

**Claims**:
- SIMD operations, efficient memory use
- 25%+ faster than xsv, 2x+ faster than polars/duckdb on `select` operations
- **Memory footprint**: 1.5 MB (vs 4 MB xsv, 76 MB duckdb, 475 MB polars)

**Features**:
- CLI + library (C)
- WebAssembly compilation support
- Extensible architecture

**Performance**: Undisclosed (marketing claims "world's fastest")

### Applicability to libvroom

**Rating**: MEDIUM (competitor, validates SIMD CSV market)

**Note**: Claims vs reality unclear (Sep achieves 21 GB/s, zsv doesn't publish numbers)

### Should we change our approach?

**NO**

**Takeaway**: Validates SIMD CSV parsing is competitive niche

---

### 6.3 DuckDB CSV Parser 2.0 (2024)

**Source**: [DuckDB CSV improvements](https://duckdb.org/2024/10/16/driving-csv-performance-benchmarking-duckdb-with-the-nyc-taxi-dataset)

**Key Improvements (2022-2024)**:

1. **Unified Parser** (Feb 2024, v0.10.0):
   - Merged single-threaded + parallel parsers
   - Pre-computed selection vectors for row→column conversion
   - Custom CSV sniffer (auto-detection)

2. **SIMD via Compiler Auto-Vectorization**:
   - **Implicit SIMD**: Write C++ for compiler to auto-generate SIMD
   - **64-bit word operations**: Process 8 bytes at a time
   - **Vectorized batches**: Process columns in cache-friendly batches

3. **Performance Results**:
   - CSV reader **only 2x slower than Parquet** (Feb 2024)
   - **15% speedup** in parallel newline finding (Feb 2025, v1.2.0)

### Comparison to libvroom (planned)

| Aspect | DuckDB | libvroom (planned) |
|--------|--------|-------------------|
| **SIMD** | Compiler auto-vectorization | Explicit SIMD intrinsics (AVX2/AVX-512) |
| **Performance** | 2x slower than Parquet | Target: Match/exceed DuckDB |
| **Portability** | High (compiler handles SIMD) | Medium (manual porting) |
| **Optimization** | Easier to maintain | Higher performance ceiling |

### Applicability to libvroom

**Rating**: HIGH (validation)

**Why High**:
- **Industry validation**: DuckDB prioritizes CSV performance (analytics DB)
- **SIMD essential**: Even with auto-vectorization, SIMD critical for performance
- **Benchmark target**: DuckDB is competitive baseline

### Should we change our approach?

**NO**

**Rationale**:
- DuckDB's auto-vectorization is conservative (compiler limitations)
- Explicit SIMD (simdjson approach) can achieve **higher performance ceiling**
- libvroom's index-based output better suited for vroom than DuckDB's columnar output

**Takeaway**: DuckDB validates CSV parsing is critical, benchmark against DuckDB as baseline

---

### 6.4 Apache Arrow CSV Reader (2020-2024)

**Source**: [Arrow Python/C++ CSV](https://arrow.apache.org/docs/python/csv.html)

**SIMD Features**:
- Columnar reading (SIMD-friendly memory layout)
- Multi-threaded parsing
- SIMD level configuration (`ARROW_SIMD_LEVEL`)

**Performance**:
- **5x speedup** vs traditional row-based readers (2022 benchmark)
- Recent optimizations (2025): BYTE_STREAM_SPLIT, fixed-length arrays 3x faster

### Applicability to libvroom

**Rating**: MEDIUM

**Why Medium**:
- Arrow's columnar format differs from libvroom's index-based output
- SIMD techniques applicable, but different data model
- Validates SIMD + columnar is industry standard

### Should we change our approach?

**NO**

**Rationale**: Different data model (columnar vs index-based), validates SIMD importance

---

## 7. Key Takeaways: Should We Change Our Approach?

### ✅ NO FUNDAMENTAL CHANGES NEEDED

#### Validation of simdjson Approach
1. **Sep (21 GB/s)**: Proves simdjson techniques work **brilliantly** for CSV
2. **DuckDB CSV 2.0**: Shows CSV parsing is critical for analytics databases
3. **Arrow**: Columnar + SIMD is industry standard

**Conclusion**: **Our simdjson-based approach is validated by 2020-2025 industry trends**

#### No Research Breakthroughs Since simdjson (2019)
- **2020-2024 academic research**: Incremental improvements, **not paradigm shifts**
- **simdjson remains state-of-the-art**: No better approach published
- **Industry adoption**: Sep, DuckDB, Arrow all use simdjson-style techniques

**Conclusion**: **Focus on excellent implementation of proven techniques, not novel algorithms**

---

## 8. Updated Implementation Roadmap

### High Priority (Unchanged) ✅

1. **Langdale & Lemire Two-Stage SIMD** (Phase 2)
   - ✅ Validated by Sep (21 GB/s), DuckDB, Arrow
   - Expected 2-4x speedup for libvroom
   - **Status**: Confirmed as highest-priority optimization
   - **Target**: >5 GB/s AVX2, >8 GB/s AVX-512

2. **CleverCSV Dialect Detection** (Phase 3)
   - Essential for vroom integration
   - Robust real-world CSV handling
   - **Status**: Confirmed medium-high priority

### Medium Priority (Updated) ⚠️

3. **AVX-512 Support** (Phase 4) - **Updated: Add runtime selection**
   - 25-40% potential speedup (if no downclocking)
   - **Mitigation**: Benchmark on target CPU, fall back to AVX2 if slower
   - **Priority**: Medium (worthwhile, but not critical given AVX2 achieves >5 GB/s)
   - **Key lesson**: Downclocking is **real** on older Intel

4. **ARM NEON/SVE Support via Highway** (Phase 5)
   - ✅ Validated: ARM market growing, no downclocking issues
   - **Recommendation**: Use Google Highway for portability
   - **Priority**: Medium (defer until AVX2 perfect)

### Low Priority (Unchanged/Added) ⬇️

5. **Mison-Style Speculation** (Future)
   - Not applicable to full CSV parsing
   - **Possible use**: Selective column parsing (low-priority feature)
   - **Status**: Defer indefinitely

6. **Sparser-Style Raw Filters** (Future Query Layer)
   - Not applicable to vroom (requires full indexing)
   - **Possible use**: Future query pushdown API
   - **Status**: Defer to post-1.0

### New Insights from Recent Work 🆕

7. **Branchless Design** (Sep validation)
   - ✅ Confirms: Bit manipulation > branching
   - **Action**: Aggressively eliminate branches in Phase 2
   - **Technique**: Use SIMD masks + arithmetic instead of `if` statements
   - **Validated by**: Sep's 21 GB/s achievement

8. **Compiler Auto-Vectorization** (DuckDB approach)
   - **Trade-off**: Easier maintenance vs lower performance ceiling
   - **Decision**: Stick with explicit SIMD for maximum performance
   - **Rationale**: libvroom targets high-performance niche (vroom integration)

---

## 9. Final Recommendations

### Should We Change Our Approach?

## **NO** - Our simdjson-based approach remains optimal

**Evidence**:
1. ✅ **Validated by Industry**: Sep (21 GB/s), DuckDB, Arrow all use similar techniques
2. ✅ **No Better Alternative**: Research 2020-2024 found no superior approach
3. ✅ **Proven Performance**: 2-5x speedup achievable with proper implementation
4. ✅ **Production Proof**: Sep demonstrates simdjson techniques translate directly to CSV

### Key Adjustments Based on Recent Research

**Minor adjustments, not fundamental changes**:

1. **AVX-512**: Add with **runtime selection** (don't assume it's always faster)
   - Downclocking is real on older Intel CPUs
   - Benchmark AVX-512 vs AVX2 at startup, use faster path

2. **ARM**: Plan **Highway-based port** in Phase 5 (growing market)
   - No downclocking issues (unlike AVX-512 on Intel)
   - Use Google Highway for NEON/SVE abstraction

3. **Benchmarking**: Continuously compare vs **Sep, DuckDB, zsv**
   - Sep is new performance bar (21 GB/s)
   - DuckDB is competitive baseline (2x slower than Parquet)

4. **Branchless**: Prioritize **branch elimination** (validated by Sep's success)
   - Use SIMD masks + arithmetic, not `if` statements
   - Carryless multiply (PCLMULQDQ) for quote handling

### Updated Success Criteria

**Conservative targets (Sep achieves 21 GB/s)**:

- **AVX2**: Target **>5 GB/s** (conservative)
- **AVX-512**: Target **>8 GB/s** (if no downclocking on target hardware)
- **vs vroom**: **2-3x faster indexing** than current vroom (unchanged)
- **vs DuckDB**: Competitive or faster than DuckDB CSV reader
- **vs Sep**: Aspirational target (21 GB/s is .NET-specific optimizations)

---

## 10. Sources & References

### Academic Papers

1. **Mison** (Li et al., VLDB 2017)
   - [VLDB Paper](http://www.vldb.org/pvldb/vol10/p1118-li.pdf)
   - [Microsoft Research](https://www.microsoft.com/en-us/research/publication/mison-fast-json-parser-data-analytics/)

2. **Sparser** (Palkar et al., VLDB 2018)
   - [VLDB Paper](https://www.vldb.org/pvldb/vol11/p1576-palkar.pdf)
   - [GitHub](https://github.com/stanford-futuredata/sparser)
   - [The Morning Paper Summary](https://blog.acolyer.org/2018/08/20/filter-before-you-parse-faster-analytics-on-raw-data-with-sparser/)

3. **Chang et al.** (SIGMOD 2019) - Already reviewed in main literature review

### AVX-512 Research (2020-2024)

4. **Daniel Lemire's Blog Posts**:
   - [AVX-512: when and how to use these new instructions](https://lemire.me/blog/2018/09/07/avx-512-when-and-how-to-use-these-new-instructions/) (2018)
   - [Parsing JSON faster with Intel AVX-512](https://lemire.me/blog/2022/05/25/parsing-json-faster-with-intel-avx-512/) (2022)
   - [Parsing integers quickly with AVX-512](https://lemire.me/blog/2023/09/22/parsing-integers-quickly-with-avx-512/) (2023)

5. **AVX-512 Downclocking**:
   - [Ice Lake AVX-512 Downclocking](https://travisdowns.github.io/blog/2020/08/19/icl-avx512-freq.html) (Travis Downs, 2020)
   - [On the dangers of Intel's frequency scaling](https://blog.cloudflare.com/on-the-dangers-of-intels-frequency-scaling/) (Cloudflare, 2020)
   - [The dangers of AVX-512 throttling](https://lemire.me/blog/2018/08/15/the-dangers-of-avx-512-throttling-a-3-impact/) (Lemire, 2018)

### ARM SVE/SVE2 (2019-2024)

6. **ARM Documentation**:
   - [ARM SVE2 Explained](https://www.techno-logs.com/2024/10/03/arm-sve2-explained/)
   - [Introduction to SVE2](https://developer.arm.com/-/media/Arm%20Developer%20Community/PDF/102340_0001_00_en_introduction-to-sve2.pdf)
   - [Critical Look at SVE2](https://gist.github.com/atlury/c9e02bfe7489b5d3b37986b066478154)

### Production CSV Parsers (2020-2025)

7. **Sep (.NET/C# CSV Parser)**:
   - [Sep 0.10.0 - 21 GB/s CSV Parsing](https://nietras.com/2025/05/09/sep-0-10-0/)
   - [Introducing Sep](https://nietras.com/2023/06/05/introducing-sep/)

8. **zsv**:
   - [GitHub - liquidaty/zsv](https://github.com/liquidaty/zsv)

9. **DuckDB CSV Parser 2.0**:
   - [Driving CSV Performance](https://duckdb.org/2024/10/16/driving-csv-performance-benchmarking-duckdb-with-the-nyc-taxi-dataset)
   - [CSV Parser 2.0 PR](https://github.com/duckdb/duckdb/pull/10209)

10. **Apache Arrow**:
    - [Arrow CSV Reader Docs](https://arrow.apache.org/docs/python/csv.html)
    - [Quick Wins: Reading CSVs in R with Arrow](https://voltrondata.com/blog/quick-wins-reading-csvs-in-r-with-apache-arrow)

### Additional Resources

11. **SIMD Text Parsing (2020-2024)**:
    - [Fast CSV processing with SIMD](https://nullprogram.com/blog/2021/12/04/) (2021)
    - [Parsing timestamps with SIMD](https://lemire.me/blog/2023/07/01/parsing-time-stamps-faster-with-simd-instructions/) (2023)
    - [Scan HTML with SIMD](https://lemire.me/blog/2024/07/20/scan-html-even-faster-with-simd-instructions-c-and-c/) (2024)

---

## 11. Action Items from Research (2026-01-03)

Based on this research review, the following GitHub issues have been created to track implementation of promising techniques:

### High Priority

- **Issue #39**: [PCLMULQDQ-based Quote Mask](https://github.com/jimhester/libvroom/issues/39)
  - Replace scalar loop in `find_quote_mask` with carryless multiply
  - Expected: 2-4x speedup in quote handling

- **Issue #41**: [Branchless CSV State Machine](https://github.com/jimhester/libvroom/issues/41)
  - Replace switch-based state machine with lookup tables
  - Expected: Eliminate 90%+ branches

### Medium Priority

- **Issue #40**: [AVX-512 with Runtime Selection](https://github.com/jimhester/libvroom/issues/40)
  - Add AVX-512 support with automatic fallback to AVX2
  - Expected: 25-40% speedup on compatible hardware

- **Issue #42**: [ARM NEON/SVE Optimization](https://github.com/jimhester/libvroom/issues/42)
  - Benchmark and optimize for Apple Silicon and Graviton
  - Expected: 5-10 GB/s on modern ARM

- **Issue #44**: [SIMD Number Parsing](https://github.com/jimhester/libvroom/issues/44)
  - Add SIMD-accelerated integer, float, and timestamp parsing
  - Expected: 5-10x speedup in type inference

### Low Priority (Post-1.0)

- **Issue #43**: [Structure-Aware Raw Filters](https://github.com/jimhester/libvroom/issues/43)
  - Sparser-style filtering for query pushdown
  - Expected: 10-22x speedup on selective queries

---

**Document Status**: ✅ Research Complete + Action Items Created
**Last Updated**: 2026-01-03
**Conclusion**: **Stick with simdjson approach. No better alternative found in 2020-2026 research.**
**Next Steps**: See Issue #39 (PCLMULQDQ) as highest priority implementation
