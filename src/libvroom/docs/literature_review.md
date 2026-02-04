# Literature Review: High-Performance CSV Parsing with SIMD

**Date**: 2026-01-01
**Purpose**: Ground libvroom design decisions in current research
**Status**: Phase 1 - Core papers reviewed

---

## Executive Summary

This literature review examines three foundational papers for high-performance CSV parsing:

1. **Chang et al. (SIGMOD 2019)**: Speculative distributed CSV parsing
2. **Langdale & Lemire (VLDB 2019)**: SIMD-based JSON parsing at GB/s speeds
3. **van den Burgh & Nazabal (2019)**: CleverCSV dialect detection

**Key Findings**:
- **Two-stage SIMD parsing** (Langdale & Lemire) is **HIGH priority** for libvroom - directly applicable with 2-4x expected speedup
- **Dialect detection** (CleverCSV) is **MEDIUM priority** - essential for vroom integration and handling messy real-world CSVs
- **Speculative parsing** (Chang) is **MEDIUM priority** - valuable if implementing distributed/multi-threaded parsing

---

## 1. Chang et al. - "Speculative Distributed CSV Data Parsing" (SIGMOD 2019)

### Paper Details
- **Authors**: Ge Chang, Yinan Li, Eric Eilebrecht, Badrish Chandramouli, Donald Kossmann
- **Published**: SIGMOD 2019, Amsterdam
- **Pages**: 883-899
- **Context**: Distributed big data systems (Apache Spark)

### Summary

Addresses parallel CSV parsing in distributed systems where chunks lack contextual information about field/record boundaries. Proposes speculation-based approach that enables robust parallel parsing by leveraging CSV's syntactic properties.

**Core Insight**: CSV's simple structure allows speculation about boundaries with >98% success rate. When speculation fails, graceful recovery maintains correctness.

### Key Techniques

#### 1. Speculative Parsing Methodology
- Partition RDD chunks are parsed speculatively without prior knowledge of boundaries
- Makes educated guesses based on statistical properties of CSV data
- Rarely fails because CSV format has predictable structure

#### 2. Quote State Handling
- Maintains state machine tracking "inside quoted field" vs "outside"
- Quoted fields safely contain delimiters, newlines, special characters
- State transitions on unescaped quote characters (RFC 4180: `""` escaping)
- Critical: determines whether subsequent commas/newlines are data or delimiters

#### 3. Speculation Failures
**Failure scenarios**:
- Quote state incorrectly inferred at chunk boundaries
- Field/record boundaries incorrectly aligned
- Non-standard escaping conventions

**Recovery**: Re-parse affected chunks with correct context
**Success rate**: >98% in 11,000+ real-world datasets

#### 4. Speculation Window Optimization
**Tradeoff**:
- **Smaller windows** (64KB): More parallelism, higher failure risk
- **Larger windows** (1MB): Better accuracy, less parallelism

**Optimal**: 64KB-1MB provides best performance/accuracy balance

#### 5. Parallelization Strategy
- Divide file into chunks parsed independently in parallel
- Use overlap regions between chunks for boundary handling
- When speculation succeeds (common): fully parallelized
- When it fails: limited serial recovery, overall speedup remains significant

### Performance Results
- **Speedup**: 2.4x faster than conservative two-pass parallel parser
- **Scalability**: Scales well with file size (KB to multi-GB)
- **Accuracy**: Reliable syntax error detection on 11,000+ datasets

### Applicability to libvroom

**Rating**: MEDIUM

**Why Medium**:
- libvroom targets single-machine performance, not distributed systems
- Speculation overhead may not justify benefits on single machine
- However, multi-threaded single-machine parsing could benefit from speculation concepts

**Potential Applications**:
- Parse chunks independently with lookahead for boundary alignment
- Optimize chunk boundaries to maximize parallelism while maintaining quote-state correctness
- Inform vroom integration: vroom uses multi-threading, could leverage speculation

### Implementation Priority: MEDIUM

**Rationale**:
- Not primary focus for Phase 1-2 (single-threaded AVX2 optimization)
- Becomes relevant in Phase 3+ if implementing aggressive multi-threading
- Quote state management at chunk boundaries immediately applicable

**When to Implement**:
- After AVX2 optimization is complete
- When adding advanced multi-threading beyond simple chunk division
- If extending to distributed parsing scenarios

### Limitations & Edge Cases

1. **Quote State Ambiguity at Boundaries**
   - Chunk ending mid-quoted-field requires lookahead
   - Non-standard escaping (backslash) breaks assumptions

2. **Performance Variance**
   - Pathological cases (single-field quoted data) may not benefit
   - Speculation overhead might outweigh benefits for very small files

3. **Non-Standard Dialects**
   - Assumes RFC 4180 quote conventions
   - Alternative quote/escape mechanisms reduce effectiveness

### References
- [ACM Digital Library](https://dl.acm.org/doi/10.1145/3299869.3319898)
- [Microsoft Research](https://www.microsoft.com/en-us/research/publication/speculative-distributed-csv-data-parsing-for-big-data-analytics/)
- [PDF](https://badrish.net/papers/dp-sigmod19.pdf)

---

## 2. Langdale & Lemire - "Parsing Gigabytes of JSON per Second" (VLDB 2019)

### Paper Details
- **Authors**: Geoff Langdale (Intel), Daniel Lemire (UQAM)
- **Published**: VLDB Journal 28, October 2019, pages 941-960
- **DOI**: 10.1007/s00778-019-00578-5
- **Implementation**: simdjson (github.com/simdjson/simdjson)
- **Impact**: Adopted by Facebook/Meta, Node.js, ClickHouse, Apache Doris

### Summary

First standard-compliant JSON parser processing gigabytes/second on single core using SIMD. Fundamental innovation: SIMD + minimal branching + decoupling structural discovery from content parsing.

**Performance**: 2-5x faster than RapidJSON using ~25% of the instructions.

**Key Insight**: JSON structure is highly regular and amenable to vectorization. Structural characters discovered in parallel, quoted strings processed with bit manipulation eliminates branch misprediction penalties.

### Key Techniques

#### 1. Two-Stage Parsing Architecture

**Stage 1 - Structural Indexing (2-4 GB/s)**:
- Scans input using wide SIMD (64-128 bytes per iteration)
- Identifies all structural characters in parallel
- Simultaneously validates UTF-8 encoding
- Outputs compact index of structural positions
- Character classification in parallel:
  - Whitespace (ignored)
  - Structural: `{`, `}`, `[`, `]`, `:`, `,`
  - String content
  - Escape sequences

**Stage 2 - Document Parsing**:
- Uses Stage 1 index to navigate without re-scanning
- Builds DOM (tape format) of structure
- Parses numbers, strings, values with type-specific algorithms
- Supports lazy evaluation (On-Demand API)

#### 2. Structural Indexing Using SIMD

**Process**:
1. SIMD bit classification: Classify 64 characters in parallel
2. Quote discovery: Find all unescaped quotes
3. Quoted region masking: Determine which structural chars are inside quotes (irrelevant)
4. Escape handling: Track backslashes to identify escaped quotes

**Algorithm**:
```
1. Scan for odd-length backslash sequences (escaping detection)
2. Find quote character positions
3. Compute quote pair mask (prefix sum of XOR operations)
4. Mark structural characters outside quotes for Stage 2
```

#### 3. Bit Manipulation Tricks for Quoted Strings

**Quote and Escape Detection**:
```c
quotes = (input == '"')          // SIMD comparison
backslashes = (input == '\\')    // SIMD comparison
```

**Escape Sequence Processing**:
```c
// Find odd-length backslash sequences
escaped_quotes = quotes & follows_odd_backslashes
unescaped_quotes = quotes ^ escaped_quotes
```

**Quoted Region Masking**:
```c
// Prefix sum of XOR on unescaped quotes determines toggle points
inside_quote_mask = prefix_xor(unescaped_quotes)

// Final structural character masking
structural_outside_quotes = structural_chars & ~inside_quote_mask
```

**Benefits**:
- No branching on every character
- Processes 64 bytes in parallel
- Eliminates branch misprediction penalties

#### 4. CSV Applicability Analysis

**What Translates Directly**:
1. **Two-stage approach**: Stage 1 finds delimiters/quotes, Stage 2 extracts values
2. **SIMD character classification**: Find commas, newlines, quotes in parallel
3. **Quoted region masking**: Exclude delimiters inside quotes from structural role
4. **Bit manipulation**: XOR/prefix-sum tricks for quote handling

**CSV Advantages Over JSON**:
- CSV has 1-3 structural characters (`,`, `\n`, `"`) vs JSON's 7+
- CSV quote escaping is simpler: `""` vs backslash escaping
- No deep nesting tracking needed (JSON requires bracket/brace stack)
- Simpler field/record structure (regular rows vs arbitrary nesting)

**CSV-Specific Adaptations**:
- Quote escaping: `""` detection requires checking adjacent characters (simpler than JSON's `\"`)
- Delimiter detection: configurable delimiter character (not fixed like JSON)
- Line ending variety: `\n`, `\r\n`, `\r` (JSON uses `\n`)

### Performance Results
- **Throughput**: 2.5-5 GB/s per core (vs 0.5-1 GB/s for RapidJSON)
- **Instruction count**: ~25% of RapidJSON
- **Memory**: Comparable to reference implementations
- **Portability**: SSE 4.2, AVX2, AVX-512 support

### Applicability to libvroom

**Rating**: HIGH (★★★★★)

**Why High**:
- CSV shares structural similarities with JSON
- Two-stage processing directly applicable
- Bit manipulation is *simpler* for CSV than JSON
- Expected 2-4x throughput gain

**Direct Applications**:
1. Use SIMD to classify characters (delimiter, newline, quote) in parallel
2. Build quoted region mask to exclude delimiters/newlines inside quotes
3. Separate structural indexing (Stage 1) from value extraction (Stage 2)
4. Use bit manipulation (XOR, prefix sum) instead of branching

### Implementation Priority: **HIGH** ⚠️

**Rationale**:
- **Most impactful** optimization for libvroom
- Proven technique with production implementations
- CSV's simpler structure makes implementation easier than JSON
- Directly addresses performance bottleneck (character-by-character scanning)

**Implementation Plan**:
1. **Phase 2**: Implement Stage 1 structural indexing with AVX2
   - SIMD character classification (comma, newline, quote)
   - Quoted region masking using bit manipulation
   - Output: compact index of field/record boundaries

2. **Phase 2**: Optimize bit manipulation for CSV quote escaping
   - Adapt JSON's backslash-escape logic to CSV's doubled-quote escaping
   - Validate with comprehensive tests

3. **Phase 3**: Implement Stage 2 value extraction (if needed for vroom)
   - May defer to vroom's Altrep lazy materialization
   - Index-based output may be sufficient

**Expected Impact**: 2-4x throughput improvement on AVX2

### Limitations & Edge Cases

1. **Quoted String Handling Complexity**
   - CSV's `""` escaping requires adjacent character checking
   - Non-standard backslash escaping breaks algorithm

2. **Performance Variance**
   - Stage 1 throughput not guaranteed for all patterns
   - All-quoted fields may reduce efficiency

3. **UTF-8 Validation Overhead**
   - simdjson validates UTF-8; may be unnecessary for ASCII-only CSV
   - However, real CSV often contains non-ASCII (prudent to validate)

4. **Cache Efficiency**
   - Two-stage creates intermediate index; large files may exceed cache
   - Index construction overhead for very small files

5. **Streaming Constraints**
   - Requires building complete index before Stage 2
   - Not ideal for incremental data arrival
   - On-Demand interface mitigates this

### References
- [VLDB Journal](https://link.springer.com/article/10.1007/s00778-019-00578-5)
- [arXiv](https://arxiv.org/abs/1902.08318)
- [simdjson GitHub](https://github.com/simdjson/simdjson)
- [Research Gate](https://www.researchgate.net/publication/336443260_Parsing_gigabytes_of_JSON_per_second)

---

## 3. van den Burgh & Nazabal - "Wrangling Messy CSV Files" (CleverCSV, 2019)

### Paper Details
- **Authors**: Gerrit J.J. van den Burgh, Alfredo Nazabal, Charles Sutton
- **Published**: Data Mining and Knowledge Discovery 33(4):1415-1441, July 2019
- **arXiv**: 1811.11242
- **Implementation**: CleverCSV (github.com/alan-turing-institute/clevercsv)

### Summary

Tackles automatic dialect detection (delimiter, quote character, escape convention) for messy real-world CSV files. Proposes consistency-based approach that searches dialect space and selects the one producing the most "table-like" result.

**Key Innovation**: Correctly parsed CSV exhibits regular row structure and consistent column types, whereas incorrect dialects produce irregular, inconsistent tables.

**Performance**: 97% accuracy on diverse corpus, 22% improvement over Python's csv module on non-standard files.

### Key Techniques

#### 1. Dialect Detection Algorithm

**Process**:
1. Generate candidate delimiters (`,`, `;`, `|`, `\t`, etc.)
2. Generate candidate quote characters (`"`, `'`, etc.)
3. Iterate through all (delimiter, quote) combinations
4. For each candidate, parse CSV sample
5. Compute consistency score
6. Select dialect with highest score

**Candidate Generation**:
- **Delimiters**: Analyze character frequencies
- **Quote characters**: Typically `"` or `'`
- **Escape conventions**: RFC 4180 (doubled quotes) or configurable

#### 2. Pattern Consistency Scoring

**Core Concept**: Regular, uniform row structure (consistent field count per row)

**Computation**:
1. Parse sample CSV with candidate dialect
2. Extract row length pattern (field count per row)
3. Represent as abstract string:
   - "C" = expected row length
   - Alternative chars for deviations
4. Compute uniformity of pattern

**Uniformity Measurement**:
- **High score**: All rows have same field count (correct dialect)
- **Low score**: Irregular row counts (incorrect dialect)
- Formula: Entropy or information content (fewer unique patterns = higher score)

#### 3. Type Score and Data Type Inference

**Type Score Concept**: Correctly parsed columns should have consistent types.

**Computation**:
1. Infer data type for each cell (numeric, string, date, bool, etc.)
2. For each column, count type consistency
3. **Type Score** = fraction of cells matching most frequent type per column

**Example**:
- **Correct dialect**:
  - Column 1: 100% numeric (score 1.0)
  - Column 2: 95% text (score 0.95)
- **Wrong dialect**:
  - Column 1: 50% numeric, 30% text, 20% dates (score 0.5)
  - Column 2: Very mixed (score 0.3)

#### 4. Combined Consistency Measure

**Two-Phase Optimization**:
1. Compute pattern score first (fast, no type inference)
2. Compute type score only if `pattern_score >= best_score_so_far`
3. **Combined score** = weighted combination

**Rationale**:
- Pattern score is O(n) with low constant
- Type score requires type inference (higher constant)
- Prune candidates with poor pattern scores early

#### 5. Sample Size and Minimal Data Requirements

**Sample-Based Approach**:
- Configurable sample size (character count, not row count)
- **Default**: Entire file (best accuracy)
- **Fast mode**: Limit to N characters

**Empirical Results**:
- **Small samples (10KB)**: Works for many files
- **Messy files**: Recommend 100KB+ samples
- **Well-formed files**: 1-2KB often sufficient

**Accuracy Scaling**:
- Larger samples → better accuracy
- 97% on full files
- Graceful degradation with smaller samples

### Performance Results
- **Overall Accuracy**: 97% on diverse corpus
- **Messy CSV Improvement**: 22% over Python csv module
- **Speed**: Fast detection, scales to multi-GB with sampling
- **vs Baselines**: Python csv (75%), others (70-85%)

### Applicability to libvroom

**Rating**: MEDIUM-HIGH

**For Dialect Detection**:
- Pre-process files to determine delimiter/quote before SIMD parsing
- Once dialect known, libvroom's optimized SIMD paths run at full speed
- Critical for vroom integration (vroom issue #105)

**For Parsing Optimization**:
- Understanding CSV characteristics could inform:
  - Chunk sizing for parallel parsing
  - Column-oriented strategies
  - Type-aware SIMD instructions

**Limitations**:
- CleverCSV is Python; not high-throughput
- Dialect detection is one-time cost (amortized)
- Main benefit: **correctness and robustness** for messy real-world data

### Implementation Priority: MEDIUM

**Rationale**:
- **Essential for vroom**: Directly addresses vroom issue #105
- CleverCSV algorithm can be implemented in C++ (minor cost)
- One-time detection cost negligible vs parsing cost
- **Significantly improves usability** for real-world messy CSVs

**Recommended Integration**:
1. **Pre-parsing phase**: Dialect detection (optional, can skip if user specifies)
2. **SIMD parsing phase**: Use detected dialect for optimized paths
3. **Fallback**: User can specify dialect to skip detection

**When to Implement**:
- **Phase 3 (vroom integration)**: High priority for vroom
- Implement C++ version of pattern/type scoring
- Integrate with libvroom API as optional preprocessing step

### Limitations & Edge Cases

1. **Ambiguous Dialects**
   - Multiple plausible dialects with similar scores
   - Example: Few-row file might score equally for comma/semicolon
   - Heuristic: Prefer simpler delimiters (`,` > `;` > `|` > `\t`)

2. **Inconsistent Files**
   - Truly malformed CSV (mixed dialects, quality issues) score poorly for all
   - Algorithm selects "best of bad options"
   - Cannot reliably detect dialect for fundamentally broken CSV

3. **Type Inference Challenges**
   - `"123"` could be string or number (ambiguous)
   - Empty fields, NULLs complicate consistency
   - Reduces discriminative power of type score

4. **Performance on Large Files**
   - Full-file analysis slow for multi-GB
   - Sampling mitigates but reduces accuracy
   - Type score is O(n); becomes bottleneck for huge samples

5. **Quote Character Detection**
   - Must try multiple quote characters (`"`, `'`, etc.)
   - Mixed quote conventions are difficult
   - Some files have no quotes (simpler) or mixed (harder)

### References
- [arXiv](https://arxiv.org/abs/1811.11242)
- [Springer](https://link.springer.com/article/10.1007/s10618-019-00646-y)
- [CleverCSV GitHub](https://github.com/alan-turing-institute/clevercsv)
- [Alan Turing Institute](https://www.turing.ac.uk/news/publications/wrangling-messy-csv-files-detecting-row-and-type-patterns)

---

## Comparative Analysis & Implementation Roadmap

### Priority Matrix

| Paper | Technique | Applicability | Priority | Expected Benefit | Implementation Effort |
|-------|-----------|---------------|----------|------------------|-----------------------|
| **Langdale & Lemire** | Two-stage SIMD parsing | ★★★★★ | **HIGH** | 2-4x throughput | High |
| **Langdale & Lemire** | Bit manipulation for quotes | ★★★★★ | **HIGH** | Simplified logic | Medium |
| **van den Burgh** | Dialect detection | ★★★★☆ | **MEDIUM** | Robustness, usability | Medium |
| **Chang et al.** | Speculative parsing | ★★★☆☆ | **MEDIUM** | Multi-threading | Medium |

### Implementation Roadmap

#### Phase 1: Foundation (Current - Month 2)
✓ Literature review complete
- Evaluate Highway vs SIMDe
- Study vroom architecture
- Set up test infrastructure

#### Phase 2: SIMD Optimization (Months 3-4)
**HIGH PRIORITY: Langdale & Lemire techniques**

1. **Implement Stage 1 - Structural Indexing**
   - SIMD character classification (`,`, `\n`, `"`)
   - Quoted region masking using bit manipulation
   - Process 64 bytes per iteration (AVX2)
   - Output: compact index of field/record boundaries

2. **Optimize Quote Handling**
   - Adapt JSON's bit tricks to CSV's `""` escaping
   - Use XOR and prefix sum instead of branching
   - Handle edge cases (unclosed quotes, mixed escaping)

3. **Benchmark**
   - Target: >5 GB/s on modern x86-64
   - Compare: current implementation vs SIMD Stage 1
   - Expected: 2-4x improvement

#### Phase 3: vroom Integration (Months 5-6)
**MEDIUM PRIORITY: CleverCSV dialect detection**

1. **Implement Dialect Detection**
   - C++ implementation of pattern consistency scoring
   - Type inference for type consistency scoring
   - Optimization: two-phase scoring (pattern first, type if promising)

2. **Integration with vroom**
   - Pre-parsing dialect detection (addresses vroom #105)
   - User-specified dialect option (skip detection)
   - Fallback heuristics for ambiguous cases

3. **Benchmark**
   - Test on vroom's messy CSV benchmarks
   - Measure: accuracy, detection time
   - Target: 95%+ accuracy, <100ms for 1MB sample

#### Phase 4+: Advanced Optimizations (Months 7+)
**OPTIONAL: Speculative parsing**

- Implement if multi-threaded performance needs improvement
- Use speculation for chunk boundary alignment
- Graceful fallback on speculation failure

### Key Takeaways for libvroom

1. **Two-stage SIMD parsing is the highest-impact optimization**
   - Proven technique (simdjson) with production use
   - CSV's simpler structure makes implementation easier than JSON
   - Expected 2-4x throughput gain

2. **Dialect detection essential for vroom integration**
   - Addresses vroom issue #105 directly
   - Improves robustness for messy real-world CSVs
   - One-time cost amortized over parsing

3. **Speculative parsing deferred to later phases**
   - Valuable for distributed/advanced multi-threading
   - Not primary focus for single-machine SIMD optimization
   - Revisit after AVX2 optimization complete

4. **Bit manipulation > branching**
   - All three papers emphasize avoiding branches
   - SIMD operations + bitmasks eliminate branch mispredictions
   - Critical for high throughput

---

## Next Steps

- [x] **Complete core papers review** (Chang, Langdale/Lemire, CleverCSV)
- [ ] **Evaluate SIMD libraries** (Highway vs SIMDe) - See Section 2.2 of production plan
- [ ] **Study vroom architecture** - Understand index format, Altrep integration
- [ ] **Prototype Stage 1 SIMD indexing** - Implement Langdale/Lemire's technique for CSV
- [ ] **Design dialect detection API** - CleverCSV-style preprocessing

---

## Additional Papers to Review (Future)

### SIMD & Performance
- **Mison** (Li et al., 2017) - Speculative parsing for JSON
- **Zebra** (Palkar et al., 2018) - Vectorized parsing
- **AVX-512 optimization papers** (2020-2024)
- **ARM SVE/SVE2 papers** (2019-2024)

### Parallel Processing
- **Parallel CSV parsing** (Ge et al., 2021)
- **High-performance text processing**

### Error Handling
- **Error handling in high-perf parsers** (recent)

---

---

## 4. Mison - "A Fast JSON Parser for Data Analytics" (Li et al., VLDB 2017)

### Paper Details
- **Authors**: Yinan Li, Nikos R. Katsipoulakis, Badrish Chandramouli, Jonathan Goldstein, Donald Kossmann
- **Published**: VLDB 2017, Proc. VLDB Endow. 10(10), 1118–1129
- **Institution**: Microsoft Research
- **Available**: [PDF](http://www.vldb.org/pvldb/vol10/p1118-li.pdf)

### Summary

Mison is a JSON parser optimized for data analytics that pushes down both projection and filter operators into the parser. It uses a **two-level speculative approach** to jump directly to queried fields without expensive tokenization.

**Core Innovation**: Level-based speculation that predicts logical locations of queried fields based on previously seen patterns, combined with structural indices to map logical to physical locations.

**Performance**: 3.6x faster than Jackson without speculation; 10.2x with speculation enabled.

### Key Techniques

#### 1. Level-Based Speculation
- **Observation**: Analytics queries typically access only a few fields in nested JSON
- **Approach**: Build a speculation index for field locations based on first N records
- **Mechanism**: Track nesting level and field offsets within each level
- **Benefit**: Skip parsing irrelevant fields entirely

#### 2. Two-Level Architecture

**Upper Level (Logical):**
- Speculates about field positions based on schema regularity
- Records: "Field X typically appears at level 2, offset 50 bytes from parent"
- Learns from initial records to predict later ones

**Lower Level (Physical):**
- Builds structural indices on JSON data using SIMD (similar to simdjson)
- Maps logical locations to physical byte positions
- Validates speculation results

#### 3. Structural Indexing (SIMD-based)
- Uses SIMD to find structural characters: `{`, `}`, `[`, `]`, `:`, `,`
- Creates bitmaps for each character type
- **Difference from simdjson**: Mison predates simdjson (2017 vs 2019) and uses simpler structural indexing

#### 4. Speculation Validation
- When speculation succeeds: Direct jump to field location (fast path)
- When speculation fails: Fallback to full parsing (slow path)
- **Success rate**: Very high for regular data (analytics workloads)

### Comparison to simdjson Approach

| Aspect | Mison (2017) | simdjson (2019) |
|--------|--------------|-----------------|
| **Primary Goal** | Analytics (selective field access) | General-purpose JSON parsing |
| **Speculation** | Level-based field location prediction | No speculation (always scans full structure) |
| **SIMD Usage** | Basic structural indexing | Advanced bit manipulation, quote handling |
| **Performance** | 10x for selective queries | 2-5x for full document parsing |
| **Complexity** | Higher (speculation logic) | Lower (deterministic two-stage) |
| **Robustness** | Requires regular schema | Works on any valid JSON |

**Key Difference**: Mison optimizes for **sparse access** (few fields), simdjson optimizes for **dense access** (full document).

### Applicability to libvroom

**Rating**: LOW-MEDIUM

**Why Low-Medium**:
- CSV parsing typically requires **full row parsing**, not selective field access
- Speculation overhead may not pay off when all fields are needed
- CSV's flat structure doesn't benefit from level-based speculation (no nesting)
- **However**: If integrating with vroom's lazy evaluation, selective column parsing could benefit

**Potential Applications**:
1. **Selective Column Parsing**: If only parsing specific columns in wide CSV (e.g., columns 5, 10, 50 out of 100)
2. **Schema Learning**: Build field offset predictions for regular CSV files
3. **Skip Optimization**: Jump directly to target columns without scanning intervening fields

**Limitations for CSV**:
- CSV lacks the nesting hierarchy that Mison exploits
- Most CSV use cases require all fields (data loading, transformation)
- Speculation complexity may not justify performance gains

### Implementation Priority: LOW

**Rationale**:
- **Not applicable to core CSV parsing** (requires full row scans)
- **Potentially useful for column-selective parsing** (low priority feature)
- vroom's Altrep already handles lazy column materialization differently
- Effort better spent on core SIMD optimizations (simdjson approach)

**Should we change our approach?**: **NO**
- Mison's speculation is orthogonal to SIMD structural indexing
- simdjson's deterministic approach is better fit for CSV
- If we need selective parsing, implement as separate optimization pass

### Limitations & Edge Cases
1. **Irregular Data**: Speculation fails frequently on non-uniform JSON
2. **Schema Changes**: Requires re-learning when structure changes
3. **Memory Overhead**: Speculation indices consume memory
4. **Fallback Cost**: Failed speculation incurs parsing overhead

### References
- [VLDB Paper](http://www.vldb.org/pvldb/vol10/p1118-li.pdf)
- [Microsoft Research](https://www.microsoft.com/en-us/research/publication/mison-fast-json-parser-data-analytics/)
- [ACM DL](https://dl.acm.org/doi/10.14778/3115404.3115416)

---

## 5. Sparser - "Filter Before You Parse" (Palkar et al., VLDB 2018)

### Paper Details
- **Authors**: Shoumik Palkar, Firas Abuzaid, Peter Bailis, Matei Zaharia
- **Published**: VLDB 2018, Proc. VLDB Endow. 11(11)
- **Institution**: Stanford DAWN Lab
- **Available**: [PDF](https://www.vldb.org/pvldb/vol11/p1576-palkar.pdf) | [GitHub](https://github.com/stanford-futuredata/sparser)

### Summary

Sparser introduces a **filter-before-parse** paradigm for faster analytics on raw data. Instead of parsing entire files, it uses SIMD-accelerated **raw filters (RFs)** to identify relevant records before invoking expensive parsers.

**Core Insight**: Most analytical queries access small fractions of data. Filtering raw bytes is 10-100x faster than parsing. Even with false positives, filter+parse is faster than parse-everything.

**Performance**: 22x faster than Mison on JSON, 9x end-to-end speedup in Apache Spark.

### Key Techniques

#### 1. Raw Filters (RFs)
- **Definition**: Fast, SIMD-based operators that search for byte sequences in raw data
- **Property**: Never yield false negatives, occasionally false positives
- **Implementation**: Substring search using SIMD parallelism

**Example**: Query for records where `city = "Seattle"`
- RF searches for byte sequence `"Seattle"` in raw file
- Returns byte offsets of potential matches
- Parser only invoked on filtered regions

#### 2. SIMD Substring Search
- **Technique**: Pack target byte sequence into SIMD vector register
- **Process**: Slide window over input, compare multiple bytes in parallel
- **Speed**: Process 16-64 bytes per instruction (SSE/AVX)

**Implementation**:
```c
// Pseudocode for SIMD substring search
needle_vec = _mm_set1_epi8(needle[0]);  // Broadcast first byte
for (i = 0; i < data_len; i += 16) {
    haystack = _mm_loadu_si128(data + i);
    mask = _mm_cmpeq_epi8(needle_vec, haystack);
    // Check full match on mask hits
}
```

#### 3. Filter Cascades
- **Observation**: Multiple filters can be chained (AND/OR logic)
- **Optimization**: Order filters by selectivity (most selective first)
- **Benefit**: Skip expensive filters when early filters eliminate data

**Example**: `city = "Seattle" AND year > 2020`
- RF1: Search for `"Seattle"` (high selectivity)
- RF2: Search for `"202"` (low selectivity, only on RF1 results)
- Parse only records passing both filters

#### 4. Optimizer
- **Challenge**: Many possible filter orderings (exponential space)
- **Solution**: Cost-based optimizer estimates filter selectivity
- **Result**: 10x performance difference between good/bad orderings

### Comparison to simdjson/Mison

| Aspect | Sparser | simdjson | Mison |
|--------|---------|----------|-------|
| **Approach** | Filter then parse | Always parse | Speculative parse |
| **SIMD Use** | Substring search | Structural indexing | Basic structural indexing |
| **Best For** | Selective queries (1% of data) | Full document parsing | Field projection |
| **False Positives** | Tolerates (filtered later) | N/A (exact) | N/A (validates speculation) |
| **Speedup vs baseline** | 22x (on selective queries) | 2-5x | 10x (on projections) |

**Key Difference**: Sparser avoids parsing most data; simdjson/Mison parse everything (just faster).

### Applicability to libvroom

**Rating**: LOW for core CSV parsing, MEDIUM-HIGH for query integration

**Why Low for Core Parsing**:
- CSV parsing typically requires scanning every field
- Filter-before-parse benefits erode when most data is relevant
- Extra filtering pass adds overhead for full-table scans

**Why Medium-High for Query Integration**:
- **If integrated with query engine** (e.g., DuckDB, vroom): High value
- **Use case**: `SELECT * FROM csv WHERE state = 'CA'`
  - RF searches for `"CA"` in raw CSV
  - Only parse rows containing `"CA"`
  - 10x+ speedup on selective queries

**Hybrid Approach**:
- **Full scan**: Use libvroom's SIMD parser (no filtering)
- **Selective query**: Apply Sparser-style RFs before parsing

### Implementation Priority: LOW (for Phase 1-3), MEDIUM (for future query optimization)

**Rationale**:
- **Not applicable to vroom integration** (vroom parses all data for indexing)
- **Valuable for future query pushdown** (if libvroom exposes query API)
- **Defer until after core SIMD parser is production-ready**

**Should we change our approach?**: **NO for core parser, YES for future query layer**
- Core libvroom: Focus on fast full-file parsing (simdjson approach)
- Future enhancement: Add optional RF layer for query integration
- Implementation: Separate module, opt-in for query workloads

### Limitations & Edge Cases
1. **False Positives**: RF matches require validation (parsing overhead)
2. **Low Selectivity**: When query matches >10% of data, filtering overhead dominates
3. **Substring Ambiguity**: Searching for `"123"` matches `"1234"`, `"0123"`, etc.
4. **Delimiter Awareness**: RFs don't respect CSV structure (may match across fields)

### Novel Ideas for libvroom
1. **Structure-Aware RFs**: Combine Sparser's filtering with simdjson's structural indexing
   - Use structural index to identify field boundaries
   - Apply RFs only within target columns (not across delimiters)
   - **Benefit**: Eliminate false positives from cross-field matches

2. **Predicate Pushdown to Indexing**:
   - During libvroom's indexing pass, apply simple predicates
   - Mark rows matching predicates in index metadata
   - **Use case**: vroom could skip loading non-matching rows

### References
- [VLDB Paper](https://www.vldb.org/pvldb/vol11/p1576-palkar.pdf)
- [Stanford DAWN](https://dawn.cs.stanford.edu/news/filter-you-parse-faster-analytics-raw-data-sparser)
- [GitHub](https://github.com/stanford-futuredata/sparser)
- [The Morning Paper Summary](https://blog.acolyer.org/2018/08/20/filter-before-you-parse-faster-analytics-on-raw-data-with-sparser/)

---

## 6. Recent Work (2020-2026): AVX-512, ARM SVE, and Modern SIMD CSV Parsers

### 6.1 AVX-512 Optimizations for Text Parsing

#### Research & Blog Posts (2020-2024)

**Daniel Lemire's Work** (2018-2025):
- **"AVX-512: when and how to use these new instructions"** (2018) - [Blog](https://lemire.me/blog/2018/09/07/avx-512-when-and-how-to-use-these-new-instructions/)
- **"Parsing JSON faster with Intel AVX-512"** (2022) - [Blog](https://lemire.me/blog/2022/05/25/parsing-json-faster-with-intel-avx-512/)
- **"Parsing integers quickly with AVX-512"** (2023) - [Blog](https://lemire.me/blog/2023/09/22/parsing-integers-quickly-with-avx-512/)

**Key Findings**:

#### 1. AVX-512 Performance Gains
- simdjson 2.0 (2022): **25-40% faster** with AVX-512 vs AVX2
- Integer parsing: **2x parallelism** (parse 2 numbers simultaneously)
- Mask registers (k1-k8): Enable branchless conditional operations

#### 2. AVX-512 Downclocking Pitfall ⚠️

**Critical Issue**: AVX-512 causes CPU frequency throttling

**Sources**:
- [Ice Lake AVX-512 Downclocking](https://travisdowns.github.io/blog/2020/08/19/icl-avx512-freq.html) (Travis Downs, 2020)
- [On the dangers of Intel's frequency scaling](https://blog.cloudflare.com/on-the-dangers-of-intels-frequency-scaling/) (Cloudflare, 2020)
- [The dangers of AVX-512 throttling](https://lemire.me/blog/2018/08/15/the-dangers-of-avx-512-throttling-a-3-impact/) (Lemire, 2018)

**Performance Impact**:
- **Worst case**: CPU runs at **50% of base frequency** during AVX-512 execution
- **License-based downclocking**: Different frequency limits for light/heavy 512-bit instructions
- **Whole-application impact**: Slowdown persists after AVX-512 code finishes (frequency ramp-up delay)
- **Multi-core**: Throttling affects all cores, not just the one executing AVX-512

**Mitigation Strategies**:
1. **Hybrid approach**: Use AVX-512 only for proven bottlenecks
2. **Benchmark on target hardware**: Some CPUs (AMD Zen 4+) handle AVX-512 better
3. **Runtime detection**: Fallback to AVX2 if AVX-512 slower on specific CPU
4. **Avoid heavy instructions**: Prefer lighter 512-bit ops over FP-intensive ones

**Recommendation for libvroom**:
- **Implement AVX-512 support**: But make it **opt-in, not default**
- **Benchmark thoroughly**: Test on Intel Ice Lake, Sapphire Rapids, AMD Zen 4
- **Runtime selection**: Use AVX-512 only if measured faster than AVX2 on specific hardware

#### 3. AMD Zen 4 AVX-512 (2022+)
- **Good news**: AMD Zen 4 (2022) implements AVX-512 **without severe downclocking**
- **Performance**: Competitive with Intel, sometimes better (no frequency penalty)
- **Caveat**: "Avoid compressing words to memory" - some ops still inefficient ([Lemire 2025](https://lemire.me/blog/2025/02/14/avx-512-gotcha-avoid-compressing-words-to-memory-with-amd-zen-4-processors/))

### Summary: AVX-512 for CSV Parsing

**Rating**: MEDIUM (worthwhile, but not critical)

**Pros**:
- 25-40% potential speedup (simdjson results)
- Mask registers simplify branchless code
- AMD Zen 4+ makes it more viable

**Cons**:
- Downclocking on older Intel CPUs (2018-2021)
- Implementation complexity (more intrinsics)
- AVX2 already provides excellent performance (>5 GB/s target achievable)

**Should we change our approach?**: **NO, but add AVX-512 as optional optimization**
- **Phase 2-3**: Perfect AVX2 implementation first
- **Phase 4**: Add AVX-512 as runtime-selected optimization
- **Testing**: Benchmark on multiple CPUs, use faster path dynamically

**Implementation Priority**: MEDIUM (Phase 4+)

---

### 6.2 ARM SVE/SVE2 (2019-2024)

#### Overview
- **SVE**: Scalable Vector Extension (ARMv8-A, 2017)
- **SVE2**: Enhanced version (ARMv9-A, 2021)
- **Key Innovation**: Variable vector length (128-2048 bits, runtime-determined)

**Sources**:
- [ARM SVE2 Explained](https://www.techno-logs.com/2024/10/03/arm-sve2-explained/)
- [Introduction to SVE2](https://developer.arm.com/-/media/Arm%20Developer%20Community/PDF/102340_0001_00_en_introduction-to-sve2.pdf)
- [Critical Look at SVE2](https://gist.github.com/atlury/c9e02bfe7489b5d3b37986b066478154)

#### Key Features

**1. Predication-Centric Design**
- **Predicate registers**: P0-P7 (govern which lanes are active)
- **Merging vs Zeroing**: `/m` (preserve inactive) vs `/z` (zero inactive)
- **Use case**: Loop control without padding/scalar cleanup

**2. Length-Agnostic Algorithms**
- Write code once, runs on 128-bit (NEON) to 2048-bit SVE
- No hard-coded vector widths
- **Benefit**: Forward compatibility (future CPUs with wider vectors)

**3. Comparison to AVX-512

| Feature | ARM SVE/SVE2 | x86 AVX-512 |
|---------|--------------|-------------|
| **Vector Width** | Variable (128-2048 bits) | Fixed 512 bits |
| **Predication** | Mandatory (all instructions) | Optional |
| **Comparison Result** | `svbool_t` predicate | Integer mask |
| **Hardware** | Neoverse V1+, Apple M-series (limited), Graviton 3+ | Intel Ice Lake+, AMD Zen 4+ |
| **Downclocking** | None reported | Severe on older Intel |

#### Applicability to libvroom

**Rating**: MEDIUM-HIGH (for ARM support)

**Why Medium-High**:
- **ARM market growing**: AWS Graviton, Apple Silicon, cloud ARM instances
- **No downclocking penalty**: Unlike AVX-512 on Intel
- **Length-agnostic code**: Single implementation scales across ARM CPUs

**Challenges**:
- **Limited hardware availability**: SVE2 still rare (mostly server chips)
- **NEON more common**: Most ARM devices use 128-bit NEON
- **Porting effort**: Requires learning SVE intrinsics/assembly

**Should we change our approach?**: **NO for Phase 1-3, YES for ARM support (Phase 5)**
- **Phase 1-3**: Focus on x86 AVX2/AVX-512
- **Phase 5**: Port to ARM using Highway or SIMDe
  - **Highway**: Abstracts NEON/SVE, easier porting
  - **Direct SVE**: Faster, but harder to maintain

**Implementation Priority**: MEDIUM (Phase 5)

**Specific Recommendation**:
- Use **Google Highway** for ARM support (see Section 2.2 of production plan)
- Highway abstracts NEON/SVE, providing portable code
- If Highway overhead acceptable (<5%), no need for hand-coded SVE

#### SVE for Text Processing
- **String operations**: SVE2 includes string processing acceleration
- **Example**: `strlen` with vector partitioning, speculative loads (`ldff1b`)
- **Benefit**: CSV parsing could use SVE2's string acceleration

**Research Gap**: No published papers on SVE-based CSV parsing found (2020-2024)

---

### 6.3 Production SIMD CSV Parsers (2020-2025)

#### Sep (.NET/C#) - 21 GB/s CSV Parser

**Project**: [nietras.com/sep](https://nietras.com/2025/05/09/sep-0-10-0/)

**Performance**: **21 GB/s** on AMD Ryzen 9950X (AVX-512, April 2025)
- ~3x improvement since initial release (June 2023)
- AVX-512-to-256 parser circumvents .NET mask register inefficiencies

**Key Techniques**:
- **Quote matching**: Carryless multiply (PCLMULQDQ) for "quote mask" → "quoted regions mask"
- **Branchless design**: SIMD asks "where are ALL commas/quotes in 64 bytes?" not "is this a comma?"
- **Data parallelism**: Transforms control-flow problem into data-parallel problem

**Insight**: Demonstrates simdjson-style techniques **translate directly to CSV** with excellent results

**Applicability**: HIGH (validates our simdjson-based approach)

---

#### zsv - "World's Fastest CSV Parser"

**Project**: [GitHub - liquidaty/zsv](https://github.com/liquidaty/zsv)

**Claims**:
- SIMD operations, efficient memory use
- 25%+ faster than xsv, 2x+ faster than polars/duckdb on `select` operations
- **Memory footprint**: 1.5 MB (vs 4 MB xsv, 76 MB duckdb, 475 MB polars)

**Features**:
- CLI + library (C)
- WebAssembly compilation support
- Extensible architecture

**Applicability**: MEDIUM (competitor, validates SIMD CSV market)

**Note**: Claims of "world's fastest" are marketing; Sep achieves 21 GB/s vs zsv's undisclosed performance

---

#### DuckDB CSV Parser 2.0 (2024)

**Project**: [DuckDB CSV improvements 2024](https://duckdb.org/2024/10/16/driving-csv-performance-benchmarking-duckdb-with-the-nyc-taxi-dataset)

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
   - Parallelism improvements (union_by_name, Sept 2024)

**Comparison to libvroom Approach**:

| Aspect | DuckDB | libvroom (planned) |
|--------|--------|-------------------|
| **SIMD** | Compiler auto-vectorization | Explicit SIMD intrinsics (AVX2/AVX-512) |
| **Performance** | 2x slower than Parquet | Target: Match/exceed DuckDB |
| **Portability** | High (compiler handles SIMD) | Medium (manual porting needed) |
| **Optimization** | Easier to maintain | Higher performance ceiling |

**Applicability**: HIGH (validates CSV parsing is critical, SIMD necessary for performance)

**Should we change our approach?**: **NO**
- DuckDB's auto-vectorization is conservative (compiler limitations)
- Explicit SIMD (simdjson approach) can achieve higher performance
- libvroom's index-based output is better suited for vroom than DuckDB's columnar output

---

#### Apache Arrow CSV Reader (2020-2024)

**Project**: [Arrow Python/C++ CSV](https://arrow.apache.org/docs/python/csv.html)

**SIMD Features**:
- Columnar reading (SIMD-friendly memory layout)
- Multi-threaded parsing
- SIMD level configuration (`ARROW_SIMD_LEVEL`)

**Performance**:
- **5x speedup** vs traditional row-based readers (2022 benchmark)
- Recent optimizations (2025): BYTE_STREAM_SPLIT, fixed-length arrays 3x faster

**Applicability**: MEDIUM
- Arrow's columnar format differs from libvroom's index-based output
- SIMD techniques applicable, but different data model

---

### 6.4 Key Takeaways from Recent Work (2020-2026)

#### ✅ Validation of simdjson Approach for CSV
- **Sep (21 GB/s)**: Proves simdjson techniques work brilliantly for CSV
- **DuckDB CSV 2.0**: Shows CSV parsing is critical for analytics databases
- **Arrow**: Columnar + SIMD is industry standard

**Conclusion**: **Our simdjson-based approach is validated by recent industry trends**

#### ⚠️ AVX-512 Caution
- **Downclocking is real**: Measure before deploying
- **AMD Zen 4 better**: Less penalty than Intel Ice Lake
- **Runtime selection essential**: Fall back to AVX2 if AVX-512 slower

#### 🚀 ARM SVE/SVE2 Opportunity
- **Growing market**: Graviton, Apple Silicon
- **No downclocking**: Unlike AVX-512
- **Use Highway**: Portable NEON/SVE support

#### 📚 No Major Research Breakthroughs Since simdjson (2019)
- **2020-2024 research**: Incremental improvements, not paradigm shifts
- **simdjson remains state-of-the-art**: No better approach published
- **Industry adoption**: Sep, DuckDB, Arrow all use similar techniques

**Should we change our approach?**: **NO**
- simdjson's two-stage SIMD parsing is still the best known technique
- Recent work validates rather than contradicts our approach
- Focus on **excellent implementation** of proven techniques, not novel algorithms

---

## 7. Updated Implementation Roadmap & Priorities

### High Priority (Unchanged)

1. **Langdale & Lemire Two-Stage SIMD** (Phase 2)
   - ✅ Validated by Sep (21 GB/s), DuckDB, Arrow
   - Expected 2-4x speedup for libvroom
   - **Status**: Confirmed as highest-priority optimization

2. **CleverCSV Dialect Detection** (Phase 3)
   - Essential for vroom integration
   - Robust real-world CSV handling
   - **Status**: Confirmed medium-high priority

### Medium Priority (Updated)

3. **AVX-512 Support** (Phase 4) - **Updated: Add runtime selection**
   - 25-40% potential speedup (if no downclocking)
   - **Mitigation**: Benchmark on target CPU, fall back to AVX2 if slower
   - **Priority**: Medium (worthwhile, but not critical given AVX2 achieves >5 GB/s)

4. **ARM NEON/SVE Support via Highway** (Phase 5)
   - ✅ Validated: ARM market growing, no downclocking issues
   - **Recommendation**: Use Highway for portability
   - **Priority**: Medium (defer until AVX2 perfect)

### Low Priority (Unchanged/Added)

5. **Mison-Style Speculation** (Future)
   - Not applicable to full CSV parsing
   - **Possible use**: Selective column parsing (low-priority feature)
   - **Status**: Defer indefinitely

6. **Sparser-Style Raw Filters** (Future Query Layer)
   - Not applicable to vroom (requires full indexing)
   - **Possible use**: Future query pushdown API
   - **Status**: Defer to post-1.0

### New Insights from Recent Work

7. **Compiler Auto-Vectorization** (DuckDB approach)
   - **Trade-off**: Easier maintenance vs lower performance ceiling
   - **Decision**: Stick with explicit SIMD for maximum performance
   - **Rationale**: libvroom targets high-performance niche (vroom integration)

8. **Branchless Design** (Sep validation)
   - ✅ Confirms: Bit manipulation > branching
   - **Action**: Aggressively eliminate branches in Phase 2
   - **Technique**: Use SIMD masks + arithmetic instead of `if` statements

---

## 8. Final Recommendations

### Should We Change Our Approach?

**NO** - Our simdjson-based approach remains optimal:

1. **Validated by Industry**: Sep (21 GB/s), DuckDB, Arrow all use similar techniques
2. **No Better Alternative**: Research 2020-2024 found no superior approach
3. **Proven Performance**: 2-5x speedup achievable with proper implementation

### Key Adjustments Based on Recent Research

1. **AVX-512**: Add with runtime selection (don't assume it's always faster)
2. **ARM**: Plan Highway-based port in Phase 5 (growing market)
3. **Benchmarking**: Continuously compare vs Sep, DuckDB, zsv
4. **Branchless**: Prioritize branch elimination (validated by Sep's success)

### Updated Success Criteria

- **AVX2**: Target **>5 GB/s** (conservative, Sep achieves 21 GB/s with AVX-512)
- **AVX-512**: Target **>8 GB/s** (if no downclocking on target hardware)
- **vs vroom**: **2-3x faster indexing** than current vroom (unchanged)
- **vs DuckDB**: Competitive or faster than DuckDB CSV reader

---

## 9. Implementation Gap Analysis (2026-01-03)

This section identifies specific techniques from the literature that are not yet fully implemented in libvroom, with links to corresponding GitHub issues for tracking.

### 9.1 High Priority Gaps

| Technique | Status | Issue | Expected Impact |
|-----------|--------|-------|-----------------|
| PCLMULQDQ Quote Mask | Not Implemented | [#39](https://github.com/jimhester/libvroom/issues/39) | 2-4x speedup in quote handling |
| Branchless State Machine | Partially Implemented | [#41](https://github.com/jimhester/libvroom/issues/41) | Eliminate 90%+ branches |

**Current Implementation Gap**: The `find_quote_mask` function uses a scalar loop instead of the PCLMULQDQ-based prefix XOR computation that simdjson and Sep use. This is a significant performance bottleneck.

### 9.2 Medium Priority Gaps

| Technique | Status | Issue | Expected Impact |
|-----------|--------|-------|-----------------|
| AVX-512 with Runtime Selection | Not Implemented | [#40](https://github.com/jimhester/libvroom/issues/40) | 25-40% speedup (on compatible hardware) |
| ARM SVE/SVE2 Optimization | Highway Only | [#42](https://github.com/jimhester/libvroom/issues/42) | 5-10 GB/s on Apple Silicon |
| SIMD Number Parsing | Not Implemented | [#44](https://github.com/jimhester/libvroom/issues/44) | 5-10x speedup in type inference |

**Current Implementation**: Using Google Highway for portable SIMD provides baseline ARM support via NEON, but explicit optimization for SVE2 and dedicated benchmarking has not been done.

### 9.3 Low Priority Gaps (Post-1.0)

| Technique | Status | Issue | Expected Impact |
|-----------|--------|-------|-----------------|
| Structure-Aware Raw Filters | Not Implemented | [#43](https://github.com/jimhester/libvroom/issues/43) | 10-22x speedup on selective queries |

### 9.4 Current Implementation Strengths

The following techniques from the literature ARE implemented:

1. **Two-Pass Algorithm** (Chang et al.): Speculative multi-threaded parsing with quote parity tracking
2. **SIMD Character Classification** (Langdale & Lemire): Using Highway for parallel character comparison
3. **CleverCSV Dialect Detection** (van den Burgh): Pattern and type scoring for delimiter/quote detection
4. **Portable SIMD via Highway**: Cross-platform support without manual intrinsics
5. **Error Collection Framework**: Three modes (STRICT, PERMISSIVE, BEST_EFFORT)

### 9.5 Recommended Implementation Order

Based on expected impact and dependencies:

1. **PCLMULQDQ Quote Mask** (#39) - Highest impact, no dependencies
2. **Branchless State Machine** (#41) - Works with or without #39
3. **AVX-512 Support** (#40) - Requires #39 and #41 for full benefit
4. **SIMD Number Parsing** (#44) - Independent, benefits vroom integration
5. **ARM Optimization** (#42) - Platform-specific, lower urgency
6. **Raw Filters** (#43) - Post-1.0, query integration feature

---

**Document Status**: ✅ Phase 1 Complete + Recent Work Review (2020-2026) + Gap Analysis
**Last Updated**: 2026-01-03
**Next Review**: After PCLMULQDQ implementation (Issue #39)
