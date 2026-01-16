/**
 * @file debug.h
 * @brief Debug mode framework for the libvroom CSV parser.
 */

#ifndef LIBVROOM_DEBUG_H
#define LIBVROOM_DEBUG_H

#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <cstring>

namespace libvroom {

struct DebugConfig {
    bool verbose = false;
    bool dump_masks = false;
    bool timing = false;
    size_t dump_context_bytes = 64;
    size_t max_indexes_dump = 100;
    FILE* output = nullptr;

    DebugConfig() = default;

    static DebugConfig all() {
        DebugConfig config;
        config.verbose = true;
        config.dump_masks = true;
        config.timing = true;
        return config;
    }

    bool enabled() const {
        return verbose || dump_masks || timing;
    }
};

struct PhaseTime {
    std::string name;
    std::chrono::nanoseconds duration;
    size_t bytes_processed = 0;

    double seconds() const {
        return duration.count() / 1e9;
    }

    double throughput_gbps() const {
        if (bytes_processed == 0 || duration.count() == 0) return 0.0;
        return (bytes_processed / 1e9) / seconds();
    }
};

/**
 * @class DebugTrace
 * @brief Provides debug logging, timing, and mask dumping facilities.
 *
 * @note Thread Safety: This class is NOT thread-safe. All methods should be
 *       called from a single thread (typically the main thread). When using
 *       multi-threaded parsing, ensure debug output calls are synchronized
 *       or made only from the main thread after parsing completes.
 */
class DebugTrace {
public:
    explicit DebugTrace(const DebugConfig& config = DebugConfig())
        : config_(config) {}

    bool enabled() const { return config_.enabled(); }
    bool verbose() const { return config_.verbose; }
    bool dump_masks() const { return config_.dump_masks; }
    bool timing() const { return config_.timing; }

    // Note: The format attribute uses index 2 for fmt because 'this' is implicit parameter 1
    #if defined(__GNUC__) || defined(__clang__)
    __attribute__((format(printf, 2, 3)))
    #endif
    void log(const char* fmt, ...) const {
        if (!config_.verbose) return;
        FILE* out = config_.output ? config_.output : stdout;
        fprintf(out, "[libvroom] ");
        va_list args;
        va_start(args, fmt);
        vfprintf(out, fmt, args);
        va_end(args);
        fprintf(out, "\n");
        fflush(out);
    }

    // Safe string logging without format string interpretation.
    // Use this when logging user-provided or untrusted strings.
    void log_str(const char* msg) const {
        if (!config_.verbose) return;
        FILE* out = config_.output ? config_.output : stdout;
        fprintf(out, "[libvroom] %s\n", msg);
        fflush(out);
    }

    void log_decision(const char* decision, const char* reason) const {
        if (!config_.verbose) return;
        FILE* out = config_.output ? config_.output : stdout;
        fprintf(out, "[libvroom] DECISION: %s | Reason: %s\n", decision, reason);
        fflush(out);
    }

    void log_simd_path(const char* path_name, size_t lanes) const {
        if (!config_.verbose) return;
        FILE* out = config_.output ? config_.output : stdout;
        fprintf(out, "[libvroom] SIMD: Using %s path (vector width: %zu bytes)\n",
                path_name, lanes);
        fflush(out);
    }

    void log_threading(size_t n_threads, size_t chunk_size) const {
        if (!config_.verbose) return;
        FILE* out = config_.output ? config_.output : stdout;
        fprintf(out, "[libvroom] Threading: %zu threads, chunk size %zu bytes\n",
                n_threads, chunk_size);
        fflush(out);
    }

    void dump_mask(const char* name, uint64_t mask, size_t offset = 0) const {
        if (!config_.dump_masks) return;
        FILE* out = config_.output ? config_.output : stdout;
        fprintf(out, "[libvroom] MASK %s @ offset %zu:\n", name, offset);
        fprintf(out, "  hex: 0x%016llx\n", (unsigned long long)mask);
        fprintf(out, "  bin: ");
        for (int i = 63; i >= 0; --i) {
            fprintf(out, "%c", (mask & (1ULL << i)) ? '1' : '0');
            if (i % 8 == 0 && i > 0) fprintf(out, " ");
        }
        fprintf(out, "\n");
        fflush(out);
    }

    void dump_buffer(const char* name, const uint8_t* buf, size_t len, size_t offset = 0) const {
        if (!config_.dump_masks) return;
        FILE* out = config_.output ? config_.output : stdout;
        size_t dump_len = (len < config_.dump_context_bytes) ? len : config_.dump_context_bytes;
        fprintf(out, "[libvroom] BUFFER %s @ offset %zu (showing %zu of %zu bytes):\n",
                name, offset, dump_len, len);
        fprintf(out, "  hex: ");
        for (size_t i = 0; i < dump_len; ++i) {
            fprintf(out, "%02x ", buf[i]);
            if ((i + 1) % 16 == 0 && i + 1 < dump_len) {
                fprintf(out, "\n       ");
            }
        }
        fprintf(out, "\n");
        fflush(out);
    }

    // Note: total_size is required to prevent out-of-bounds access.
    // For strided access, pass the total capacity of the indexes array.
    void dump_indexes(const uint64_t* indexes, size_t count, size_t thread_id,
                      size_t stride, size_t total_size) const {
        if (!config_.dump_masks) return;
        FILE* out = config_.output ? config_.output : stdout;
        size_t dump_count = (count < config_.max_indexes_dump) ? count : config_.max_indexes_dump;
        fprintf(out, "[libvroom] INDEXES thread %zu (showing %zu of %zu):\n",
                thread_id, dump_count, count);
        fprintf(out, "  ");
        for (size_t i = 0; i < dump_count; ++i) {
            size_t idx = thread_id + i * stride;
            // Bounds check: skip if index would exceed total array size
            if (idx >= total_size) break;
            fprintf(out, "%llu", (unsigned long long)indexes[idx]);
            if (i + 1 < dump_count) fprintf(out, ", ");
            if ((i + 1) % 10 == 0 && i + 1 < dump_count) fprintf(out, "\n  ");
        }
        fprintf(out, "\n");
        fflush(out);
    }

    void dump_chunk_boundaries(const std::vector<uint64_t>& chunk_pos, size_t n_threads) const {
        if (!config_.verbose) return;
        FILE* out = config_.output ? config_.output : stdout;
        fprintf(out, "[libvroom] CHUNK BOUNDARIES:\n");
        for (size_t i = 0; i <= n_threads; ++i) {
            fprintf(out, "  chunk[%zu]: %llu", i, (unsigned long long)chunk_pos[i]);
            if (i > 0) {
                fprintf(out, " (size: %llu bytes)",
                        (unsigned long long)(chunk_pos[i] - chunk_pos[i-1]));
            }
            fprintf(out, "\n");
        }
        fflush(out);
    }

    void start_phase(const char* phase_name) {
        if (!config_.timing) return;
        current_phase_ = phase_name;
        phase_start_ = std::chrono::high_resolution_clock::now();
    }

    void end_phase(size_t bytes_processed = 0) {
        if (!config_.timing) return;
        auto end = std::chrono::high_resolution_clock::now();
        PhaseTime pt;
        pt.name = current_phase_;
        pt.duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - phase_start_);
        pt.bytes_processed = bytes_processed;
        phase_times_.push_back(pt);
    }

    void print_timing_summary() const {
        if (!config_.timing || phase_times_.empty()) return;
        FILE* out = config_.output ? config_.output : stdout;
        fprintf(out, "\n[libvroom] TIMING SUMMARY:\n");
        fprintf(out, "  %-30s %12s %12s %12s\n",
                "Phase", "Time (ms)", "Bytes", "Throughput");
        fprintf(out, "  %s\n", std::string(70, '-').c_str());

        std::chrono::nanoseconds total_time{0};
        size_t total_bytes = 0;

        for (const auto& pt : phase_times_) {
            double ms = pt.duration.count() / 1e6;
            fprintf(out, "  %-30s %12.3f %12zu",
                    pt.name.c_str(), ms, pt.bytes_processed);
            if (pt.bytes_processed > 0) {
                fprintf(out, " %9.2f GB/s", pt.throughput_gbps());
            }
            fprintf(out, "\n");
            total_time += pt.duration;
            total_bytes += pt.bytes_processed;
        }

        fprintf(out, "  %s\n", std::string(70, '-').c_str());
        double total_ms = total_time.count() / 1e6;
        fprintf(out, "  %-30s %12.3f %12zu", "TOTAL", total_ms, total_bytes);
        if (total_bytes > 0 && total_time.count() > 0) {
            double gbps = (total_bytes / 1e9) / (total_time.count() / 1e9);
            fprintf(out, " %9.2f GB/s", gbps);
        }
        fprintf(out, "\n\n");
        fflush(out);
    }

    const std::vector<PhaseTime>& get_phase_times() const {
        return phase_times_;
    }

    void clear_timing() {
        phase_times_.clear();
    }

    void log_first_pass_result(size_t chunk_id, size_t n_quotes,
                               uint64_t first_even_nl, uint64_t first_odd_nl) const {
        if (!config_.verbose) return;
        FILE* out = config_.output ? config_.output : stdout;
        fprintf(out, "[libvroom] FIRST PASS chunk %zu: quotes=%zu, first_even_nl=%llu, first_odd_nl=%llu\n",
                chunk_id, n_quotes,
                (unsigned long long)first_even_nl,
                (unsigned long long)first_odd_nl);
        fflush(out);
    }

    void log_dialect(char delimiter, char quote_char, double confidence) const {
        if (!config_.verbose) return;
        FILE* out = config_.output ? config_.output : stdout;
        char delim_str[8], quote_str[8];
        format_char(delimiter, delim_str, sizeof(delim_str));
        format_char(quote_char, quote_str, sizeof(quote_str));
        fprintf(out, "[libvroom] DIALECT: delimiter='%s', quote='%s', confidence=%.2f%%\n",
                delim_str, quote_str, confidence * 100);
        fflush(out);
    }

    void log_state_transition(const char* from_state, const char* to_state,
                              char trigger, size_t pos) const {
        if (!config_.verbose) return;
        FILE* out = config_.output ? config_.output : stdout;
        char trigger_str[8];
        if (trigger == '\n') std::strcpy(trigger_str, "\\n");
        else if (trigger == '\r') std::strcpy(trigger_str, "\\r");
        else if (trigger == '\t') std::strcpy(trigger_str, "\\t");
        else if (trigger >= 32 && trigger < 127) {
            trigger_str[0] = trigger;
            trigger_str[1] = '\0';
        } else {
            snprintf(trigger_str, sizeof(trigger_str), "\\x%02x", (unsigned char)trigger);
        }
        fprintf(out, "[libvroom] STATE @ %zu: %s -> %s (trigger: '%s')\n",
                pos, from_state, to_state, trigger_str);
        fflush(out);
    }

private:
    DebugConfig config_;
    std::string current_phase_;
    std::chrono::high_resolution_clock::time_point phase_start_;
    std::vector<PhaseTime> phase_times_;

    static void format_char(char c, char* buf, size_t buf_size) {
        if (c == '\t') snprintf(buf, buf_size, "\\t");
        else if (c == '\n') snprintf(buf, buf_size, "\\n");
        else if (c == '\r') snprintf(buf, buf_size, "\\r");
        else if (c >= 32 && c < 127) snprintf(buf, buf_size, "%c", c);
        else snprintf(buf, buf_size, "\\x%02x", (unsigned char)c);
    }
};

class ScopedPhaseTimer {
public:
    ScopedPhaseTimer(DebugTrace& trace, const char* phase_name, size_t bytes = 0)
        : trace_(trace), bytes_(bytes) {
        trace_.start_phase(phase_name);
    }

    ~ScopedPhaseTimer() {
        trace_.end_phase(bytes_);
    }

    void set_bytes(size_t bytes) { bytes_ = bytes; }

private:
    DebugTrace& trace_;
    size_t bytes_;
};

#define LIBVROOM_TIMED_PHASE(trace, name, bytes) \
    libvroom::ScopedPhaseTimer _phase_timer_##__LINE__(trace, name, bytes)

namespace debug {

inline DebugConfig& global_config() {
    static DebugConfig config;
    return config;
}

inline void set_config(const DebugConfig& config) {
    global_config() = config;
}

inline DebugTrace& global_trace() {
    static DebugTrace trace(global_config());
    return trace;
}

inline bool enabled() {
    return global_config().enabled();
}

}  // namespace debug

}  // namespace libvroom

#endif  // LIBVROOM_DEBUG_H
