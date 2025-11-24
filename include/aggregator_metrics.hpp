#pragma once

#include <vector>
#include <cstdint>
#include <algorithm>
#include <numeric>

using namespace std;

// 0us ~ 500ms 측정 (O(1) Record)
class LatencyHistogram {
public:
    static const size_t MAX_VAL = 500000; // 500ms (us단위)
    vector<uint32_t> buckets;
    uint32_t overflow_count = 0;
    uint32_t total_count = 0;

    LatencyHistogram() : buckets(MAX_VAL + 1, 0) {}

    void record(uint64_t val) {
        if (val > MAX_VAL) {
            overflow_count++;
        } else {
            buckets[val]++;
        }
        total_count++;
    }

    void reset() {
        fill(buckets.begin(), buckets.end(), 0);
        overflow_count = 0;
        total_count = 0;
    }

    uint64_t get_percentile(double p) {
        if (total_count == 0) return 0;
        uint32_t target = (uint32_t)(total_count * p);
        uint32_t accumulated = 0;

        for (size_t i = 0; i <= MAX_VAL; ++i) {
            accumulated += buckets[i];
            if (accumulated >= target) return i;
        }
        return MAX_VAL + 1; 
    }
};

struct AggregatorMetrics {
    LatencyHistogram cycle_time_hist; // Capacity 튜닝용
    LatencyHistogram skew_hist;       // Watermark 튜닝용

    uint64_t total_windows = 0;
    uint64_t incomplete_windows = 0; // Window Size 튜닝용
    uint64_t report_counter = 0;

    void reset() {
        cycle_time_hist.reset();
        skew_hist.reset();
        total_windows = 0;
        incomplete_windows = 0;
        report_counter = 0;
    }
};