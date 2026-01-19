#pragma once

#include <vector>
#include <cstdint>
#include <algorithm>
#include <cmath>
#include <numeric>

using namespace std;

// O(1) Record
class LatencyHistogram {
public:
    const size_t max_val_; // ms 혹은 us 단위
    vector<uint32_t> buckets;
    uint32_t overflow_count = 0;
    uint32_t total_count = 0;

    explicit LatencyHistogram(size_t max_val) 
        : max_val_(max_val), buckets(max_val + 1, 0) {}

    void record(uint64_t val) {
        if (val > max_val_) {
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

        for (size_t i = 0; i <= max_val_; ++i) {
            accumulated += buckets[i];
            if (accumulated >= target) return i;
        }
        return max_val_ + 1; 
    }

    pair<double, double> get_stats() {
        if (total_count == 0) return {0.0, 0.0};

        double sum = 0.0;
        double sum_sq = 0.0; // 제곱의 합

        for (size_t i = 0; i <= max_val_; ++i) {
            if (buckets[i] > 0) {
                double val = (double)i;
                double count = (double)buckets[i];
                
                sum += val * count;
                sum_sq += (val * val) * count;
            }
        }

        double mean = sum / total_count;
        double variance = (sum_sq / total_count) - (mean * mean);
        double std_dev = (variance > 0) ? sqrt(variance) : 0.0;

        return {mean, std_dev};
    }
};

struct AggregatorMetrics {
    LatencyHistogram cycle_time_hist{50000}; // Capacity 튜닝용, 50ms 까지 측정 (us 단위)
    LatencyHistogram skew_hist{500};       // Watermark 튜닝용, 500ms 까지 측정

    uint64_t total_windows = 0;
    uint64_t incomplete_windows = 0; // Window Size 튜닝용
    uint64_t report_counter = 0;
    uint64_t jumped_count = 0;       // Capacity 초과로 인한 강제 점프 (처리량 부족)

    void reset() {
        cycle_time_hist.reset();
        skew_hist.reset();
        total_windows = 0;
        incomplete_windows = 0;
        report_counter = 0;
        jumped_count = 0;
    }
};