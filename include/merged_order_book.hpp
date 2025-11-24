#pragma once

#include <vector>
#include <tuple>
#include <string>
#include <cstdint>

using namespace std;

// Aggregator가 최종 발행(Publish)할 글로벌 스냅샷
struct MergedOrderBook {
    uint64_t window_start_time;
    // [ price, size, exchange ]
    vector<tuple<double, double, string>> global_bids;
    vector<tuple<double, double, string>> global_asks;
};