#pragma once

#include <vector>
#include <tuple>
#include <string>
#include <cstdint>

using namespace std;

// Aggregator가 최종 발행(Publish)할 글로벌 스냅샷
struct MergedOrderBook {
    uint64_t window_start_time;
    vector<uint64_t> fresh_updates_ts; // 각 거래소의 최신 스냅샷 event_ts
    // [ price, size, exchange ]
    vector<tuple<double, double, string>> global_bids;
    vector<tuple<double, double, string>> global_asks;
};