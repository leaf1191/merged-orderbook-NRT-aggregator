#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <utility> // for std::pair

using namespace std;

struct CleanData {
    string exchange;
    string symbol;
    uint64_t ts_event;

    // vector를 통해 메모리 연속성 보장 (최적화)
    vector<pair<double, double>> bids;
    vector<pair<double, double>> asks;

    // proto 파싱 함수 선언 (구현은 cpp로 이동)
    static CleanData from_proto(const string& binary_data);
};