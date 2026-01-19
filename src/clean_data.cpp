#include "clean_data.hpp"
#include "orderbook.pb.h"
#include <iostream>

using namespace std;

CleanData CleanData::from_proto(const string& binary_data) {
    CleanData data;
    orderbook::OrderbookDelta proto_delta;

    if (!proto_delta.ParseFromString(binary_data)) {
        cerr << "Protobuf Decoding Failed!" << endl;
        data.exchange = "unknown"; 
        return data;
    }
    data.exchange = proto_delta.exchange();
    data.symbol = proto_delta.symbol();
    data.ts_event = static_cast<uint64_t>(proto_delta.event_ts() * 1000.0);
    data.bids.reserve(proto_delta.bids_size());
    for (const auto& level : proto_delta.bids()) {
        data.bids.emplace_back(level.price(), level.quantity());
    }

    data.asks.reserve(proto_delta.asks_size());
    for (const auto& level : proto_delta.asks()) {
        data.asks.emplace_back(level.price(), level.quantity());
    }
    return data;
}