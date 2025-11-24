#include "clean_data.hpp"
#include "orderbook.pb.h"
#include <iostream>

using namespace std;

CleanData CleanData::from_proto(const string& binary_data) {
    CleanData data;
    orderbook_pb::Delta proto_delta;

    if (!proto_delta.ParseFromString(binary_data)) {
        cerr << "❌ Protobuf Decoding Failed!" << endl;
        data.exchange = "unknown"; 
        return data;
    }
    data.exchange = proto_delta.exchange();
    data.symbol = proto_delta.symbol();
    data.ts_event = proto_delta.event_ts_ms(); 
    for (const auto& level : proto_delta.bids()) {
        data.bids.emplace_back(level.price(), level.quantity());
    }
    for (const auto& level : proto_delta.asks()) {
        data.asks.emplace_back(level.price(), level.quantity());
    }
    return data;
}