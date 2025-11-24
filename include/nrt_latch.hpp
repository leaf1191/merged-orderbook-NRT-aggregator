#pragma once

#include <memory>
#include <mutex>

using namespace std;

// NRT 래치 (Processor -> Publisher)
// Snapshot Overwrite 방식
template<typename T>
class NrtLatch {
public:
    NrtLatch() : data_ptr_(nullptr) {}

    // Processor (Producer)
    void store(shared_ptr<T> new_ptr) {
        std::lock_guard<std::mutex> lock(mtx_);
        data_ptr_ = new_ptr;
    }

    // Publisher (Consumer)
    shared_ptr<T> load() {
        std::lock_guard<std::mutex> lock(mtx_);
        return data_ptr_;
    }

private:
    std::shared_ptr<T> data_ptr_;
    mutable std::mutex mtx_;
};