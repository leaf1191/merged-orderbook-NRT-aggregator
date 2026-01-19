#pragma once
#include <vector>
#include <mutex>
#include <stdexcept>
#include "iring_buffer.hpp"

using namespace std;

template<typename T>
class MutexRingBuffer : public IRingBuffer<T> {
private:
    struct Entry {
        T data;
    };

public:
    explicit MutexRingBuffer(size_t capacity) 
        : capacity_(capacity), mask_(capacity - 1), buffer_(capacity)
    {
        if ((capacity & (capacity - 1)) != 0) {
            throw runtime_error("Capacity must be power of 2");
        }
    }

    void push(const T& item) {
        lock_guard<std::mutex> lock(mtx_);
        
        uint64_t cursor = write_cursor_;
        uint64_t idx = cursor & mask_;
        buffer_[idx].data = item;
        write_cursor_++;
    }

    ReadResult try_read(uint64_t cursor, T& out_item) {
        lock_guard<std::mutex> lock(mtx_);
        if (cursor >= write_cursor_) ReadResult::NotReady;
        if (write_cursor_ > cursor + capacity_) ReadResult::Overwritten;

        uint64_t idx = cursor & mask_;
        out_item = buffer_[idx].data;
        
        return ReadResult::Success;
    }

    uint64_t get_write_cursor() const {
        lock_guard<std::mutex> lock(const_cast<mutex&>(mtx_));
        return write_cursor_;
    }

private:
    const size_t capacity_;
    const size_t mask_;
    vector<Entry> buffer_;
    
    uint64_t write_cursor_ = 0;
    mutable mutex mtx_;
    char pad_[64]; 
};