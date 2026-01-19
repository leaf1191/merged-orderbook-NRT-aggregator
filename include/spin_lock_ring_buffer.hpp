#pragma once
#include <vector>
#include <atomic>
#include <stdexcept>
#include <immintrin.h> // _mm_pause
#include "iring_buffer.hpp"

using namespace std;

// 간단한 스핀락 클래스 (TAS + Exponential Backoff)
class SpinLock {
    std::atomic_flag flag_ = ATOMIC_FLAG_INIT;

public:
    void lock() {
        int backoff = 1;
        while (flag_.test_and_set(std::memory_order_acquire)) {
            for (int i = 0; i < backoff; ++i) {
                _mm_pause();
            }
            // 백오프 주기 증가 (최대치 제한 필요하지만 여기선 간단히)
            if (backoff < 1024) backoff <<= 1;
        }
    }

    void unlock() {
        flag_.clear(std::memory_order_release);
    }
};

template<typename T>
class SpinLockRingBuffer : public IRingBuffer<T>{
private:
    struct Entry {
        T data;
    };

public:
    explicit SpinLockRingBuffer(size_t capacity) 
        : capacity_(capacity), mask_(capacity - 1), buffer_(capacity)
    {
        if ((capacity & (capacity - 1)) != 0) {
            throw runtime_error("Capacity must be power of 2");
        }
    }

    void push(const T& item) {
        spinlock_.lock();

        uint64_t cursor = write_cursor_;
        uint64_t idx = cursor & mask_;
        
        buffer_[idx].data = item;
        write_cursor_++;

        spinlock_.unlock();
    }

    ReadResult try_read(uint64_t cursor, T& out_item) {
        spinlock_.lock();

        if (cursor >= write_cursor_) {
            spinlock_.unlock();
            return ReadResult::NotReady;
        }

        if (write_cursor_ > cursor + capacity_) {
            spinlock_.unlock();
            return ReadResult::Overwritten;
        }

        uint64_t idx = cursor & mask_;
        out_item = buffer_[idx].data;

        spinlock_.unlock();
        return ReadResult::Success;
    }

    uint64_t get_write_cursor() const {
        return std::atomic_load_explicit(&write_cursor_, std::memory_order_relaxed);
    }

private:
    const size_t capacity_;
    const size_t mask_;
    vector<Entry> buffer_;

    alignas(64) atomic<uint64_t> write_cursor_{0};
    mutable SpinLock spinlock_;
};