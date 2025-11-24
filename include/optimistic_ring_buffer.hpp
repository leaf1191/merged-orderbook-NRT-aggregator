#pragma once

#include <vector>
#include <atomic>
#include <stdexcept>
#include <thread> // atomic_thread_fence

using namespace std;

enum class ReadResult {
    Success,
    NotReady,    // Writer가 아직 안 씀 (기다려야 함)
    Overwritten  // Writer가 한 바퀴 이상 돎 (데이터 유실 -> 점프해야 함)
};

template <typename T>
struct alignas(64) Aligned {
    T value;
};

// NRT MPSC Lock-Free Ring Buffer (LMAX Style + Seqlock)
template<typename T>
class OptimisticRingBuffer {
private:
    struct Entry {
        atomic<uint64_t> version;
        T data;
    };

public:
    // 넉넉한 용량 (예: 65536) 필수!
    explicit OptimisticRingBuffer(size_t capacity) 
        : capacity_(capacity), mask_(capacity - 1), buffer_(capacity)
    {
        if ((capacity & (capacity - 1)) != 0) {
            throw runtime_error("Capacity must be power of 2");
        }

        for (size_t i = 0; i < capacity; ++i) {
            buffer_[i].version.store(0, memory_order_relaxed);
        }
        write_cursor_.value.store(0, memory_order_relaxed);
    }

    void push(const T& item) {
        uint64_t cursor = write_cursor_.value.fetch_add(1, memory_order_relaxed);
        uint64_t idx = cursor & mask_;
        Entry& slot = buffer_[idx];

        uint64_t target_version = (cursor + 1) << 1; 
        uint64_t current_version = slot.version.load(memory_order_acquire);
        
        if (current_version >= target_version) {
            return; // 이미 미래의 Writer가 덮어씀 (Drop)
        }
        
        slot.version.store(target_version - 1, memory_order_release); // 마킹 홀수 (쓰기)
        slot.data = item;
        slot.version.store(target_version, memory_order_release); // 마킹 짝수 (완료)
    }

    ReadResult try_read(uint64_t cursor, T& out_item) {
        uint64_t idx = cursor & mask_;
        Entry& slot = buffer_[idx];
        uint64_t expected_version = (cursor + 1) << 1;

        uint64_t ver_before = slot.version.load(memory_order_acquire);
        if (ver_before < expected_version) return ReadResult::NotReady;
        if (ver_before > expected_version) return ReadResult::Overwritten;
        if (ver_before & 1) return ReadResult::NotReady; 

        out_item = slot.data;
        atomic_thread_fence(memory_order_acquire);
        
        uint64_t ver_after = slot.version.load(memory_order_relaxed);
        if (ver_before != ver_after) return ReadResult::Overwritten;

        return ReadResult::Success;
    }

    uint64_t get_write_cursor() const {
        return write_cursor_.value.load(memory_order_acquire);
    }

private:
    const size_t capacity_;
    const size_t mask_;
    alignas(64) vector<Entry> buffer_;
    Aligned<atomic<uint64_t>> write_cursor_;
};