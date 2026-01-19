#pragma once
#include <cstdint>

// 공통 열거형
enum class ReadResult {
    Success,
    NotReady,
    Overwritten
};

// 인터페이스 (추상 클래스)
template<typename T>
class IRingBuffer {
public:
    virtual ~IRingBuffer() = default;

    virtual void push(const T& item) = 0;
    virtual ReadResult try_read(uint64_t cursor, T& out_item) = 0;
    virtual uint64_t get_write_cursor() const = 0;
};