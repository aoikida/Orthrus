#pragma once

#include <array>
#include <atomic>
#include <cstddef>

#include "compiler.hpp"

namespace scee {

constexpr size_t LOG_QUEUE_CAPACITY = 2048;

template <typename T, size_t Capacity>
class SpscQueue {
    static_assert(Capacity > 0, "Capacity must be > 0");
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two");

public:
    bool push(T value) {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t tail = tail_.load(std::memory_order_acquire);
        if (head - tail >= Capacity) {
            return false;
        }
        buffer_[head & (Capacity - 1)] = value;
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    bool pop(T &value) {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        const size_t head = head_.load(std::memory_order_acquire);
        if (tail == head) {
            return false;
        }
        value = buffer_[tail & (Capacity - 1)];
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    bool empty() const {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        const size_t head = head_.load(std::memory_order_acquire);
        return tail == head;
    }

private:
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    std::array<T, Capacity> buffer_{};
};

using LogQueue = SpscQueue<void *, LOG_QUEUE_CAPACITY>;

extern thread_local LogQueue log_queue;

inline void log_enqueue(void *log) {
    while (!log_queue.push(log)) {
        cpu_relax();
    }
}

inline void *log_dequeue(LogQueue *q) {
    void *log;
    if (!q->pop(log)) {
        return nullptr;
    }
    return log;
}

}  // namespace scee
