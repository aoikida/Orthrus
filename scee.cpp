#include "scee.hpp"

#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <pthread.h>
#include <sched.h>

#include <cassert>

#include "compiler.hpp"
#include "free_log.hpp"
#include "log.hpp"
#include "profile.hpp"
#include "queue.hpp"
#include "thread.hpp"

// #define DISABLE_VALIDATION

namespace scee {

// queue.hpp
thread_local LogQueue log_queue;

// free_log.hpp
thread_local ThreadGC thread_gc_instance;
thread_local ThreadGC *app_thread_gc_instance = nullptr;
ClosureStartLog closure_start_log;

// log.hpp
SpinLock GlobalLogBufferAllocator::spin_lock;
std::queue<void *> GlobalLogBufferAllocator::free_buffers;
thread_local ThreadLogManager thread_log_manager;
thread_local LogReader log_reader;

// thread.hpp
int sampling_rate = 100, sampling_method = 1, core_id = 0;

std::atomic_size_t n_validation_core = 0;
size_t max_validation_core = 0;

namespace {

bool parse_cpuset(const char *spec, cpu_set_t *cpuset, std::string *err) {
    if (spec == nullptr || *spec == '\0') {
        if (err != nullptr) {
            *err = "empty cpuset";
        }
        return false;
    }

    CPU_ZERO(cpuset);

    const char *p = spec;
    while (*p != '\0') {
        while (*p != '\0' && (std::isspace(static_cast<unsigned char>(*p)) ||
                              *p == ',')) {
            ++p;
        }
        if (*p == '\0') break;

        errno = 0;
        char *end = nullptr;
        const long start = std::strtol(p, &end, 10);
        if (end == p || errno != 0) {
            if (err != nullptr) {
                *err = std::string("failed to parse cpuset near: ") + p;
            }
            return false;
        }
        p = end;

        long finish = start;
        if (*p == '-') {
            ++p;
            errno = 0;
            end = nullptr;
            finish = std::strtol(p, &end, 10);
            if (end == p || errno != 0) {
                if (err != nullptr) {
                    *err = std::string("failed to parse cpuset range near: ") +
                           p;
                }
                return false;
            }
            p = end;
        }

        if (start < 0 || finish < 0 || start > finish) {
            if (err != nullptr) {
                *err = "invalid cpuset range";
            }
            return false;
        }
        if (finish >= CPU_SETSIZE) {
            if (err != nullptr) {
                *err = "cpuset exceeds CPU_SETSIZE";
            }
            return false;
        }
        for (long cpu = start; cpu <= finish; ++cpu) {
            CPU_SET(static_cast<int>(cpu), cpuset);
        }
    }

    return true;
}

void maybe_set_thread_affinity(pthread_t thread, const char *env_key) {
    const char *spec = std::getenv(env_key);
    if (spec == nullptr || *spec == '\0') {
        return;
    }

    cpu_set_t cpuset;
    std::string err;
    if (!parse_cpuset(spec, &cpuset, &err)) {
        std::fprintf(stderr, "Invalid %s='%s': %s\n", env_key, spec,
                     err.c_str());
        std::abort();
    }

    const int r = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    if (r != 0) {
        std::fprintf(stderr, "Failed to set affinity (%s='%s'): %d\n", env_key,
                     spec, r);
        std::abort();
    }
}

}  // namespace

// scee.hpp
void validate_one(LogHead *log) {
    bool do_validation = true;
#ifndef SCEE_SYNC_VALIDATE
    if (sampling_rate < 100) {
        if (sampling_method == 1) {
            do_validation = rand() % 100 < sampling_rate;
        } else {
            // NOT IMPLEMENTED
            assert(false);
        }
    }
#endif
#ifdef DISABLE_VALIDATION
    do_validation = false;
#endif
    auto validate = [&] {
        log_reader.open(log);
        reset_bulk_buffer();
        const auto *validable = log_reader.peek<Validable>();
        validable->validate(&log_reader);
        log_reader.close();
    };

    if (do_validation) {
        if (max_validation_core != 0) {
#ifdef SCEE_SYNC_VALIDATE
            while (true) {
                size_t current =
                    n_validation_core.load(std::memory_order_relaxed);
                if (current >= max_validation_core) {
                    n_validation_core.wait(current, std::memory_order_relaxed);
                    continue;
                }
                if (n_validation_core.compare_exchange_strong(
                        current, current + 1, std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    break;
                }
            }
            validate();
            n_validation_core.fetch_sub(1, std::memory_order_relaxed);
            n_validation_core.notify_one();
#else
            if (n_validation_core.fetch_add(1, std::memory_order_relaxed) <
                max_validation_core) {
                validate();
            } else {
                reclaim_log(log);
            }
            n_validation_core.fetch_sub(1, std::memory_order_relaxed);
#endif
        } else {
            validate();
        }
    } else {
        reclaim_log(log);
    }
}

// thread.hpp
thread_local Thread validator_thread;
thread_local std::atomic<bool> stop_validation;

// memmgr.hpp
thread_local void *bulk_buffer = nullptr;
thread_local size_t bulk_cursor = BULK_BUFFER_SIZE;

void validate(LogQueue *queue, std::atomic<bool> &stop, ThreadGC *thread_gc) {
    maybe_set_thread_affinity(pthread_self(), "SCEE_VALIDATION_CPUSET");
    app_thread_gc_instance = thread_gc;
    while (!stop) {
        while (queue->empty() && !stop) {
            cpu_relax();
        }
        const uint64_t start = rdtsc();
        size_t validation_count = 0;
        while (true) {
            auto *log = static_cast<LogHead *>(log_dequeue(queue));
            if (log == nullptr) {
                break;
            }
            validate_one(log);
            validation_count++;
        }
        const uint64_t end = rdtsc();
        if (validation_count > 0) {
            profile::record_validation_cpu_time(end - start, validation_count);
        }
    }
}

void AppThread::register_queue() {
    maybe_set_thread_affinity(pthread_self(), "SCEE_WORK_CPUSET");
    stop_validation = false;
    LogQueue *queue = &log_queue;
#ifndef DISABLE_SCEE
    validator_thread =
        Thread(validate, queue, std::ref(stop_validation), &thread_gc_instance);
#endif
}

void AppThread::unregister_queue() {
    stop_validation = true;
#ifndef DISABLE_SCEE
    validator_thread.join();
#endif
}

// scheduler.hpp

}  // namespace scee
