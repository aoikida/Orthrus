#pragma once

#include <x86intrin.h>

#include <iostream>
#include <type_traits>
#include <utility>

#include "log.hpp"
#include "profile.hpp"

namespace scee {

struct Validable {
    virtual void validate(LogReader *) const = 0;
};

template <typename Ret, typename... Args>
struct Closure : public Validable {
    using Fn = Ret (*)(Args...);

    Fn fn;
    std::tuple<Args...> args;

    explicit Closure(Fn fn, Args &&...args)
        : fn(std::move(fn)), args(std::move(args)...) {}

    auto run() const { return std::apply(fn, args); }

    auto run_with_fn(Fn fn) const { return std::apply(fn, args); }

    void validate(LogReader *reader) const override {
        reader->template skip<sizeof(*this)>();
        if constexpr (std::is_void_v<Ret>) {
            run();
        } else {
            auto ret = run();
            reader->cmp_log_typed(ret);
        }
    }
};

template <typename Ret, typename... Args>
Ret run(Ret (*fn)(Args...), Args... args) {
    static_assert(std::is_trivial_v<Ret>);
#ifdef SCEE_SYNC_VALIDATE
    std::atomic<uint32_t> validation_ticket{0};
#endif
    new_log();
    const auto *func =
        append_log_typed(Closure(fn, std::forward<Args>(args)...));
    Ret ret = func->run();
    append_log_typed(ret);
#ifdef SCEE_SYNC_VALIDATE
    commit_log(&validation_ticket);
    validation_ticket.wait(0, std::memory_order_acquire);
#else
    commit_log();
#endif
    return ret;
}

template <typename Ret, typename... Args>
Ret run2(Ret (*app_fn)(Args...), Ret (*val_fn)(Args...), Args... args) {
    static_assert(std::is_void_v<Ret> ||
                  (std::is_trivially_copyable_v<Ret> &&
                   std::is_trivially_destructible_v<Ret>));
#ifdef SCEE_SYNC_VALIDATE
    std::atomic<uint32_t> validation_ticket{0};
#endif
    new_log();
    const auto *func =
        append_log_typed(Closure(val_fn, std::forward<Args>(args)...));
    if constexpr (std::is_void_v<Ret>) {
        func->run_with_fn(app_fn);
#ifdef SCEE_SYNC_VALIDATE
        commit_log(&validation_ticket);
        validation_ticket.wait(0, std::memory_order_acquire);
#else
        commit_log();
#endif
    } else {
        Ret ret = func->run_with_fn(app_fn);
        append_log_typed(ret);
#ifdef SCEE_SYNC_VALIDATE
        commit_log(&validation_ticket);
        validation_ticket.wait(0, std::memory_order_acquire);
#else
        commit_log();
#endif
        return ret;
    }
    // commit_log();
    // return app_fn(std::forward<Args>(args)...);
}

template <typename Ret, typename... Args>
Ret run2_profile(uint64_t &cycles, Ret (*app_fn)(Args...),
                 Ret (*val_fn)(Args...), Args... args) {
    static_assert(std::is_void_v<Ret> ||
                  (std::is_trivially_copyable_v<Ret> &&
                   std::is_trivially_destructible_v<Ret>));
#ifdef SCEE_SYNC_VALIDATE
    std::atomic<uint32_t> validation_ticket{0};
#endif
    new_log();
    const auto *func =
        append_log_typed(Closure(val_fn, std::forward<Args>(args)...));
    if constexpr (std::is_void_v<Ret>) {
        uint64_t start = _rdtsc();
        func->run_with_fn(app_fn);
        cycles = _rdtsc() - start;
#ifdef SCEE_SYNC_VALIDATE
        commit_log(&validation_ticket);
        validation_ticket.wait(0, std::memory_order_acquire);
#else
        commit_log();
#endif
    } else {
        uint64_t start = _rdtsc();
        Ret ret = func->run_with_fn(app_fn);
        cycles = _rdtsc() - start;
        append_log_typed(ret);
#ifdef SCEE_SYNC_VALIDATE
        commit_log(&validation_ticket);
        validation_ticket.wait(0, std::memory_order_acquire);
#else
        commit_log();
#endif
        return ret;
    }
}

void validate_one(LogHead *log);

extern size_t max_validation_core;
#define limvc(n) scee::max_validation_core = n;

}  // namespace scee

// #define DISABLE_SCEE

#ifdef DISABLE_SCEE
#define __scee_run(fn, args...) (raw::fn(args))
#else
#define __scee_run(fn, args...) (scee::run2(app::fn, validator::fn, args))
#endif
