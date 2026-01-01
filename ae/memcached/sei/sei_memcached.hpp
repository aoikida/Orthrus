#pragma once

#ifdef __cplusplus
extern "C" {
#endif
#include <sei/tmi.h>
#include <sei/compat.h>
#include <sei/crc.h>
#ifdef __cplusplus
}
#endif

/* Ensure pthread mutex APIs are transaction-safe-callable from __transaction_atomic.
 * We mask system declarations first to avoid conflicting prototypes. */
#define pthread_mutex_lock __system_pthread_mutex_lock
#define pthread_mutex_unlock __system_pthread_mutex_unlock
#define pthread_mutex_trylock __system_pthread_mutex_trylock
#include <pthread.h>
#undef pthread_mutex_lock
#undef pthread_mutex_unlock
#undef pthread_mutex_trylock

extern "C" int pthread_mutex_lock(pthread_mutex_t *mutex) SEI_SAFE;
extern "C" int pthread_mutex_unlock(pthread_mutex_t *mutex) SEI_SAFE;
extern "C" int pthread_mutex_trylock(pthread_mutex_t *mutex) SEI_SAFE;

