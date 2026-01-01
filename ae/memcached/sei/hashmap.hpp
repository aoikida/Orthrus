#include "sei_memcached.hpp"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>

enum RetType {
    kError,
    kDeleted,
    kNotFound,
    kStored,
    kCreated,
    kEnd,
    kValue,
    kNumRetVals,
};

static const char *kRetVals[] = {"ERROR\r\n",  "DELETED\r\n", "NOT_FOUND\r\n",
                                 "STORED\r\n", "CREATED\r\n", "END\r\n",
                                 "VALUE "};
static const char kCrlf[] = "\r\n";

constexpr size_t KEY_LEN = 64;
constexpr size_t VAL_LEN = 256;

struct Key {
    char ch[KEY_LEN];
    uint32_t hash() const {
        uint32_t hash = 5381;
        for (size_t i = 0; i < KEY_LEN; i++) {
            hash = ((hash << 5) + hash) + ((uint32_t)ch[i]);
        }
        return hash;
    }
    bool operator==(const Key &other) const {
        return memcmp(ch, other.ch, KEY_LEN) == 0;
    }
    std::string to_string() const { return std::string(ch, KEY_LEN); }
};

struct Val {
    char ch[VAL_LEN];
    static const Val empty() {
        Val val;
        memset(val.ch, 0, VAL_LEN);
        return val;
    }
    bool operator==(const Val &other) const {
        return memcmp(ch, other.ch, VAL_LEN) == 0;
    }
    std::string to_string() const { return std::string(ch, VAL_LEN); }
};

struct lock_guard_t {
    pthread_mutex_t *mtx;
    lock_guard_t(pthread_mutex_t *mtx) SEI_SAFE : mtx(mtx) {
        pthread_mutex_lock(mtx);
    }
    ~lock_guard_t() SEI_SAFE { pthread_mutex_unlock(mtx); }
};

struct hashmap_t {
    struct entry_t {
        Key key;
        Val *val_ptr;
        entry_t *next;
        entry_t(Key key, Val *val, entry_t *next);
        void destroy() SEI_SAFE;
        void setv(Val val) SEI_SAFE;
        const Val *getv() SEI_SAFE;
    };
    size_t capacity;
    entry_t **buckets;
    pthread_mutex_t *locks;
    hashmap_t() { capacity = 0; }
    // make: create a hashmap instance in non-versioned memory
    static hashmap_t *make(size_t cap);
    void destroy();
    const Val *get(const Key &key) SEI_SAFE;
    RetType set(const Key &key, const Val &val) SEI_SAFE;
    RetType del(const Key &key) SEI_SAFE;
};

const Val *hashmap_get(hashmap_t *hmap, Key key) SEI_SAFE;
RetType hashmap_set(hashmap_t *hmap, Key key, Val val) SEI_SAFE;
RetType hashmap_del(hashmap_t *hmap, Key key) SEI_SAFE;
