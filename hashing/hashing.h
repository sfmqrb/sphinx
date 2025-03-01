#pragma once

#include "../bitset_wrapper/bitset_wrapper.h"
#include "../config/config.h"
#include "../lib/xxhash/xxhash64.h"

template <typename Traits = DefaultTraits>
class Hashing {
   public:
    static BitsetWrapper<FINGERPRINT_SIZE> hash_digest(typename Traits::KEY_TYPE key) {
        if constexpr (Traits::USE_XXHASH) {
            uint64_t hash_value1 = XXHash64::hash(&key, sizeof(key), 0);
            uint64_t hash_value2 = XXHash64::hash(&key, sizeof(key), 1);
            BitsetWrapper<FINGERPRINT_SIZE> fingerprint;
            fingerprint.bitset[0] = hash_value1;
            fingerprint.bitset[1] = hash_value2;
            return fingerprint;
        } else {
            auto fingerprint = BitsetWrapper<FINGERPRINT_SIZE>();
            fingerprint.setInputInt64(key);
            return fingerprint;
        }
    }
};
