#include <iostream>
#include <cmath>
#include <memory>
#include <vector>
#include <optional>
#include <stdexcept>
#include <atomic>
#include <functional>
#include <optional>
#include <mutex> // Include the mutex header
#include "../bitset_wrapper/bitset_wrapper.h"
#include "../lib/xxhash/xxhash64.h"
#include "../config/config.h"

template <typename Traits = DefaultTraits>
class LinearProbingHashTable {
public:
    using KEY_TYPE = typename Traits::PAYLOAD_TYPE;
    using ORG_KEY_TYPE = typename Traits::KEY_TYPE;
    using VALUE_TYPE = typename Traits::VALUE_TYPE;
    using INDEX_TYPE = size_t;

    std::atomic<size_t> cacheHitCount{0};
    std::atomic<size_t> totalQueryCount{0};
    std::atomic<size_t> size_;
    std::atomic<size_t> clock_hand_; // For clock eviction policy
    size_t num_locks_;
    struct Entry {
        KEY_TYPE key;
        ORG_KEY_TYPE org_key;
        VALUE_TYPE val;
        bool occupied;
        bool reference; // For clock eviction policy
        bool valid;

        Entry() : key(), val(), org_key(), occupied(false), reference(false), valid(true) {}
        Entry(KEY_TYPE key, ORG_KEY_TYPE org_key, VALUE_TYPE val)
                : key(key), org_key(org_key), val(val), occupied(true), reference(true), valid(true) {}
    };


    explicit LinearProbingHashTable(INDEX_TYPE capacity)
            : capacity_(capacity), size_(0), clock_hand_(0),
              table_(std::make_unique<Entry[]>(capacity)) {
        num_locks_ = (capacity_ + Traits::LOCK_LENGTH - 1) / Traits::LOCK_LENGTH + 1;
        locks_ = std::make_unique<std::mutex[]>(num_locks_);
    }

    INDEX_TYPE getHash(const KEY_TYPE& key) const {
        if constexpr (Traits::USE_XXHASH) {
            return XXHash64::hash(&key, sizeof(key), 0);
        } else {
            if constexpr (std::is_class<KEY_TYPE>::value && requires { key.bitset[0]; }) {
                return key.bitset[0];
            } else {
                static_assert(std::is_integral<KEY_TYPE>::value, "Unsupported KEY_TYPE for getHash");
                return static_cast<INDEX_TYPE>(key); // Handle primitive key types
            }
        }
    }

    std::vector<ORG_KEY_TYPE> getAllKeys() {
        std::vector<ORG_KEY_TYPE> keys;
        for (INDEX_TYPE i = 0; i < capacity_; ++i) {
            if (table_[i].occupied) {
                keys.push_back(table_[i].org_key);
            }
        }
        return keys;
    }

    bool put(const KEY_TYPE& key, const VALUE_TYPE& val, const ORG_KEY_TYPE& org_key) {
        if (Traits::BUFFER_POOL_CAP == 0) {
            return false;
        }
        if (loadFactor() > Traits::MAX_LF) {
            bool res;
            if (Traits::BATCH_EVICTION)
                res = batchEvict();
            else
                res = evict();
            if (!res)
                return false;
        }

        auto hash = getHash(key);
        INDEX_TYPE index = getInitialIndex(hash);
        INDEX_TYPE current = index;
        size_t lock_index = getLockIndex(current);
        std::unique_lock<std::mutex> lock(locks_[lock_index]);
        std::unique_lock<std::mutex> lock2(locks_[lock_index + 1]);
        const auto rbound = getIndex(lock_index + 1).second;
        while (current < (lock_index + 2) * Traits::LOCK_LENGTH) {
            Entry& entry = table_[current];
            auto e_hash = getHash(entry.key);
            if (!entry.occupied) {
                entry = Entry(key, org_key, val);
                ++size_;
                return true;
            } else {
                if (e_hash > hash) {
                    // Need to insert here, shift entries to the right
                    if (!shiftRight(current, rbound)) {
                        // Table is full
                        return false;
                    }
                    table_[current] = Entry(key, org_key, val);
                    ++size_;
                    return true;
                } else if (e_hash == hash) {
                    // Key already exists, update value and set reference bit
                    entry.val = val;
                    entry.reference = true;
                    entry.valid = true;
                    return true;
                } else {
                    // Move to next slot
                    ++current;
                }
            }
        }
        return false;
    }

    void reset_count_and_hit() {
        cacheHitCount = 0;
        totalQueryCount = 0;
    }

    std::optional<std::pair<ORG_KEY_TYPE, VALUE_TYPE>> get(const KEY_TYPE& key, const bool set_ref = true) {
        if (Traits::BUFFER_POOL_CAP == 0) {
            return std::nullopt;
        }
        ++totalQueryCount;
        auto hash = getHash(key);
        INDEX_TYPE index = getInitialIndex(hash);
        INDEX_TYPE current = index;

        size_t lock_index = getLockIndex(current);
        std::unique_lock<std::mutex> lock(locks_[lock_index]);
        std::unique_lock<std::mutex> lock2(locks_[lock_index + 1]);
        while (current < getIndex(lock_index + 1).second + 1 && current < capacity_) {
            Entry& entry = table_[current];
            auto e_hash = getHash(entry.key);
            if (!entry.occupied)
                return std::nullopt;
            else {
                if (e_hash == hash && entry.valid) {
                    if (set_ref)
                        entry.reference = true; // Set reference bit on access
                    ++cacheHitCount;
                    return std::make_pair(entry.org_key, entry.val);
                } else if (e_hash > hash)
                    return std::nullopt;
                else
                    current += 1;
            }
        }
        return std::nullopt;
    }

    bool invalidate(const KEY_TYPE& key) {
        if (Traits::BUFFER_POOL_CAP == 0) {
            return false;
        }
        auto hash = getHash(key);
        INDEX_TYPE index = getInitialIndex(hash);
        INDEX_TYPE current = index;

        size_t lock_index = getLockIndex(current);
        std::unique_lock<std::mutex> lock(locks_[lock_index]);
        std::unique_lock<std::mutex> lock2(locks_[lock_index + 1]);
        while (current < getIndex(lock_index + 1).second + 1 && current < capacity_) {
            Entry& entry = table_[current];
            auto e_hash = getHash(entry.key);
            if (!entry.occupied)
                return false;
            else {
                if (e_hash == hash && entry.valid) {
                    entry.valid = false; // Set reference bit on access
                    return true;
                } else if (e_hash > hash)
                    return false;
                else
                    current += 1;
            }
        }
        return false;
    }

    bool evict() {
        std::unique_lock<std::mutex> lockClk(lockClk_);
        size_t lock_index = getLockIndex(clock_hand_);
        auto toBeLocked = lock_index == num_locks_ - 2 ? 0 : lock_index + 1; // Determine the next lock index.
        // Ensure consistent lock acquisition order to avoid deadlocks.
        size_t first_lock_index = std::min(lock_index, toBeLocked);
        size_t second_lock_index = std::max(lock_index, toBeLocked);
        if (first_lock_index == second_lock_index)
            second_lock_index = first_lock_index + 1;
        // Lock both mutexes in a consistent order using std::lock to prevent deadlocks.
        std::unique_lock<std::mutex> lock1(locks_[first_lock_index]);
        std::unique_lock<std::mutex> lock2(locks_[second_lock_index]);
        auto probe = 0;
        while (true) {
            probe += 1;
            Entry& entry = table_[clock_hand_];
            if (entry.occupied) {
                if (!entry.reference || probe > Traits::LOCK_LENGTH || !entry.valid) {
                    // Evict this entry
                    bool res = false;
                    if (isWithin(clock_hand_, first_lock_index)) {
                        if (( getIndex(first_lock_index).second) + 1 == getIndex(second_lock_index).first)
                            res = shiftLeft(clock_hand_, getIndex(second_lock_index).second);
                        else
                            res = shiftLeft(clock_hand_, getIndex(first_lock_index).second);
                    } else if (isWithin(clock_hand_, second_lock_index))
                        res = shiftLeft(clock_hand_, getIndex(second_lock_index).second);
                    if (!res) {
//                        std::cout << first_lock_index * Traits::LOCK_LENGTH << "  " << (first_lock_index + 1) * Traits::LOCK_LENGTH << "  "
//                        << second_lock_index * Traits::LOCK_LENGTH << "  " << (second_lock_index + 1) * Traits::LOCK_LENGTH << "  "
//                        << clock_hand_ << std::endl;
                        return false;
                    }
                    --size_;
                    // Move clock hand to next position
                    clock_hand_ = (clock_hand_ + 1) % capacity_;
                    return true;
                } else {
                    // Reset reference bit and move clock hand
                    entry.reference = false;
                }
            }
            clock_hand_ = (clock_hand_ + 1) % capacity_;
        }
    }

    bool batchEvict() {
        std::unique_lock<std::mutex> lockClk(lockClk_);
        size_t lock_index = getLockIndex(clock_hand_);
        auto toBeLocked = lock_index == num_locks_ - 2 ? 0 : lock_index + 1; // Determine the next lock index.
        // Ensure consistent lock acquisition order to avoid deadlocks.
        size_t first_lock_index = std::min(lock_index, toBeLocked);
        size_t second_lock_index = std::max(lock_index, toBeLocked);
        // Lock both mutexes in a consistent order using std::lock to prevent deadlocks.
        std::unique_lock<std::mutex> lock1(locks_[first_lock_index]);
        std::unique_lock<std::mutex> lock2(locks_[second_lock_index]);
        bool evicted = false;

        size_t start1 = lock_index * Traits::LOCK_LENGTH;
        size_t end1 = std::min(start1 + Traits::LOCK_LENGTH, capacity_);
        for (size_t i = start1; i < end1; ++i) {
            Entry& entry = table_[i];
            if (entry.occupied && (!entry.reference || !entry.valid)) {
                auto res = shiftLeft(i, getIndex(second_lock_index).second + 1);
                if (res) {
                    --size_;
                    evicted = true;
                }
            }
            else if (entry.occupied && entry.reference) {
                entry.reference = false;
            }
        }
        clock_hand_ = (clock_hand_ + Traits::LOCK_LENGTH) % capacity_;
        if  (clock_hand_ < Traits::LOCK_LENGTH)
            clock_hand_ = 0;

        return evicted;
    }
    size_t getOccupied() const {
        size_t count = 0;
        for (int i = 0; i < capacity_; i++) {
            count += (table_[i].occupied);
        }
        return count;
    }
    double getOccupiedRatio(size_t num_keys) const {
        return (double)getOccupied() * 100 / (double)num_keys;
    }
    double getCacheHitRatio() const {
        return (double)cacheHitCount * 100 / (double)totalQueryCount;
    }
    float loadFactor() const {
        return static_cast<float>(size_) / static_cast<float>((1 << getK()));
    }

    void printTable() const {
        std::cout << "Print HT:\n";
        for (INDEX_TYPE i = 0; i < capacity_; ++i) {
            const Entry& entry = table_[i];
            std::cout << "Index " << i << ": ";
            if (entry.occupied) {
                std::cout << "FPHash=" << getHash(entry.key)
                          << ", Value=" << entry.val
                          << ", Org Key=" << entry.org_key
                          << ", OrgIdx=" << getInitialIndex(getHash(entry.key))
                          << ", Ref=" << entry.reference << "\n";
            } else {
                std::cout << "Empty\n";
            }
        }
    }
    [[nodiscard]] INDEX_TYPE getInitialIndex(const INDEX_TYPE& hash) const {
        size_t k = getK();
        INDEX_TYPE initial_index = hash >> (64 - k);
        return initial_index;
    }

private:
    INDEX_TYPE capacity_;
    std::unique_ptr<Entry[]> table_;
    std::unique_ptr<std::mutex[]> locks_;
    std::mutex lockClk_;

    [[nodiscard]] size_t getK() const {
        return static_cast<size_t>(std::floor(std::log2(capacity_)));
    }

    [[nodiscard]] size_t getLockIndex(INDEX_TYPE index) const {
        return index / Traits::LOCK_LENGTH;
    }

    bool isWithin(INDEX_TYPE index, size_t lckIdx) {
        auto [a, b] = getIndex(lckIdx);
        return index >= a && index <= b;
    }

    [[nodiscard]] auto getIndex(INDEX_TYPE index) const {
        return std::make_pair(Traits::LOCK_LENGTH * index, (Traits::LOCK_LENGTH * (index + 1) - 1));
    }
    bool shiftRight(INDEX_TYPE index, INDEX_TYPE rbound) {
        INDEX_TYPE last = index;

        // Find the last occupied slot
        while (last + 1 < capacity_ && last + 1 < rbound) {
            ++last;
            if (!table_[last].occupied) {
                break;
            }
        }
        if (last + 1 == rbound || last + 1 == capacity_)
            return false;

        // Shift entries to the right
        while (last != index) {
            INDEX_TYPE prev = last - 1;
            table_[last] = table_[prev];
            last = prev;
        }
        return true;
    }

    bool shiftLeft(INDEX_TYPE index, const INDEX_TYPE rbound) {
        {
            INDEX_TYPE current = index;
            while (current + 1 < capacity_ && current + 1 < rbound) {
                INDEX_TYPE next = current + 1;
                auto hash = getHash(table_[next].key);
                if (!table_[next].occupied || next == getInitialIndex(hash)) {
                    // Clear current slot
                    break;
                }
                current = next;
            }
            if (current + 1 == rbound || current + 1 == capacity_) {
                return false;
            }
        }
        INDEX_TYPE current = index;
        while (current + 1 < capacity_) {
            INDEX_TYPE next = current + 1;
            auto hash = getHash(table_[next].key);
            if (!table_[next].occupied || next == getInitialIndex(hash)) {
                // Clear current slot
                table_[current] = Entry();
                return true;
            }
            table_[current] = table_[next];
            current = next;
        }
        throw std::invalid_argument("why here?");
    }
};
