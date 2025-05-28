#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>
#include <random>
#include <cmath>
#include <unordered_map>

#include "../directory/directory.h"
#include "recorder/recorder.h"
#include "../lib/memento/memento.hpp"
#include "../lib/zipfYCSB/zipfian_generator.h"
#ifdef IN_MEMORY_FILE
// parameters memory
constexpr size_t RSQF_STATIC_FP_SIZE = 15;
constexpr auto SAMPLE_LOG = 0.03;
constexpr auto MAX_INFINI_EXP = 5;
constexpr auto RANDOM_CNT = 400;
#else
// parameters SSD & Optane
constexpr size_t RSQF_STATIC_FP_SIZE = 15;
constexpr auto SAMPLE_LOG = 0.03;
constexpr auto MAX_INFINI_EXP = 5;
constexpr auto RANDOM_CNT = 400;
#endif

// consts
const std::string HOME = std::getenv("HOME");
constexpr size_t INIT_SIZE_LOG = 12;
constexpr size_t APPEND_ONLY_LOG_SIZE = 2000000;
constexpr size_t HASH_TABLE_FP_SIZE = 7;
constexpr size_t INIT_SIZE = 1ull << INIT_SIZE_LOG;
constexpr size_t NUM_KEYS_TOTAL = static_cast<size_t>(0.91 * INIT_SIZE * (1 << RSQF_STATIC_FP_SIZE));
constexpr auto SIZE_KEY_INFINI_LOG = MAX_INFINI_EXP + INIT_SIZE_LOG;
constexpr size_t TAIL_LATENCY_CNT = 1000;
std::mt19937 gen(12345); // Mersenne Twister random number generator
ycsbc::ZipfianGenerator zipfian_gen(1, NUM_KEYS_TOTAL);

inline size_t get_random_key(size_t min, size_t max) {
    std::uniform_int_distribution<size_t> dist(min, max);
    return dist(gen);
}

std::vector<size_t> _generate_zipfian_keys(size_t kTotalKeys, size_t count, size_t min, size_t max) {
    std::vector<size_t> keys;
    while (keys.size() <= count) {
        size_t key = zipfian_gen.Next(max + 1);
        assert(key > 0);
        assert(key <= kTotalKeys);
        if (key < min || key > max) {
            continue;
        }
        keys.push_back(key);
    }
    return keys;
}

std::vector<size_t> generate_random_keys(size_t min, size_t max, size_t count, bool override = false) {
    if (MAIN_BENCHMARK_ZIPF == true && !override) {
        return _generate_zipfian_keys(NUM_KEYS_TOTAL, count, min, max);
    } else {
        std::vector<size_t> keys;
        for (size_t j = 0; j < count; ++j) {
            keys.push_back(get_random_key(min, max));
        }
        return keys;
    }
}

// New helper function: returns true when the logarithm of the current step
// crosses a new 0.1 interval. This ensures more frequent sampling in the beginning.
bool should_sample(size_t step, bool override = false) {
    if (override)
        return true;
    if (step == 1)
        return false;
    double currentLog = std::log2(static_cast<double>(step));
    double prevLog = std::log2(static_cast<double>(step - 1));
    int currentBucket = static_cast<int>(std::floor(currentLog / SAMPLE_LOG));
    int prevBucket = static_cast<int>(std::floor(prevLog / SAMPLE_LOG));
    return (currentBucket != prevBucket);
}
template <typename T>
std::vector<T> select_percentiles(std::vector<T> data, const std::vector<double>& percentiles) {
    std::sort(data.begin(), data.end());
    std::vector<T> results;
    results.reserve(percentiles.size());
    for (double percentile : percentiles) {
        size_t index = static_cast<size_t>(std::ceil(percentile * data.size() / 100.0)) - 1;
        results.push_back(data[index]);
    }
    return results;
}

// filter helper functions
template <typename Traits>
double get_lf_filter(const memento::Memento<Traits::IS_INFINI>& filter, const size_t i) { return static_cast<double>(i) / static_cast<double>(filter.metadata_->nslots); }

template <typename Traits>
double get_fpr(const memento::Memento<Traits::IS_INFINI>& filter, const size_t init_slot_log, const size_t num_keys_inserted) {
    auto load_factor = get_lf_filter<Traits>(filter, num_keys_inserted);
    auto fpr = load_factor / (1 << filter.metadata_->fingerprint_bits);

    if (Traits::IS_INFINI) {
        auto number_of_expansions = std::floor(std::log2(filter.metadata_->nslots)) - init_slot_log;
        fpr = 0.5 * (1.0 / (1 << filter.metadata_->fingerprint_bits)) * (load_factor) * (number_of_expansions + 2);
        std::cout << "FPR: " << fpr << std::endl;
        std::cout << " num Exps: " << number_of_expansions << std::endl;
        std::cout << " loadf: " << load_factor<< std::endl;
    }
    return fpr;
}


template<typename Traits>
bool would_expand(const memento::Memento<Traits::IS_INFINI> &filter) {
    return filter.metadata_->noccupied_slots >= filter.metadata_->nslots * filter.metadata_->max_lf||
            filter.metadata_->noccupied_slots + 1 >= filter.metadata_->nslots;
}

template<typename Traits>
std::vector<uint64_t> get_range(const memento::Memento<Traits::IS_INFINI> &filter, uint64_t key) {
    auto mem_bits = filter.get_num_memento_bits();
    uint64_t b_key_l = key << mem_bits;
    uint64_t b_key_r = b_key_l | 0xFFFFFFFFull;
    auto it = filter.begin(/*l_key*/ b_key_l, /*r_key*/ b_key_r);

    auto res_vec = std::vector<uint64_t>();

    while (it != filter.end()) {
        uint64_t full_key = *it;

        const uint64_t memento_mask = (1ULL << filter.get_num_memento_bits()) - 1;
        uint64_t extracted_memento = full_key & memento_mask;
        res_vec.push_back(extracted_memento);
        ++it; // Move to next element
    }
    return res_vec;
}

template<typename Traits>
void readFilter(const memento::Memento<Traits::IS_INFINI> &filter, const SSDLog<Traits> &ssdLog, long selectedKey) {
    auto res = get_range<Traits>(filter, selectedKey);
    if (res.empty()) {
        std::cout << "key not found empty\n";
        throw std::invalid_argument("Error: key not found");
    }
    auto flag = false;
    for (auto &r : res) {
        DefaultTraits::ENTRY_TYPE entry_type;
        ssdLog.read(r, entry_type);
        if (entry_type.key == selectedKey) {
            flag = true;
            break;
        }
    }
    if (!flag) {
        std::cout << " : key not found readFilter\n";
        throw std::invalid_argument("Error: key not found");
    }
}

template<typename Traits>
void putFilter(memento::Memento<Traits::IS_INFINI> &filter, const SSDLog<Traits> &ssdLog, long selectedKey, DefaultTraits::PAYLOAD_TYPE pt) {
    auto res = get_range<Traits>(filter, selectedKey);
    if (res.empty()) {
        filter.insert(selectedKey, pt, memento::Memento<true>::flag_no_lock);
        return;
    }
    auto flag = false;
    for (auto &r : res) {
        DefaultTraits::ENTRY_TYPE entry_type;
        ssdLog.read(r, entry_type);
        if (entry_type.key == selectedKey) {
            flag = true;
            auto res_delete = filter.delete_single(selectedKey, r, memento::Memento<true>::flag_no_lock);
            if (res_delete != 0) {
                throw std::invalid_argument("Error: key delete unsuccessful in filter");
            }
            auto res_insert = filter.insert(selectedKey, pt, memento::Memento<true>::flag_no_lock);
            if (res_insert < 0) {
                throw std::invalid_argument("Error: key update(insert) unsuccessful in filter");
            }
            return;
        }
    }
    if (!flag) {
        auto res_insert = filter.insert(selectedKey, pt, memento::Memento<true>::flag_no_lock);
        if (res_insert < 0) {
            throw std::invalid_argument("Error: key insert unsuccessful in filter");
        }
    }
}
template<typename Traits>
void verifyFPR(const memento::Memento<Traits::IS_INFINI> &filter, size_t num_keys_inserted, size_t init_slot_log) {
#ifndef IN_MEMORY_FILE
    std::cout << "Skipping FPR verification for storage\n";
    return;
#endif
    constexpr size_t total_test_size = 100000;
    auto fpr = get_fpr<Traits>(filter, init_slot_log, num_keys_inserted);
    auto keys = generate_random_keys(num_keys_inserted + 10, num_keys_inserted + total_test_size * 100, total_test_size, true);
    size_t false_positives = 0;
    for (auto key : keys) {
        auto res = get_range<Traits>(filter, key);
        false_positives += res.size();
    }
    auto fpr_actual = static_cast<double>(false_positives) / total_test_size;
    auto lf = get_lf_filter<Traits>(filter, num_keys_inserted);
    std::cout << "FPR: " << fpr << " - FPR actual: " << fpr_actual << std::endl;
    // if (std::abs(fpr - fpr_actual) > 0.01 && std::abs(fpr - fpr_actual) / fpr > 0.1 && lf > 0.9) {
    //     throw std::invalid_argument("Error: FPR not matching");
    // }
}

template<typename Traits>
void updateFilter(memento::Memento<Traits::IS_INFINI>& filter, const SSDLog<Traits>& ssdLog, long selectedKey, int32_t newMem) {
    auto res = get_range<Traits>(filter, selectedKey);
    // std::cout << "key: " << selectedKey << " - read set size: " << res.size() << std::endl;
    if (res.empty()) {
        std::cout << "key not found\n";
        throw std::invalid_argument("Error: key not found here1");
    }
    bool flag = false;
    int32_t memento = -1;
    for (auto &r : res) {
        DefaultTraits::ENTRY_TYPE entry_type;
        ssdLog.read(r, entry_type);
        if (entry_type.key == selectedKey) {
            flag = true;
            memento = r;
            break;
        }
    }
    if (!flag) {
        std::cout << " : key not found\n";
        throw std::invalid_argument("Error: key not found here2");
    }
    auto res_update = filter.update_single(selectedKey, memento, newMem, memento::Memento<true>::flag_no_lock);
    if (res_update != 0) {
        throw std::invalid_argument("Error: key update unsuccessful in filter");
    }
}

// Perfect HT helper functions
template <typename Traits>
void insert_pht(memento::Memento<true>& filter, std::unordered_map<size_t, size_t>& stash_dict, SSDLog<Traits>& ssdLog, const size_t key, const size_t value) {
    auto pt = ssdLog.write(key, value);
    // update if present in stash list
    auto it = stash_dict.find(key);
    if (it != stash_dict.end()) {
        if (it->first == key) {
            it->second = value;
            return;
        }
    }
    auto res = get_range<Traits>(filter, key);

    bool stashed = false;
    for (auto& r : res) {
        DefaultTraits::ENTRY_TYPE entry_type;
        ssdLog.read(r, entry_type);
        if (entry_type.key != key) {
            stash_dict[key] = pt;
            stashed = true;
            break;
        }
    }
    if (!stashed) {
        putFilter(filter, ssdLog, key, pt);
    }
}
template <typename Traits>
void query_pht(memento::Memento<true>& filter, std::unordered_map<size_t, size_t>& stash_dict, SSDLog<Traits>& ssdLog, unsigned long vv) {
    auto it = stash_dict.find(vv);
    if (it != stash_dict.end()) {
        DefaultTraits::ENTRY_TYPE entry_type;
        ssdLog.read(it->second, entry_type);
        if (entry_type.key != vv) {
            readFilter<Traits>(filter, ssdLog, vv);
        }
    } else {
        readFilter<Traits>(filter, ssdLog, vv);
    }
}
template <typename Traits>
void update_pht(memento::Memento<true>& filter, std::unordered_map<size_t, size_t>& stash_dict, SSDLog<Traits>& ssdLog, unsigned long uk) {
    auto new_value = uk + 1000000;
    auto pt_update = ssdLog.write(uk, new_value);

    // update if present in stash list
    auto it = stash_dict.find(uk);
    if (it != stash_dict.end()) {
        if (it->first == uk) {
            it->second = pt_update;
        }
    } else {
        updateFilter<Traits>(filter, ssdLog, uk, pt_update);
    }
}
// memory helper functions
template <typename Traits>
double get_memory_filter(memento::Memento<Traits::IS_INFINI>& filter, bool including_ptr=false) { 
    size_t reduced_size = filter.get_num_memento_bits() * (filter.metadata_->xnslots);
    if (including_ptr)
        reduced_size = 0;
    return static_cast<double>(8 * filter.size_in_bytes() - reduced_size); 
}

template <typename Traits>
size_t get_memory_stash(std::unordered_map<size_t, size_t>& stash_dict) {
    // size_t stash_memory = stash_dict.bucket_count() * (sizeof(void*) + sizeof(size_t)));
    // this is in favor of pht
    size_t stash_memory = stash_dict.bucket_count() * sizeof(size_t);
    stash_memory *= 8;
    return stash_memory;
}
// Function to perform insertions into the directory
template <typename Traits>
void Run(
    Directory<Traits>& dir,
    SSDLog<Traits>& ssdLog,
    size_t numKeys,
    const std::string& folder) {
    Metrics metrics;
    size_t insertion_total = 0;
    size_t insertion_count = 0;
    std::vector<size_t> insertions;
    for (size_t i = 1; i <= numKeys; ++i) {
        if (should_sample(i))
            std::cout << i << std::endl;
        // insertion
        const auto key = i;
        const auto value = key;
        auto start_insertion = std::chrono::high_resolution_clock::now();
        auto pt = ssdLog.write(key, value);
        dir.writeSegmentSingleThread(key, value, ssdLog, pt);
        auto end_insertion = std::chrono::high_resolution_clock::now();
        auto du_insertion = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insertion - start_insertion).count();
        insertions.push_back(du_insertion);
        insertion_total += du_insertion;
        insertion_count++;

        // insertion record
        if (should_sample(i)) {
            metrics.record("insertion_time", insertion_total / insertion_count);
            insertion_count = 0;
            insertion_total = 0;

            insertions = select_percentiles(insertions, {99.0, 99.9});
            std::cout << "tail-99-i: " << insertions[0] << " - tail-99.9-i: " << insertions[1] << std::endl;
            metrics.record("tail-99-i", insertions[0]);
            insertions.clear();
        }

        if (Traits::NUMBER_EXTRA_BITS > 1 && i > 2) {
            auto rej_key = get_random_key(1, i - 1);
            dir.readSegmentSingleThread(rej_key, ssdLog);
        }

        // queries record
        if (should_sample(i)) {
            auto v = generate_random_keys(1, i, RANDOM_CNT);
            auto start_query = std::chrono::high_resolution_clock::now();
            for (auto vv : v)
                dir.readSegmentSingleThread(vv, ssdLog);
            auto end_query = std::chrono::high_resolution_clock::now();
            auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count() / RANDOM_CNT;
            metrics.record("query_time", du_query);
        }

        // query tail latency
        if (should_sample(i)) {
            auto v = generate_random_keys(i/2, i, TAIL_LATENCY_CNT);
            std::vector<size_t> res;
            for (auto vv : v) {
                auto start_query = std::chrono::high_resolution_clock::now();
                dir.readSegmentSingleThread(vv, ssdLog);
                auto end_query = std::chrono::high_resolution_clock::now();
                auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count();
                res.push_back(du_query);
            }
            auto percentiles = std::vector<double>{99.0, 99.9};
            auto tail_exs_read = select_percentiles(res, percentiles);
            std::cout << "tail-99-q: " << tail_exs_read[0] << " - tail-99.9-q: " << tail_exs_read[1] << std::endl;
            metrics.record("tail-99-q", tail_exs_read[0]);
            metrics.record("tail-99.9-q", tail_exs_read[1]);
        }

        // update record
        if (should_sample(i)) {
            auto update_keys = generate_random_keys(1, i, RANDOM_CNT);
            auto start_update = std::chrono::high_resolution_clock::now();
            for (auto uk : update_keys) {
                auto new_value = uk + 1000000;
                auto pt_update = ssdLog.write(uk, new_value);
                dir.updateSegmentSingleThread(uk, new_value, ssdLog, pt_update);
            }
            auto end_update = std::chrono::high_resolution_clock::now();
            auto du_update = std::chrono::duration_cast<std::chrono::nanoseconds>(end_update - start_update).count() / RANDOM_CNT;
            metrics.record("update_time", du_update);
        }

        // update tail latency
        if (should_sample(i)) {
            auto update_keys = generate_random_keys(i/2, i, TAIL_LATENCY_CNT);
            std::vector<size_t> res;
            for (auto uk : update_keys) {
                auto start_query = std::chrono::high_resolution_clock::now();
                auto new_value = uk + 1000000;
                auto pt_update = ssdLog.write(uk, new_value);
                dir.updateSegmentSingleThread(uk, new_value, ssdLog, pt_update);
                auto end_query = std::chrono::high_resolution_clock::now();
                auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count();
                res.push_back(du_query);
            }
            auto percentiles = std::vector<double>{99.0, 99.9};
            auto tail_exs_read = select_percentiles(res, percentiles);
            std::cout << "tail-99-u: " << tail_exs_read[0] << " - tail-99.9-u: " << tail_exs_read[1] << std::endl;
            metrics.record("tail-99-u", tail_exs_read[0]);
            metrics.record("tail-99.9-u", tail_exs_read[1]);
        }

        // FPR and memory record
        if (should_sample(i)) {
            auto memory_footprint = dir.get_memory_footprint(i);
            auto memory_inc_ptr = dir.get_memory_including_ptr();
            metrics.record("memory", memory_footprint);
            metrics.record("memory_including_ptr", memory_inc_ptr / i);
            metrics.record("FPR", 0);
            metrics.record("num_entries", i);
        }
    }

    std::string name;
    if constexpr (Traits::DHT_EVERYTHING) {
        name = "DHT";
    } else if constexpr (Traits::READ_OFF_STRATEGY == 20) {
        name = "Sphinx-Loop";
    } else {
        name = "Sphinx";
    }
    if (Traits::NUMBER_EXTRA_BITS > 1) {
        name += "-ReserveBits" + std::to_string(Traits::NUMBER_EXTRA_BITS);
    }
    metrics.printToFile({"num_entries", "insertion_time", "query_time", "update_time", "memory", "memory_including_ptr", "FPR", "tail-99-q", "tail-99.9-q", "tail-99-u", "tail-99.9-u", "tail-99-i"},
                        folder + "/benchmark_", name);
}

// Function to perform insertions into the directory (Perfect HT variant)
template <typename Traits>
void RunPHT(
    memento::Memento<true>& filter,
    std::unordered_map<size_t, size_t>& stash_dict,
    SSDLog<Traits>& ssdLog,
    size_t numKeys,
    const std::string& folder) {
    Metrics metrics;
    size_t insertion_total = 0;
    size_t insertion_count = 0;
    std::vector<size_t> insertions;
    for (size_t i = 1; i <= numKeys; ++i) {
        if (should_sample(i))
            std::cout << i << std::endl;
        const auto key = i;
        const auto value = key;
        auto start_insertion = std::chrono::high_resolution_clock::now();
        insert_pht<Traits>(filter, stash_dict, ssdLog, key, value);
        auto end_insertion = std::chrono::high_resolution_clock::now();
        auto du_insertion = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insertion - start_insertion).count();
        insertion_total += du_insertion;
        insertion_count++;
        insertions.push_back(du_insertion);
        const bool gonna_expand = would_expand<Traits>(filter);
        // insertion record
        if (should_sample(i, gonna_expand)) {
            metrics.record("insertion_time", insertion_total / insertion_count);
            insertion_count = 0;
            insertion_total = 0;
            insertions = select_percentiles(insertions, {99.0, 99.9});
            std::cout << "tail-99-i: " << insertions[0] << " - tail-99.9-i: " << insertions[1] << std::endl;
            metrics.record("tail-99-i", insertions[0]);
            insertions.clear();
        }

        // queries record
        if (should_sample(i, gonna_expand)) {
            auto v = generate_random_keys(1, i, RANDOM_CNT);
            auto start_query = std::chrono::high_resolution_clock::now();
            for (auto vv : v) {
                query_pht<Traits>(filter, stash_dict, ssdLog, vv);
            }
            auto end_query = std::chrono::high_resolution_clock::now();
            auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count() / RANDOM_CNT;
            metrics.record("query_time", du_query);
        }

        // tail latency query
        if (should_sample(i, gonna_expand)) {
            auto v = generate_random_keys(i/2, i, TAIL_LATENCY_CNT);
            std::vector<size_t> res;
            for (auto vv : v) {
                auto start_query = std::chrono::high_resolution_clock::now();
                query_pht<Traits>(filter, stash_dict, ssdLog, vv);
                auto end_query = std::chrono::high_resolution_clock::now();
                auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count();
                res.push_back(du_query);
            }
            auto percentiles = std::vector<double>{99.0, 99.9};
            auto tail_exs_read = select_percentiles(res, percentiles);
            std::cout << "tail-99-q: " << tail_exs_read[0] << " - tail-99.9-q: " << tail_exs_read[1] << std::endl;
            metrics.record("tail-99-q", tail_exs_read[0]);
            metrics.record("tail-99.9-q", tail_exs_read[1]);
        }


        // update record
        if (should_sample(i, gonna_expand)) {
            auto update_keys = generate_random_keys(1, i, RANDOM_CNT);
            auto start_update = std::chrono::high_resolution_clock::now();
            for (auto uk : update_keys) {
                update_pht<Traits>(filter, stash_dict, ssdLog, uk);
            }
            auto end_update = std::chrono::high_resolution_clock::now();
            auto du_update = std::chrono::duration_cast<std::chrono::nanoseconds>(end_update - start_update).count() / RANDOM_CNT;
            metrics.record("update_time", du_update);
        }

        // update tail latency
        if (should_sample(i, gonna_expand)) {
            auto update_keys = generate_random_keys(i/2, i, TAIL_LATENCY_CNT);
            std::vector<size_t> res;
            for (auto uk : update_keys) {
                auto start_query = std::chrono::high_resolution_clock::now();
                update_pht<Traits>(filter, stash_dict, ssdLog, uk);
                auto end_query = std::chrono::high_resolution_clock::now();
                auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count();
                res.push_back(du_query);
            }
            auto percentiles = std::vector<double>{99.0, 99.9};
            auto tail_exs_read = select_percentiles(res, percentiles);
            std::cout << "tail-99-u: " << tail_exs_read[0] << " - tail-99.9-u: " << tail_exs_read[1] << std::endl;
            metrics.record("tail-99-u", tail_exs_read[0]);
            metrics.record("tail-99.9-u", tail_exs_read[1]);
        }

        // memory
        if (should_sample(i, gonna_expand)) {
            auto memento_memory = get_memory_filter<Traits>(filter);
            auto stash_memory = get_memory_stash<Traits>(stash_dict);
            auto memory_footprint = (stash_memory + memento_memory) / i;
            metrics.record("memory", memory_footprint);
            std::cout << "memory footprint: " << memory_footprint << std::endl;
        }
        // memory with ptrs
        if (should_sample(i, gonna_expand)) {
            auto memento_memory = get_memory_filter<Traits>(filter, true);
            auto stash_memory = get_memory_stash<Traits>(stash_dict);
            auto memory_footprint = (stash_memory + memento_memory) / i;
            metrics.record("memory_including_ptr", memory_footprint);
            std::cout << "memory including ptr footprint: " << memory_footprint << std::endl;
        }
        if (should_sample(i, gonna_expand)) {
            metrics.record("FPR", 0);
            metrics.record("num_entries", i);
        }
    }

    metrics.printToFile({"num_entries", "insertion_time", "query_time", "update_time", "memory", "memory_including_ptr", "FPR", "tail-99-q", "tail-99.9-q", "tail-99-u", "tail-99.9-u", "tail-99-i"},
                        folder + "/benchmark_", "Perfect_HT");
}

// Function to perform insertions into the directory (InfiniFilter variant)
template <typename Traits>
void RunFilter(
        memento::Memento<Traits::IS_INFINI>& filter,
        SSDLog<Traits>& ssdLog,
        size_t numKeys,
        bool should_exp,
        const std::string& folder)
{
    auto init_slot_log = std::floor(std::log2(filter.metadata_->nslots));
    Metrics metrics;
    size_t insertion_total = 0;
    size_t insertion_count = 0;
    std::vector<size_t> insertions;
    for (size_t i = 1; i <= numKeys; ++i) {
        const auto key = i;
        const auto value = key;
        auto start_insertion = std::chrono::high_resolution_clock::now();
        auto pt = ssdLog.write(key, value);
        putFilter<Traits>(filter, ssdLog, key, pt);
        auto end_insertion = std::chrono::high_resolution_clock::now();
        auto du_insertion = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insertion - start_insertion).count();
        insertion_total += du_insertion;
        insertion_count++;
        insertions.push_back(du_insertion);
        const bool gonna_expand = would_expand<Traits>(filter);
        if (should_sample(i, gonna_expand)) {
            metrics.record("insertion_time", insertion_total / insertion_count);
            insertion_count = 0;
            insertion_total = 0;
            insertions = select_percentiles(insertions, {99.0, 99.9});
            std::cout << "tail-99-i: " << insertions[0] << " - tail-99.9-i: " << insertions[1] << std::endl;
            metrics.record("tail-99-i", insertions[0]);
            insertions.clear();
        }

        if (should_sample(i, gonna_expand)) {
            auto v = generate_random_keys(1, i, RANDOM_CNT);
            auto start_query = std::chrono::high_resolution_clock::now();
            for (auto vv:v)
                readFilter(filter, ssdLog, vv);
            auto end_query = std::chrono::high_resolution_clock::now();
            auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count() / RANDOM_CNT;
            metrics.record("query_time", du_query);
        }

        // tail latency query
        if (should_sample(i, gonna_expand)) {
            auto v = generate_random_keys(i/2, i, TAIL_LATENCY_CNT);
            std::vector<size_t> res;
            for (auto vv : v) {
                auto start_query = std::chrono::high_resolution_clock::now();
                readFilter(filter, ssdLog, vv);
                auto end_query = std::chrono::high_resolution_clock::now();
                auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count();
                res.push_back(du_query);
            }
            auto percentiles = std::vector<double>{99.0, 99.9};
            auto tail_exs_read = select_percentiles(res, percentiles);
            metrics.record("tail-99-q", tail_exs_read[0]);
            metrics.record("tail-99.9-q", tail_exs_read[1]);
            std::cout << "tail-99-q: " << tail_exs_read[0] << " - tail-99.9-q: " << tail_exs_read[1] << std::endl;
        }

        if (should_sample(i, gonna_expand)) {
            std::cout << "update: " << i << std::endl;
            auto update_keys = generate_random_keys(1, i, RANDOM_CNT);
            auto start_update = std::chrono::high_resolution_clock::now();
            for (auto uk : update_keys) {
                auto new_value = uk + 1000000;
                auto pt_update = ssdLog.write(uk, new_value);
                updateFilter<Traits>(filter, ssdLog, uk, pt_update);
            }
            auto end_update = std::chrono::high_resolution_clock::now();
            auto du_update = std::chrono::duration_cast<std::chrono::nanoseconds>(end_update - start_update).count() / RANDOM_CNT;
            metrics.record("update_time", du_update);
        }

        // tail latency update
        if (should_sample(i, gonna_expand)) {
            auto update_keys = generate_random_keys(i/2, i, TAIL_LATENCY_CNT);
            std::vector<size_t> res;
            for (auto uk : update_keys) {
                auto start_query = std::chrono::high_resolution_clock::now();
                auto new_value = uk + 1000000;
                auto pt_update = ssdLog.write(uk, new_value);
                updateFilter<Traits>(filter, ssdLog, uk, pt_update);
                auto end_query = std::chrono::high_resolution_clock::now();
                auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count();
                res.push_back(du_query);
            }
            auto percentiles = std::vector<double>{99.0, 99.9};
            auto tail_exs_read = select_percentiles(res, percentiles);
            std::cout << "tail-99-u: " << tail_exs_read[0] << " - tail-99.9-u: " << tail_exs_read[1] << std::endl;
            metrics.record("tail-99-u", tail_exs_read[0]);
            metrics.record("tail-99.9-u", tail_exs_read[1]);
        }

        if (should_sample(i, gonna_expand)) {
            auto memory_footprint = get_memory_filter<Traits>(filter) / i;
            metrics.record("memory", memory_footprint);
            std::cout << "memory footprint: " << memory_footprint << std::endl;
        }

        if (should_sample(i, gonna_expand)) {
            auto memory_footprint = get_memory_filter<Traits>(filter, true) / i;
            metrics.record("memory_including_ptr", memory_footprint);
            std::cout << "memory including ptr footprint: " << memory_footprint << std::endl;
        }

        if (should_sample(i, gonna_expand)) {
            auto fpr = get_fpr<Traits>(filter, init_slot_log, i);
            if (i > 10000)
                verifyFPR<Traits>(filter, i, init_slot_log);
            metrics.record("FPR", fpr);
            metrics.record("num_entries", i);
        }
    }

    std::string name;
    if constexpr (Traits::IS_INFINI) {
        name = "InfiniFilter";
    } else {
        if (should_exp) {
            name = "RSQF";
        } else {
            name = "RSQF-Static";
        }
    }

    metrics.printToFile( { "num_entries", "insertion_time", "query_time", "update_time", "memory", "memory_including_ptr", "FPR", "tail-99-q", "tail-99.9-q", "tail-99-u", "tail-99.9-u", "tail-99-i" },
                        folder + "/benchmark_", name);

}

// PerformTest function with added CSV output
template <typename TemplateClass>
void performTest(const std::string &ssdLogPath, const std::string &folder) {
    size_t log_size_dir = 0; // init_filter_size should be 4096
    if constexpr (TemplateClass::DHT_EVERYTHING) {
        log_size_dir = RSQF_STATIC_FP_SIZE;
        std::cout << "DHT is on\n";
    }
    std::cout << "log size: " << log_size_dir << std::endl;
    Directory<TemplateClass> dir(log_size_dir, 1);
    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>(ssdLogPath, APPEND_ONLY_LOG_SIZE);

    Run(dir, *ssdLog, NUM_KEYS_TOTAL, folder);
}

// PerformTest function with added CSV output
template <typename TemplateClass>
void performTestFilterRSQF(const std::string &ssdLogPath, const std::string &folder) {
    memento::Memento<TestRSQF::IS_INFINI> filter(INIT_SIZE, /* nslots */
                            12 + RSQF_STATIC_FP_SIZE,   /* key_bits (fingerprint size) */
                            32,  /* memento_bits = payload bit size */
                            memento::Memento<TemplateClass::IS_INFINI>::hashmode::Default,
                            12345 /* seed */, 0, 1);
    filter.set_auto_resize(true);

    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>(ssdLogPath, APPEND_ONLY_LOG_SIZE);

    RunFilter(filter, *ssdLog, NUM_KEYS_TOTAL, true, folder);
}

// PerformTest function with added CSV output
template <typename TemplateClass>
void performTestFilterInfini(const std::string &ssdLogPath, const std::string &folder) {
    memento::Memento<TestInfini::IS_INFINI> filter(INIT_SIZE, /* nslots */
                                                 SIZE_KEY_INFINI_LOG,   /* key_bits (fingerprint size) */
                                                 32,  /* memento_bits = payload bit size */
                                                 memento::Memento<TemplateClass::IS_INFINI>::hashmode::Default,
                                                 12345 /* seed */, 0, 1);
    filter.set_auto_resize(true);

    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>(ssdLogPath, APPEND_ONLY_LOG_SIZE);

    RunFilter(filter, *ssdLog, NUM_KEYS_TOTAL, true, folder);
}

// PerformTest function with added CSV output
template <typename TemplateClass>
void performTestPHT(const std::string &ssdLogPath, const std::string &folder) {
    memento::Memento<TestInfini::IS_INFINI> filter(INIT_SIZE, /* nslots */
                            INIT_SIZE_LOG + HASH_TABLE_FP_SIZE,   /* key_bits (fingerprint size) */
                            32,  /* memento_bits = payload bit size */
                            memento::Memento<TemplateClass::IS_INFINI>::hashmode::Default,
                            12345 /* seed */, 0, 1);
    filter.set_auto_resize(true);
    std::unordered_map<size_t, size_t> stash_dict;
    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>(ssdLogPath, APPEND_ONLY_LOG_SIZE);

    RunPHT(filter, stash_dict, *ssdLog, NUM_KEYS_TOTAL, folder);
}


int main() {
#ifndef IN_MEMORY_FILE
    std::vector<std::pair<std::string, std::string>> configs = {
        {HOME + "/research/sphinx/benchmark/data-ssd", "/data/fleck/directory_test.txt"},
//        {HOME + "/research/sphinx/benchmark/data-optane", "/optane/log/directory_test.txt"},
    };

    if constexpr (MAIN_BENCHMARK_ZIPF) {
        configs = {
            {HOME + "/research/sphinx/benchmark/data-ssd-zipf", "/data/fleck/directory_test.txt"},
//            {HOME + "/research/sphinx/benchmark/data-optane-zipf", "/optane/log/directory_test.txt"},
        };
    }
    // Iterate over each configuration
    for (const auto& [dataFolder, ssdLogPath] : configs) {
        std::cout << "Running tests with folder: " << dataFolder << " and SSD log: " << ssdLogPath << std::endl;
        // Create the directory (and any necessary parent directories)
        std::filesystem::create_directories(dataFolder);
        std::cout << " Test RSQF\n";
        performTestFilterRSQF<TestRSQF>(ssdLogPath, dataFolder);
        std::cout << " Test InfiniFilter\n";
        performTestFilterInfini<TestInfini>(ssdLogPath, dataFolder);
        std::cout << " Test RSQF\n";
        performTestFilterRSQF<TestRSQF>(ssdLogPath, dataFolder);
        std::cout << " Test PHT\n";
        performTestPHT<TestInfini>(ssdLogPath, dataFolder);
        std::cout << " Test Fleck-Loop\n";
        performTest<TestDHTInMemory>(ssdLogPath, dataFolder);
        std::cout << " Test Fleck\n";
        performTest<TestFleckInMemory>(ssdLogPath, dataFolder);
        std::cout << " Test Fleck 4\n";
        performTest<TestFleckInMemoryExtraBits4P32>(ssdLogPath, dataFolder);
    }
#else
    auto dataFolder = HOME + "/research/sphinx/benchmark/data-memory";
    if constexpr (MAIN_BENCHMARK_ZIPF) {
        dataFolder += "-zipf";
    }
    const auto ssdLogPath = "directory_cpu_test.txt";
    std::filesystem::create_directories(dataFolder);
    std::cout << " Test RSQF\n";
    performTestFilterRSQF<TestRSQFInMem>(ssdLogPath, dataFolder);
    performTestFilterRSQF<TestRSQFInMem>(ssdLogPath, dataFolder);
    std::cout << " Test InfiniFilter\n";
    performTestFilterInfini<TestInfiniInMem>(ssdLogPath, dataFolder);
    performTestFilterInfini<TestInfiniInMem>(ssdLogPath, dataFolder);
    std::cout << " Test Perfect HT\n";
    performTestPHT<TestInfiniInMem>(ssdLogPath, dataFolder);
    performTestPHT<TestInfiniInMem>(ssdLogPath, dataFolder);
    std::cout << " Test DHT\n";
    performTest<TestRealDHTInMemoryInMem>(ssdLogPath, dataFolder);
    performTest<TestRealDHTInMemoryInMem>(ssdLogPath, dataFolder);
    std::cout << " Test Fleck-Loop\n";
    performTest<TestDHTInMemoryInMem>(ssdLogPath, dataFolder);
    performTest<TestDHTInMemoryInMem>(ssdLogPath, dataFolder);
    std::cout << " Test Sphinx\n";
    performTest<TestFleckInMemoryInMem>(ssdLogPath, dataFolder);
    performTest<TestFleckInMemoryInMem>(ssdLogPath, dataFolder);
    std::cout << " Test Sphinx-4\n";
    performTest<TestFleckInMemoryExtraBits4P32InMem>(ssdLogPath, dataFolder);
    performTest<TestFleckInMemoryExtraBits4P32InMem>(ssdLogPath, dataFolder);
    return 0;
#endif
}
