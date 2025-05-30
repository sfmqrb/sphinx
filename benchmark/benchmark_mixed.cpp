#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "../directory/directory.h"
#include "../lib/memento/memento.hpp"
#include "../lib/zipfYCSB/zipfian_generator.h"
#include "../xdp/xdp.h"
#include "recorder/recorder.h"
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
constexpr size_t NUM_KEYS_TOTAL =
    static_cast<size_t>(0.91 * INIT_SIZE * (1 << RSQF_STATIC_FP_SIZE));
constexpr auto SIZE_KEY_INFINI_LOG = MAX_INFINI_EXP + INIT_SIZE_LOG;
constexpr size_t TAIL_LATENCY_CNT = 1000;
std::mt19937 gen(12345); // Mersenne Twister random number generator
ycsbc::ZipfianGenerator zipfian_gen(1, NUM_KEYS_TOTAL);

inline size_t get_random_key(size_t min, size_t max) {
    std::uniform_int_distribution<size_t> dist(min, max);
    return dist(gen);
}

std::vector<size_t> _generate_zipfian_keys(size_t kTotalKeys, size_t count,
                                           size_t min, size_t max) {
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

std::vector<bool> generate_random_50_50(size_t count) {
    std::vector<bool> keys;
    for (size_t j = 0; j < count; ++j) {
        keys.push_back(get_random_key(0, 1));
    }
    return keys;
}

std::vector<size_t> generate_random_keys(size_t min, size_t max, size_t count) {
    return _generate_zipfian_keys(NUM_KEYS_TOTAL, count, min, max);
}

std::vector<size_t> generate_random_keys_uniform(size_t min, size_t max,
                                                  size_t count) {
    std::vector<size_t> keys;
    for (size_t j = 0; j < count; ++j) {
        keys.push_back(get_random_key(min, max));
    }
    return keys;
}

// New helper function: returns true when the logarithm of the current step
// crosses a new 0.1 interval. This ensures more frequent sampling in the
// beginning.
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
std::vector<T> select_percentiles(std::vector<T> data,
                                  const std::vector<double> &percentiles) {
    std::sort(data.begin(), data.end());
    std::vector<T> results;
    results.reserve(percentiles.size());
    for (double percentile : percentiles) {
        size_t index =
            static_cast<size_t>(std::ceil(percentile * data.size() / 100.0)) -
            1;
        results.push_back(data[index]);
    }
    return results;
}

// filter helper functions
template <typename Traits>
double get_lf_filter(const memento::Memento<Traits::IS_INFINI> &filter,
                     const size_t i) {
    return static_cast<double>(i) /
           static_cast<double>(filter.metadata_->nslots);
}

template <typename Traits>
double get_fpr(const memento::Memento<Traits::IS_INFINI> &filter,
               const size_t init_slot_log, const size_t num_keys_inserted) {
    auto load_factor = get_lf_filter<Traits>(filter, num_keys_inserted);
    auto fpr = load_factor / (1 << filter.metadata_->fingerprint_bits);

    if (Traits::IS_INFINI) {
        auto number_of_expansions =
            std::floor(std::log2(filter.metadata_->nslots)) - init_slot_log;
        fpr = 0.5 * (1.0 / (1 << filter.metadata_->fingerprint_bits)) *
              (load_factor) * (number_of_expansions + 2);
        std::cout << "FPR: " << fpr << std::endl;
        std::cout << " num Exps: " << number_of_expansions << std::endl;
        std::cout << " loadf: " << load_factor << std::endl;
    }
    return fpr;
}

template <typename Traits>
bool would_expand(const memento::Memento<Traits::IS_INFINI> &filter) {
    return filter.metadata_->noccupied_slots >=
               filter.metadata_->nslots * filter.metadata_->max_lf ||
           filter.metadata_->noccupied_slots + 1 >= filter.metadata_->nslots;
}

template <typename Traits>
std::vector<uint64_t>
get_range(const memento::Memento<Traits::IS_INFINI> &filter, uint64_t key) {
    auto mem_bits = filter.get_num_memento_bits();
    uint64_t b_key_l = key << mem_bits;
    uint64_t b_key_r = b_key_l | 0xFFFFFFFFull;
    auto it = filter.begin(/*l_key*/ b_key_l, /*r_key*/ b_key_r);

    auto res_vec = std::vector<uint64_t>();

    while (it != filter.end()) {
        uint64_t full_key = *it;

        const uint64_t memento_mask =
            (1ULL << filter.get_num_memento_bits()) - 1;
        uint64_t extracted_memento = full_key & memento_mask;
        res_vec.push_back(extracted_memento);
        ++it; // Move to next element
    }
    return res_vec;
}

template <typename Traits>
void readFilter(const memento::Memento<Traits::IS_INFINI> &filter,
                const SSDLog<Traits> &ssdLog, long selectedKey) {
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

template <typename Traits>
void putFilter(memento::Memento<Traits::IS_INFINI> &filter,
               const SSDLog<Traits> &ssdLog, long selectedKey,
               DefaultTraits::PAYLOAD_TYPE pt) {
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
            auto res_delete = filter.delete_single(
                selectedKey, r, memento::Memento<true>::flag_no_lock);
            if (res_delete != 0) {
                throw std::invalid_argument(
                    "Error: key delete unsuccessful in filter");
            }
            auto res_insert = filter.insert(
                selectedKey, pt, memento::Memento<true>::flag_no_lock);
            if (res_insert < 0) {
                throw std::invalid_argument(
                    "Error: key update(insert) unsuccessful in filter");
            }
            return;
        }
    }
    if (!flag) {
        auto res_insert = filter.insert(selectedKey, pt,
                                        memento::Memento<true>::flag_no_lock);
        if (res_insert < 0) {
            throw std::invalid_argument(
                "Error: key insert unsuccessful in filter");
        }
    }
}
template <typename Traits>
void updateFilter(memento::Memento<Traits::IS_INFINI> &filter,
                  const SSDLog<Traits> &ssdLog, long selectedKey,
                  int32_t newMem) {
    auto res = get_range<Traits>(filter, selectedKey);
    // std::cout << "key: " << selectedKey << " - read set size: " << res.size()
    // << std::endl;
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
    auto res_update = filter.update_single(
        selectedKey, memento, newMem, memento::Memento<true>::flag_no_lock);
    if (res_update != 0) {
        throw std::invalid_argument("Error: key update unsuccessful in filter");
    }
}

// Perfect HT helper functions
template <typename Traits>
void insert_pht(memento::Memento<true> &filter,
                std::unordered_map<size_t, size_t> &stash_dict,
                SSDLog<Traits> &ssdLog, const size_t key, const size_t value) {
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
    for (auto &r : res) {
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
void query_pht(memento::Memento<true> &filter,
               std::unordered_map<size_t, size_t> &stash_dict,
               SSDLog<Traits> &ssdLog, unsigned long vv) {
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
void update_pht(memento::Memento<true> &filter,
                std::unordered_map<size_t, size_t> &stash_dict,
                SSDLog<Traits> &ssdLog, unsigned long uk, unsigned long uv) {
    auto new_value = uv;
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
double get_memory_filter(memento::Memento<Traits::IS_INFINI> &filter,
                         bool including_ptr = false) {
    size_t reduced_size =
        filter.get_num_memento_bits() * (filter.metadata_->xnslots);
    if (including_ptr)
        reduced_size = 0;
    return static_cast<double>(8 * filter.size_in_bytes() - reduced_size);
}

template <typename Traits>
size_t get_memory_stash(std::unordered_map<size_t, size_t> &stash_dict) {
    // size_t stash_memory = stash_dict.bucket_count() * (sizeof(void*) +
    // sizeof(size_t))); this is in favor of pht
    size_t stash_memory = stash_dict.bucket_count() * sizeof(size_t);
    stash_memory *= 8;
    return stash_memory;
}
// Function to perform insertions into the directory
template <typename Traits>
void Run(Directory<Traits> &dir, SSDLog<Traits> &ssdLog, size_t numKeys,
         const std::string &folder) {
    Metrics metrics;
    for (size_t i = 1; i <= numKeys; ++i) {
        if (should_sample(i))
            std::cout << i << std::endl;
        // insertion
        const auto key = i;
        const auto value = key;
        auto pt = ssdLog.write(key, value);
        dir.writeSegmentSingleThread(key, value, ssdLog, pt);
        if (should_sample(i)) {
            auto v = generate_random_keys(1, i, 2 * RANDOM_CNT);
            auto updated_v = generate_random_keys_uniform(1, i, 2 * RANDOM_CNT);
            auto upd_or_read = generate_random_50_50(2 * RANDOM_CNT);
            auto start = std::chrono::high_resolution_clock::now();
            for (int j = 0; j < 2 * RANDOM_CNT; ++j) {
                auto vv = v[j];
                auto is_update = upd_or_read[j];
                if (!is_update) {
                    dir.readSegmentSingleThread(vv, ssdLog);
                } else {
                    auto new_value = updated_v[j];
                    auto pt_update = ssdLog.write(vv, new_value);
                    dir.updateSegmentSingleThread(vv, new_value, ssdLog,
                                                  pt_update);
                }
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto du = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          end - start)
                          .count();
            auto ops_per_sec = 2 * RANDOM_CNT / (du / 1e9);
            metrics.record("ops/sec", ops_per_sec);
            std::cout << "i : " << i << " - ops/sec: " << ops_per_sec
                      << std::endl;
        }

        if (should_sample(i)) {
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
    metrics.printToFile({"num_entries", "ops/sec"}, folder + "/benchmark_",
                        name);
}

// Function to perform insertions into the directory (Perfect HT variant)
template <typename Traits>
void RunPHT(memento::Memento<true> &filter,
            std::unordered_map<size_t, size_t> &stash_dict,
            SSDLog<Traits> &ssdLog, size_t numKeys, const std::string &folder) {
    Metrics metrics;
    for (size_t i = 1; i <= numKeys; ++i) {
        const auto key = i;
        const auto value = key;
        insert_pht<Traits>(filter, stash_dict, ssdLog, key, value);
        const bool gonna_expand = would_expand<Traits>(filter);
        if (should_sample(i, gonna_expand)) {
            auto v = generate_random_keys(1, i, 2 * RANDOM_CNT);
            auto updated_v = generate_random_keys_uniform(1, i, 2 * RANDOM_CNT);
            auto upd_or_read = generate_random_50_50(2 * RANDOM_CNT);
            auto start = std::chrono::high_resolution_clock::now();
            for (int j = 0; j < 2 * RANDOM_CNT; ++j) {
                auto vv = v[j];
                auto is_update = upd_or_read[j];
                if (!is_update) {
                    query_pht<Traits>(filter, stash_dict, ssdLog, vv);
                } else {
                    auto new_value = updated_v[j];
                    update_pht<Traits>(filter, stash_dict, ssdLog, vv, new_value);
                }
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto du = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          end - start)
                          .count();
            auto ops_per_sec = 2 * RANDOM_CNT / (du / 1e9);
            metrics.record("ops/sec", ops_per_sec);
            std::cout << "i : " << i << " - ops/sec: " << ops_per_sec
                      << std::endl;
        }

        if (should_sample(i, gonna_expand)) {
            metrics.record("num_entries", i);
        }
    }

    metrics.printToFile({"num_entries", "ops/sec"}, folder + "/benchmark_",
                        "Perfect_HT");
}

// Function to perform insertions into the directory (InfiniFilter variant)
template <typename Traits>
void RunFilter(memento::Memento<Traits::IS_INFINI> &filter,
               SSDLog<Traits> &ssdLog, size_t numKeys, bool should_exp,
               const std::string &folder) {
    Metrics metrics;
    for (size_t i = 1; i <= numKeys; ++i) {
        const auto key = i;
        const auto value = key;
        auto pt = ssdLog.write(key, value);
        putFilter<Traits>(filter, ssdLog, key, pt);
        const bool gonna_expand = would_expand<Traits>(filter);

        if (should_sample(i, gonna_expand)) {
            auto v = generate_random_keys(1, i, 2 * RANDOM_CNT);
            auto updated_v = generate_random_keys_uniform(1, i, 2 * RANDOM_CNT);
            auto upd_or_read = generate_random_50_50(2 * RANDOM_CNT);
            auto start = std::chrono::high_resolution_clock::now();
            for (int j = 0; j < 2 * RANDOM_CNT; ++j) {
                auto vv = v[j];
                auto is_update = upd_or_read[j];
                if (!is_update) {
                    readFilter(filter, ssdLog, vv);
                } else {
                    auto new_value = updated_v[j];
                    auto pt_update = ssdLog.write(vv, new_value);
                    updateFilter<Traits>(filter, ssdLog, vv, pt_update);
                }
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto du = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          end - start)
                          .count();
            auto ops_per_sec = 2 * RANDOM_CNT / (du / 1e9);
            metrics.record("ops/sec", ops_per_sec);
            std::cout << "i : " << i << " - ops/sec: " << ops_per_sec
                      << std::endl;
        }

        if (should_sample(i, gonna_expand)) {
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

    metrics.printToFile({"num_entries", "ops/sec"}, folder + "/benchmark_",
                        name);
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
    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>(ssdLogPath,
                                                          APPEND_ONLY_LOG_SIZE);

    Run(dir, *ssdLog, NUM_KEYS_TOTAL, folder);
}

// PerformTest function with added CSV output
template <typename TemplateClass>
void performTestFilterRSQF(const std::string &ssdLogPath,
                           const std::string &folder) {
    memento::Memento<TestRSQF::IS_INFINI> filter(
        INIT_SIZE,                /* nslots */
        12 + RSQF_STATIC_FP_SIZE, /* key_bits (fingerprint size) */
        32,                       /* memento_bits = payload bit size */
        memento::Memento<TemplateClass::IS_INFINI>::hashmode::Default,
        12345 /* seed */, 0, 1);
    filter.set_auto_resize(true);

    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>(ssdLogPath,
                                                          APPEND_ONLY_LOG_SIZE);

    RunFilter(filter, *ssdLog, NUM_KEYS_TOTAL, true, folder);
}

// PerformTest function with added CSV output
template <typename TemplateClass>
void performTestFilterInfini(const std::string &ssdLogPath,
                             const std::string &folder) {
    memento::Memento<TestInfini::IS_INFINI> filter(
        INIT_SIZE,           /* nslots */
        SIZE_KEY_INFINI_LOG, /* key_bits (fingerprint size) */
        32,                  /* memento_bits = payload bit size */
        memento::Memento<TemplateClass::IS_INFINI>::hashmode::Default,
        12345 /* seed */, 0, 1);
    filter.set_auto_resize(true);

    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>(ssdLogPath,
                                                          APPEND_ONLY_LOG_SIZE);

    RunFilter(filter, *ssdLog, NUM_KEYS_TOTAL, true, folder);
}

// PerformTest function with added CSV output
template <typename TemplateClass>
void performTestPHT(const std::string &ssdLogPath, const std::string &folder) {
    memento::Memento<TestInfini::IS_INFINI> filter(
        INIT_SIZE,                          /* nslots */
        INIT_SIZE_LOG + HASH_TABLE_FP_SIZE, /* key_bits (fingerprint size) */
        32, /* memento_bits = payload bit size */
        memento::Memento<TemplateClass::IS_INFINI>::hashmode::Default,
        12345 /* seed */, 0, 1);
    filter.set_auto_resize(true);
    std::unordered_map<size_t, size_t> stash_dict;
    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>(ssdLogPath,
                                                          APPEND_ONLY_LOG_SIZE);

    RunPHT(filter, stash_dict, *ssdLog, NUM_KEYS_TOTAL, folder);
}

int main() {
#ifndef IN_MEMORY_FILE
    std::vector<std::pair<std::string, std::string>> configs = {
        // {
            // HOME + "/research/sphinx/benchmark/data-optane-zipf",
            // "/optane/log/directory_test.txt"},
        {HOME + "/research/sphinx/benchmark/data-ssd-zipf",
         "/data/fleck/directory_test.txt"},
    };
    // Iterate over each configuration
    for (const auto &[dataFolder, ssdLogPath] : configs) {
        std::cout << "Running tests with folder: " << dataFolder
                  << " and SSD log: " << ssdLogPath << std::endl;
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
    auto dataFolder =
        HOME + "/research/sphinx/benchmark/data-memory-zipf";
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
