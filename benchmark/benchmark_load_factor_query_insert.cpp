#include <chrono>
#include <iostream>
#include <iterator>
#include <memory>
#include <vector>
#include <random>
#include <cmath>
#include <numeric>
#include <unordered_map>
#include <filesystem>    // For directory creation (C++17)

#include "../directory/directory.h"
#include "recorder/recorder.h"
#include "../lib/memento/memento.hpp"

//----------------------------------------------------------------------
// Global settings and helper functions
//----------------------------------------------------------------------

constexpr size_t NUM_KEYS_TOTAL = 1 << 12;
constexpr size_t extra_numKeys = 2 * NUM_KEYS_TOTAL;
std::string HOME = std::getenv("HOME");
constexpr size_t SMPL = 5; // left for backward compatibility if needed
std::mt19937 gen(12345); // Mersenne Twister random number generator
auto RANDOM_CNT = 50; // (will be replaced by 20 in one case below)

// New averaging window constant:
constexpr size_t SAMPLE_WINDOW = 5;

inline size_t get_random_key(size_t min, size_t max) {
    std::uniform_int_distribution<size_t> dist(min, max);
    return dist(gen);
}

std::vector<size_t> generate_random_keys(size_t min, size_t max, size_t count) {
    std::vector<size_t> keys;
    for (size_t j = 0; j < count; ++j) {
        keys.push_back(get_random_key(min, max));
    }
    return keys;
}

// New helper function: returns true when the logarithm of the current step
// crosses a new 0.1 interval. This ensures more frequent sampling in the beginning.
bool should_sample(size_t step) {
    return step % SMPL == 0;
}

template<typename Traits>
inline std::vector<uint64_t> get_range(const memento::Memento<Traits::IS_INFINI> &filter, uint64_t key) {
    auto mem_bits = filter.get_num_memento_bits();
    uint64_t b_key_l = key << mem_bits;
    uint64_t b_key_r = b_key_l | 0xFFFFFFFFull;
    auto it = filter.begin(/*l_key*/ b_key_l, /*r_key*/ b_key_r);

    std::vector<uint64_t> res_vec;
    while (it != filter.end()) {
        uint64_t full_key = *it;
        const uint64_t memento_mask = (1ULL << filter.get_num_memento_bits()) - 1;
        uint64_t extracted_memento = full_key & memento_mask;
        res_vec.push_back(extracted_memento);
        ++it;
    }
    return res_vec;
}

template<typename Traits>
inline void readFilter(const memento::Memento<Traits::IS_INFINI> &filter, const SSDLog<Traits> &ssdLog, long selectedKey) {
    auto res = get_range<Traits>(filter, selectedKey);
    bool flag = false;
    for (auto &r : res) {
        DefaultTraits::ENTRY_TYPE entry_type;
        ssdLog.read(r, entry_type);
        if (entry_type.key == selectedKey) {
            flag = true;
        }
    }
    if (!flag) {
        std::cout << " : key not found\n";
        throw std::invalid_argument("Error: key not found");
    }
}

template<typename Traits>
int putFilter(memento::Memento<Traits::IS_INFINI> &filter, const SSDLog<Traits> &ssdLog, long selectedKey, DefaultTraits::PAYLOAD_TYPE pt) {
    auto res = get_range<Traits>(filter, selectedKey);
    if (res.empty()) {
        return filter.insert(selectedKey, pt, memento::Memento<true>::flag_no_lock);
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
            return filter.insert(selectedKey, pt, memento::Memento<true>::flag_no_lock);
        }
    }
    if (!flag) {
        return filter.insert(selectedKey, pt, memento::Memento<true>::flag_no_lock);
    }
}
//----------------------------------------------------------------------
// Modified Functions: Run, RunPHT, and RunFilter with averaged (smoothed)
// metrics and random-key reads.
//----------------------------------------------------------------------

// Function to perform insertions into the directory
template <typename Traits>
void Run(Directory<Traits>& dir,
         SSDLog<Traits>& ssdLog,
         size_t numKeys)
{
    Metrics metrics;
    uint64_t total_insertion_time = 0;
    uint64_t total_query_time = 0;
    size_t sample_count = 0;

    for (size_t i = 1; i <= numKeys; ++i) {
        const auto key = i;
        const auto value = key;
        auto start_insertion = std::chrono::high_resolution_clock::now();
        auto pt = ssdLog.write(key, value);
        auto res = dir.writeSegmentSingleThread(key, value, ssdLog, pt);
        auto end_insertion = std::chrono::high_resolution_clock::now();
        auto du_insertion = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insertion - start_insertion).count();

        // (Optionally, you can still print every SMPL-th operation)
        if (!res) {
            std::cout << "Failed to insert key: " << key << std::endl;
            break;
        }

        // For the query, we now generate a random key in the range [1, i]
        size_t randomKey = get_random_key(1, i);
        auto start_query = std::chrono::high_resolution_clock::now();
        dir.readSegmentSingleThread(randomKey, ssdLog);
        auto end_query = std::chrono::high_resolution_clock::now();
        auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count();

        // Accumulate the measurements:
        total_insertion_time += du_insertion;
        total_query_time += du_query;
        ++sample_count;

        // Every SAMPLE_WINDOW iterations, compute averages and record metrics:
        if (i % SAMPLE_WINDOW == 0) {
            uint64_t avg_insertion_time = total_insertion_time / sample_count;
            uint64_t avg_query_time = total_query_time / sample_count;
            metrics.record("insertion_time", avg_insertion_time);
            metrics.record("query_time", avg_query_time);
            auto LF = dir.get_load_factor(i);
            metrics.record("LF", LF);
            metrics.record("num_entries", i);
            auto memory_footprint = dir.get_memory_footprint(i);
            metrics.record("memory", memory_footprint);

            // Optionally flush data to file every SAMPLE_WINDOW writes
            // (Assuming metrics.printToFile overwrites or appends as needed.)
            // metrics.printToFile( { "num_entries", "insertion_time", "query_time", "LF", "memory" },
            //                      HOME + "/research/sphinx/benchmark/data-lf/benchmark_[Name]_partial");

            // Reset accumulators:
            total_insertion_time = 0;
            total_query_time = 0;
            sample_count = 0;
        }
    }

    std::string name;
    if constexpr (Traits::DHT_EVERYTHING) {
        name = "DHT";
    } else if constexpr (Traits::READ_OFF_STRATEGY == 20) {
        name = "Fleck-Loop";
    } else {
        name = "Fleck";
    }
    metrics.printToFile( { "num_entries", "insertion_time", "query_time", "LF", "memory" },
                         HOME + "/research/sphinx/benchmark/data-lf/benchmark_", name);
}


// Function to perform insertions into the directory (Perfect HT variant)
template <typename Traits>
void RunPHT(memento::Memento<true>& filter,
            std::unordered_map<size_t, size_t>& stash_dict,
            SSDLog<Traits>& ssdLog,
            size_t numKeys)
{
    Metrics metrics;
    uint64_t total_insertion_time = 0;
    uint64_t total_query_time = 0;
    size_t sample_count = 0;

    for (size_t i = 1; i <= numKeys; ++i) {
        if (i % SAMPLE_WINDOW == 0)
            std::cout << "PHT insertion: " << i << std::endl;
        const auto key = i;
        const auto value = key;
        auto start_insertion = std::chrono::high_resolution_clock::now();
        {
            auto pt = ssdLog.write(key, value);
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
                auto res = filter.insert(key, pt, memento::Memento<true>::flag_no_lock);
                if (res == memento::Memento<true>::err_no_space)
                    break;
            }
        }
        auto end_insertion = std::chrono::high_resolution_clock::now();
        auto du_insertion = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insertion - start_insertion).count();

        // For the query we generate a random key from the range [1, i]
        size_t randomKey = get_random_key(1, i);
        auto start_query = std::chrono::high_resolution_clock::now();
        {
            auto it = stash_dict.find(randomKey);
            DefaultTraits::ENTRY_TYPE entry_type;
            if (it != stash_dict.end()) {
                ssdLog.read(it->second, entry_type);
                if (entry_type.key != randomKey) {
                    readFilter<Traits>(filter, ssdLog, randomKey);
                }
            } else {
                readFilter<Traits>(filter, ssdLog, randomKey);
            }
        }
        auto end_query = std::chrono::high_resolution_clock::now();
        auto du_query = std::chrono::duration_cast<std::chrono::nanoseconds>(end_query - start_query).count();

        total_insertion_time += du_insertion;
        total_query_time += du_query;
        ++sample_count;

        if (i % SAMPLE_WINDOW == 0) {
            uint64_t avg_insertion_time = total_insertion_time / sample_count;
            uint64_t avg_query_time = total_query_time / sample_count;
            metrics.record("insertion_time", avg_insertion_time);
            metrics.record("query_time", avg_query_time);
            // Here, LF is computed using the filter’s nslots and stash dictionary
            auto LF = (double) i / (filter.metadata_->nslots + stash_dict.bucket_count() * 4);
            metrics.record("LF", LF);
            metrics.record("num_entries", i);
            auto memento_memory = static_cast<double>(8 * filter.size_in_bytes() - filter.get_num_memento_bits() * (filter.metadata_->xnslots));
            auto stash_memory = (stash_dict.size() * (sizeof(size_t) + sizeof(void*)) + stash_dict.bucket_count() * (sizeof(void*) + sizeof(size_t))) * 8;
            auto memory_footprint = (stash_memory + memento_memory) / i;
            metrics.record("memory", memory_footprint);

            total_insertion_time = 0;
            total_query_time = 0;
            sample_count = 0;
        }
    }
    
    metrics.printToFile( { "num_entries", "insertion_time", "query_time", "LF", "memory" },
                         HOME + "/research/sphinx/benchmark/data-lf/benchmark_", "Perfect_HT");
}


// Function to perform insertions into the directory (InfiniFilter variant)
template <typename Traits>
void RunFilter(memento::Memento<Traits::IS_INFINI>& filter,
               SSDLog<Traits>& ssdLog,
               size_t numKeys,
               bool should_exp = true)
{
    auto init_slot_log = std::floor(std::log2(filter.metadata_->nslots));
    Metrics metrics;
    uint64_t total_insertion_time = 0;
    uint64_t total_query_time = 0;
    size_t sample_count = 0;

    for (size_t i = 1; i <= numKeys; ++i) {
        if (i % SAMPLE_WINDOW == 0)
            std::cout << "InfiniFilter insertion: " << i << std::endl;
        const auto key = i;
        const auto value = key;
        auto start_insertion = std::chrono::high_resolution_clock::now();
        auto pt = ssdLog.write(key, value);
        auto res = putFilter(filter, ssdLog, key, pt);
        auto end_insertion = std::chrono::high_resolution_clock::now();
        auto du_insertion = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insertion - start_insertion).count();
        if (res == memento::Memento<true>::err_no_space)
            break;
        total_insertion_time += du_insertion;

        // For the query, generate 20 random keys (instead of using random_key_num = 50)
        auto query_keys = generate_random_keys(1, i, 20);
        uint64_t query_time_sum = 0;
        for (auto qKey : query_keys) {
            auto start_q = std::chrono::high_resolution_clock::now();
            readFilter(filter, ssdLog, qKey);
            auto end_q = std::chrono::high_resolution_clock::now();
            query_time_sum += std::chrono::duration_cast<std::chrono::nanoseconds>(end_q - start_q).count();
        }
        uint64_t avg_query_this_iteration = query_time_sum / 20;
        total_query_time += avg_query_this_iteration;
        ++sample_count;

        if (i % SAMPLE_WINDOW == 0) {
            uint64_t avg_insertion_time = total_insertion_time / sample_count;
            uint64_t avg_query_time = total_query_time / sample_count;
            metrics.record("insertion_time", avg_insertion_time);
            metrics.record("query_time", avg_query_time);
            auto memory_footprint = static_cast<double>(8 * filter.size_in_bytes() - filter.get_num_memento_bits() * (filter.metadata_->xnslots)) / i;
            metrics.record("memory", memory_footprint);
            auto load_factor = static_cast<double>(i) / static_cast<double>(filter.metadata_->nslots);
            metrics.record("LF", load_factor);
            metrics.record("num_entries", i);

            total_insertion_time = 0;
            total_query_time = 0;
            sample_count = 0;
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
    metrics.printToFile( { "num_entries", "insertion_time", "query_time", "memory", "LF" },
                         HOME + "/research/sphinx/benchmark/data-lf/benchmark_", name);
}

//----------------------------------------------------------------------
// Test functions: performTest, performTestFilterRSQFNoExp, performTestPHT, and main
//----------------------------------------------------------------------

// PerformTest function with added CSV output
template <typename TemplateClass>
void performTest() {
    auto log_size_dir = std::floor(std::log2(NUM_KEYS_TOTAL / 4096));
    std::cout << "log size: " << log_size_dir << std::endl;
    Directory<TemplateClass> dir(log_size_dir, 1);
    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>("directory_cpu_test.txt", 1000000);
    Run(dir, *ssdLog, extra_numKeys);
}

// PerformTest function for RSQFNoExp
void performTestFilterRSQFNoExp() {
    size_t key_bits_size = std::floor(std::log2(NUM_KEYS_TOTAL)) + 15;
    memento::Memento<TestRSQF::IS_INFINI> filter(NUM_KEYS_TOTAL, /* nslots */
                                                 key_bits_size,   /* key_bits (fingerprint size) */
                                                 32,  /* memento_bits = payload bit size */
                                                 memento::Memento<TestRSQF::IS_INFINI>::hashmode::Default,
                                                 12345 /* seed */);
    filter.metadata_->max_lf = 0.99;
    filter.set_auto_resize(false);
    auto ssdLog = std::make_unique<SSDLog<TestRSQFInMem>>("directory_cpu_test.txt", 1000000);
    RunFilter(filter, *ssdLog, extra_numKeys, false);
}

// PerformTest function for Perfect HT
void performTestPHT() {
    size_t key_bits_size = std::floor(std::log2(NUM_KEYS_TOTAL)) + 7;
    memento::Memento<TestInfini::IS_INFINI> filter(NUM_KEYS_TOTAL, /* nslots */
                                                    key_bits_size,   /* key_bits (fingerprint size) */
                                                    32,  /* memento_bits = payload bit size */
                                                    memento::Memento<TestInfini::IS_INFINI>::hashmode::Default,
                                                    12345 /* seed */);
    filter.metadata_->max_lf = 0.99;
    filter.set_auto_resize(false);
    std::unordered_map<size_t, size_t> stash_dict;
    stash_dict.reserve(4);
    auto ssdLog = std::make_unique<SSDLog<TestInfiniInMem>>("directory_cpu_test.txt", 1000000);
    RunPHT(filter, stash_dict, *ssdLog, extra_numKeys);
}

int main() {
    std::string dataFolder = HOME + "/research/sphinx/benchmark/data-lf";
    std::filesystem::create_directories(dataFolder);
    // ===============================================================
    std::cout << " Test Perfect HT\n";
    performTestPHT();
    std::cout << " Test DHT\n";
    performTest<TestRealDHTInMemoryInMem>();
    std::cout << " Test Fleck\n";
    performTest<TestFleckInMemoryNoExpandInMem>();
    std::cout << " Test RSQFNoExp\n";
    performTestFilterRSQFNoExp();
    return 0;
}
