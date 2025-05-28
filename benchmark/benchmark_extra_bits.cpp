#include <algorithm>  // For std::min
#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>  // For directory creation (C++17)
#include <iostream>
#include <iterator>
#include <memory>
#include <random>
#include <utility>  // For std::pair
#include <vector>
#include <set>

#include "../directory/directory.h"
#include "../lib/memento/memento.hpp"
#include "recorder/recorder.h"

// parameters SSD & Optane
constexpr size_t RSQF_STATIC_FP_SIZE = 15;
constexpr auto SAMPLE_LOG = 0.01;
constexpr auto MAX_INFINI_EXP = 5;
size_t BATCH_SIZE_DEF = 4000;
size_t BATCH_SIZE_REP = 500;

// consts
const std::string HOME = std::getenv("HOME");
constexpr size_t INIT_SIZE_LOG = 12;
constexpr size_t APPEND_ONLY_LOG_SIZE = 2000000;
constexpr size_t INIT_SIZE = 1ull << INIT_SIZE_LOG;
constexpr size_t NUM_KEYS_TOTAL = static_cast<size_t>(0.94 * INIT_SIZE * (1 << RSQF_STATIC_FP_SIZE));
constexpr auto SIZE_KEY_INFINI_LOG = MAX_INFINI_EXP + INIT_SIZE_LOG;
constexpr float ratio_exs_read = 0.75; 

std::mt19937 gen(12345);  // Mersenne Twister random number generator

// Helper to generate a random key between min and max (inclusive)
inline size_t get_random_key(size_t min, size_t max) {
    std::uniform_int_distribution<size_t> dist(min, max);
    return dist(gen);
}

std::pair<size_t, size_t> get_half(size_t small, size_t big) {
    auto big_min_po2 = floor(std::log2(big));
    auto big_biggest_po2 = static_cast<size_t>(std::pow(2, big_min_po2));
    auto small_min_po2 = floor(std::log2(small));
    auto small_biggest_po2 = static_cast<size_t>(std::pow(2, small_min_po2));

    if (big_biggest_po2 == small_biggest_po2) {
        return {small - small_biggest_po2, big - big_biggest_po2};
    }
    return {0, big - big_biggest_po2};
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

// Generate a vector of random keys
std::vector<size_t> generate_random_keys(size_t min, size_t max, size_t count) {
    std::vector<size_t> keys;
    keys.reserve(count);
    for (size_t j = 0; j < count; ++j) {
        keys.push_back(get_random_key(min, max));
    }
    return keys;
}

std::vector<size_t> generate_unique_random_keys(size_t min, size_t max, size_t count) {
    std::set<size_t> unique_keys;
    while (unique_keys.size() < count) {
        unique_keys.insert(get_random_key(min, max));
    }
    return std::vector<size_t>(unique_keys.begin(), unique_keys.end());
}

template <typename Traits>
bool would_expand(const memento::Memento<Traits::IS_INFINI>& filter) {
    return filter.metadata_->noccupied_slots >= filter.metadata_->nslots * filter.metadata_->max_lf ||
           filter.metadata_->noccupied_slots + 1 >= filter.metadata_->nslots;
}

template <typename Traits>
std::vector<uint64_t> get_range(const memento::Memento<Traits::IS_INFINI>& filter, uint64_t key) {
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
        ++it;  // Move to next element
    }
    return res_vec;
}

template <typename Traits>
void readFilter(const memento::Memento<Traits::IS_INFINI>& filter, const SSDLog<Traits>& ssdLog, long selectedKey) {
    auto res = get_range<Traits>(filter, selectedKey);
    if (res.empty()) {
        return;
    }
    auto flag = false;
    for (auto& r : res) {
        DefaultTraits::ENTRY_TYPE entry_type;
        ssdLog.read(r, entry_type);
        if (entry_type.key == selectedKey) {
            flag = true;
            break;
        }
    }
    if (!flag) {
        return;
    }
}

template <typename Traits>
void putFilter(memento::Memento<Traits::IS_INFINI>& filter, const SSDLog<Traits>& ssdLog, long selectedKey, DefaultTraits::PAYLOAD_TYPE pt) {
    auto res = get_range<Traits>(filter, selectedKey);
    if (res.empty()) {
        filter.insert(selectedKey, pt, memento::Memento<true>::flag_no_lock);
        return;
    }
    auto flag = false;
    for (auto& r : res) {
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

// memory helper functions
template <typename Traits>
double get_memory_filter(memento::Memento<Traits::IS_INFINI>& filter) { return static_cast<double>(8 * filter.size_in_bytes() - filter.get_num_memento_bits() * (filter.metadata_->xnslots)); }

// Helper function: returns true when the logarithm of the current step
// crosses a new 0.1 interval. This ensures more frequent sampling in the beginning.
bool should_sample(size_t step) {
//    std::cout << "Step: " << step << std::endl;
    if (step == 1)
        return true;
    if (step < 100000)
        return true;
    step /= BATCH_SIZE_DEF;
    double currentLog = std::log10(static_cast<double>(step));
    double prevLog = std::log10(static_cast<double>(step - 1));
    int currentBucket = static_cast<int>(std::floor(currentLog / SAMPLE_LOG));
    int prevBucket = static_cast<int>(std::floor(prevLog / SAMPLE_LOG));
    return (currentBucket != prevBucket);
}

template <typename Traits>
auto RunFilter(
    memento::Memento<Traits::IS_INFINI>& filter,
    SSDLog<Traits>& ssdLog,
    size_t total_writes,
    const bool should_exp,
    const std::string& folder) -> void {
    Metrics metrics;
    size_t current_key = 0;
    size_t prev_sample_key = 0;

    // Loop until we have done all the writes.
    while (current_key < total_writes) {
        // --- Batch of 100 writes ---
        size_t BATCH_SIZE = current_key > 100000 ? BATCH_SIZE_DEF : BATCH_SIZE_REP;
        size_t writes_this_batch = std::min(BATCH_SIZE, total_writes - current_key);
        long long batch_insertion_latency = 0;
        for (size_t i = 0; i < writes_this_batch; i++) {
            current_key++;  // New key to write
            size_t key = current_key;
            size_t value = key;  // For simplicity, value==key
            auto start_insertion = std::chrono::high_resolution_clock::now();
            auto pt = ssdLog.write(key, value);
            putFilter<Traits>(filter, ssdLog, key, pt);
            auto end_insertion = std::chrono::high_resolution_clock::now();
            const auto du_insertion = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insertion - start_insertion).count();
            batch_insertion_latency += du_insertion;
        }
        const long long avg_insertion_latency = batch_insertion_latency / writes_this_batch;

        std::vector<size_t> tail_exs_read;
        if (should_sample(current_key)) {
            std::vector<size_t> latency_values;
            auto pairs = get_half(prev_sample_key, current_key);
            prev_sample_key = current_key;
            auto left = pairs.first;
            auto right = pairs.second;
            size_t sampling_size = (right - left) * ratio_exs_read;
            auto random_keys = generate_unique_random_keys(left, right, sampling_size);

            for (auto key : random_keys) {
                auto start_read = std::chrono::high_resolution_clock::now();
                readFilter<Traits>(filter, ssdLog, key);
                auto end_read = std::chrono::high_resolution_clock::now();
                const long long latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_read - start_read).count();
                latency_values.push_back(latency);
            }

            auto percentiles = std::vector<double>{99.0};
            tail_exs_read = select_percentiles(latency_values, percentiles);
        }

        std::vector<size_t> tail_non_exs_read;
        if (should_sample(current_key)) {
            std::vector<size_t> latency_values;
            auto non_existent_keys = generate_random_keys(current_key + 1, current_key + 10 * BATCH_SIZE, BATCH_SIZE);
            for (auto key : non_existent_keys) {
                auto start_read = std::chrono::high_resolution_clock::now();
                                readFilter<Traits>(filter, ssdLog, key);
                auto end_read = std::chrono::high_resolution_clock::now();
                long long latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_read - start_read).count();
                latency_values.push_back(latency);
            }

            auto percentiles = std::vector<double>{99.0};
            tail_non_exs_read = select_percentiles(latency_values, percentiles);
        }

        long long avg_nonexistent_read_latency = 0;
        if (should_sample(current_key)) {
            auto nonexistent_keys = generate_random_keys(total_writes + 1, total_writes + 10000, BATCH_SIZE);
            long long total_nonexistent_read_latency = 0;
            for (auto key : nonexistent_keys) {
                auto start_read = std::chrono::high_resolution_clock::now();
                readFilter(filter, ssdLog, key);
                auto end_read = std::chrono::high_resolution_clock::now();
                long long latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_read - start_read).count();
                total_nonexistent_read_latency += latency;
            }
            avg_nonexistent_read_latency = total_nonexistent_read_latency / BATCH_SIZE;
        }

        if (should_sample(current_key)) {
            std::cout << "Current key: " << current_key << std::endl;
            double memory_footprint = get_memory_filter<Traits>(filter) / current_key;
            metrics.record("insertion_time", avg_insertion_latency);
            metrics.record("tail-99", tail_exs_read[0]);
            metrics.record("tail-99-non-exs", tail_non_exs_read[0]);
            metrics.record("non_exs_query_time", avg_nonexistent_read_latency);
            metrics.record("memory", memory_footprint);
            metrics.record("num_entries", current_key);
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

    metrics.printToFile(
        {"num_entries", "insertion_time",
         "non_exs_query_time", "memory",
         "tail-99", "tail-99-non-exs"},
        folder + "/benchmark_", name);
}

template <typename Traits>
void Run(Directory<Traits>& dir, SSDLog<Traits>& ssdLog, size_t total_writes, const std::string& folder) {
    Metrics metrics;
    size_t current_key = 0;
    size_t prev_sample_key = 0;

    // Loop until we have done all the writes.
    while (current_key < total_writes) {
        // --- Batch of 100 writes ---
        size_t BATCH_SIZE = current_key > 100000 ? BATCH_SIZE_DEF : BATCH_SIZE_REP;
        size_t writes_this_batch = std::min(BATCH_SIZE, total_writes - current_key);
        long long batch_insertion_latency = 0;
        for (size_t i = 0; i < writes_this_batch; i++) {
            current_key++;  // New key to write
            size_t key = current_key;
            size_t value = key;  // For simplicity, value==key
            auto start_insertion = std::chrono::high_resolution_clock::now();
            auto pt = ssdLog.write(key, value);
            dir.writeSegmentSingleThread(key, value, ssdLog, pt);
            auto end_insertion = std::chrono::high_resolution_clock::now();
            auto du_insertion = std::chrono::duration_cast<std::chrono::nanoseconds>(end_insertion - start_insertion).count();
            batch_insertion_latency += du_insertion;
        }
        long long avg_insertion_latency = batch_insertion_latency / writes_this_batch;

        std::vector<size_t> tail_exs_read;
        if (should_sample(current_key)) {
            std::vector<size_t> latency_values;

            auto pairs = get_half(prev_sample_key, current_key);
            prev_sample_key = current_key;
            auto left = pairs.first;
            auto right = pairs.second;
            size_t sampling_size = (right - left) * ratio_exs_read;
            auto random_keys = generate_unique_random_keys(left, right, sampling_size);

            for (auto key : random_keys) {
                auto start_read = std::chrono::high_resolution_clock::now();
                auto res = dir.readSegmentSingleThread(key, ssdLog);
                auto end_read = std::chrono::high_resolution_clock::now();
                long long latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_read - start_read).count();
                latency_values.push_back(latency);
            }

            auto percentiles = std::vector<double>{99.0};
            tail_exs_read = select_percentiles(latency_values, percentiles);
        }

        std::vector<size_t> tail_non_exs_read;
        if (should_sample(current_key)) {
            std::vector<size_t> latency_values;
            auto non_existent_keys = generate_random_keys(current_key + 1, current_key + 10 * BATCH_SIZE, BATCH_SIZE);
            for (auto key : non_existent_keys) {
                auto start_read = std::chrono::high_resolution_clock::now();
                auto res = dir.readSegmentSingleThread(key, ssdLog);
                auto end_read = std::chrono::high_resolution_clock::now();
                long long latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_read - start_read).count();
                latency_values.push_back(latency);
            }

            auto percentiles = std::vector<double>{99.0};
            tail_non_exs_read = select_percentiles(latency_values, percentiles);
        }

        long long avg_nonexistent_read_latency = 0;
        if (should_sample(current_key)) {
            auto nonexistent_keys = generate_random_keys(total_writes + 1, total_writes + 10000, BATCH_SIZE);
            long long total_nonexistent_read_latency = 0;
            for (auto key : nonexistent_keys) {
                auto start_read = std::chrono::high_resolution_clock::now();
                auto res = dir.readSegmentSingleThread(key, ssdLog);
                auto end_read = std::chrono::high_resolution_clock::now();
                long long latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end_read - start_read).count();
                total_nonexistent_read_latency += latency;
            }
            avg_nonexistent_read_latency = total_nonexistent_read_latency / BATCH_SIZE;
        }

        if (should_sample(current_key)) {
            std::cout << "Current key: " << current_key << std::endl;
            // auto average_age = dir.get_average_age();
            // std::cout << "Average age: " << average_age << std::endl;
            metrics.record("insertion_time", avg_insertion_latency);
            metrics.record("tail-99", tail_exs_read[0]);
            metrics.record("tail-99-non-exs", tail_non_exs_read[0]);
            metrics.record("non_exs_query_time", avg_nonexistent_read_latency);
            double memory_footprint = dir.get_memory_footprint(current_key);
            metrics.record("memory", memory_footprint);
            metrics.record("num_entries", current_key);
        }
    }

    // --- At the end, print all collected metrics to file ---
    std::string name;
    if constexpr (Traits::DHT_EVERYTHING) {
        name = "DHT";
    } else if constexpr (Traits::READ_OFF_STRATEGY == 20) {
        name = "Fleck-Loop";
    } else {
        name = "Fleck";
    }
    name += "_ExtraBits" + std::to_string(Traits::NUMBER_EXTRA_BITS);
    metrics.printToFile(
        {"num_entries", "insertion_time",
         "non_exs_query_time", "memory",
         "tail-99", "tail-99-non-exs"},
        folder + "/benchmark_", name);
}

/// ----------------------------------------------------------------------
///
/// performTest: prepares the directory and SSD log, then runs the test.
///
template <typename TemplateClass>
void performTest(const std::string& ssdLogPath, const std::string& folder) {
    auto log_ = INIT_SIZE / 4096;
    log_ = std::floor(std::log2(log_));
    size_t log_size_dir = log_;
    std::cout << "log size: " << log_size_dir << std::endl;

    Directory<TemplateClass> dir(log_size_dir, 1);
    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>(ssdLogPath, APPEND_ONLY_LOG_SIZE);
    Run(dir, *ssdLog, NUM_KEYS_TOTAL, folder);
}
// PerformTest function with added CSV output
void performTestFilterInfini(const std::string& ssdLogPath, const std::string& folder) {
    memento::Memento<TestInfini::IS_INFINI> filter(INIT_SIZE,           /* nslots */
                                                   SIZE_KEY_INFINI_LOG, /* key_bits (fingerprint size) */
                                                   32,                  /* memento_bits = payload bit size */
                                                   memento::Memento<TestInfini::IS_INFINI>::hashmode::Default,
                                                   12345 /* seed */ 
                                                   , 0, 1
                                                   );
    filter.set_auto_resize(true);

    auto ssdLog = std::make_unique<SSDLog<TestInfini>>(ssdLogPath, APPEND_ONLY_LOG_SIZE);

    RunFilter(filter, *ssdLog, NUM_KEYS_TOTAL, true, folder);
}
int main() {
    // Define a vector of configurations (folder for output and SSD log file path)
    std::vector<std::pair<std::string, std::string>> configs = {
        {HOME + "/research/sphinx/benchmark/data-extra-bits", "/data/fleck/directory_test_3.txt"},
    };

    // Iterate over each configuration folder
    for (const auto& [dataFolder, ssdLogPath] : configs) {
        std::cout << "Running tests with folder: " << dataFolder
                  << " and SSD log: " << ssdLogPath << std::endl;

        // Create the directory (and any necessary parent directories)
        std::filesystem::create_directories(dataFolder);

        // #ifdef IN_MEMORY_FILE
        // throw std::invalid_argument("This benchmark is not compatible with IN_MEMORY_FILE");
        // #endif
        // Run the test for all configuration types
        performTestFilterInfini(ssdLogPath, dataFolder);
        performTest<TestFleckInMemoryExtraBits1>(ssdLogPath, dataFolder);
        performTest<TestFleckInMemoryExtraBits2>(ssdLogPath, dataFolder);
        performTest<TestFleckInMemoryExtraBits4>(ssdLogPath, dataFolder);
        performTest<TestFleckInMemoryExtraBits6>(ssdLogPath, dataFolder);
    }

    return 0;
}
