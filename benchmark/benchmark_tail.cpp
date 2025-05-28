#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

// Include your library headers
#include "../directory/directory.h"
#include "../lib/memento/memento.hpp"
#include "recorder/recorder.h"

// ––– Constants –––
constexpr size_t NUM_INSERT_KEYS = 0.93 * (1ull << 23);  // ~8M keys to insert
constexpr size_t NUM_QUERY_KEYS = 4000000;               // 4M query keys
constexpr int NUM_ROUNDS = 21;                           // 21 rounds of queries
const std::string HOME = std::getenv("HOME");
constexpr size_t INIT_SIZE_LOG = 12;
constexpr size_t INIT_SIZE = 1ull << INIT_SIZE_LOG;
constexpr auto MAX_INFINI_EXP = 5;
constexpr size_t SIZE_KEY_INFINI_LOG = MAX_INFINI_EXP + INIT_SIZE_LOG;

// ––– Helper Functions –––
template <typename Traits>
double get_lf_filter(const memento::Memento<Traits::IS_INFINI>& filter, const size_t i) {
    return static_cast<double>(i) / static_cast<double>(filter.metadata_->nslots);
}

template <typename Traits>
std::vector<uint64_t> get_range(const memento::Memento<Traits::IS_INFINI> &filter, uint64_t key) {
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
        ++it;  // Move to next element
    }
    return res_vec;
}

template <typename Traits>
void readFilter(const memento::Memento<Traits::IS_INFINI> &filter,
                const SSDLog<Traits> &ssdLog, long selectedKey) {
    auto res = get_range<Traits>(filter, selectedKey);
    if (res.empty()) {
        std::cout << "key not found\n";
        throw std::invalid_argument("Error: key not found");
    }
    bool found = false;
    for (auto &r : res) {
        DefaultTraits::ENTRY_TYPE entry_type;
        ssdLog.read(r, entry_type);
        if (entry_type.key == selectedKey) {
            found = true;
            break;
        }
    }
    if (!found) {
        std::cout << " : key not found\n";
        throw std::invalid_argument("Error: key not found");
    }
}

template <typename Traits>
double get_fpr(const memento::Memento<Traits::IS_INFINI> &filter,
               const size_t init_slot_log,
               const size_t num_keys_inserted) {
    auto load_factor = get_lf_filter<Traits>(filter, num_keys_inserted);
    auto fpr = load_factor / (1 << filter.metadata_->fingerprint_bits);

    if (Traits::IS_INFINI) {
        auto number_of_expansions = std::floor(std::log2(filter.metadata_->nslots)) - init_slot_log;
        fpr = 0.5 * (1.0 / (1 << filter.metadata_->fingerprint_bits)) *
              load_factor * (number_of_expansions + 2);
        std::cout << "FPR: " << fpr << std::endl;
        std::cout << " num Exps: " << number_of_expansions << std::endl;
        std::cout << " loadf: " << load_factor << std::endl;
    }
    return fpr;
}

// ––– Random Key Generation –––
std::mt19937 gen(12345);
std::vector<size_t> generate_random_keys(size_t min, size_t max, size_t count) {
    std::vector<size_t> keys;
    keys.reserve(count);
    std::uniform_int_distribution<size_t> dist(min, max);
    for (size_t i = 0; i < count; ++i)
        keys.push_back(dist(gen));
    return keys;
}

// ––– Query Timing Helper –––
// Runs the provided query operation for each key over NUM_ROUNDS rounds
// and returns a vector containing the median query duration for each key.
template <typename QueryOp>
std::vector<double> runQueryTest(const std::vector<size_t>& queryKeys,
                                 int numRounds,
                                 QueryOp queryOp) {
    std::vector<std::vector<double>> queryTimes(queryKeys.size());
    for (int round = 0; round < numRounds; ++round) {
        std::cout << "Round " << round << std::endl;
        for (size_t i = 0; i < queryKeys.size(); ++i) {
            size_t key = queryKeys[i];
            auto start = std::chrono::high_resolution_clock::now();
            queryOp(key);
            auto end = std::chrono::high_resolution_clock::now();
            double duration =
                std::chrono::duration<double, std::nano>(end - start).count();
            queryTimes[i].push_back(duration);
        }
    }
    // Compute median for each query key.
    std::vector<double> medians;
    medians.reserve(queryKeys.size());
    for (auto &times : queryTimes) {
        std::sort(times.begin(), times.end());
        double median;
        size_t n = times.size();
        if (n % 2 == 0)
            median = (times[n / 2 - 1] + times[n / 2]) / 2.0;
        else
            median = times[n / 2];
        medians.push_back(median);
    }
    return medians;
}

int main() {
    std::string dataFolder = HOME + "/research/dht/benchmark/data-tail";
    std::filesystem::create_directories(dataFolder);

    std::vector<std::string> ssdLogPaths = {
        "/data/fleck/directory_test.txt", // SSD path
        // "/optane/log/directory_tests.txt"   // Optane path
    };

    // Loop over each SSD log path.
    for (const auto& ssdLogPath : ssdLogPaths) {
        std::string suffix =
            (ssdLogPath.find("/optane/") != std::string::npos) ? "optane" : "ssd";

        auto queryKeys = generate_random_keys(1, NUM_INSERT_KEYS, NUM_QUERY_KEYS);
        // –––– InfiniFilter Test ––––
        // Create an InfiniFilter and its SSD log.
        {
            memento::Memento<TestInfini::IS_INFINI> infFilter(
                INIT_SIZE, SIZE_KEY_INFINI_LOG, 32,
                memento::Memento<TestInfini::IS_INFINI>::hashmode::Default,
                12345
            );
            infFilter.set_auto_resize(true);
            SSDLog<TestInfini> ssdLogInf(ssdLogPath, 1000000);

            // Insert keys into InfiniFilter.
            for (size_t key = 1; key <= NUM_INSERT_KEYS; ++key) {
                std::cout << "Inserting key: " << key << std::endl;
                auto pt = ssdLogInf.write(key, key);
                infFilter.insert(key, pt, memento::Memento<true>::flag_no_lock);
            }
            auto fpr = get_fpr<TestInfini>(infFilter, INIT_SIZE_LOG, NUM_INSERT_KEYS);
            // Write the false-positive rate to file.
            std::ofstream outFpr(dataFolder + "/infinifilter_fpr_" + suffix + ".txt");
            outFpr << fpr << "\n";
            outFpr.close();

            // Generate random query keys.

            // Measure median query times for InfiniFilter.
            auto medianTimesInf = runQueryTest(queryKeys, NUM_ROUNDS, [&](size_t key) {
                readFilter<TestInfini>(infFilter, ssdLogInf, key);
            });
            std::sort(medianTimesInf.begin(), medianTimesInf.end());
            std::ofstream outInf(dataFolder + "/infinifilter_query_times_" + suffix + ".csv");
            for (auto t : medianTimesInf)
                outInf << t << "\n";
            outInf.close();
        }

        // –––– Fleck Test ––––
        // Create a Fleck directory and its SSD log.
        {
            Directory<TestFleckInMemory> fleckDir(0, 1);
            SSDLog<TestFleckInMemory> ssdLogFleck(ssdLogPath, 1000000);

            // Insert keys into Fleck.
            for (size_t key = 1; key <= NUM_INSERT_KEYS; ++key) {
                std::cout << "Inserting key: " << key << std::endl;
                auto pt = ssdLogFleck.write(key, key);
                fleckDir.writeSegmentSingleThread(key, key, ssdLogFleck, pt);
            }

            // Measure median query times for Fleck.
            auto medianTimesFleck = runQueryTest(queryKeys, NUM_ROUNDS, [&](size_t key) {
                fleckDir.readSegmentSingleThread(key, ssdLogFleck);
            });
            std::sort(medianTimesFleck.begin(), medianTimesFleck.end());
            std::ofstream outFleck(dataFolder + "/fleck_query_times_" + suffix + ".csv");
            for (auto t : medianTimesFleck)
                outFleck << t << "\n";
            outFleck.close();
        }
    }

    return 0;
}

