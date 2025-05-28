#include <chrono>
#include <iostream>
#include <memory>
#include <vector>
#include <random>
#include <cmath>
#include <numeric>
#include <bitset>
#include <fstream>
#include <filesystem>
#include <algorithm>
#include <unordered_map>
#include <array>
#include <cstdlib>  // for std::getenv


#include "../directory/directory.h"
#include "recorder/recorder.h"

namespace fs = std::filesystem;

std::string HOME = std::getenv("HOME") ? std::getenv("HOME") : ".";

// Function to perform insertions into the directory
template <typename Traits>
void performInsertions(
        Directory<Traits>& dir,
        SSDLog<Traits>& ssdLog,
        const std::vector<TestCPU::KEY_TYPE>& keys,
        size_t numKeys)
{
    Metrics metrics;
    for (size_t i = 0; i < numKeys; ++i) {
        auto ii = numKeys - 1 - i;
        const auto key = keys[ii];
        const auto value = key;
        auto pt = ssdLog.write(key, value);
        dir.writeSegmentSingleThread(key, value, ssdLog, pt);
    }
}

// PerformTest function with added CSV output
template <typename TemplateClass>
void performTest(double inBufferRatio, std::ofstream &csvFile, const std::string &testName, float buffer_size_ratio) {
    constexpr size_t numKeys = BASE_EXP_INSERT_SIZE;
    constexpr size_t exp_size = 1 << 18;
    std::vector<TestCPU::KEY_TYPE> keys(numKeys);
    std::iota(keys.begin(), keys.end(), 1);

    unsigned int seed = 12345; // Replace with your desired seed
    std::mt19937 gen(seed);

    Directory<TemplateClass> dir(0, 1);
    // auto ssdLog = std::make_unique<SSDLog<TemplateClass>>("/data/fleck/directory_cpu_test.txt", 1000000);
    auto ssdLog = std::make_unique<SSDLog<TemplateClass>>("/data/fleck/directory_real.txt", 1000000);

    performInsertions(dir, *ssdLog, keys, numKeys);
    auto bufferKeys = ssdLog->BP->getAllKeys();

    // Prepare the combined key vector based on inBufferRatio
    std::vector<TestCPU::KEY_TYPE> exp_keys;
    size_t bufferKeyCount = static_cast<size_t>(exp_size * inBufferRatio);
    size_t originalKeyCount = exp_size - bufferKeyCount;

    // Random sampling with replacement for bufferKeys
    for (size_t i = 0; i < bufferKeyCount; ++i) {
        exp_keys.push_back(bufferKeys[gen() % bufferKeys.size()]);
    }
    
    // Use the decayed type (removing reference) for the unordered_map key type.
    using key_type = std::decay_t<decltype(bufferKeys[0])>;
    std::unordered_map<key_type, bool> buffer_map;
    
    // Populate the unordered_map with the sampled buffer keys.
    for (const auto& key : bufferKeys) {
        buffer_map[key] = true;
    }
    
    // Random sampling with replacement for SSD keys,
    // but only add the key if it's not already in the buffer_map.
    size_t added = 0;
    while (added < originalKeyCount) {
        key_type candidate = keys[gen() % keys.size()];
        if (buffer_map.find(candidate) == buffer_map.end()) {
            exp_keys.push_back(candidate);
            ++added;
        }
    }


    // Shuffle the combined keys for unbiased queries
    std::shuffle(exp_keys.begin(), exp_keys.end(), gen);

    // Count the number of keys in exp_keys that are in bufferKeys
    size_t keysInBufferCount = std::count_if(exp_keys.begin(), exp_keys.end(),
                                             [&bufferKeys](const TestCPU::KEY_TYPE& key) {
                                                 return std::find(bufferKeys.begin(), bufferKeys.end(), key) != bufferKeys.end();
                                             });

    std::cout << "Number of keys in bufferKeys: " 
              << static_cast<double>(keysInBufferCount) / exp_keys.size() << std::endl;

    const auto rep_size = 10;
    std::array<int, rep_size> latencies;

    for (int i = 0; i < rep_size; ++i) {
        ssdLog->BP->reset_count_and_hit();
        auto lat_mean = performQueries(dir, exp_keys, *ssdLog);
        latencies[i] = lat_mean;
        std::cout << "Rep " << i << " done" << std::endl;
        std::cout << "Buffer hit ratio: " << ssdLog->BP->getCacheHitRatio() << std::endl;
        std::cout << "Lateny: " << lat_mean << std::endl;
    }
    
    std::sort(latencies.begin(), latencies.end());

    for (auto middleLatency : latencies) {
        csvFile << testName << "," << inBufferRatio << "," << buffer_size_ratio << "," << middleLatency << "\n";
    }
}

// Function to perform queries using a biased distribution
template <typename Traits>
int performQueries(
        Directory<Traits>& dir,
        const std::vector<TestCPU::KEY_TYPE>& exp_keys,
        SSDLog<Traits>& ssdLog,
        bool warm_up = false)
{
    Metrics metrics;

    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < exp_keys.size(); ++i) {
        auto selectedKey = exp_keys[i];
        dir.readSegmentSingleThread(selectedKey, ssdLog);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    if (warm_up) {
        return -1;
    }
    std::cout << "Mean: " << duration / exp_keys.size() << " ns" << std::endl;
    return duration / exp_keys.size();
}


int main() {
#ifndef ENABLE_BP_FOR_READ
    throw std::invalid_argument("ENABLE_BP_FOR_READ must be enabled");
#endif

    // Ensure the data-skew directory exists
    std::string dirPath = HOME + "/research/sphinx-review/benchmark/data-skew";
    if (!fs::exists(dirPath)) {
        fs::create_directory(dirPath);
    }
    std::ofstream csvFile(dirPath + "/read_latency.csv");
    csvFile << "name,buffer_hit_ratio,buffer_size_ratio,query_latency\n";

    std::vector<double> biasFactors = {
        0.6, 0.65,
        0.7,
        0.75, 
        0.8, 0.85, 
        0.9,  0.95,
        0.96, 0.97, 0.98, 0.99,
        1.0
    };

    for (auto biasFactor : biasFactors) {
        std::cout << "Bias factor: " << biasFactor << " Spinx-Loop" << std::endl;
        performTest<TestRead5DHT>(biasFactor, csvFile, "Sphinx-Loop", 0.05);
        std::cout << "Bias factor: " << biasFactor << " Spinx" << std::endl;
        performTest<TestRead5>(biasFactor, csvFile, "Sphinx", 0.05);
        std::cout << "Bias factor: " << biasFactor << " Spinx-Loop" << std::endl;
        performTest<TestRead10DHT>(biasFactor, csvFile, "Sphinx-Loop", 0.1);
        std::cout << "Bias factor: " << biasFactor << " Spinx" << std::endl;
        performTest<TestRead10>(biasFactor, csvFile, "Sphinx", 0.1);
    }

    csvFile.close();
    return 0;
}

