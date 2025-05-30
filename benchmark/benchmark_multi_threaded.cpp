#include <chrono>
#include <fstream>
#include <iostream>
#include <vector>
#include <future>
#include <algorithm>
#include <numeric>
#include <random>
#include <stdexcept>
#include <cstdlib>  // for getenv
#include <random>

// Include your Directory header, SSDLog, etc.
#include "../directory/directory.h"
#include "recorder/recorder.h"
#include "../lib/memento/memento.hpp"

// Assume that DefaultTraits, TestMT, etc., are defined elsewhere.
std::string HOME = std::getenv("HOME");

// For demonstration, define a constant for how many keys we insert/read.
// (Adjust to match your actual test size)
static constexpr size_t NUM_KEYS = 1 << 22; // 1 million keys, e.g.

// ***********************************************************
// Benchmark functions (unchanged)
// ***********************************************************
template <typename Traits>
double measureInsertionThroughput(Directory<Traits>& dir,
                                  SSDLog<Traits>& ssdLog,
                                  const std::vector<typename Traits::KEY_TYPE>& keys)
{
    std::vector<std::future<bool>> futures;
    futures.reserve(keys.size());

    auto startTime = std::chrono::high_resolution_clock::now();
    for (auto key : keys) {
        ssdLog.write(key, key);
    }
    for (auto key : keys) {
        futures.push_back(dir.writeSegment(key, 3 * key, ssdLog, key));
    }
    // Wait for all writes to complete
    for (auto &f : futures) {
        f.get();
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto durationNs = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();

    // Throughput = operations per second
    double opsPerSec = (double)keys.size() / ((double)durationNs * 1e-9);
    return opsPerSec;
}

template <typename Traits>
double measureReadThroughput(Directory<Traits>& dir,
                             SSDLog<Traits>& ssdLog,
                             const std::vector<typename Traits::KEY_TYPE>& keys)
{
    std::mt19937 gen(12345); // Mersenne Twister pseudo-random generator
    std::uniform_int_distribution<int> dist(0, keys.size() - 1);

    std::vector<std::future<std::optional<typename Traits::ENTRY_TYPE>>> futures;
    futures.reserve(keys.size());

    auto startTime = std::chrono::high_resolution_clock::now();

    for (auto _: keys) {
        auto p = dist(gen);
        futures.push_back(dir.readSegment(p, ssdLog));
    }

    for (auto &f : futures) {
        f.get();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto durationNs = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();

    double opsPerSec = (double)keys.size() / ((double)durationNs * 1e-9);
    return opsPerSec;
}

template <typename Traits>
double measureRandomReadThroughput(Directory<Traits>& dir,
                                   SSDLog<Traits>& ssdLog,
                                   const std::vector<typename Traits::KEY_TYPE>& keys)
{
    std::mt19937 gen(12345); // Mersenne Twister pseudo-random generator
    std::uniform_int_distribution<int> dist(0, keys.size() - 1);
    std::vector<std::future<bool>> futures;

    auto startTime = std::chrono::high_resolution_clock::now();

    for (auto key : keys) {
        // generate  payload

        auto p = dist(gen);
        futures.push_back(dir.readRandom(p, ssdLog));
    }

    for (auto &f : futures) {
        f.get();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto durationNs = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();

    double opsPerSec = (double)keys.size() / ((double)durationNs * 1e-9);
    return opsPerSec;
}

// ***********************************************************
// Main function with 6 repetitions per thread-count
// ***********************************************************
int main()
{
    auto modes = {
        // "optane",
        "ssd",
    };
    std::filesystem::create_directories(HOME + "/research/sphinx/benchmark/data-mt");
    // Open CSV file for throughput results
    for (auto mode: modes) {
        std::ofstream csvFile(HOME + "/research/sphinx/benchmark/data-mt/throughput_vs_threads_4ReserveBits" + mode + ".csv");
        std::string ssdFilePath;
        if (mode == "optane") {
            ssdFilePath = "/optane/log/directory_test2.txt";
        } else if (mode == "ssd") {
            ssdFilePath = "/data/fleck/directory_test2.txt";
        }
        // Updated header to include RandomRead throughput
        csvFile << "ThreadCount,InsertThroughput_ops_s,ReadThroughput_ops_s,RandomReadThroughput_ops_s\n";

        std::vector<DefaultTraits::KEY_TYPE> keys(NUM_KEYS);
        std::iota(keys.begin(), keys.end(), 0ULL);

        // Thread counts to test
        std::vector<size_t> threadCounts = {};

        constexpr size_t segmentCountLog = 6;
        constexpr int repetitions = 3;

        for (auto tCount : threadCounts) {
            // Vectors to store throughput results for the 6 repetitions
            std::vector<double> insertResults;
            std::vector<double> readResults;
            std::vector<double> randomReadResults;

            for (int rep = 0; rep < repetitions; rep++) {
                // For each repetition, create a new Directory and SSDLog instance
                Directory<TestMT4ReserveBits> dir(segmentCountLog, tCount);
                auto ssdLog = std::make_unique<SSDLog<TestMT4ReserveBits>>(ssdFilePath, 1000000);

                // Run benchmarks
                double insertThroughput = measureInsertionThroughput(dir, *ssdLog, keys);
                std::cout << "N Q insert" << ssdLog->numQ << std::endl;
                std::cout << "throughput: " << insertThroughput << std::endl;
                ssdLog->numQ = 0;
                double readThroughput = measureReadThroughput(dir, *ssdLog, keys);
                std::cout << "N Q read" << ssdLog->numQ << std::endl;
                std::cout << "throughput: " << readThroughput << std::endl;
                ssdLog->numQ = 0;
                double randomReadThroughput = measureRandomReadThroughput(dir, *ssdLog, keys);
                std::cout << "N Q random read" << ssdLog->numQ << std::endl;
                std::cout << "throughput: " << randomReadThroughput << std::endl;
                ssdLog->numQ = 0;

                // Collect results
                insertResults.push_back(insertThroughput);
                readResults.push_back(readThroughput);
                randomReadResults.push_back(randomReadThroughput);
            }

            // Sort the 6 measurements for each benchmark
            std::sort(insertResults.begin(), insertResults.end());
            std::sort(readResults.begin(), readResults.end());
            std::sort(randomReadResults.begin(), randomReadResults.end());

            // Select the 3rd and 4th middle ones (indices 2 and 3) and average them
            double avgInsert = insertResults[1];
            double avgRead = readResults[1];
            double avgRandomRead = randomReadResults[1];

            // Print the averaged results to the console
            std::cout << "Threads = " << tCount << "\n"
                      << "  Insert Throughput = " << avgInsert << " ops/s\n"
                      << "  Read Throughput   = " << avgRead << " ops/s\n"
                      << "  Random Read Throughput = " << avgRandomRead << " ops/s\n"
                      << "---------------------------------------------" << std::endl;

            // Write the averaged results to the CSV file
            csvFile << tCount << "," << avgInsert << "," << avgRead << "," << avgRandomRead
            << "\n";
        }

        csvFile.close();
    }
    for (auto mode: modes) {
        std::ofstream csvFile(HOME + "/research/sphinx/benchmark/data-mt/throughput_vs_threads_" + mode + ".csv");
        std::string ssdFilePath;
        if (mode == "optane") {
            ssdFilePath = "/optane/log/directory_test2.txt";
        } else if (mode == "ssd") {
            ssdFilePath = "/data/fleck/directory_test2.txt";
        }
        // Updated header to include RandomRead throughput
        csvFile << "ThreadCount,InsertThroughput_ops_s,ReadThroughput_ops_s,RandomReadThroughput_ops_s\n";

        std::vector<DefaultTraits::KEY_TYPE> keys(NUM_KEYS);
        std::iota(keys.begin(), keys.end(), 0ULL);

        // Thread counts to test
        std::vector<size_t> threadCounts = {32, 64};

        constexpr size_t segmentCountLog = 6;
        constexpr int repetitions = 3;

        for (auto tCount : threadCounts) {
            // Vectors to store throughput results for the 6 repetitions
            std::vector<double> insertResults;
            std::vector<double> readResults;
            std::vector<double> randomReadResults;

            for (int rep = 0; rep < repetitions; rep++) {
                // For each repetition, create a new Directory and SSDLog instance
                Directory<TestMT> dir(segmentCountLog, tCount);
                auto ssdLog = std::make_unique<SSDLog<TestMT>>(ssdFilePath, 1000000);

                // Run benchmarks
                double insertThroughput = measureInsertionThroughput(dir, *ssdLog, keys);
                std::cout << "N Q insert" << ssdLog->numQ << std::endl;
                std::cout << "N Q insert throughput: " << insertThroughput << std::endl;
                ssdLog->numQ = 0;
                double readThroughput = measureReadThroughput(dir, *ssdLog, keys);
                std::cout << "N Q read" << ssdLog->numQ << std::endl;
                std::cout << "N Q read throughput: " << readThroughput << std::endl;
                ssdLog->numQ = 0;
                double randomReadThroughput = measureRandomReadThroughput(dir, *ssdLog, keys);
                std::cout << "N Q random read" << ssdLog->numQ << std::endl;
                std::cout << "N Q random read throughput: " << randomReadThroughput << std::endl;
                ssdLog->numQ = 0;

                // Collect results
                insertResults.push_back(insertThroughput);
                readResults.push_back(readThroughput);
                randomReadResults.push_back(randomReadThroughput);
            }

            // Sort the 6 measurements for each benchmark
            std::sort(insertResults.begin(), insertResults.end());
            std::sort(readResults.begin(), readResults.end());
            std::sort(randomReadResults.begin(), randomReadResults.end());

            // Select the 3rd and 4th middle ones (indices 2 and 3) and average them
            double avgInsert = insertResults[1];
            double avgRead = readResults[1];
            double avgRandomRead = randomReadResults[1];

            // Print the averaged results to the console
            std::cout << "Threads = " << tCount << "\n"
                      << "  Insert Throughput = " << avgInsert << " ops/s\n"
                      << "  Read Throughput   = " << avgRead << " ops/s\n"
                      << "  Random Read Throughput = " << avgRandomRead << " ops/s\n"
                      << "---------------------------------------------" << std::endl;

            // Write the averaged results to the CSV file
            csvFile << tCount << "," << avgInsert << "," << avgRead << "," << avgRandomRead
            << "\n";
        }

        csvFile.close();
    }
    return 0;
}
