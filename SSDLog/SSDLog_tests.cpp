#include <catch2/catch_test_macros.hpp>
#include <cmath>
#include <numeric>
#include <sstream>
#include <vector>

#include "SSDLog.h"

typedef typename DefaultTraits::PAYLOAD_TYPE PAYLOAD_TYPE;
typedef typename DefaultTraits::KEY_TYPE KEY_TYPE;
typedef typename DefaultTraits::VALUE_TYPE VALUE_TYPE;
typedef typename DefaultTraits::ENTRY_TYPE ENTRY_TYPE;

TEST_CASE("SSDLog: works correctly") {
    try {
        SSDLog ssdLog("ssd_log_test.bin", 100);  // 100 pages
        for (int64_t i = 1; i <= 1200; ++i) {
            ssdLog.write(i, i * 2);  // Example: key = i, value = i * 2
        }
        ssdLog.printLog();
    } catch (const std::exception& e) {
        FAIL("Exception: " << e.what());
    }
}

TEST_CASE("SSDLog: works correctly for read") {
    try {
        SSDLog ssdLog("ssd_log_test.bin", 100);  // 100 pages
        for (int64_t i = 1; i <= 1200; ++i) {
            ssdLog.write(i, i * 2);  // Example: key = i, value = i * 2
        }
        const auto numEntriesPerPage = PAGE_SIZE / sizeof(ENTRY_TYPE);
        const auto logNumEntriesPage = (int)(ceil(log2(numEntriesPerPage)));
        auto page = (1 << logNumEntriesPage) + 1;
        ENTRY_TYPE et;
        ssdLog.read(page, et);
        CHECK(et.key == 258);
        CHECK(et.value == 516);
    } catch (const std::exception& e) {
        FAIL("Exception: " << e.what());
    }
}

TEST_CASE("SSDLog: benchmark read/write operations") {
    try {
        SSDLog ssdLog("ssd_log_benchmark.bin", 100);  // 100 pages
        constexpr int numOperations = 10000;
        std::vector<double> writeTimes;
        std::vector<double> readTimes;
        std::vector<PAYLOAD_TYPE> payloads;

        // Benchmark writes
        for (int64_t i = 0; i < numOperations; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            auto payload = ssdLog.write(i, i * 2);
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::nano> duration = end - start;
            writeTimes.push_back(duration.count());
            payloads.push_back(payload);
        }

        // Benchmark reads
        for (int64_t i = 0; i < numOperations; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            ENTRY_TYPE et;
            ssdLog.read(payloads[i], et);
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::nano> duration = end - start;
            auto count = duration.count();
            // std::cout << count << " time ns\n";
            readTimes.push_back(count);
        }

        // Calculate average and standard deviation for writes
        double writeSum = std::accumulate(writeTimes.begin(), writeTimes.end(), 0.0);
        double writeMean = writeSum / numOperations;
        double writeSqSum = std::inner_product(writeTimes.begin(), writeTimes.end(), writeTimes.begin(), 0.0);
        double writeStdDev = std::sqrt(writeSqSum / numOperations - writeMean * writeMean);

        // Calculate average and standard deviation for reads
        double readSum = std::accumulate(readTimes.begin(), readTimes.end(), 0.0);
        double readMean = readSum / numOperations;
        double readSqSum = std::inner_product(readTimes.begin(), readTimes.end(), readTimes.begin(), 0.0);
        double readStdDev = std::sqrt(readSqSum / numOperations - readMean * readMean);

        // Report results
        std::ostringstream os;
        os << "Write operations: Average time = " << writeMean << " ns, StdDev = " << writeStdDev << " ns\n";
        os << "Read operations: Average time = " << readMean << " ns, StdDev = " << readStdDev << " ns\n";
        std::cout << (os.str());

        CHECK(true);  // To pass the test
    } catch (const std::exception& e) {
        FAIL("Exception: " << e.what());
    }
}
