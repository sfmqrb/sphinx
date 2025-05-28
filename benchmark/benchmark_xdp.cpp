#include <iostream>
#include <stdexcept>
#include <vector>
#include <cmath>

#include "../directory/directory.h"
#include "../xdp/xdp.h"
#include "recorder/recorder.h"

constexpr size_t RSQF_STATIC_FP_SIZE = 15;
constexpr auto SAMPLE_LOG = 0.03;

// consts
const std::string HOME = std::getenv("HOME");
constexpr size_t INIT_SIZE_LOG = 12;
constexpr size_t INIT_SIZE = 1ull << INIT_SIZE_LOG;
constexpr size_t NUM_KEYS_TOTAL = static_cast<size_t>(0.91 * INIT_SIZE * (1 << RSQF_STATIC_FP_SIZE));

#ifdef ENABLE_XDP
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


void runXDPMemTest(const std::string &folder) {
    Metrics metrics;
    int LI_INDEX_SIZE = 525000;
    int total_entries = NUM_KEYS_TOTAL;
    XDP<TraitsGI, TraitsLI, TraitsLIBuffer> xdp(LI_INDEX_SIZE);
    for (int i = 1; i <= total_entries; ++i) {
        xdp.performWriteTask(i, i);
        if (should_sample(i)) {
            std::cout << "num_entries: " << i << std::endl;
            metrics.record("num_entries", i);

            auto mem_index_arr = xdp.get_memory_index_size();
            metrics.record("index_global", mem_index_arr[0] / static_cast<double>(i));
            metrics.record("index_local", mem_index_arr[1] / static_cast<double>(i));
            metrics.record("index_buffer", mem_index_arr[2] / static_cast<double>(i));

            auto mem_ptr_arr = xdp.get_memory_footprint();
            metrics.record("ptr_global", (mem_ptr_arr[0] - mem_index_arr[0]) / static_cast<double>(i) );
            metrics.record("ptr_local", (mem_ptr_arr[1] - mem_index_arr[1]) / static_cast<double>(i));
            metrics.record("ptr_buffer", (mem_ptr_arr[2] - mem_index_arr[2]) / static_cast<double>(i));

            std::cout << "index global: " << mem_index_arr[0] / static_cast<double>(i) << std::endl;
            std::cout << "index local: " << mem_index_arr[1] / static_cast<double>(i) << std::endl;
            std::cout << "index buffer: " << mem_index_arr[2] / static_cast<double>(i) << std::endl;

            std::cout << "ptr global: " << (mem_ptr_arr[0] - mem_index_arr[0]) / static_cast<double>(i) << std::endl;
            std::cout << "ptr local: " << (mem_ptr_arr[1] - mem_index_arr[1]) / static_cast<double>(i) << std::endl;
            std::cout << "ptr buffer: " << (mem_ptr_arr[2] - mem_index_arr[2]) / static_cast<double>(i) << std::endl;
            std::cout << "----------------------------------------" << std::endl;
        }
    }

    metrics.printToFile({"num_entries", "index_global", "index_local", "index_buffer",
                         "ptr_global", "ptr_local", "ptr_buffer"}, 
                        folder + "/benchmark_", "XDP");
}
#endif

int main() {
#ifdef ENABLE_XDP
    auto dataFolder = HOME + "/research/sphinx/benchmark/data-memory";
    std::filesystem::create_directories(dataFolder);
    std::cout << " Test XDP\n";
    runXDPMemTest(dataFolder);
#endif
    return 0;
}
