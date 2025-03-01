#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <numeric>

#include "../fingerprint_gen_helper/fingerprint_gen_helper.h"
#include "catch2/internal/catch_stdstreams.hpp"
#include "directory.h"

typedef typename DefaultTraits::PAYLOAD_TYPE PAYLOAD_TYPE;
typedef typename DefaultTraits::KEY_TYPE KEY_TYPE;
typedef typename DefaultTraits::VALUE_TYPE VALUE_TYPE;
typedef typename DefaultTraits::ENTRY_TYPE ENTRY_TYPE;
// Function to print binary representation of a 64-bit number
void printBinary(uint64_t value) {
    for (int i = 63; i >= 0; --i) {
        std::cout << ((value >> i) & 1);
    }
    std::cout << '\n';
}

TEST_CASE("DirectoryInsertion") {
    constexpr size_t x = 2;
    constexpr size_t NUM_THREADS = 128;
    Directory<TestDefaultTraits> dir(x, NUM_THREADS);
    constexpr size_t FP_index = 12 + x;

    const std::vector<std::string> fps = {"101", "0"};
    const std::vector<size_t> lslots = {40, 61, 3, 33};
    const std::vector<size_t> blocks = {2, 55};
    const std::vector<size_t> segments = {1, 0, 3, 2};

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("directory_test_1.txt", 10);

    std::vector<std::future<bool>> writeFutures;
    std::vector<std::future<std::unique_ptr<ENTRY_TYPE>>> readFutures;
    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    // Insertions
    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < blocks.size(); ++k) {
                for (size_t k2 = 0; k2 < segments.size(); ++k2) {
                    auto key = static_cast<KEY_TYPE>(getFP(lslots[j], segments[k2], blocks[k], FP_index, fps[i]));
                    const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                    PAYLOAD_TYPE pt = ssdLog->write(key, value);

                    writeFutures.push_back(dir.writeSegment(key, value, *ssdLog.get(), pt));
                    keys.push_back(key);
                    values.push_back(value);
                }
            }
        }
    }

    // Check write futures
    for (auto& future : writeFutures) {
        CHECK(future.get() == true);
    }

    // // Ensure all tasks are completed
    while (dir.isActive()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Read operations
    for (size_t i = 0; i < keys.size(); ++i) {
        readFutures.push_back(dir.readSegment(keys[i], *ssdLog.get()));
    }

    // Check read futures
    for (size_t i = 0; i < readFutures.size(); ++i) {
        auto [oKey, oValue] = *readFutures[i].get();
        CHECK(oKey == keys[i]);
        CHECK(oValue == values[i]);
    }
}

TEST_CASE("DirectoryInsertionToExpand") {
    constexpr size_t segment_count_log = 0;
    constexpr size_t NUM_THREADS = 128;
    Directory<TestDefaultTraits> dir(segment_count_log, NUM_THREADS);
    constexpr size_t FP_index = 12 + segment_count_log;

    auto a = (*dir.segDataVec)[0].segment;
    const std::vector<std::string> fps = {"11110101", "1111100", "111001"};
    const std::vector<size_t> lslots = {0, 1, 2, 3, 4,
                                        5, 6, 7, 8, 9,
                                        10, 11, 12, 13, 14,
                                        15, 40, 61, 63};
    const std::vector<size_t> blocks = {0, 4, 8, 12, 16};
    const std::vector<size_t> segments = {0};

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("directory_test_1.txt", 10);

    std::vector<std::future<bool>> writeFutures;
    std::vector<std::future<std::unique_ptr<ENTRY_TYPE>>> readFutures;
    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    CHECK(a.use_count() == 2);  // one for a the other one for the directory
    // Insertions
    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < blocks.size(); ++k) {
                for (size_t k2 = 0; k2 < segments.size(); ++k2) {
                    auto key = static_cast<KEY_TYPE>(getFP(lslots[j], segments[k2], blocks[k], FP_index, fps[i]));
                    const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                    PAYLOAD_TYPE pt = ssdLog->write(key, value);

                    writeFutures.push_back(dir.writeSegment(key, value, *ssdLog.get(), pt));
                    keys.push_back(key);
                    values.push_back(value);
                }
            }
        }
    }

    // Check write futures
    for (auto& future : writeFutures) {
        CHECK(future.get() == true);
    }
    CHECK(a.use_count() == 1);  // one for a the other one for the directory
    // dir.print();
    CHECK((*dir.segDataVec)[0].segment.use_count() == 1);
    CHECK((*dir.segDataVec)[1].segment.use_count() == 1);
    // Ensure all tasks are completed
    while (dir.isActive()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Read operations
    for (size_t i = 0; i < keys.size(); ++i) {
        readFutures.push_back(dir.readSegment(keys[i], *ssdLog.get()));
    }

    // Check read futures
    for (size_t i = 0; i < readFutures.size(); ++i) {
        auto [oKey, oValue] = *readFutures[i].get();
        CHECK(oKey == keys[i]);
        CHECK(oValue == values[i]);
    }
    CHECK((*dir.segDataVec)[0].segment.use_count() == 1);
    CHECK((*dir.segDataVec)[1].segment.use_count() == 1);
}

TEST_CASE("DirectoryInsertionToExpand2") {
    constexpr size_t segment_count_log = 3;
    constexpr size_t NUM_THREADS = 64;
    Directory<TestDefaultTraits> dir(segment_count_log, NUM_THREADS);
    constexpr size_t FP_index = 12 + segment_count_log;

    const std::vector<std::string> fps = {"11110101", "1111100", "111001"};
    const std::vector<size_t> lslots = {0, 1, 2, 3, 4,
                                        5, 6, 7, 8, 9,
                                        10, 11, 12, 13, 14,
                                        15, 40, 61, 63};
    const std::vector<size_t> blocks = {0, 4, 8, 12, 15, 63};
    const std::vector<size_t> segments = {0, 3, 2, 4, 5, 1};

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("directory_test_1.txt", 100);

    std::vector<std::future<bool>> writeFutures;
    std::vector<std::future<std::unique_ptr<ENTRY_TYPE>>> readFutures;
    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    // Insertions
    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < blocks.size(); ++k) {
                for (size_t k2 = 0; k2 < segments.size(); ++k2) {
                    auto key = static_cast<KEY_TYPE>(getFP(lslots[j], segments[k2], blocks[k], FP_index, fps[i]));
                    const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                    PAYLOAD_TYPE pt = ssdLog->write(key, value);

                    writeFutures.push_back(dir.writeSegment(key, value, *ssdLog.get(), pt));
                    keys.push_back(key);
                    values.push_back(value);
                }
            }
        }
    }

    const std::vector<size_t> segments_part2 = {5};
    const std::vector<size_t> blocks_part2 = {12, 16, 20, 24, 28};
    // Insertions
    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < blocks_part2.size(); ++k) {
                for (size_t k2 = 0; k2 < segments_part2.size(); ++k2) {
                    auto key = static_cast<KEY_TYPE>(getFP(lslots[j], segments_part2[k2], blocks_part2[k], FP_index + 1, fps[i]));
                    const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                    PAYLOAD_TYPE pt = ssdLog->write(key, value);

                    writeFutures.push_back(dir.writeSegment(key, value, *ssdLog.get(), pt));
                    keys.push_back(key);
                    values.push_back(value);
                }
            }
        }
    }
    auto key = static_cast<KEY_TYPE>(getFP(0, 15, 0, FP_index, "00010"));
    VALUE_TYPE val = 100;
    PAYLOAD_TYPE pt = ssdLog->write(key, val);
    writeFutures.push_back(dir.writeSegment(key, val, *ssdLog.get(), pt));
    keys.push_back(key);
    values.push_back(val);
    // Check write futures
    for (auto& future : writeFutures) {
        CHECK(future.get() == true);
    }
    // dir.print();
//    dir.print_segs_info();

    // Read operations
    for (size_t i = 0; i < keys.size(); ++i) {
        readFutures.push_back(dir.readSegment(keys[i], *ssdLog.get()));
    }

    // Check read futures
    for (size_t i = 0; i < readFutures.size(); ++i) {
        auto [oKey, oValue] = *readFutures[i].get();
        CHECK(oKey == keys[i]);
        CHECK(oValue == values[i]);
    }
}

TEST_CASE("DirectoryInsertionToExpandSingleThread") {
    constexpr size_t segment_count_log = 3;
    Directory<TestDefaultTraits> dir(segment_count_log, 1);
    constexpr size_t FP_index = 12 + segment_count_log;

    const std::vector<std::string> fps = {"11110101", "1111100", "111001"};
    const std::vector<size_t> lslots = {0, 1, 2, 3, 4,
                                        5, 6, 7, 8, 9,
                                        10, 11, 12, 13, 14,
                                        15, 40, 61, 63};
    const std::vector<size_t> blocks = {0, 4, 8, 12, 15, 63};
    const std::vector<size_t> segments = {0, 3, 2, 4, 5, 1};

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("directory_test_1.txt", 100);

    std::vector<bool> writeFutures;
    std::vector<std::unique_ptr<ENTRY_TYPE>> readFutures;
    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    // Insertions
    auto counter = 0;
    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < blocks.size(); ++k) {
                for (size_t k2 = 0; k2 < segments.size(); ++k2) {
                    auto key = static_cast<KEY_TYPE>(getFP(lslots[j], segments[k2], blocks[k], FP_index, fps[i]));
                    const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                    counter++;
                    PAYLOAD_TYPE pt = ssdLog->write(key, value);

                    writeFutures.push_back(dir.writeSegmentSingleThread(key, value, *ssdLog.get(), pt));
                    auto readData = dir.readSegmentSingleThread(key, *ssdLog.get());
                    CHECK(readData->key == key);
                    keys.push_back(key);
                    values.push_back(value);
                }
            }
        }
    }


    const std::vector<size_t> segments_part2 = {5};
    const std::vector<size_t> blocks_part2 = {12, 16, 20, 24, 28};
    // Insertions
    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < blocks_part2.size(); ++k) {
                for (size_t k2 = 0; k2 < segments_part2.size(); ++k2) {
                    auto key = static_cast<KEY_TYPE>(getFP(lslots[j], segments_part2[k2], blocks_part2[k], FP_index + 1, fps[i]));
                    const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                    counter++;
                    PAYLOAD_TYPE pt = ssdLog->write(key, value);
                    writeFutures.push_back(dir.writeSegmentSingleThread(key, value, *ssdLog.get(), pt));
                    auto readData = dir.readSegmentSingleThread(key, *ssdLog.get());
                    CHECK(readData->key == key);
                    keys.push_back(key);
                    values.push_back(value);
                }
            }
        }
    }
    auto key = static_cast<KEY_TYPE>(getFP(0, 15, 0, FP_index, "00010"));
    VALUE_TYPE val = 100;
    counter++;
    PAYLOAD_TYPE pt = ssdLog->write(key, val);
    writeFutures.push_back(dir.writeSegmentSingleThread(key, val, *ssdLog.get(), pt));
    auto readData = dir.readSegmentSingleThread(key, *ssdLog.get());
    CHECK(readData->key == key);
    keys.push_back(key);
    values.push_back(val);
    // Check write futures
    for (auto future : writeFutures) {
        CHECK(future == true);
    }
//    dir.print();
//    dir.print_segs_info();
//    dir.print();
    CHECK(dir.get_ten_all() == counter);
    // Read operations
    for (size_t i = 0; i < keys.size(); ++i) {
        readFutures.push_back(dir.readSegmentSingleThread(keys[i], *ssdLog.get()));
    }

    // Check read futures
    for (size_t i = 0; i < readFutures.size(); ++i) {
        auto [oKey, oValue] = *readFutures[i];
        CHECK(oKey == keys[i]);
        CHECK(oValue == values[i]);
    }
}

TEST_CASE("DirectoryRemoveSingleThread") {
    constexpr size_t segment_count_log = 3;
    Directory<TestDefaultTraits> dir(segment_count_log, 1);
    constexpr size_t FP_index = 12 + segment_count_log;

    const std::vector<std::string> fps = {"11110101", "1111100", "111001"};
    const std::vector<size_t> lslots = {0, 1, 2, 3, 4,
                                        5, 6, 7, 8, 9,
                                        10, 11, 12, 13, 14,
                                        15, 40, 61, 63};
    const std::vector<size_t> blocks = {0, 4, 8, 12, 15, 63};
    const std::vector<size_t> segments = {0, 3, 2, 4, 5, 1};

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("directory_test_1.txt", 100);

    std::vector<bool> writeFutures;
    std::vector<std::unique_ptr<ENTRY_TYPE>> readFutures;
    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    // Insertions
    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < blocks.size(); ++k) {
                for (size_t k2 = 0; k2 < segments.size(); ++k2) {
                    auto key = static_cast<KEY_TYPE>(getFP(lslots[j], segments[k2], blocks[k], FP_index, fps[i]));
                    const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                    PAYLOAD_TYPE pt = ssdLog->write(key, value);

                    writeFutures.push_back(dir.writeSegmentSingleThread(key, value, *ssdLog.get(), pt));
                    keys.push_back(key);
                    values.push_back(value);
                }
            }
        }
    }

    const std::vector<size_t> segments_part2 = {5};
    const std::vector<size_t> blocks_part2 = {12, 16, 20, 24, 28};
    // Insertions
    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < blocks_part2.size(); ++k) {
                for (size_t k2 = 0; k2 < segments_part2.size(); ++k2) {
                    auto key = static_cast<KEY_TYPE>(getFP(lslots[j], segments_part2[k2], blocks_part2[k], FP_index + 1, fps[i]));
                    const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                    PAYLOAD_TYPE pt = ssdLog->write(key, value);

                    writeFutures.push_back(dir.writeSegmentSingleThread(key, value, *ssdLog.get(), pt));
                    keys.push_back(key);
                    values.push_back(value);
                }
            }
        }
    }
    auto key = static_cast<KEY_TYPE>(getFP(0, 15, 0, FP_index, "00010"));
    VALUE_TYPE val = 100;
    PAYLOAD_TYPE pt = ssdLog->write(key, val);
    writeFutures.push_back(dir.writeSegmentSingleThread(key, val, *ssdLog.get(), pt));
    keys.push_back(key);
    values.push_back(val);
    // Check write futures
    for (auto future : writeFutures) {
        CHECK(future == true);
    }
    // dir.print();
    // dir.print_segs_info();

    // Read operations
    for (size_t i = 0; i < keys.size(); ++i) {
        readFutures.push_back(dir.readSegmentSingleThread(keys[i], *ssdLog.get()));
    }

    // Check read futures
    for (size_t i = 0; i < readFutures.size(); ++i) {
        auto [oKey, oValue] = *readFutures[i];
        CHECK(oKey == keys[i]);
        CHECK(oValue == values[i]);
        dir.removeSegmentSingleThread(oKey, *ssdLog.get());
    }
    // dir.print_segs_info();
    CHECK(dir.get_ten_all() == 0);
}

TEST_CASE("DirectoryInsertCatchTheBug") {
    constexpr size_t segment_count_log = 1;
    Directory<TestBugWrite> dir(segment_count_log, 1);

    const auto ssdLog = std::make_unique<SSDLog<TestBugWrite>>("directory_test_1.txt", 100000);

//    constexpr size_t numKeys = 5096 * 1024;
    constexpr size_t numKeys = 1 << 16;
    std::vector<TestCPU::KEY_TYPE> keys(numKeys);
    std::iota(keys.begin(), keys.end(), 1);


    for (size_t i = 0; i < numKeys; ++i) {
        auto key = keys[i];
        auto value = key;
        PAYLOAD_TYPE pt = ssdLog->write(key, value);
//        auto stop_at = 102191;
//        if (key == stop_at) {
//            std::cout << "key: " << key << " value: " << value << std::endl;
//        }
        dir.writeSegmentSingleThread(key, value, *ssdLog.get(), pt);
//        auto target_key = 704603;
//        if (key >= target_key) {
//        auto res = dir.readSegmentSingleThread(key, *ssdLog.get());
//        if (res->key != key) {
//            std::cout << "key: " << key << " value: " << value << " res: " << res->value << std::endl;
//        }
//        }
    }
    for (size_t i = 0; i < numKeys; ++i) {
        auto key = keys[i];
        auto exp_value = key;
        auto res = dir.readSegmentSingleThread(key, *ssdLog.get());
        if (res->value != exp_value) {
            std::cout << "key: " << key << " value: " << exp_value << " res: " << res->value << std::endl;
        }
    }
//    dir.print_segs_info();
}

TEST_CASE("XXHASH DHT DirectoryInsert") {
    constexpr size_t segment_count_log = 6;
    Directory<TestDefaultTraitsDHTXXHASH> dir(segment_count_log, 1);

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraitsDHTXXHASH>>("directory_test_1.txt", 100000);

//    constexpr size_t numKeys = 5096 * 1024;
    constexpr size_t numKeys = (1 << 16) * 4;
    std::vector<TestCPU::KEY_TYPE> keys(numKeys);
    std::iota(keys.begin(), keys.end(), 1);


    for (size_t i = 0; i < numKeys; ++i) {
        auto key = keys[i];
        auto value = key;
        PAYLOAD_TYPE pt = ssdLog->write(key, value);
        dir.writeSegmentSingleThread(key, value, *ssdLog.get(), pt);
    }
    for (size_t i = 0; i < numKeys; ++i) {
        auto key = keys[i];
        auto exp_value = key;
        auto res = dir.readSegmentSingleThread(key, *ssdLog.get());
        if (res->value != exp_value) {
            std::cout << "key: " << key << " value: " << exp_value << " res: " << res->value << std::endl;
        }
    }
    dir.print();
}
TEST_CASE("DirectoryInsertCatchTheBug2") {
    constexpr size_t segment_count_log = 0;
    Directory<TestBugWrite> dir(segment_count_log, 1);

    const auto ssdLog = std::make_unique<SSDLog<TestBugWrite>>("directory_test_1.txt", 100);

    constexpr size_t numKeys = 4000;
    std::vector<TestCPU::KEY_TYPE> keys(numKeys);
    std::iota(keys.begin(), keys.end(), 1);


    for (size_t i = 0; i < numKeys; ++i) {
        auto key = keys[i];
        auto value = key;
        PAYLOAD_TYPE pt = ssdLog->write(key, value);
        dir.writeSegmentSingleThread(key, value, *ssdLog.get(), pt);
    }
    for (size_t i = 0; i < numKeys; ++i) {
        auto key = keys[i];
        auto exp_value = key;
        auto res = dir.readSegmentSingleThread(key, *ssdLog.get());
        CHECK(res->value == exp_value);
    }
    auto seg = dir.getSegmentPtr(0);
    auto blk = seg->getBLock(1);
//    printBinary(blk->bits.bitset[0]);
//    printBinary(blk->bits.bitset[1]);
//    printBinary(blk->bits.bitset[2]);
//    printBinary(blk->bits.bitset[3]);
}


TEST_CASE("DirectoryInsertCatchTheBug3") {

}
