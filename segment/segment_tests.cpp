#include <catch2/catch_test_macros.hpp>
#include <iostream>

#include "../fingerprint_gen_helper/fingerprint_gen_helper.h"
#include "catch2/generators/catch_generators.hpp"
#include "segment.h"
#include <vector>
#include <numeric>

typedef typename TestDefaultTraits::PAYLOAD_TYPE PAYLOAD_TYPE;
typedef typename TestDefaultTraits::KEY_TYPE KEY_TYPE;
typedef typename TestDefaultTraits::VALUE_TYPE VALUE_TYPE;
typedef typename TestDefaultTraits::ENTRY_TYPE ENTRY_TYPE;

TEST_CASE("Segment<TestDefaultTraits>: test 1") {
    Segment<TestDefaultTraits> sg(12);

    const std::vector<std::string> fps = {"101", "010", "111", "000"};
    const std::vector<size_t> lslots = {61, 63, 62, 0, 3, 12, 44, 55};
    const std::vector<size_t> segments = {0, 1, 2, 3, 4, 10, 12, 56, 63};

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("segment_test_1.txt", 10);

    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < segments.size(); ++k) {
                const auto key = static_cast<KEY_TYPE>(getFP(lslots[j], 0, segments[k], 12, fps[i]));
                const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);

                CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);

                auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }
    // sg.print();
}

TEST_CASE("Segment<TestDefaultTraits>: test 2") {
    Segment<TestDefaultTraits> sg(12);

    const std::vector<std::string> fps = {"0101", "0010", "0111", "0000", "00100011", "00100001"};
    const std::vector<size_t> lslots = {61, 63, 62, 0, 3, 12, 44, 55};
    const std::vector<size_t> segments = {0};

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("segment_test_1.txt", 10);

    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < segments.size(); ++k) {
                const auto key = static_cast<KEY_TYPE>(getFP(lslots[j], 0, segments[k], 12, fps[i]));
                const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);

                CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);

                auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }
    const auto key = static_cast<KEY_TYPE>(getFP(57, 0, 0, 12, "00100010"));
    constexpr VALUE_TYPE value = 10;
    PAYLOAD_TYPE pt = ssdLog->write(key, value);
    auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);

    CHECK(sg.blockList[0].get_block_info()->isExtended == false);
    CHECK(sg.blockList[0].get_block_info()->remainingBits == 0);
    CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);
    CHECK(sg.blockList[0].get_block_info()->isExtended == true);
    CHECK(sg.blockList[0].get_block_info()->remainingBits == 15);

//    sg.print();
    sg.print();
}

TEST_CASE("Segment<TestDefaultTraits>: expand without extension") {
    size_t FP_index = 12;
    Segment<TestDefaultTraits> sg(FP_index);
    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("segment_test_1.txt", 10);
    const std::vector<std::string> fps = {"101", "010", "111"};
    const std::vector<size_t> lslotsIdx = {61, 63, 62, 0, 3, 12, 44, 55};
    const std::vector<size_t> blockIdx = {0, 1, 2, 3, 4, 10, 12, 56, 63};

    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslotsIdx.size(); ++j) {
            for (size_t k = 0; k < blockIdx.size(); ++k) {
                const auto key = static_cast<KEY_TYPE>(getFP(lslotsIdx[j], 0, blockIdx[k], FP_index, fps[i]));
                const VALUE_TYPE value = blockIdx[k] + lslotsIdx[j] + i;

                keys.push_back(key);
                values.push_back(value);

                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);
            }
        }
    }

//    sg.print();

    auto [sg1, sg2] = sg.expand(*ssdLog);
    // sg1->print();
    // sg2->print();
    for (auto i = 0; i < keys.size(); i++) {
        auto key = keys[i];
        auto value = values[i];

        auto h = Hashing<TestDefaultTraits>::hash_digest(key);
        const auto &cand_seg = h.get(0) ? sg2 : sg1;
        auto [rkey, rvalue] = *cand_seg->read(h, *ssdLog.get());
        CHECK(rkey == key);
        CHECK(rvalue == value);
    }
}

TEST_CASE("Segment<TestDefaultTraits>: expand with extension") {
    size_t FP_index = 12;
    Segment<TestDefaultTraits> sg(FP_index);
    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("segment_test_1.txt", 10);
    const std::vector<std::string> fps = {"101", "010", "110001", "111001"};
    const std::vector<size_t> lslotsIdx = {61, 63, 62, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 44, 55};
    const std::vector<size_t> blockIdx = {0};

    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslotsIdx.size(); ++j) {
            for (size_t k = 0; k < blockIdx.size(); ++k) {
                const auto key = static_cast<KEY_TYPE>(getFP(lslotsIdx[j], 0, blockIdx[k], FP_index, fps[i]));
                const VALUE_TYPE value = blockIdx[k] + lslotsIdx[j] + i;

                keys.push_back(key);
                values.push_back(value);

                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);
                auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }

//    sg.print();
    // std::cout << "Hi \n";
    auto [sg1, sg2] = sg.expand(*ssdLog);
    // sg1->print();
    // sg2->print();
    for (auto i = 0; i < keys.size(); i++) {
        auto key = keys[i];
        auto value = values[i];
        auto h = Hashing<TestDefaultTraits>::hash_digest(key);
        const auto &cand_seg = h.get(0) ? sg2 : sg1;
        auto [rkey, rvalue] = *cand_seg->read(h, *ssdLog.get());
        CHECK(rkey == key);
        CHECK(rvalue == value);
    }
}

TEST_CASE("Segment<TestDefaultTraits>: expand with extension harder test") {
    size_t FP_index = 12;
    Segment<TestDefaultTraits> sg(FP_index);
    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("segment_test_1.txt", 10);
    const std::vector<std::string> fps = {"000111", "000110", "001100", "001011"};
    const std::vector<size_t> lslotsIdx = {61, 63, 62, 0, 1, 2, 3, 4, 9, 10, 12, 59, 8};
    const std::vector<size_t> blockIdx = {0, 3, 4, 5, 8, 43, 32, 2, 56};

    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslotsIdx.size(); ++j) {
            for (size_t k = 0; k < blockIdx.size(); ++k) {
                const auto key = static_cast<KEY_TYPE>(getFP(lslotsIdx[j], 0, blockIdx[k], FP_index, fps[i]));
                const VALUE_TYPE value = blockIdx[k] + lslotsIdx[j] + i;

                keys.push_back(key);
                values.push_back(value);

                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);
                auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }

    // sg.print();
    auto [sg1, sg2] = sg.expand(*ssdLog);
    // sg1->print();
    // sg2->print();
    for (auto i = 0; i < keys.size(); i++) {
        auto key = keys[i];
        auto value = values[i];
        auto h = Hashing<TestDefaultTraits>::hash_digest(key);
        const auto &cand_seg = h.get(0) ? sg2 : sg1;
        auto [rkey, rvalue] = *cand_seg->read(h, *ssdLog.get());
        CHECK(rkey == key);
        CHECK(rvalue == value);
    }
}

TEST_CASE("Segment<TestDefaultTraits>: expand with extension much harder test") {
    size_t FP_index = 12;
    Segment<TestDefaultTraits> sg(FP_index);
    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("segment_test_1.txt", 10);
    const std::vector<std::string> fps = {"000111", "000110", "001100", "001011"};
    const std::vector<size_t> lslotsIdx = {61, 63, 62, 0,
                                           1, 2, 3, 4,
                                           9, 10, 58, 59,
                                           8, 11, 17};
    const std::vector<size_t> blockIdx = {0};

    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslotsIdx.size(); ++j) {
            for (size_t k = 0; k < blockIdx.size(); ++k) {
                const auto key = static_cast<KEY_TYPE>(getFP(lslotsIdx[j], 0, blockIdx[k], FP_index, fps[i]));
                const VALUE_TYPE value = blockIdx[k] + lslotsIdx[j] + i;

                keys.push_back(key);
                values.push_back(value);

                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);
                auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }
    // sg.blockList[0].print();
    // sg.printExtension();

//    sg.print();
    auto [sg1, sg2] = sg.expand(*ssdLog);
    // sg1->print();
    // sg2->print();
    for (auto i = 0; i < keys.size(); i++) {
        auto key = keys[i];
        auto value = values[i];
        auto h = Hashing<TestDefaultTraits>::hash_digest(key);
        const auto &cand_seg = h.get(0) ? sg2 : sg1;
        auto [rkey, rvalue] = *cand_seg->read(h, *ssdLog.get());
        CHECK(rkey == key);
        CHECK(rvalue == value);
    }
}

TEST_CASE("Segment<TestDefaultTraits>: extension and delete") {
    size_t FP_index = 12;
    Segment<TestDefaultTraits> sg(FP_index);
    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("segment_test_1.txt", 10);
    const std::vector<std::string> fps = {"000111", "000110", "001100", "001011"};
    const std::vector<size_t> lslotsIdx = {61, 63, 62, 0,
                                           1, 2, 3, 4,
                                           9, 10, 58, 59,
                                           8, 11, 17};
    const std::vector<size_t> blockIdx = {0};

    std::vector<KEY_TYPE> keys;
    std::vector<VALUE_TYPE> values;

    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslotsIdx.size(); ++j) {
            for (size_t k = 0; k < blockIdx.size(); ++k) {
                const auto key = static_cast<KEY_TYPE>(getFP(lslotsIdx[j], 0, blockIdx[k], FP_index, fps[i]));
                const VALUE_TYPE value = blockIdx[k] + lslotsIdx[j] + i;

                keys.push_back(key);
                values.push_back(value);

                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);
                auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }
    // sg.blockList[0].print();
    // sg.printExtension();
//    sg.print();
    for (auto i = 0; i < keys.size(); i++) {
        auto key = keys[i];

        auto h = Hashing<TestDefaultTraits>::hash_digest(key);
        auto res = sg.remove(h, *ssdLog.get());
        CHECK(res == true);
    }
    CHECK(sg.get_ten_all() == 0);
}
TEST_CASE("Segment<TestDefaultTraitsDHT>: test 1") {
    Segment<TestDefaultTraitsDHT> sg(12);

    const std::vector<std::string> fps = {"101", "010", "111", "000"};
    const std::vector<size_t> lslots = {61, 63, 62, 0, 3, 12, 44, 55};
    const std::vector<size_t> segments = {0, 1, 2, 3, 4, 10, 12, 56, 63};

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraitsDHT>>("segment_test_1.txt", 10);

    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < segments.size(); ++k) {
                const auto key = static_cast<KEY_TYPE>(getFP(lslots[j], 0, segments[k], 12, fps[i]));
                const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraitsDHT>::hash_digest(key);

                CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);

                auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }
    // ten all
    std::cout << sg.get_ten_all() << std::endl;
    sg.print();
}

TEST_CASE("Segment<TestDefaultTraitsDHT>: test 2") {
    Segment<TestDefaultTraitsDHT> sg(12);

    const std::vector<std::string> fps = {"0101", "0010", "0111", "0000", "00100011", "00100001"};
    const std::vector<size_t> lslots = {61, 63, 62, 0, 3, 12, 44, 55};
    const std::vector<size_t> segments = {0};

    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraitsDHT>>("segment_test_1.txt", 10);

    for (size_t i = 0; i < fps.size(); ++i) {
        for (size_t j = 0; j < lslots.size(); ++j) {
            for (size_t k = 0; k < segments.size(); ++k) {
                const auto key = static_cast<KEY_TYPE>(getFP(lslots[j], 0, segments[k], 12, fps[i]));
                const VALUE_TYPE value = lslots[j] * 10 + i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraitsDHT>::hash_digest(key);

                CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);

                auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }
    const auto key = static_cast<KEY_TYPE>(getFP(57, 0, 0, 12, "00100010"));
    constexpr VALUE_TYPE value = 10;
    PAYLOAD_TYPE pt = ssdLog->write(key, value);
    auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);

    CHECK(sg.blockList[0].get_block_info()->isExtended == false);
    CHECK(sg.blockList[0].get_block_info()->remainingBits == 0);
    CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);
    CHECK(sg.blockList[0].get_block_info()->isExtended == true);
    CHECK(sg.blockList[0].get_block_info()->remainingBits == 15);

    std::cout << sg.get_ten_all() << std::endl;
//    sg.print();
    sg.print();
}


TEST_CASE("Segment<TestDefaultTraitsDHTXXHASH>: test 2") {
    Segment<TestDefaultTraitsDHTXXHASH> sg(12);
    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraitsDHTXXHASH>>("segment_test_1.txt", 1000);

    constexpr size_t numKeys = 5046;
    std::vector<TestCPU::KEY_TYPE> keys(numKeys);
    std::iota(keys.begin(), keys.end(), 1);


    for (size_t i = 0; i < numKeys; ++i) {
        auto key = keys[i];
        auto value = key;
        PAYLOAD_TYPE pt = ssdLog->write(key, value);
        auto hash_val = Hashing<TestDefaultTraitsDHTXXHASH>::hash_digest(key);
        CHECK(sg.write(hash_val, *ssdLog.get(), pt) == true);
        auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
        CHECK(oKey == key);
        CHECK(oValue == value);
    }

    for (size_t i = 0; i < numKeys; i++) {
        auto key = keys[i];
        auto hash_val = Hashing<TestDefaultTraitsDHTXXHASH>::hash_digest(key);
        auto [oKey, oValue] = *sg.read(hash_val, *ssdLog.get());
        CHECK(oKey == key);
    }
    std::cout << " all ten: " << sg.get_ten_all() << std::endl;
    sg.print();
}