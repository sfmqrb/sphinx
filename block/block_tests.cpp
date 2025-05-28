#include <algorithm>
#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <string>

#include "../fingerprint_gen_helper/fingerprint_gen_helper.h"
#include "block.h"

// Define a struct to hold leaf information
typedef DefaultTraits::PAYLOAD_TYPE PAYLOAD_TYPE;
typedef DefaultTraits::KEY_TYPE KEY_TYPE;
typedef DefaultTraits::VALUE_TYPE VALUE_TYPE;
typedef DefaultTraits::ENTRY_TYPE ENTRY_TYPE;


TEST_CASE("Block: finding index of the payload correctly") {
    generate_hashtable(ht1, signatures_h1, important_bits_h1, indices_h1, arr_h1);
    auto block = Block<TestDefaultTraits>();

    // Set bitsets based on provided data
    block.bits.bitset[0] = 0b1111011111111111010111011100101001111111111001000100101111001100;
    block.bits.bitset[1] = 0b1010110010011010010110100010101110101100010100101000110101010111;
    block.bits.bitset[2] = 0b1011111111111111100011001110111011011010111111100011111111111011;
    block.bits.bitset[3] = 0b1000000000000000001100110110111011100001001111110111011000110101;
    std::vector<std::pair<size_t, size_t>> boundaries = {
            {140, 140}, {140, 140}, {140, 140}, {140, 140}, {140, 140}, {140, 140}, {140, 140},
            {140, 142}, {142, 147}, {147, 149}, {149, 149}, {149, 151}, {151, 151}, {151, 151},
            {151, 151}, {151, 151}, {151, 151}, {151, 151}, {151, 160}, {160, 160}, {160, 160},
            {160, 163}, {163, 168}, {168, 172}, {172, 181}, {181, 181}, {181, 183}, {183, 185},
            {185, 185}, {185, 185}, {185, 187}, {187, 187}, {187, 187}, {187, 189}, {189, 189},
            {189, 198}, {198, 198}, {198, 198}, {198, 203}, {203, 203}, {203, 206}, {206, 206},
            {206, 211}, {211, 213}, {213, 213}, {213, 213}, {213, 223}, {223, 223}, {223, 228},
            {228, 228}, {228, 231}, {231, 234}, {234, 234}, {234, 234}, {234, 238}, {238, 238},
            {238, 238}, {238, 238}, {238, 238}, {238, 238}, {238, 238}, {238, 238}, {238, 238},
            {238, 238}
    };


    // Store expected ten values and boundaries in vectors
    std::vector<int> expected_ten = {
            0, 0, 1, 1, 0, 0, 1, 2, 2, 2, 0, 2, 0, 0, 1, 0, 0, 0, 4, 0, 0, 2, 3, 2,
            4, 1, 2, 2, 1, 1, 2, 0, 0, 2, 0, 4, 0, 0, 2, 1, 2, 0, 3, 2, 1, 0, 3, 0,
            3, 1, 2, 2, 1, 1, 2, 1, 1, 1, 1, 0, 1, 1, 1, 1
    };



    SECTION("Validate ten and boundaries") {
        for (size_t slot_idx = 0; slot_idx < COUNT_SLOT; slot_idx++) {
            auto slot_start = block.get_lslot_start(slot_idx);
            auto next_slot_start = block.get_lslot_start(slot_idx + 1);
            auto ten = block.get_ten(slot_idx);

            auto [start, end] = boundaries[slot_idx];
            // Validate boundaries
            CHECK(slot_start == start);
            CHECK(next_slot_start == end);

            // Validate ten value
            CHECK(ten == expected_ten[slot_idx]);

            // Additional validation for BST creation
            auto bst = BST<N>(ten, slot_start, 0);
            bst.createBST(block.bits);
            CHECK(bst.getBitRepWrapper().firstInvalidIndex == next_slot_start - slot_start);
            CHECK(bst.getBitRepWrapper().bw.range(0, next_slot_start - slot_start) == block.bits.range(slot_start, next_slot_start));
        }
    }
    SECTION("Validate Get Index") {
        for (size_t slot_idx = 0; slot_idx < COUNT_SLOT; slot_idx++) {
            // create random fp
            for (size_t i = 0; i < 10; i++) {
                auto fp = Hashing<DefaultTraits>::hash_digest(i);
                auto res1 = block.get_index(fp, 0);
                auto res2 = block.get_index_dht(fp, 0);
                auto res3 = block.get_index_withoutHT(fp, 0);
//                std::cout << "slot_idx: " << slot_idx << " res1: " << res1.first << " res2: " << res2.first << " res3: " << res3.first << std::endl;
                CHECK(res1.first == res2.first);
                CHECK(res1.first == res3.first);
                CHECK(res1.second == res2.second);
                CHECK(res1.second == res3.second);
            }
        }
    }
}

TEST_CASE("Block: write in block") {
    Block<TestDefaultTraits> block;
    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("Block<>test_2.txt", 10);
    SECTION("Test1: simple write") {
        for (auto i = 0; i < 8; i++) {
            const KEY_TYPE key = 0x0000000000000000 + (static_cast<KEY_TYPE>(i) << (8));
            const VALUE_TYPE value = i * 2 + 1;
            PAYLOAD_TYPE pt = ssdLog->write(key, value);
            auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
            auto info = block.write(hash_val, *ssdLog.get(), 6, pt);
            CHECK(block.read(hash_val, *ssdLog.get(), 6)->key == key);
        }
    }

    SECTION("Test3: simple write - multiple lslots") {
        for (auto lslot = 0; lslot < 8; lslot++) {
            for (auto i = 0; i < 4; i++) {
                const KEY_TYPE key = (static_cast<KEY_TYPE>(lslot) << (5)) + (static_cast<KEY_TYPE>(i) << (8));
                const VALUE_TYPE value = i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto info = block.write(hash_val, *ssdLog.get(), 6, pt);
                CHECK(block.read(hash_val, *ssdLog.get(), 6)->key == key);
            }
        }
    }
}

TEST_CASE("Block: write test for fingerprint with specific pattern") {
    Block<TestDefaultTraits> block;
    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("Block<>test_1.txt", 10);
    SECTION("Test1: simple write") {
        const std::vector<std::string> fps = {"101", "010", "111"};

        for (auto i = 0; i < fps.size(); i++) {
            const auto key = static_cast<KEY_TYPE>(getFP(61, 0, 0, 12, fps[i]));
            const VALUE_TYPE value = i * 2 + 1;
            PAYLOAD_TYPE pt = ssdLog->write(key, value);
            auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
            auto info = block.write(hash_val, *ssdLog.get(), 12, pt);
            CHECK(block.read(hash_val, *ssdLog.get(), 12)->key == key);
        }
        if (DefaultTraits::NUMBER_EXTRA_BITS) {
            CHECK(block.payload_list.get_extra_bits_at(0).second == 2);
            CHECK(block.payload_list.get_extra_bits_at(1).second == 5);
            CHECK(block.payload_list.get_extra_bits_at(2).second == 7);
        }
    }
    SECTION("Block: write test payload size overflow") {
        const std::vector<std::string> fps = {"101", "010", "111"};
        size_t FP_size = 2 * COUNT_SLOT_BITS;

        for (auto i = 0; i < COUNT_SLOT; i++) {
            const auto key = static_cast<KEY_TYPE>(getFP(i, 0, 0, FP_size, fps[0]));
            const VALUE_TYPE value = i * 2 + 1;
            PAYLOAD_TYPE pt = ssdLog->write(key, value);
            auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
            auto info = block.write(hash_val, *ssdLog.get(), FP_size, pt);
            CHECK(block.read(hash_val, *ssdLog.get(), FP_size)->key == key);
        }
        for (auto j = 0; j < TestDefaultTraits::SAFETY_PAYLOADS; j++) {
            const auto key = static_cast<KEY_TYPE>(getFP(j, 0, 0, FP_size, fps[2]));
            const VALUE_TYPE value = j * 2 + 1;
            PAYLOAD_TYPE pt = ssdLog->write(key, value);
            auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
            auto info = block.write(hash_val, *ssdLog.get(), FP_size, pt);
            CHECK(info.rs == WriteReturnStatusSuccessful);
            CHECK(block.read(hash_val, *ssdLog.get(), FP_size)->key == key);
        }
        const auto key = static_cast<KEY_TYPE>(getFP(COUNT_SLOT - 1, 0, 0, FP_size, fps[1]));
        constexpr VALUE_TYPE value = 1010;
        PAYLOAD_TYPE pt = ssdLog->write(key, value);
        auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
        auto info = block.write(hash_val, *ssdLog.get(), FP_size, pt);
        CHECK(info.rs == WriteReturnStatusNotEnoughPayloadSpace);
        CHECK(info.blockInfo.isExtended == false);
        CHECK(info.blockInfo.firstExtendedLSlot == COUNT_SLOT);
    }
}


TEST_CASE("Block: update") {
    SECTION("TEST1") {
        Block<TestDefaultTraits> block;
        const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("Block<>test_update.txt", 10);
        KEY_TYPE key = 1;
        VALUE_TYPE val = 1;
        auto addr = ssdLog->write(key, val);
        BitsetWrapper<FINGERPRINT_SIZE> fingerprint = Hashing<TestDefaultTraits>::hash_digest(key);
        block.write(fingerprint, *ssdLog.get(), 6, addr);
        auto bef = block.bits.getInputString();
        auto addr2 = ssdLog->write(key, val);
        block.write(fingerprint, *ssdLog.get(), 6, addr2);
        auto aft = block.bits.getInputString();
        CHECK(bef == aft);
        auto a = block.getTenBeforeAndTen(64);
        CHECK(a.tenBefore == 1);
        if (TestDefaultTraits::NUMBER_EXTRA_BITS)
            CHECK(block.payload_list.get_extra_bits_at(0).second == 0);
    }
}

TEST_CASE("Block: simple remove") {
    Block<TestDefaultTraits> block;
    const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("Block<>test_remove.txt", 10);
    SECTION("TEST1") {
        KEY_TYPE key = 1;
        VALUE_TYPE val = 1;
        auto addr = ssdLog->write(key, val);
        BitsetWrapper<FINGERPRINT_SIZE> fingerprint = Hashing<TestDefaultTraits>::hash_digest(key);
        auto bef = block.bits.getInputString();
        block.write(fingerprint, *ssdLog.get(), 6, addr);
        block.remove(fingerprint, *ssdLog.get(), 6);
        auto aft = block.bits.getInputString();
        CHECK(bef == aft);
        auto a = block.getTenBeforeAndTen(64);
        CHECK(a.tenBefore == 0);
    }
    SECTION("TEST2") {
        KEY_TYPE key = 1ull << 7;
        VALUE_TYPE val = 1;
        KEY_TYPE key2 = 1ull << 8;
        VALUE_TYPE val2 = 2;
        auto addr = ssdLog->write(key, val);
        auto addr2 = ssdLog->write(key2, val2);
        BitsetWrapper<FINGERPRINT_SIZE> fingerprint = Hashing<TestDefaultTraits>::hash_digest(key);
        BitsetWrapper<FINGERPRINT_SIZE> fingerprint2 = Hashing<TestDefaultTraits>::hash_digest(key2);
        auto bef = block.bits.getInputString();
        block.write(fingerprint, *ssdLog.get(), 6, addr);
        block.remove(fingerprint, *ssdLog.get(), 6);
        auto aft = block.bits.getInputString();
        CHECK(bef == aft);
        block.write(fingerprint, *ssdLog.get(), 6, addr);
        bef = block.bits.getInputString();
        block.write(fingerprint2, *ssdLog.get(), 6, addr2);
        auto a = block.getTenBeforeAndTen(64);
        CHECK(a.tenBefore == 2);
        if (TestDefaultTraits::NUMBER_EXTRA_BITS) {
            CHECK(block.payload_list.get_extra_bits_at(0).second == 4);
            CHECK(block.payload_list.get_extra_bits_at(1).second == 2);
        }
        block.remove(fingerprint2, *ssdLog.get(), 6);
        aft = block.bits.getInputString();
        CHECK(bef == aft);
        a = block.getTenBeforeAndTen(64);
        CHECK(a.tenBefore == 1);
        CHECK(block.read(fingerprint, *ssdLog.get(), 6)->key == key);
        if (TestDefaultTraits::NUMBER_EXTRA_BITS)
            CHECK(block.payload_list.get_extra_bits_at(0).second == 2);
    }

    SECTION("Block: remove test") {
        const std::vector<std::string> fps = {"10", "01", "00"};
        size_t FP_size = 2 * COUNT_SLOT_BITS;

        auto bef = block.bits.getInputString();
        for (auto blkIdx = 0; blkIdx < COUNT_SLOT; blkIdx += 4) {
            for (auto i = 0; i < fps.size(); i++) {
                const auto key = static_cast<KEY_TYPE>(getFP(blkIdx, 0, 0, FP_size, fps[i]));
                const VALUE_TYPE value = i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto info = block.write(hash_val, *ssdLog.get(), FP_size, pt);
                CHECK(block.read(hash_val, *ssdLog.get(), FP_size)->key == key);
            }
        }
        for (auto blkIdx = 0; blkIdx < COUNT_SLOT; blkIdx += 4) {
            for (auto i = 0; i < fps.size(); i++) {
                const auto key = static_cast<KEY_TYPE>(getFP(blkIdx, 0, 0, FP_size, fps[i]));
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto info = block.remove(hash_val, *ssdLog.get(), FP_size);
                CHECK(block.get_ten(blkIdx) == fps.size() - i - 1);
            }
            CHECK(block.getLSlotString(*ssdLog.get(), blkIdx).empty());
        }
        auto aft = block.bits.getInputString();
        CHECK(aft == bef);
    }
    SECTION("Block: write test block size overflow") {
        const std::vector<std::string> fps = {"10000", "10001", "10011"};
        size_t FP_size = 2 * COUNT_SLOT_BITS;

        for (auto blkIdx = 0; blkIdx < COUNT_SLOT; blkIdx += 4) {
            for (auto i = 0; i < fps.size(); i++) {
                const auto  key = static_cast<KEY_TYPE>(getFP(blkIdx, 0, 0, FP_size, fps[i]));
                const VALUE_TYPE value = i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto info = block.write(hash_val, *ssdLog.get(), FP_size, pt);
                CHECK(block.read(hash_val, *ssdLog.get(), FP_size)->key == key);
            }
        }
        for (auto blkIdx = 0; blkIdx < COUNT_SLOT; blkIdx += COUNT_SLOT / 2 + 1) {
            for (auto i = 0; i < fps.size(); i++) {
                const auto  key = static_cast<KEY_TYPE>(getFP(blkIdx, 0, 0, FP_size, fps[i]));
                const VALUE_TYPE value = i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto info = block.write(hash_val, *ssdLog.get(), FP_size, pt);
                CHECK(block.read(hash_val, *ssdLog.get(), FP_size)->key == key);
            }
        }
        for (int i = 0; i < 4; ++i) {
            //  tested with threshold 1
            const int count_slot_offsets[] = {1, 3, 5, 7};
            const int remaining_bits[] = {3, 2, 1, 1};
            const WriteReturnStatus expected_return_status[] = {WriteReturnStatusSuccessful, WriteReturnStatusSuccessful, WriteReturnStatusSuccessful, WriteReturnStatusNotEnoughBlockSpace};

            const auto key = static_cast<KEY_TYPE>(getFP(COUNT_SLOT - count_slot_offsets[i], 0, 0, FP_size, fps[1]));
            const VALUE_TYPE value = 1010;
            PAYLOAD_TYPE pt = ssdLog->write(key, value);
            auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
            auto info = block.write(hash_val, *ssdLog.get(), FP_size, pt);
            CHECK(info.rs == expected_return_status[i]);
            CHECK(info.blockInfo.isExtended == false);
            CHECK(info.blockInfo.firstExtendedLSlot == COUNT_SLOT);
            CHECK(info.blockInfo.remainingBits == remaining_bits[i]);

            if (expected_return_status[i] == WriteReturnStatusSuccessful) {
                CHECK(block.read(hash_val, *ssdLog.get(), FP_size)->key == key);
            }
        }
    }
}

TEST_CASE("Test Dummy") {
    SECTION("Test1") {
//        uint64_t a = 10313030370844167500ull;
        uint64_t a = 1ULL << 31;
        uint64_t b = 1ULL << 31;

        auto c = __builtin_ia32_pext_di(a, b);
        std::cout << c << std::endl;
    }
    SECTION("Test1") {
//        uint64_t a = 10313030370844167500ull;
        uint64_t a = 1 << 31;
        uint64_t b = 1 << 31;

        auto c = __builtin_ia32_pext_di(a, b);
        std::cout << c << std::endl;
    }
}
