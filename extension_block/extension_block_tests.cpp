#include <catch2/catch_test_macros.hpp>

#include "../SSDLog/SSDLog.h"
#include "../fingerprint_gen_helper/fingerprint_gen_helper.h"
#include "block.h"
#include "extension_block.h"

typedef typename TestDefaultTraits::PAYLOAD_TYPE PAYLOAD_TYPE;
typedef typename TestDefaultTraits::KEY_TYPE KEY_TYPE;
typedef typename TestDefaultTraits::VALUE_TYPE VALUE_TYPE;
typedef typename TestDefaultTraits::ENTRY_TYPE ENTRY_TYPE;

size_t FPIdx = 12;

TEST_CASE("ExtensionBlock: simple insertion") {
    ExtensionBlock<TestDefaultTraits> eb;
    const size_t blkIdx = 0;
    const size_t lslotIdx = 0;

    SECTION("Test1: simple write") {
        const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("extension_Block<>test_1.txt", 10);

        for (auto i = 0; i < 8; i++) {
            const KEY_TYPE key = static_cast<KEY_TYPE>(i) << 14;
            const VALUE_TYPE value = i * 2 + 1;
            PAYLOAD_TYPE pt = ssdLog->write(key, value);
            auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
            auto info = eb.write(hash_val, *ssdLog.get(), FPIdx, pt, blkIdx, lslotIdx);
            CHECK(eb.read(hash_val, *ssdLog.get(), FPIdx, blkIdx, lslotIdx)->key == key);
        }
        // eb.print();
    }

    SECTION("Test2: write multiple lslots") {
        const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("extension_Block<>test_2.txt", 10);
        for (auto i = 0; i < 8; i++) {
            auto newLslotIdx = lslotIdx + i;
            const KEY_TYPE key = static_cast<KEY_TYPE>(i) << 14;
            const VALUE_TYPE value = i * 2 + 1;
            PAYLOAD_TYPE pt = ssdLog->write(key, value);
            auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
            auto info = eb.write(hash_val, *ssdLog.get(), FPIdx, pt, blkIdx, newLslotIdx);
            auto [oKey, oValue] = *(eb.read(hash_val, *ssdLog.get(), FPIdx, blkIdx, newLslotIdx));
            CHECK(oKey == key);
            CHECK(oValue == value);
        }
        // eb.print();
    }

    SECTION("Test3: write multiple lslots with more than 1 ten") {
        const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("extension_Block<>test_3.txt", 10);
        for (auto i = 0; i < 8; i++) {
            auto newLslotIdx = lslotIdx + i;
            for (auto j = 0; j < 2; j++) {
                const KEY_TYPE key = static_cast<KEY_TYPE>(j) << 14;
                const VALUE_TYPE value = i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto info = eb.write(hash_val, *ssdLog.get(), FPIdx, pt, blkIdx, newLslotIdx);
                CHECK(eb.read(hash_val, *ssdLog.get(), FPIdx, blkIdx, lslotIdx)->key == key);
            }
        }
        // eb.print();
    }

    SECTION("Test4: write multiple lslots with more than 1 ten from different blocks") {
        const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("extension_Block<>test_4.txt", 10);
        for (auto k = 6; k >= 0; k -= 3) {
            for (auto i = 0; i < 8; i++) {
                auto newLslotIdx = lslotIdx + i;
                auto newBlockIdx = blkIdx + k;
                for (auto j = 0; j < 2; j++) {
                    const KEY_TYPE key = static_cast<KEY_TYPE>(j) << 14;
                    const VALUE_TYPE value = i * 2 + 1;
                    PAYLOAD_TYPE pt = ssdLog->write(key, value);
                    auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                    auto info = eb.write(hash_val, *ssdLog.get(), FPIdx, pt, newBlockIdx, newLslotIdx);
                    CHECK(eb.read(hash_val, *ssdLog.get(), FPIdx, blkIdx, lslotIdx)->key == key);
                }
            }
        }
        // eb.print();
    }
}

TEST_CASE("ExtensionBlock: normal block integration") {
    Block<TestDefaultTraits> orgBlock;
    std::array<ExtensionBlock<TestDefaultTraits>, TestDefaultTraits::SEGMENT_EXTENSION_BLOCK_SIZE> extBlocks;
    const auto blkIdx = 0;

    SECTION("Test1: simple write") {
        const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("extension_Block<>test_1.txt", 10);
        std::vector<std::string> fps = {"100010101", "1011110101", "1111110101"};
        for (auto j = 63; j >= 56; j -= 2) {
            for (auto i = 0; i < 3; i++) {
                const auto key = static_cast<KEY_TYPE>(getFP(j, 0, 0, FPIdx, fps[i]));
                const VALUE_TYPE value = j * 10 + i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto info = orgBlock.write(hash_val, *ssdLog.get(), FPIdx, pt);
                auto [oKey, oValue] = *orgBlock.read(hash_val, *ssdLog.get(), FPIdx);
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
        // orgBlock.print();
        // orgBlock.payload_list.printPayload();
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 63, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 62, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 61, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 60, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 59, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 58, blkIdx, extBlocks.data());

        // orgBlock.print();
        // for (auto &c:extBlocks) {
        //     c.print();
        //     c.blk.payload_list.printPayload();
        // }

        for (auto j = 57; j >= 56; j -= 2) {
            for (auto i = 0; i < 3; i++) {
                const KEY_TYPE key = (int64_t)getFP(j, 0, blkIdx, FPIdx, fps[i]);
                const VALUE_TYPE value = j * 10 + i * 2 + 1;
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto [oKey, oValue] = *orgBlock.read(hash_val, *ssdLog.get(), FPIdx);
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }

        for (auto j = 63; j >= 58; j -= 2) {
            for (auto i = 0; i < 3; i++) {
                const KEY_TYPE key = (int64_t)getFP(j, 0, blkIdx, FPIdx, fps[i]);
                const VALUE_TYPE value = j * 10 + i * 2 + 1;
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                size_t lslotBefore = ExtensionBlock<>::CALCULATE_LSLOT_BEFORE(j);
                size_t exBlkIdx = ExtensionBlock<>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, j);
                auto extBlock = extBlocks[exBlkIdx];
                auto [oKey, oValue] = *extBlock.read(hash_val, *ssdLog.get(), FPIdx, blkIdx, lslotBefore);
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }
}

TEST_CASE("ExtensionBlock: a bit more challenging") {
    Block<TestDefaultTraits> orgBlock;
    std::array<ExtensionBlock<TestDefaultTraits>, TestDefaultTraits::SEGMENT_EXTENSION_BLOCK_SIZE> extBlocks;
    auto blkIdx = 0;

    SECTION("Test1: simple write") {
        const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("extension_Block<>test_1.txt", 10);
        std::vector<std::string> fps = {"110010101", "1011110101", "1111110101", "0010101"};
        for (auto j = 63; j >= 56; j -= 2) {
            for (auto i = 0; i < 4; i++) {
                const KEY_TYPE key = (int64_t)getFP(j, 0, 0, FPIdx, fps[i]);
                const VALUE_TYPE value = j * 10 + i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto info = orgBlock.write(hash_val, *ssdLog.get(), FPIdx, pt);
                CHECK(orgBlock.read(hash_val, *ssdLog.get(), FPIdx)->key == key);
            }
        }
        for (auto j = 63; j >= 56; j -= 2) {
            for (auto i = 0; i < 4; i++) {
                const KEY_TYPE key = (int64_t)getFP(j, 0, 0, FPIdx, fps[i]);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                CHECK(orgBlock.read(hash_val, *ssdLog.get(), FPIdx)->key == key);
            }
        }
        // orgBlock.print();
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 63, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 62, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 61, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 60, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 59, blkIdx, extBlocks.data());
        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 58, blkIdx, extBlocks.data());

        // orgBlock.print();
        // extBlock.print();

        for (auto j = 57; j >= 56; j -= 2) {
            for (auto i = 0; i < 4; i++) {
                const KEY_TYPE key = (int64_t)getFP(j, 0, 0, FPIdx, fps[i]);
                const VALUE_TYPE value = j * 10 + i * 2 + 1;
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                CHECK(orgBlock.read(hash_val, *ssdLog.get(), FPIdx)->key == key);
            }
        }

        for (auto j = 63; j >= 59; j -= 2) {
            for (auto i = 0; i < 4; i++) {
                const KEY_TYPE key = (int64_t)getFP(j, 0, 0, FPIdx, fps[i]);
                const VALUE_TYPE value = j * 10 + i * 2 + 1;
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                size_t lslotBefore = ExtensionBlock<>::CALCULATE_LSLOT_BEFORE(j);
                size_t exBlkIdx = ExtensionBlock<>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, j);
                auto extBlock = extBlocks[exBlkIdx];
                auto [oKey, oValue] = *extBlock.read(hash_val, *ssdLog.get(), FPIdx, 0, lslotBefore);
                CHECK(oKey == key);
                CHECK(oValue == value);
            }
        }
    }
}

TEST_CASE("ExtensionBlock: lslot extended") {
    Block<TestDefaultTraits> orgBlock;
    ExtensionBlock<TestDefaultTraits> extBlock;
    auto blkIdx = 0;

    SECTION("Test1: simple write") {
        const auto ssdLog = std::make_unique<SSDLog<TestDefaultTraits>>("extension_Block<>test_1.txt", 10);
        std::vector<std::string> fps = {"110010101", "1011110101", "1111110101", "0010101"};
        for (auto j = 63; j >= 56; j -= 2) {
            for (auto i = 0; i < 4; i++) {
                const KEY_TYPE key = (int64_t)getFP(j, 0, 0, FPIdx, fps[i]);
                const VALUE_TYPE value = j * 10 + i * 2 + 1;
                PAYLOAD_TYPE pt = ssdLog->write(key, value);
                auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
                auto info = orgBlock.write(hash_val, *ssdLog.get(), FPIdx, pt);
                CHECK(orgBlock.read(hash_val, *ssdLog.get(), FPIdx)->key == key);
            }
        }
        // orgBlock.print();

        ExtensionBlock<TestDefaultTraits>::moveLSlotsToMakeSpace(orgBlock, 63, blkIdx, &extBlock);

//        orgBlock.print();
        // extBlock.print();

        const KEY_TYPE key = (int64_t)getFP(63, 0, 0, FPIdx, "010101");
        const VALUE_TYPE value = 2;
        auto hash_val = Hashing<TestDefaultTraits>::hash_digest(key);
        PAYLOAD_TYPE pt = ssdLog->write(key, value);
        auto info = orgBlock.write(hash_val, *ssdLog.get(), FPIdx, pt);

        CHECK(info.rs == WriteReturnStatusLslotExtended);
        CHECK(info.blockInfo.isExtended == true);
        CHECK(info.blockInfo.firstExtendedLSlot == COUNT_SLOT - 1);
    }
}
