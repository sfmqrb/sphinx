#include <catch2/catch_test_macros.hpp>

#include "payload.h"

constexpr auto PAYLOADS_LENGTH = DefaultTraits::PAYLOADS_LENGTH;
constexpr auto NUMBER_EXTRA_BITS = DefaultTraits::NUMBER_EXTRA_BITS;

TEST_CASE("Payload: class tests") {
    Payload<TestPayloadUint32> payload;
    SECTION("Setting and getting payloads") {
        REQUIRE(payload.set_payload_at(0, 42));
        REQUIRE(payload.get_payload_at(0) == 42);
        // REQUIRE_THROWS_AS(payload.get_payload_at(PAYLOADS_LENGTH), std::out_of_range);
    }

    SECTION("Shifting payloads right by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0);
        REQUIRE(payload.get_payload_at(1) == 1);
        REQUIRE(payload.get_payload_at(2) == 2);
    }

    SECTION("Shifting payloads left by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0);
        REQUIRE(payload.get_payload_at(0) == 2);
    }

    SECTION("Shifting payloads right by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0, 2);
        REQUIRE(payload.get_payload_at(2) == 1);
        REQUIRE(payload.get_payload_at(3) == 2);
    }

    SECTION("Shifting payloads left by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.shift_left_from_index(0, 2);
        REQUIRE(payload.get_payload_at(0) == 3);
    }

    SECTION("Max index management") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(1, 95);
        payload.set_payload_at(2, 99);
        REQUIRE(payload.get_payload_at(2) == 99);
        REQUIRE(payload.set_payload_at(3, 100));
        REQUIRE(payload.get_payload_at(3) == 100);
    }

    SECTION("Max index handling when setting payloads at different indices") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(2, 95);
    }

    SECTION("Edge case: Setting payload at maximum index") {
        REQUIRE(payload.set_payload_at(PAYLOADS_LENGTH - 1, 100));
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 100);
    }

    SECTION("Edge case: Shifting right when at capacity") {
        for (size_t i = 0; i < PAYLOADS_LENGTH; ++i) {
            payload.set_payload_at(i, static_cast<int>(i));
        }
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 67);
        payload.shift_right_from_index(0, 1);
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 66);
    }

    SECTION("Edge case: Shifting left beyond zero") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0, 1);
        REQUIRE(payload.get_payload_at(0) == 2);
    }

    SECTION("Operator [] access") {
        payload.set_payload_at(0, 42);
        REQUIRE(payload[0] == 42);
        // REQUIRE_THROWS_AS(payload[PAYLOADS_LENGTH], std::out_of_range);
    }
}

TEST_CASE("Payload: uint8_t functionality") {
    Payload<TestPayloadUint8> payload;

    SECTION("Initial state") {
    }

    SECTION("Setting and getting payloads") {
        REQUIRE(payload.set_payload_at(0, 42));
        REQUIRE(payload.get_payload_at(0) == 42);
        // REQUIRE_THROWS_AS(payload.get_payload_at(PAYLOADS_LENGTH), std::out_of_range);
    }

    SECTION("Shifting payloads right by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0);
        REQUIRE(payload.get_payload_at(1) == 1);
        REQUIRE(payload.get_payload_at(2) == 2);
    }

    SECTION("Shifting payloads left by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0);
        REQUIRE(payload.get_payload_at(0) == 2);
    }

    SECTION("Shifting payloads right by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0, 2);
        REQUIRE(payload.get_payload_at(2) == 1);
        REQUIRE(payload.get_payload_at(3) == 2);
    }

    SECTION("Shifting payloads left by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.shift_left_from_index(0, 2);
        REQUIRE(payload.get_payload_at(0) == 3);
    }

    SECTION("Max index management") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(1, 95);
        payload.set_payload_at(2, 99);
        REQUIRE(payload.get_payload_at(2) == 99);
        REQUIRE(payload.set_payload_at(3, 100));
        REQUIRE(payload.get_payload_at(3) == 100);
    }

    SECTION("Max index handling when setting payloads at different indices") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(2, 95);
    }

    SECTION("Edge case: Setting payload at maximum index") {
        REQUIRE(payload.set_payload_at(PAYLOADS_LENGTH - 1, 100));
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 100);
    }

    SECTION("Edge case: Shifting right when at capacity") {
        for (size_t i = 0; i < PAYLOADS_LENGTH; ++i) {
            payload.set_payload_at(i, static_cast<int>(i));
        }
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 67);
        payload.shift_right_from_index(0, 1);
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 66);
    }

    SECTION("Edge case: Shifting left beyond zero") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0, 1);
        REQUIRE(payload.get_payload_at(0) == 2);
    }

    SECTION("Operator [] access") {
        payload.set_payload_at(0, 42);
        REQUIRE(payload[0] == 42);
        // REQUIRE_THROWS_AS(payload[PAYLOADS_LENGTH], std::out_of_range);
    }

    SECTION("Setting big number") {
        payload.set_payload_at(0, 255);
        REQUIRE(payload.get_payload_at(0) == 255);
        payload.set_payload_at(1, 256);
        REQUIRE(payload.get_payload_at(1) == 0);  // Overflow
    }
}

TEST_CASE("Payload: uint16_t functionality") {
    Payload<TestPayloadUint16> payload;

    SECTION("Setting and getting payloads") {
        REQUIRE(payload.set_payload_at(0, 42));
        REQUIRE(payload.get_payload_at(0) == 42);
        // REQUIRE_THROWS_AS(payload.get_payload_at(PAYLOADS_LENGTH), std::out_of_range);
    }

    SECTION("Shifting payloads right by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0);
        REQUIRE(payload.get_payload_at(1) == 1);
        REQUIRE(payload.get_payload_at(2) == 2);
    }

    SECTION("Shifting payloads left by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0);
        REQUIRE(payload.get_payload_at(0) == 2);
    }

    SECTION("Shifting payloads right by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0, 2);
        REQUIRE(payload.get_payload_at(2) == 1);
        REQUIRE(payload.get_payload_at(3) == 2);
    }

    SECTION("Shifting payloads left by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.shift_left_from_index(0, 2);
        REQUIRE(payload.get_payload_at(0) == 3);
    }

    SECTION("Max index management") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(1, 95);
        payload.set_payload_at(2, 99);
        REQUIRE(payload.get_payload_at(2) == 99);
        REQUIRE(payload.set_payload_at(3, 100));
        REQUIRE(payload.get_payload_at(3) == 100);
    }
    SECTION("Max index handling when setting payloads at different indices") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(2, 95);
    }
    SECTION("Edge case: Setting payload at maximum index") {
        REQUIRE(payload.set_payload_at(PAYLOADS_LENGTH - 1, 100));
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 100);
    }
    SECTION("Edge case: Shifting right when at capacity") {
        for (size_t i = 0; i < PAYLOADS_LENGTH; ++i) {
            payload.set_payload_at(i, static_cast<int>(i));
        }
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 67);
        payload.shift_right_from_index(0, 1);
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 66);
    }
    SECTION("Edge case: Shifting left beyond zero") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0, 1);
        REQUIRE(payload.get_payload_at(0) == 2);
    }
    SECTION("Operator [] access") {
        payload.set_payload_at(0, 42);
        REQUIRE(payload[0] == 42);
        // REQUIRE_THROWS_AS(payload[PAYLOADS_LENGTH], std::out_of_range);
    }
    SECTION("Setting big number") {
        payload.set_payload_at(0, 65535);
        REQUIRE(payload.get_payload_at(0) == 65535);
        payload.set_payload_at(1, 65536);
        REQUIRE(payload.get_payload_at(1) == 0);  // Overflow
    }
    SECTION("Setting negative number") {
        payload.set_payload_at(0, -1);
        REQUIRE(payload.get_payload_at(0) == 65535);  // Underflow
        payload.set_payload_at(1, -2);
        REQUIRE(payload.get_payload_at(1) == 65534);  // Underflow
    }
    SECTION("Setting zero") {
        payload.set_payload_at(0, 0);
        REQUIRE(payload.get_payload_at(0) == 0);
        payload.set_payload_at(1, 0);
        REQUIRE(payload.get_payload_at(1) == 0);
    }
    SECTION("Setting maximum value") {
        payload.set_payload_at(0, 65535);
        REQUIRE(payload.get_payload_at(0) == 65535);
        payload.set_payload_at(1, 65535);
        REQUIRE(payload.get_payload_at(1) == 65535);
    }
    SECTION("Setting minimum value") {
        payload.set_payload_at(0, 0);
        REQUIRE(payload.get_payload_at(0) == 0);
        payload.set_payload_at(1, 0);
        REQUIRE(payload.get_payload_at(1) == 0);
    }
} 

TEST_CASE("Payload: uint64_t functionality") {
    Payload<TestPayloadUint64> payload;

    SECTION("Setting and getting payloads") {
        REQUIRE(payload.set_payload_at(0, 42));
        REQUIRE(payload.get_payload_at(0) == 42);
        // REQUIRE_THROWS_AS(payload.get_payload_at(PAYLOADS_LENGTH), std::out_of_range);
    }

    SECTION("Shifting payloads right by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0);
        REQUIRE(payload.get_payload_at(1) == 1);
        REQUIRE(payload.get_payload_at(2) == 2);
    }

    SECTION("Shifting payloads left by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0);
        REQUIRE(payload.get_payload_at(0) == 2);
    }

    SECTION("Shifting payloads right by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0, 2);
        REQUIRE(payload.get_payload_at(2) == 1);
        REQUIRE(payload.get_payload_at(3) == 2);
    }

    SECTION("Shifting payloads left by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.shift_left_from_index(0, 2);
        REQUIRE(payload.get_payload_at(0) == 3);
    }

    SECTION("Max index management") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(1, 95);
        payload.set_payload_at(2, 99);
        REQUIRE(payload.get_payload_at(2) == 99);
        REQUIRE(payload.set_payload_at(3, 100));
        REQUIRE(payload.get_payload_at(3) == 100);
    }
    SECTION("Max index handling when setting payloads at different indices") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(2, 95);
    }
    SECTION("Edge case: Setting payload at maximum index") {
        REQUIRE(payload.set_payload_at(PAYLOADS_LENGTH - 1, 100));
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 100);
    }
    SECTION("Edge case: Shifting right when at capacity") {
        for (size_t i = 0; i < PAYLOADS_LENGTH; ++i) {
            payload.set_payload_at(i, static_cast<int>(i));
        }
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 67);
        payload.shift_right_from_index(0, 1);
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1) == 66);
    }
    SECTION("Edge case: Shifting left beyond zero") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0, 1);
        REQUIRE(payload.get_payload_at(0) == 2);
    }
    SECTION("Operator [] access") {
        payload.set_payload_at(0, 42);
        REQUIRE(payload[0] == 42);
        // REQUIRE_THROWS_AS(payload[PAYLOADS_LENGTH], std::out_of_range);
    }
    SECTION("Setting big number") {
        payload.set_payload_at(0, 18446744073709551615ULL);
        REQUIRE(payload.get_payload_at(0) == 18446744073709551615ULL);
        payload.set_payload_at(1, 18446744073709551616ULL);
        REQUIRE(payload.get_payload_at(1) == 0);  // Overflow
    }
    SECTION("Setting negative number") {
        payload.set_payload_at(0, -1);
        REQUIRE(payload.get_payload_at(0) == 18446744073709551615ULL);  // Underflow
        payload.set_payload_at(1, -2);
        REQUIRE(payload.get_payload_at(1) == 18446744073709551614ULL);  // Underflow
    }
    SECTION("Setting zero") {
        payload.set_payload_at(0, 0);
        REQUIRE(payload.get_payload_at(0) == 0);
        payload.set_payload_at(1, 0);
        REQUIRE(payload.get_payload_at(1) == 0);
    }
    SECTION("Setting maximum value") {
        payload.set_payload_at(0, 18446744073709551615ULL);
        REQUIRE(payload.get_payload_at(0) == 18446744073709551615ULL);
        payload.set_payload_at(1, 18446744073709551615ULL);
        REQUIRE(payload.get_payload_at(1) == 18446744073709551615ULL);
    }
}

TEST_CASE("Payload: extraBits functionality") {
    Payload<> payload;

    SECTION("Set and get extra bits at an index") {
        payload.set_payload_at(0, 42);
        payload.set_extra_bits_at(5, 0, 2);  // Set extra bits at index 0
        auto extraBits = payload.get_extra_bits_at(0);
        REQUIRE(extraBits.first == (NUMBER_EXTRA_BITS - 2 - 1));  // Valid bits count after considering age
        REQUIRE(extraBits.second == 5);                           // The extra bits value should match
    }

    SECTION("Set and get extra bits at multiple indices") {
        payload.set_payload_at(0, 42);
        payload.set_payload_at(1, 84);
        payload.set_extra_bits_at(3, 0, 1);
        payload.set_extra_bits_at(7, 1, 3);

        auto extraBits0 = payload.get_extra_bits_at(0);
        auto extraBits1 = payload.get_extra_bits_at(1);

        REQUIRE(extraBits0.first == (NUMBER_EXTRA_BITS - 1 - 1));  // Valid bits count for index 0
        REQUIRE(extraBits0.second == 3);

        REQUIRE(extraBits1.first == (NUMBER_EXTRA_BITS - 3 - 1));  // Valid bits count for index 1
        REQUIRE(extraBits1.second == 7);
    }

    SECTION("Shifting payloads and extra bits right by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_extra_bits_at(5, 0, 1);
        payload.set_extra_bits_at(10, 1, 2);
        payload.shift_right_from_index(0);

        REQUIRE(payload.get_payload_at(1) == 1);
        REQUIRE(payload.get_payload_at(2) == 2);

        auto extraBits1 = payload.get_extra_bits_at(1);
        auto extraBits2 = payload.get_extra_bits_at(2);

        REQUIRE(extraBits1.first == (NUMBER_EXTRA_BITS - 1 - 1));
        REQUIRE(extraBits1.second == 5);

        REQUIRE(extraBits2.first == (NUMBER_EXTRA_BITS - 2 - 1));
        REQUIRE(extraBits2.second == 10);
    }

    SECTION("Shifting payloads and extra bits left by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.set_extra_bits_at(5, 0, 1);
        payload.set_extra_bits_at(10, 1, 2);
        payload.set_extra_bits_at(15, 2, 3);

        payload.shift_left_from_index(0);

        REQUIRE(payload.get_payload_at(0) == 2);
        REQUIRE(payload.get_payload_at(1) == 3);

        auto extraBits0 = payload.get_extra_bits_at(0);
        auto extraBits1 = payload.get_extra_bits_at(1);

        REQUIRE(extraBits0.first == (NUMBER_EXTRA_BITS - 2 - 1));
        REQUIRE(extraBits0.second == 10);

        REQUIRE(extraBits1.first == (NUMBER_EXTRA_BITS - 3 - 1));
        REQUIRE(extraBits1.second == 15);
    }

    SECTION("Shifting payloads and extra bits right by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.set_extra_bits_at(5, 0, 1);
        payload.set_extra_bits_at(10, 1, 2);
        payload.set_extra_bits_at(15, 2, 3);

        payload.shift_right_from_index(0, 2);

        REQUIRE(payload.get_payload_at(2) == 1);
        REQUIRE(payload.get_payload_at(3) == 2);

        auto extraBits2 = payload.get_extra_bits_at(2);
        auto extraBits3 = payload.get_extra_bits_at(3);

        REQUIRE(extraBits2.first == (NUMBER_EXTRA_BITS - 1 - 1));
        REQUIRE(extraBits2.second == 5);

        REQUIRE(extraBits3.first == (NUMBER_EXTRA_BITS - 2 - 1));
        REQUIRE(extraBits3.second == 10);
    }

    SECTION("Shifting payloads and extra bits left by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.set_payload_at(3, 4);
        payload.set_extra_bits_at(5, 0, 1);
        payload.set_extra_bits_at(10, 1, 2);
        payload.set_extra_bits_at(15, 2, 3);
        payload.set_extra_bits_at(20, 3, 4);

        payload.shift_left_from_index(0, 2);
        REQUIRE(payload.get_payload_at(0) == 3);
        REQUIRE(payload.get_payload_at(1) == 4);

        auto extraBits0 = payload.get_extra_bits_at(0);
        auto extraBits1 = payload.get_extra_bits_at(1);

        REQUIRE(extraBits0.first == (NUMBER_EXTRA_BITS - 3 - 1));
        REQUIRE(extraBits0.second == 15);

        REQUIRE(extraBits1.first == (NUMBER_EXTRA_BITS - 4 - 1));
        REQUIRE(extraBits1.second == 4);
    }
}

TEST_CASE("Payload: extraBits for uint64_t") {
    Payload<TestPayloadUint64ExtraBits> payload;

    SECTION("Set and get extra bits at an index") {
        payload.set_payload_at(0, 42);
        payload.set_extra_bits_at(5, 0, 2);  // Set extra bits at index 0
        auto extraBits = payload.get_extra_bits_at(0);
        REQUIRE(extraBits.first == (NUMBER_EXTRA_BITS - 2 - 1));  // Valid bits count after considering age
        REQUIRE(extraBits.second == 5);                           // The extra bits value should match
    }

    SECTION("Set and get extra bits at multiple indices") {
        payload.set_payload_at(0, 42);
        payload.set_payload_at(1, 84);
        payload.set_extra_bits_at(3, 0, 1);
        payload.set_extra_bits_at(7, 1, 3);

        auto extraBits0 = payload.get_extra_bits_at(0);
        auto extraBits1 = payload.get_extra_bits_at(1);

        REQUIRE(extraBits0.first == (NUMBER_EXTRA_BITS - 1 - 1));  // Valid bits count for index 0
        REQUIRE(extraBits0.second == 3);

        REQUIRE(extraBits1.first == (NUMBER_EXTRA_BITS - 3 - 1));  // Valid bits count for index 1
        REQUIRE(extraBits1.second == 7);
    }

    SECTION("Shifting payloads and extra bits right by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_extra_bits_at(5, 0, 1);
        payload.set_extra_bits_at(10, 1, 2);
        payload.shift_right_from_index(0);

        REQUIRE(payload.get_payload_at(1) == 1);
        REQUIRE(payload.get_payload_at(2) == 2);

        auto extraBits1 = payload.get_extra_bits_at(1);
        auto extraBits2 = payload.get_extra_bits_at(2);

        REQUIRE(extraBits1.first == (NUMBER_EXTRA_BITS - 1 - 1));
        REQUIRE(extraBits1.second == 5);

        REQUIRE(extraBits2.first == (NUMBER_EXTRA_BITS - 2 - 1));
        REQUIRE(extraBits2.second == 10);
    }
    SECTION("Shifting payloads and extra bits left by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.set_extra_bits_at(5, 0, 1);
        payload.set_extra_bits_at(10, 1, 2);
        payload.set_extra_bits_at(15, 2, 3);

        payload.shift_left_from_index(0);

        REQUIRE(payload.get_payload_at(0) == 2);
        REQUIRE(payload.get_payload_at(1) == 3);

        auto extraBits0 = payload.get_extra_bits_at(0);
        auto extraBits1 = payload.get_extra_bits_at(1);

        REQUIRE(extraBits0.first == (NUMBER_EXTRA_BITS - 2 - 1));
        REQUIRE(extraBits0.second == 10);

        REQUIRE(extraBits1.first == (NUMBER_EXTRA_BITS - 3 - 1));
        REQUIRE(extraBits1.second == 15);
    }
    SECTION("Shifting payloads and extra bits right by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.set_extra_bits_at(5, 0, 1);
        payload.set_extra_bits_at(10, 1, 2);
        payload.set_extra_bits_at(15, 2, 3);

        payload.shift_right_from_index(0, 2);

        REQUIRE(payload.get_payload_at(2) == 1);
        REQUIRE(payload.get_payload_at(3) == 2);

        auto extraBits2 = payload.get_extra_bits_at(2);
        auto extraBits3 = payload.get_extra_bits_at(3);

        REQUIRE(extraBits2.first == (NUMBER_EXTRA_BITS - 1 - 1));
        REQUIRE(extraBits2.second == 5);

        REQUIRE(extraBits3.first == (NUMBER_EXTRA_BITS - 2 - 1));
        REQUIRE(extraBits3.second == 10);
    }
}
#ifdef ENABLE_XDP
TEST_CASE("Payload Var Len: class tests") {
    Payload<TraitsLI> payload;
    
    SECTION("Initial state") {
        payload.set_init_page_of_block(10);
        REQUIRE(payload.set_payload_at(0, 1));
        REQUIRE(payload.get_payload_at(0) == 10);
        REQUIRE(payload.set_payload_at(1, 1));
        REQUIRE(payload.get_payload_at(0) == 10);
        REQUIRE(payload.get_payload_at(1) == 11);
        REQUIRE(payload.set_payload_at(2, 2));
        REQUIRE(payload.get_payload_at(2) == 256);
    }

    SECTION("set_init_page_of_block and basic payload operations") {
        // Initialize base page
        payload.set_init_page_of_block(10);

        // Insert first payload of length 1
        REQUIRE(payload.set_payload_at(0, 1));
        REQUIRE(payload.get_payload_at(0) == 10);

        // Insert second payload of length 1
        REQUIRE(payload.set_payload_at(1, 1));
        REQUIRE(payload.get_payload_at(1) == 11);

        // Insert third payload of length 2
        REQUIRE(payload.set_payload_at(2, 2));
        REQUIRE(payload.get_payload_at(2) == 256);
    }

    SECTION("setting a zero-length payload should throw") {
        payload.set_init_page_of_block(5);
        REQUIRE_THROWS_AS(payload.set_payload_at(0, 0), std::invalid_argument);
    }

    SECTION("shift operations are unsupported for var-len payloads") {
        payload.set_init_page_of_block(0);
        REQUIRE_THROWS_AS(payload.shift_right_from_index(0), std::runtime_error);
        REQUIRE_THROWS_AS(payload.shift_left_from_index(0), std::runtime_error);
    }

    SECTION("has_space becomes false when capacity is reached") {
        payload.set_init_page_of_block(5);
        size_t max_slots = COUNT_SLOT - 1;  // Based on SIZE_VAR_LEN_BITSET
        // Fill with single-bit payloads until full
        for (size_t i = 0; i < max_slots; ++i) {
            REQUIRE(payload.set_payload_at(i, 2));
        }

        REQUIRE(payload.set_payload_at(max_slots, 2));
    }
}
#endif
