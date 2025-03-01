#include <catch2/catch_test_macros.hpp>

#include "payload.h"

constexpr auto PAYLOADS_LENGTH = DefaultTraits::PAYLOADS_LENGTH;
constexpr auto NUMBER_EXTRA_BITS = DefaultTraits::NUMBER_EXTRA_BITS;

TEST_CASE("Payload: class tests") {
    Payload<> payload;

    SECTION("Initial state") {
        REQUIRE(payload.get_payload_at(0, true) == -1);
        REQUIRE(payload.has_space());
        REQUIRE(payload.get_max_index() == -1);
    }

    SECTION("Setting and getting payloads") {
        REQUIRE(payload.set_payload_at(0, 42));
        REQUIRE(payload.get_payload_at(0, false) == 42);
        REQUIRE_THROWS_AS(payload.get_payload_at(PAYLOADS_LENGTH, false), std::out_of_range);
    }

    SECTION("Ignoring out of range") {
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH, true) == -1);
    }

    SECTION("Shifting payloads right by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0);
        REQUIRE(payload.get_payload_at(1, false) == 1);
        REQUIRE(payload.get_payload_at(2, false) == 2);
    }

    SECTION("Shifting payloads left by one") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0);
        REQUIRE(payload.get_payload_at(0, false) == 2);
        REQUIRE(payload.get_max_index() == 0);
    }

    SECTION("Shifting payloads right by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_right_from_index(0, 2);
        REQUIRE(payload.get_payload_at(2, false) == 1);
        REQUIRE(payload.get_payload_at(3, false) == 2);
    }

    SECTION("Shifting payloads left by multiple steps") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.set_payload_at(2, 3);
        payload.shift_left_from_index(0, 2);
        REQUIRE(payload.get_payload_at(0, false) == 3);
        REQUIRE(payload.get_max_index() == 0);
    }

    SECTION("Max index management") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(1, 95);
        payload.set_payload_at(2, 99);
        REQUIRE(payload.get_payload_at(2, false) == 99);
        REQUIRE(payload.set_payload_at(3, 100));
        REQUIRE(payload.get_payload_at(3, false) == 100);
        REQUIRE(payload.get_max_index() == 3);
    }

    SECTION("Max index handling when setting payloads at different indices") {
        payload.set_payload_at(0, 94);
        payload.set_payload_at(2, 95);
        REQUIRE(payload.get_max_index() == 2);
    }

    SECTION("Edge case: Setting payload at maximum index") {
        REQUIRE(payload.set_payload_at(PAYLOADS_LENGTH - 1, 100));
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1, false) == 100);
        REQUIRE(payload.get_max_index() == PAYLOADS_LENGTH - 1);
        REQUIRE_FALSE(payload.has_space());  // No more space left
    }

    SECTION("Edge case: Shifting right when at capacity") {
        for (size_t i = 0; i < PAYLOADS_LENGTH; ++i) {
            payload.set_payload_at(i, static_cast<int>(i));
        }
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1, false) == 67);
        payload.shift_right_from_index(0, 1);
        REQUIRE(payload.get_payload_at(PAYLOADS_LENGTH - 1, false) == 66);
        REQUIRE(payload.get_max_index() == PAYLOADS_LENGTH - 1);
    }

    SECTION("Edge case: Shifting left beyond zero") {
        payload.set_payload_at(0, 1);
        payload.set_payload_at(1, 2);
        payload.shift_left_from_index(0, 1);
        REQUIRE(payload.get_payload_at(0, false) == 2);
        REQUIRE(payload.get_max_index() == 0);
    }

    SECTION("Operator [] access") {
        payload.set_payload_at(0, 42);
        REQUIRE(payload[0] == 42);
        REQUIRE_THROWS_AS(payload[PAYLOADS_LENGTH], std::out_of_range);
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

        REQUIRE(payload.get_payload_at(1, false) == 1);
        REQUIRE(payload.get_payload_at(2, false) == 2);

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

        REQUIRE(payload.get_payload_at(0, false) == 2);
        REQUIRE(payload.get_payload_at(1, false) == 3);

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

        REQUIRE(payload.get_payload_at(2, false) == 1);
        REQUIRE(payload.get_payload_at(3, false) == 2);

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
        REQUIRE(payload.get_payload_at(0, false) == 3);
        REQUIRE(payload.get_payload_at(1, false) == 4);

        auto extraBits0 = payload.get_extra_bits_at(0);
        auto extraBits1 = payload.get_extra_bits_at(1);

        REQUIRE(extraBits0.first == (NUMBER_EXTRA_BITS - 3 - 1));
        REQUIRE(extraBits0.second == 15);

        REQUIRE(extraBits1.first == (NUMBER_EXTRA_BITS - 4 - 1));
        REQUIRE(extraBits1.second == 4);
    }
}
