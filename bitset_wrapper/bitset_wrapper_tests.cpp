#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <iostream>

#include "bitset_wrapper.h"

TEST_CASE("BitsetWrapper: Initialization and Basic Setting/Getting",
          "[BitsetWrapper]") {
    BitsetWrapper<128> bitset;
    REQUIRE(bitset.getInputString() ==
            std::string(128, '0')); // All bits should be initialized to 0

    bitset.set(0, true);
    REQUIRE(bitset.get(0) == true);

    bitset.set(127, true);
    REQUIRE(bitset.get(127) == true);
    REQUIRE(bitset.getInputString() == "1" + std::string(126, '0') + "1");
}

TEST_CASE("BitsetWrapper: fast set", "[BitsetWrapper]") {
    BitsetWrapper<64> bitset;
    bitset.set(0, true);
    bitset.set(10, true);
    bitset.set(18, true);

    bitset.set_fast_one_reg(0, 11, 13, 0b01);

    REQUIRE(bitset.get(0) == true);
    REQUIRE(bitset.get(10) == true);
    REQUIRE(bitset.get(18) == true);
    REQUIRE(bitset.get(11) == true);
    REQUIRE(bitset.get(12) == false);

    bitset.set_fast_one_reg(0, 15, 20, 0b00100);
    REQUIRE(bitset.get(17) == true);
    REQUIRE(bitset.get(18) == false);
    REQUIRE(bitset.get(19) == false);

    bitset.set(63, true);

    bitset.set_fast_one_reg(0, 0, 64, 0);
    REQUIRE(bitset.bitset[0] == 0);
}

TEST_CASE("BitsetWrapper: fast set two reg", "[BitsetWrapper]") {
    BitsetWrapper<512> bitset;

    bitset.set_fast_two_reg(63, 66, 0b101);
    REQUIRE(bitset.get(62) == false);
    REQUIRE(bitset.get(63) == true);
    REQUIRE(bitset.get(64) == false);
    REQUIRE(bitset.get(65) == true);
    REQUIRE(bitset.get(66) == false);

    bitset.set_fast_two_reg(127, 130, 0b101);
    REQUIRE(bitset.get(127) == true);
    REQUIRE(bitset.get(128) == false);
    REQUIRE(bitset.get(129) == true);
    REQUIRE(bitset.get(130) == false);
}

TEST_CASE("BitsetWrapper: Input String Handling", "[BitsetWrapper]") {
    SECTION("Setting and getting valid input string") {
        BitsetWrapper<64> bitset;
        bitset.setInputString("1100");
        REQUIRE(bitset.getInputString() == "1100" + std::string(60, '0'));
    }

    SECTION("Handling invalid characters in input string") {
        BitsetWrapper<64> bitset;
        REQUIRE_THROWS_AS(bitset.setInputString("11002"),
                          std::invalid_argument);
    }

    SECTION("Input string longer than bitset size") {
        BitsetWrapper<4> bitset;
        bitset.setInputString("11110"); // Only the first 4 bits should be set
        REQUIRE(bitset.getInputString() == "1111");
    }
}

TEST_CASE("BitsetWrapper: range and range_fast functionality",
          "[BitsetWrapper]") {
    SECTION("range functionality") {
        BitsetWrapper<64> bitset("1111000000001111");
        REQUIRE(bitset.range(8, 16) == 0b00001111);
        REQUIRE(bitset.range(0, 8) == 0b11110000);
        REQUIRE_THROWS_AS(bitset.range(63, 65),
                          std::invalid_argument); // Testing out-of-bounds
    }

    SECTION("range_fast functionality with valid ranges") {
        BitsetWrapper<64> bitset("1111000000001111");
        REQUIRE(bitset.range_fast(0, 8) == 0b00001111);
        REQUIRE(bitset.range_fast(8, 16) == 0b11110000);
    }
    SECTION("range_fast functionality with valid ranges") {
        BitsetWrapper<64> bitset("1111001000001111");
        REQUIRE(bitset.range_fast(3, 8) == 0b01001);
    }
    SECTION("range_fast invalid ranges") {
        BitsetWrapper<64> bitset(
            "0000000000000000000000000000000000000000000000000000000000000000");
        REQUIRE_THROWS_AS(bitset.range_fast(0, 65),
                          std::invalid_argument); // Out-of-bounds
        REQUIRE_THROWS_AS(
            bitset.range_fast(60, 5),
            std::invalid_argument); // Invalid range: index2 < index1
    }

    SECTION("range with 128-bit bitset") {
        BitsetWrapper<128> bitset(
            "000000000000000000000000000000000000000000000000000000000000110011"
            "00110000000000000000000000000000000000000000000000000000000000");
        REQUIRE(bitset.range(60, 71) == 0b11001100110);
    }

    SECTION("range_fast with 128-bit bitset") {
        BitsetWrapper<128> bitset(
            "000000000000000000000000000000000000000000000000000000000000110011"
            "00110000000000000000000000000000000000000000000000000000000000");
        REQUIRE(bitset.range_fast(60, 71) == 0b01100110011);
        REQUIRE(bitset.range_fast_2(60, 71) == 0b01100110011);
    }
}

TEST_CASE("BitsetWrapper: range_fast_same_index functionality",
          "[BitsetWrapper]") {
    SECTION("range_fast functionality with valid ranges") {
        BitsetWrapper<64> bitset("11110000000001111");
        REQUIRE(bitset.range_fast_2(0, 8) ==
                bitset.range_fast_one_reg(0, 0, 8));
        REQUIRE(bitset.range_fast_2(8, 16) ==
                bitset.range_fast_one_reg(0, 8, 16));
    }
}

TEST_CASE("BitsetWrapper: Rank and Select", "[BitsetWrapper]") {
    BitsetWrapper<128> bitset("100101");
    BitsetWrapper<1024> bitset2(
        "1011011010100100001100011011001111111010111101100010101011100111011000"
        "0100011000100010100101000100110011000000101101010011001110101001101001"
        "0101001011010011101010010001100100011001000001010001000111011000110110"
        "0001110101110011001110010100101011110101010010110010000101001011100011"
        "1011011010001010111110111111100011010111101011111111011000111001110100"
        "0100110000010000010000011100101100110001011100110101010101011101011111"
        "1001100001000110010001111101011000011101100011101010010000011001100010"
        "1111010101101110000111100100101010101111110101101101111000001011101111"
        "0010000010100001010100111110100110110000010111110010110110010000001101"
        "1100010110101000011101010001001101000100000000011100001100000100011110"
        "0011000101000100011000101100010000001110000100000110110101000011111100"
        "0001110101110000110001001110000100110110111010001100000001111100010100"
        "0110110101100101100101101011110001011110100010111011000001100000010011"
        "1110000010001011010001010110100101101001010011101111000110000110001111"
        "10110001011111011100101010100010100010100111");
    BitsetWrapper<64> bitset3;
    bitset3.setInputInt64(~0ll);
    SECTION("Rank operation") {
        REQUIRE(bitset.rank(0) == 0); // Only one set bit at index 0
        REQUIRE(bitset.rank(2) == 1); // Two set bits up to index 2
        REQUIRE(bitset.rank(4) == 2); // Two set bits up to index 2
        REQUIRE(bitset.rank(6) ==
                3); // Three set bits in the entire input string
        REQUIRE(bitset.rank_dumb(0) == 0); // Only one set bit at index 0
        REQUIRE(bitset.rank_dumb(2) == 1); // Two set bits up to index 2
        REQUIRE(bitset.rank_dumb(4) == 2); // Two set bits up to index 2
        REQUIRE(bitset.rank_dumb(6) ==
                3); // Three set bits in the entire input string
    }

    SECTION("Select operation") {
        REQUIRE(bitset.select(1) == 0);      // First set bit is at index 0
        REQUIRE(bitset.select(2) == 3);      // Second set bit is at index 2
        REQUIRE(bitset.select(3) == 5);      // Third set bit is at index 5
        REQUIRE(bitset.select_dumb(1) == 0); // First set bit is at index 0
        REQUIRE(bitset.select_dumb(2) == 3); // Second set bit is at index 2
        REQUIRE(bitset.select_dumb(3) == 5); // Third set bit is at index 5
    }

    SECTION("Rank operation bigger") {
        REQUIRE(bitset2.rank(629) ==
                bitset2.rank_dumb(
                    629)); // Three set bits in the entire input string
        REQUIRE(
            bitset2.rank(29) ==
            bitset2.rank_dumb(29)); // Three set bits in the entire input string
        REQUIRE(bitset2.rank(1023) ==
                bitset2.rank_dumb(
                    1023)); // Three set bits in the entire input string
        REQUIRE(
            bitset2.rank(1) ==
            bitset2.rank_dumb(1)); // Three set bits in the entire input string
    }

    SECTION("Select operation") {
        REQUIRE(bitset2.select(400) ==
                bitset2.select_dumb(
                    400)); // Three set bits in the entire input string
        REQUIRE(bitset2.select(29) ==
                bitset2.select_dumb(
                    29)); // Three set bits in the entire input string
        REQUIRE(bitset2.select(490) ==
                bitset2.select_dumb(
                    490)); // Three set bits in the entire input string
        REQUIRE(bitset2.select(1) ==
                bitset2.select_dumb(
                    1)); // Three set bits in the entire input string
    }

    SECTION("Select operation 2") {
        REQUIRE(bitset3.select(64) ==
                63); // Three set bits in the entire input string
    }
}
TEST_CASE("BitsetWrapper: setInputInt64 functionality", "[BitsetWrapper]") {
    SECTION("Setting int64 with repeating pattern") {
        BitsetWrapper<64> bitset;
        int64_t repeating_number =
            0b1010101010101010101010101010101010101010101010101010101010101010;
        bitset.setInputInt64(repeating_number);

        // Verify that the bitset matches the repeating pattern
        std::string expected_pattern =
            "0101010101010101010101010101010101010101010101010101010101010101";
        REQUIRE(bitset.getInputString() == expected_pattern);

        // Verify specific bits
        REQUIRE(bitset.get(0) == !true);
        REQUIRE(bitset.get(1) == !false);
        REQUIRE(bitset.get(2) == !true);
        REQUIRE(bitset.get(63) == !false);
    }
}
TEST_CASE("BitsetWrapper: Shift Functionality", "[BitsetWrapper]") {
    BitsetWrapper<128> bitset;

    SECTION("Right Shift") {
        bitset.set(0, true);
        bitset.set(1, true);
        bitset.set(2, true);
        bitset.shift_smart(2, 0);

        REQUIRE(bitset.get(0) == false);
        REQUIRE(bitset.get(1) == false);
        REQUIRE(bitset.get(2) == true);
        REQUIRE(bitset.get(3) == true);
        REQUIRE(bitset.get(4) == true);
    }

    SECTION("left Shift2") {
        bitset.set(64 + 6, true);
        bitset.set(8, true);
        bitset.set(0, true);
        bitset.shift_smart(-6, 6);
        REQUIRE(bitset.get(0) == true);
        REQUIRE(bitset.get(64) == true);
    }
    SECTION("right Shift2") {
        bitset.set(64 + 4, true);
        bitset.set(8, true);
        bitset.set(0, true);
        bitset.shift_smart(5, 6);

        REQUIRE(bitset.get(0) == true);
        REQUIRE(bitset.get(13) == true);
        REQUIRE(bitset.get(73) == true);
    }
    SECTION("Right Shift") {
        bitset.set(0, true);
        bitset.set(1, true);
        bitset.set(2, true);
        bitset.deprecated_shift(2, 0);

        REQUIRE(bitset.get(0) == false);
        REQUIRE(bitset.get(1) == false);
        REQUIRE(bitset.get(2) == true);
        REQUIRE(bitset.get(3) == true);
        REQUIRE(bitset.get(4) == true);
    }

    SECTION("Right Shift harder") {
        bitset.set(0, true);
        bitset.set(1, true);
        bitset.set(2, true);
        bitset.set(120, true);
        bitset.set(123, true);
        bitset.set(124, false);
        bitset.deprecated_shift(1, 2, 124);

        REQUIRE(bitset.get(0) == true);
        REQUIRE(bitset.get(1) == true);
        REQUIRE(bitset.get(2) == false);
        REQUIRE(bitset.get(3) == true);
        REQUIRE(bitset.get(4) == false);
        REQUIRE(bitset.get(120) == false);
        REQUIRE(bitset.get(121) == true);
        REQUIRE(bitset.get(123) == false);
        REQUIRE(bitset.get(124) == false);
    }
    SECTION("Right Shift harder smart") {
        bitset.set(0, true);
        bitset.set(1, true);
        bitset.set(2, true);
        bitset.set(120, true);
        bitset.set(123, true);
        bitset.set(124, false);
        bitset.shift_smart(1, 2, 124);

        REQUIRE(bitset.get(0) == true);
        REQUIRE(bitset.get(1) == true);
        REQUIRE(bitset.get(2) == false);
        REQUIRE(bitset.get(3) == true);
        REQUIRE(bitset.get(4) == false);
        REQUIRE(bitset.get(120) == false);
        REQUIRE(bitset.get(121) == true);
        REQUIRE(bitset.get(123) == false);
        REQUIRE(bitset.get(124) == false);
    }
    SECTION("Left Shift") {
        bitset.set(127, true);
        bitset.set(126, true);
        bitset.set(125, true);
        bitset.deprecated_shift(-2, 0);

        REQUIRE(bitset.get(125) == true);
        REQUIRE(bitset.get(124) == true);
        REQUIRE(bitset.get(123) == true);
        REQUIRE(bitset.get(126) == false);
        REQUIRE(bitset.get(127) == false);
    }
    SECTION("Left Shift") {
        bitset.set(127, true);
        bitset.set(126, true);
        bitset.set(125, true);
        bitset.shift_smart(-2, 0);

        REQUIRE(bitset.get(125) == true);
        REQUIRE(bitset.get(124) == true);
        REQUIRE(bitset.get(123) == true);
        REQUIRE(bitset.get(126) == false);
        REQUIRE(bitset.get(127) == false);
    }
    SECTION("Shift with From Index") {
        bitset.set(10, true);
        bitset.set(11, true);
        bitset.set(12, true);
        bitset.deprecated_shift(2, 10);

        REQUIRE(bitset.get(10) == false);
        REQUIRE(bitset.get(11) == false);
        REQUIRE(bitset.get(12) == true);
        REQUIRE(bitset.get(13) == true);
        REQUIRE(bitset.get(14) == true);
    }
    SECTION("ShiftSmart vs. Deprecated Shift") {
        bitset.set(10, true);
        bitset.set(11, true);
        bitset.set(12, true);
        bitset.deprecated_shift(2, 10);
        BitsetWrapper<128> bitset2;
        bitset2.set(10, true);
        bitset2.set(11, true);
        bitset2.set(12, true);
        bitset2.shift_smart(2, 10);
        REQUIRE(bitset.getInputString() == bitset2.getInputString());
    }

    SECTION("Combined ShiftSmart and Deprecated Shift") {
        BitsetWrapper<256> bitset1;
        BitsetWrapper<256> bitset2;

        // Initialize bits
        for (size_t i = 0; i < 20; i += 3) {
            bitset1.set(i, true);
            bitset2.set(i, true);
        }

        // Apply deprecated_shift on the first bitset
        bitset1.deprecated_shift(5, 15);

        // Apply shift_smart on the second bitset
        bitset2.shift_smart(5, 15);

        // Verify the states are identical
        REQUIRE(bitset1.getInputString() == bitset2.getInputString());

        // Apply additional shifts
        bitset1.deprecated_shift(3, 10);
        bitset2.shift_smart(3, 10);

        // Verify again
        REQUIRE(bitset1.getInputString() == bitset2.getInputString());
    }

    SECTION("Boundary Cases with Overlapping Indices") {
        BitsetWrapper<128> bitset1;
        BitsetWrapper<128> bitset2;

        // Set overlapping regions
        for (size_t i = 50; i < 60; ++i) {
            bitset1.set(i, true);
            bitset2.set(i, true);
        }

        // Apply shifts that overlap
        bitset1.deprecated_shift(8, 55); // Affects the same region
        bitset2.shift_smart(8, 55);

        // Verify the results
        REQUIRE(bitset1.getInputString() == bitset2.getInputString());

        // Check an unrelated area remains unaffected
        for (size_t i = 20; i < 30; ++i) {
            REQUIRE(bitset1.get(i) == bitset2.get(i));
        }
    }

    SECTION("Performance and Consistency for Large Shifts") {
        BitsetWrapper<1024> bitset1;

        // Set random patterns
        for (size_t i = 0; i < 1024; i += 17) {
            bitset1.set(i, true);
        }

        // Perform large shifts
        bitset1.deprecated_shift(128, 128, 512);

        // Validate shifted pattern
        for (size_t i = 256; i < 512; ++i) {
            REQUIRE(bitset1.get(i) == ((i - 128) % 17 == 0));
        }

        // Ensure unshifted regions remain unchanged
        for (size_t i = 0; i < 128; ++i) {
            REQUIRE(bitset1.get(i) == (i % 17 == 0));
        }
        for (size_t i = 512; i < 1024; ++i) {
            REQUIRE(bitset1.get(i) == (i % 17 == 0));
        }
    }

    SECTION("Shift with From Index") {
        bitset.set(10, true);
        bitset.set(11, true);
        bitset.set(12, true);
        bitset.shift_smart(2, 10);

        REQUIRE(bitset.get(10) == false);
        REQUIRE(bitset.get(11) == false);
        REQUIRE(bitset.get(12) == true);
        REQUIRE(bitset.get(13) == true);
        REQUIRE(bitset.get(14) == true);
    }
    SECTION("Shift with From Index and Left Shift") {
        bitset.set(10, true);
        bitset.set(11, true);
        bitset.set(12, true);
        bitset.deprecated_shift(-2, 10);

        REQUIRE(bitset.get(10) == true);
        REQUIRE(bitset.get(11) == false);
        REQUIRE(bitset.get(12) == false);
    }
    SECTION("Shift with From Index and Left Shift") {
        bitset.set(10, true);
        bitset.set(11, true);
        bitset.set(12, true);
        bitset.shift_smart(-2, 10);

        REQUIRE(bitset.get(10) == true);
        REQUIRE(bitset.get(11) == false);
        REQUIRE(bitset.get(12) == false);
    }
    SECTION("Shift with zero steps does nothing") {
        bitset.set(5, true);
        bitset.deprecated_shift(0, 0);

        REQUIRE(bitset.get(5) == true);
    }
    SECTION("Shift with zero steps does nothing") {
        bitset.set(5, true);
        bitset.shift_smart(0, 0);

        REQUIRE(bitset.get(5) == true);
    }
}

TEST_CASE("BitsetWrapper::get_trailing_zero", "[bitset]") {
    BitsetWrapper<64> bw;
    bw.setInputString(
        "0000000000000000000000000000000000000000000000000000000000001111");
    REQUIRE(bw.get_trailing_zeros(0) == 60);

    bw.setInputString(
        "1000000000000000000000000000000000000000000000000000000000000000");
    REQUIRE(bw.get_trailing_zeros(0) == 0);

    bw.setInputString(
        "0000000000000000000000000000000000000000000000000000000000000001");
    REQUIRE(bw.get_trailing_zeros(0) == 63);
}

TEST_CASE("BitsetWrapper::get_second_leading_zeros", "[bitset]") {
    BitsetWrapper<64> bw;
    bw.setInputString(
        "0110000000000000000000000000000000000000000000000000000000000000");
    REQUIRE(bw.get_second_leading_zeros(0) == 62);

    bw.setInputString(
        "0011000000000000000000000000000000000000000000000000000000000000");
    REQUIRE(bw.get_second_leading_zeros(0) == 61);

    bw.setInputString(
        "1111000000000000000000000000000000000000000000000000000000000000");
    REQUIRE(bw.get_second_leading_zeros(0) == 61);

    bw.setInputString(
        "1111000000000000000000000000000000000000000000000000000000000010");
    REQUIRE(bw.get_second_leading_zeros(0) == 60);

    bw.setInputString(
        "1111000000000000000000000000000000000000000000000000000000000011");
    REQUIRE(bw.get_second_leading_zeros(0) == 1);

    bw.setInputString(
        "0000000000000000000000000000000000000000000000000000000001000000");
    REQUIRE(bw.get_second_leading_zeros(0) == 64);
}

TEST_CASE("BitsetWrapper::get_leading_zero", "[bitset]") {
    BitsetWrapper<64> bw;
    bw.setInputString(
        "0000000000000000000000000000000000000000000000000000000000001110");
    REQUIRE(bw.get_leading_zeros(0) == 1);

    bw.setInputString(
        "0000000000000000000000000000000000000000000000000000000000000001");
    REQUIRE(bw.get_leading_zeros(0) == 0);

    bw.setInputString(
        "1000000000000000000000000000000000000000000000000000000000000000");
    size_t res = bw.get_leading_zeros(0);
    REQUIRE(res == 63);
}

TEST_CASE("BitsetWrapper::GET_ONE and GET_ZERO MSB") {
    uint64_t zeroOnMSB = GET_ZERO_MSB(64);
    CHECK(zeroOnMSB == 0xffffffffffffffff);
    zeroOnMSB = GET_ZERO_MSB(8);
    CHECK(zeroOnMSB == 0xff);
    zeroOnMSB = GET_ZERO_MSB(0);
    CHECK(zeroOnMSB == 000000000);
    zeroOnMSB = GET_ZERO_MSB(1);
    CHECK(zeroOnMSB == 1);
    zeroOnMSB = GET_ONE_MSB(1);
    CHECK(zeroOnMSB == ~(GET_ZERO_MSB(1)));
    zeroOnMSB = GET_ONE_MSB(7);
    CHECK(zeroOnMSB == ~(GET_ZERO_MSB(7)));
    zeroOnMSB = GET_ONE_MSB(64);
    CHECK(zeroOnMSB == 0x0);
}

TEST_CASE("BitsetWrapper::op overloading equal") {
    BitsetWrapper<128> bw;
    BitsetWrapper<128> bw2;
    CHECK(bw == bw2);
    bw.bitset[0] = 101;
    CHECK(bw != bw2);
    bw2.bitset[0] = 101;
    CHECK(bw == bw2);
    bw2.bitset[1] = 1010;
    CHECK(bw != bw2);
    bw.bitset[1] = 1010;
    CHECK(bw == bw2);
}

TEST_CASE("BitsetWrapper::shift_smart bug") {
    {
        BitsetWrapper<256> bw;
        bw.setInputString(
            "1111100110111111111100110111111011110111111101011000000000000000"
            "0110101111111111110101010010011010110110101010011110101111000111"
            "0011001101111111001001110011100110110110110001111101110011110000"
            "0000000000000000000000000000000000000000000000000000000000000001");
        BitsetWrapper<256> bw2 = bw;
        bw.shift_smart(8, 188, bw.size() - 1);
        CHECK(bw == bw2);
    }
    {
        BitsetWrapper<256> bw;
        bw.setInputString(
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000111100000000000000000000000000000000000000000000000000000001");
        BitsetWrapper<256> bw2 = bw;
        bw.shift_smart(-8, 0, 196);
        CHECK(bw == bw2);
    }
}

TEST_CASE("BitsetWrapper: shift performance test") {
    auto dep_time = 0;
    auto smart_time = 0;
    auto test_time = 0;
    auto random_string = [](size_t size) {
        std::string str;
        for (size_t i = 0; i < size; ++i) {
            str += (rand() % 2) ? '1' : '0';
        }
        return str;
    };
    // write a function to generate random numbers between 0 and 255
    auto random_number = [](int left, int right) {
        return left + (rand() % (right - left));
    };
    auto loop_size = 100000;
    for (int i = 0; i < loop_size; ++i) {
        BitsetWrapper<256> bw;
        BitsetWrapper<256> bw2;
        auto left = random_number(0, 256), right = random_number(0, 256);
        int steps = random_number(-32, 32);
        if (left > right) {
            std::swap(left, right);
        }
        auto random_string1 = random_string(256);
        bw.setInputString(random_string1);
        bw2.setInputString(random_string1);
        {
            auto start = std::chrono::high_resolution_clock::now();
            bw.shift_smart(steps, left, right);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                      start);
            smart_time += duration.count();
        }
        {
            auto start = std::chrono::high_resolution_clock::now();
            bw2.deprecated_shift(steps, left, right);
            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                      start);
            dep_time += duration.count();
        }
        {
            auto start = std::chrono::high_resolution_clock::now();
            auto end = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::nanoseconds>(end -
                                                                      start);
            test_time += duration.count();
        }

        CHECK(bw == bw2);
    }
    std::cout << "smart time: " << smart_time / loop_size << std::endl;
    std::cout << "dep time: " << dep_time / loop_size << std::endl;
    std::cout << "test time: " << test_time / loop_size << std::endl;
}
