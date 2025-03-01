#include <catch2/catch_test_macros.hpp>
#include <string>

#include "../bitset_wrapper/bitset_wrapper.h"
#include "BST.h"

TEST_CASE("BST: test creation 1") {
    /*
    └── [Node] Index: 0, FP till now:
        ├── [Leaf] FP: 00..
        └── [Node] Index: 1, FP till now: 1
            ├── [Leaf] FP: 100..
            └── [Leaf] FP: 110..
    */
    BitsetWrapper<REGISTER_SIZE> bw1;
    constexpr size_t bwTen = 3;
    bw1.setInputString("01111");

    BST<REGISTER_SIZE> bst(bwTen, 0);
    bst.createBST(bw1);
    auto [bw_res, firstInvalidIndex] = bst.getBitRepWrapper();
    REQUIRE(bw_res.getInputString(firstInvalidIndex) == "01111");
}

TEST_CASE("BST: test creation 2") {
    /*
    └── [Node] Index: 4, FP till now: 1100
        ├── [Leaf] FP: 110001..
        └── [Leaf] FP: 110011..
    */
    BitsetWrapper<REGISTER_SIZE> bw1;
    constexpr size_t bwTen = 2;
    bw1.setInputString("000011");

    BST<REGISTER_SIZE> bst(bwTen, 0);
    bst.createBST(bw1);
    auto [bw_res, firstInvalidIndex] = bst.getBitRepWrapper();
    REQUIRE(bw_res.getInputString(firstInvalidIndex) == "000011");
}

TEST_CASE("BST: test creation 3") {
    /*
    └── [Node] Index: 0, FP till now:
        ├── [Node] Index: 1, FP till now: 0
        │   ├── [Leaf] FP: 000..
        │   └── [Node] Index: 3, FP till now: 011
        │       ├── [Leaf] FP: 01101..
        │       └── [Leaf] FP: 01110..
        └── [Node] Index: 1, FP till now: 1
            ├── [Leaf] FP: 100..
            └── [Leaf] FP: 110..
    */
    BitsetWrapper<REGISTER_SIZE> bw1;
    constexpr size_t bwTen = 5;
    bw1.setInputString("001101110111");

    BST<REGISTER_SIZE> bst(bwTen, 0);
    bst.createBST(bw1);
    auto [bw_res, firstInvalidIndex] = bst.getBitRepWrapper();
    REQUIRE(bw_res.getInputString(firstInvalidIndex) == "001101110111");
}


TEST_CASE("BST: insertion test 1") {
    auto fp1 = "0101111";
    auto fp2 = "0011111";
    auto fp3 = "1111111";
    auto fp4 = "1111101";
    auto fp5 = "1110111";
    auto fp6 = "1111011";

    BitsetWrapper<FINGERPRINT_SIZE> bw1(fp1);
    BitsetWrapper<FINGERPRINT_SIZE> bw2(fp2);
    BitsetWrapper<FINGERPRINT_SIZE> bw3(fp3);
    BitsetWrapper<FINGERPRINT_SIZE> bw4(fp4);
    BitsetWrapper<FINGERPRINT_SIZE> bw5(fp5);
    BitsetWrapper<FINGERPRINT_SIZE> bw6(fp6);

    BST<REGISTER_SIZE> bst(1, 0);  // let's assume we have added fp1 before
    // add fp2
    auto fd = bst.get_first_diff_index(bw1, bw2);
    bst.insert(bw2, fd);
    auto rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "011");
    // add fp3
    fd = bst.get_first_diff_index(bw1, bw3);
    bst.insert(bw3, fd);
    rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "01111");
    // add fp4
    fd = bst.get_first_diff_index(bw3, bw4);
    bst.insert(bw4, fd);
    rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "001111000011");
    // add fp5
    fd = bst.get_first_diff_index(bw3, bw5);
    bst.insert(bw5, fd);
    rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "00111110001011");
    // add fp6
    fd = bst.get_first_diff_index(bw3, bw6);
    bst.insert(bw6, fd);
    rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "0011111000110111");
}

TEST_CASE("BST: delete test 1") {
    auto fp1 = "0101111";
    auto fp2 = "0011111";
    auto fp3 = "1111111";
    auto fp4 = "1111101";
    auto fp5 = "1110111";
    auto fp6 = "1111011";

    BitsetWrapper<FINGERPRINT_SIZE> bw1(fp1);
    BitsetWrapper<FINGERPRINT_SIZE> bw2(fp2);
    BitsetWrapper<FINGERPRINT_SIZE> bw3(fp3);
    BitsetWrapper<FINGERPRINT_SIZE> bw4(fp4);
    BitsetWrapper<FINGERPRINT_SIZE> bw5(fp5);
    BitsetWrapper<FINGERPRINT_SIZE> bw6(fp6);

    BST<REGISTER_SIZE> bst(1, 0);  // let's assume we have added fp1 before
    // add fp2
    auto fd = bst.get_first_diff_index(bw1, bw2);
    bst.insert(bw2, fd);
    // add fp3
    fd = bst.get_first_diff_index(bw1, bw3);
    bst.insert(bw3, fd);
    // add fp4
    fd = bst.get_first_diff_index(bw3, bw4);
    bst.insert(bw4, fd);
    // add fp5
    fd = bst.get_first_diff_index(bw3, bw5);
    bst.insert(bw5, fd);
    // add fp6
    fd = bst.get_first_diff_index(bw3, bw6);
    bst.insert(bw6, fd);
    auto rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "0011111000110111");

    bst.remove(bw6);
    rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "00111110001011");

    bst.remove(bw5);
    rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "001111000011");

    bst.remove(bw4);
    rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "01111");

    bst.remove(bw3);
    rep = bst.getBitRepWrapper();
    REQUIRE(rep.bw.getInputString(rep.firstInvalidIndex) == "011");

    bst.remove(bw2);
    rep = bst.getBitRepWrapper();
    CHECK(bst.getTenSize() == 1);
    CHECK(rep.firstInvalidIndex == 0);
}
