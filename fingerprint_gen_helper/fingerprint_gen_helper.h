#pragma once
#include <iostream>

#include "../bitset_wrapper/bitset_wrapper.h"
#include "../config/config.h"

uint64_t getFP(size_t lslotIdx, size_t segmentIdx, size_t blockIdx, const size_t FP_index, const std::string &fingerprint) {
    BitsetWrapper<REGISTER_SIZE> fp;
    int end_segment = static_cast<int>(FP_index) - 2 * COUNT_SLOT_BITS;
    for (auto i = 0; i < end_segment; i++) {
        fp.set(i, segmentIdx % 2);
        segmentIdx /= 2;
    }
    for (auto i = (int)(FP_index - 2 * COUNT_SLOT_BITS); i < (int)(FP_index - COUNT_SLOT_BITS); i++) {
        fp.set(i, blockIdx % 2);
        blockIdx /= 2;
    }
    for (auto i = (int)(FP_index - COUNT_SLOT_BITS); i < (int)FP_index; i++) {
        fp.set(i, lslotIdx % 2);
        lslotIdx /= 2;
    }
    size_t i = FP_index;
    for (const auto c : fingerprint) {
        fp.set(i, c - '0' == 1);
        i++;
    }
    // std::cout << "fp:\n\t";
    // fp.printBitset();
    return fp.bitset[0];
}
