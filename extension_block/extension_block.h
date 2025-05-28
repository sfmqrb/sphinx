#pragma once

#include <chrono>
#include <iostream>

#include "../SSDLog/SSDLog.h"
#include "../block/block.h"
#include "../config/config.h"

// Forward‐declare the template so we can hold pointers to it
#ifdef ENABLE_XDP
template <
  typename TraitsGI, 
  typename TraitsLI, 
  typename TraitsLIBuffer
>
class XDP;
#endif

struct pairStruct {
    size_t tenBefore;
    size_t ten;
};


struct ExtendedBlockInfo {
    size_t remaining_bits_BW_index{0};
    BlockInfo blockInfo{};
};

struct StateExtendedBlk {
    int blockSizeChange;
    int bitWrapperSizeChange;
    int payloadSizeChange;
};

template<typename Traits = DefaultTraits>
class ExtensionBlock {
   public:
    inline static auto CALCULATE_EXTENDED_BLOCK_INDEX(const size_t blkIdx, const size_t lslotIdx) {
        return ((blkIdx + COUNT_SLOT - lslotIdx - 1) % Traits::SEGMENT_EXTENSION_BLOCK_SIZE);
    }
    inline static auto CALCULATE_LSLOT_BEFORE(const size_t lslotIdx) {
        return ((COUNT_SLOT - lslotIdx - 1) / Traits::SEGMENT_EXTENSION_BLOCK_SIZE);
    }
    // not sure if it's a good choice to put it here after the blk performance wise
    BitsetWrapper<EXTENDED_BIT_WRAPPER_SIZE> lslotSizesBW;
    Block<Traits> blk;
    inline ExtendedBlockInfo get_extended_block_info() const {
        ExtendedBlockInfo info;
        info.blockInfo = blk.get_block_info();
        info.remaining_bits_BW_index = lslotSizesBW.get_leading_zeros(EXTENDED_BIT_WRAPPER_SIZE / REGISTER_SIZE - 1);
        return info;
    }
    inline static ExtendedBlockInfo get_extended_block_info_DHT(Block<Traits> &_blk) {
        ExtendedBlockInfo info;
        info.blockInfo = _blk.get_block_info();
        info.remaining_bits_BW_index = COUNT_SLOT;
        return info;
    }
    inline size_t calculatePhysicalLSlotIndex(const size_t blkIdx, const size_t lslot_before) const {
        size_t number_of_l_slots_before_this_blk;
        const auto number_blocks_before = lslotSizesBW.rank(blkIdx);
        if (number_blocks_before == 0)
            number_of_l_slots_before_this_blk = 0;
        else 
            number_of_l_slots_before_this_blk = lslotSizesBW.select(number_blocks_before, 1) + 1;
        const auto num_all_lslots_before_this_lslot = number_of_l_slots_before_this_blk + lslot_before;
        return num_all_lslots_before_this_lslot;
    }

    pairStruct getNumberOfLSlotsAndSize(size_t blkIdx) const {
        auto number_of_blocks_before = lslotSizesBW.rank(blkIdx);
        if (number_of_blocks_before == 0)
            return pairStruct{0, lslotSizesBW.get(blkIdx) ? lslotSizesBW.select(1, 1) + 1 : 0};
        size_t lslots_before_minus_1 = lslotSizesBW.select(number_of_blocks_before, 1);
        auto pair_val = lslotSizesBW.get(blkIdx) ? lslotSizesBW.select_two(number_of_blocks_before, number_of_blocks_before + 1, 1) : std::make_pair(lslots_before_minus_1, lslots_before_minus_1);
        return pairStruct{pair_val.first + 1, pair_val.second - pair_val.first};
    }

    bool checkExpansion(Block<Traits> &originalBlock, const size_t originalBlockSlotIndex) const {
        const size_t originalSlotStartIndex = originalBlock.get_lslot_start(originalBlockSlotIndex);
        const size_t nextSlotStartIndex = originalBlock.get_lslot_start(originalBlockSlotIndex + 1);
        const size_t slotLength = nextSlotStartIndex - originalSlotStartIndex;
        const size_t tenValue = originalBlock.get_ten(originalBlockSlotIndex);

        const BlockInfo originalBlockInfo = originalBlock.get_block_info();
        const ExtendedBlockInfo extendedBlockInfo = get_extended_block_info();

        const int blockSizeChange = static_cast<int>(tenValue + slotLength);
        const int bitWrapperSizeChange = 1;
        const int originalBlockSizeChange = static_cast<int>(-slotLength + 1);

        const bool isExpansionPossible = blockSizeChange <= static_cast<int>(extendedBlockInfo.blockInfo.remainingBits) &&
                                         bitWrapperSizeChange <= static_cast<int>(extendedBlockInfo.remaining_bits_BW_index) &&
                                         originalBlockSizeChange <= static_cast<int>(originalBlockInfo.remainingBits);

        return isExpansionPossible;
    }
    static size_t getLSlotWithSuccessfulExpansion(Block<Traits> &originalBlock, const ExtensionBlock extension_blocks[], const size_t slotIdxSrcBlk, const size_t start_from_ex_blk) {
        int originalBlockSizeChange = 0;
        const auto originalBlockInfo = originalBlock.get_block_info();
        auto statusExBlks = std::array<StateExtendedBlk, Traits::SEGMENT_EXTENSION_BLOCK_SIZE>();
        for (size_t slotIdxIt = slotIdxSrcBlk; slotIdxIt > 0; slotIdxIt--) {
            const auto exBlkIdx = (start_from_ex_blk + (slotIdxSrcBlk - slotIdxIt)) % Traits::SEGMENT_EXTENSION_BLOCK_SIZE;
            const auto &exBlk = extension_blocks[exBlkIdx];

            const ExtendedBlockInfo extendedBlockInfo = exBlk.get_extended_block_info();

            const size_t originalSlotStartIndex = originalBlock.get_lslot_start(slotIdxIt);
            const size_t nextSlotStartIndex = originalBlock.get_lslot_start(slotIdxIt + 1);
            const size_t slotLength = nextSlotStartIndex - originalSlotStartIndex;
            const size_t tenValue = originalBlock.get_ten(slotIdxIt);

            statusExBlks[exBlkIdx].blockSizeChange += static_cast<int>(tenValue + slotLength);
            statusExBlks[exBlkIdx].bitWrapperSizeChange += 1; // may set two bits but adding only one bit into the second register
            statusExBlks[exBlkIdx].payloadSizeChange += static_cast<int>(tenValue);
            originalBlockSizeChange += static_cast<int>(-slotLength + 1); // -slotLength for the slot and +1 for overflowing bit
            // TODO: there is a bug here, caught in the memory benchmark fleck. Last correct commit: 18ef289846e53f9947d0e7c46ee1489a6aa46033
            if (statusExBlks[exBlkIdx].blockSizeChange > static_cast<int>(extendedBlockInfo.blockInfo.remainingBits) ||
                statusExBlks[exBlkIdx].bitWrapperSizeChange > static_cast<int>(extendedBlockInfo.remaining_bits_BW_index) ||
                statusExBlks[exBlkIdx].payloadSizeChange > static_cast<int>(extendedBlockInfo.blockInfo.remainingPayload)) {
                return COUNT_SLOT;
            }
            if (originalBlockSizeChange <= static_cast<int>(originalBlockInfo.remainingBits)) {
                return slotIdxIt;
            }
        }
        return COUNT_SLOT;
    }
    static size_t getLSlotWithSuccessfulExpansionDHT(Block<Traits> &originalBlock, Block<Traits> &targetBlock, const size_t slotIdxSrcBlk) {
        int originalBlockSizeChange = 0;
        const BlockInfo originalBlockInfo = originalBlock.get_block_info();
        auto statusTargetBlk = StateExtendedBlk();
        for (size_t slotIdxIt = slotIdxSrcBlk; slotIdxIt > 0; slotIdxIt--) {
            const ExtendedBlockInfo targetBlockInfo = get_extended_block_info_DHT(targetBlock);

            const size_t originalSlotStartIndex = originalBlock.get_lslot_start(slotIdxIt);
            const size_t nextSlotStartIndex = originalBlock.get_lslot_start(slotIdxIt + 1);
            const size_t slotLength = nextSlotStartIndex - originalSlotStartIndex;
            const size_t tenValue = originalBlock.get_ten(slotIdxIt);

            statusTargetBlk.blockSizeChange += static_cast<int>(tenValue + slotLength);
            statusTargetBlk.payloadSizeChange += static_cast<int>(tenValue);
            originalBlockSizeChange += static_cast<int>(-slotLength + 1); // -slotLength for the slot and +1 for overflowing bit

            if (statusTargetBlk.blockSizeChange > static_cast<int>(targetBlockInfo.blockInfo.remainingBits) ||
                statusTargetBlk.payloadSizeChange > static_cast<int>(targetBlockInfo.blockInfo.remainingPayload)) {
                return COUNT_SLOT;
            }
            if (originalBlockSizeChange <= static_cast<int>(originalBlockInfo.remainingBits)) {
                return slotIdxIt;
            }
        }
        return COUNT_SLOT;
    }
    bool moveLslotHere(const Block<Traits> &beforeExpBlk, const size_t lslotBefore, const ExpandedLSlot &expLSlot, const size_t orgBlkIdx) {
        // only works for the last lslot in the current block
        const bool age = true;
        const auto bw = expLSlot.bw;
        const auto firstInvalidIndex = expLSlot.firstInvalidIndex;
        const auto lslot_len = firstInvalidIndex;
        const auto ten = expLSlot.ten;
        const auto blkIdx = orgBlkIdx;

        const auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);

        // handle meta data
        const auto sizeAndLSlotsInfo = getNumberOfLSlotsAndSize(blkIdx);
        handleExtensionMetaData(blkIdx, lslotBefore, sizeAndLSlotsInfo);
        // this is not supposed to overflow
        blk.set_ten(actualIndex, ten, N - 1);
        if (ten == 0)
            return true;
        const auto new_lslot_start_index = blk.get_lslot_start(actualIndex);

        blk.bits.shift_smart(lslot_len, new_lslot_start_index, N - 1);
        blk.bits.set_fast_two_reg(new_lslot_start_index, new_lslot_start_index + lslot_len, bw.bitset[0]);

        const auto old_payload_index = expLSlot.payload_start;
        const auto new_payload_index = blk.get_index_start_lslot(actualIndex);

        blk.payload_list.shift_right_from_index(new_payload_index, ten);
        // could be faster benchmark it later
        for (size_t i = 0; i < ten; i++) {
            Payload<Traits>::swap(beforeExpBlk.payload_list, old_payload_index + i,
                                                           blk.payload_list, new_payload_index + i, age);
        }
        return true;
    }
    static bool moveLSlotsToMakeSpace(Block<Traits> &originalBlk, const size_t lslotIndexOriginalBlk, const size_t blkIdx, ExtensionBlock extension_blocks[]) {
        // only works for the last lslot in the current block
        const auto age = false;
        const size_t start_from_ex_blk = CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, lslotIndexOriginalBlk);
        const auto tillLSlotIdx = getLSlotWithSuccessfulExpansion(originalBlk, extension_blocks, lslotIndexOriginalBlk, start_from_ex_blk);
        if (tillLSlotIdx == COUNT_SLOT)
            return false;
        const size_t last_one_index = COUNT_SLOT - 1 - originalBlk.bits.get_leading_zeros(originalBlk.bits.NUM_REGS - 1) + REGISTER_SIZE * (originalBlk.bits.NUM_REGS - 1);

        for (size_t curr = lslotIndexOriginalBlk; curr >= tillLSlotIdx; curr--) {
            const auto exBlkIdx = (start_from_ex_blk + (lslotIndexOriginalBlk - curr)) % Traits::SEGMENT_EXTENSION_BLOCK_SIZE;
            auto &exBlk = extension_blocks[exBlkIdx];
            const size_t lslot_before = CALCULATE_LSLOT_BEFORE(curr);
            const size_t lslot_start_index = originalBlk.get_lslot_start(curr);
            const size_t next_lslot_start_index = originalBlk.get_lslot_start(curr + 1);
            const size_t lslot_len = next_lslot_start_index - lslot_start_index;
            const size_t ten = originalBlk.get_ten(curr);
            const auto actualIndex = exBlk.calculatePhysicalLSlotIndex(blkIdx, lslot_before);

            // handle meta data
            auto sizeAndLSlotsInfo = exBlk.getNumberOfLSlotsAndSize(blkIdx);
            exBlk.handleExtensionMetaData(blkIdx, lslot_before, sizeAndLSlotsInfo);

            exBlk.blk.set_ten(actualIndex, ten, N - 1);
            if (ten == 0)
                continue;

            size_t new_lslot_start_index = exBlk.blk.get_lslot_start(actualIndex);

            size_t old_payload_index = originalBlk.get_index_start_lslot(curr);
            auto max_index_ex = exBlk.blk.get_max_index();
            auto max_index_orig = originalBlk.get_max_index();

            exBlk.blk.bits.shift_smart(lslot_len, new_lslot_start_index, N - 1);
            exBlk.blk.bits.set_fast_two_reg(new_lslot_start_index, new_lslot_start_index + lslot_len,
                                            originalBlk.bits.range_fast_2(lslot_start_index, lslot_start_index + lslot_len));
            originalBlk.bits.set_fast_two_reg(lslot_start_index, lslot_start_index + lslot_len, 0);

            size_t new_payload_index = exBlk.blk.get_index_start_lslot(actualIndex);
            exBlk.blk.payload_list.shift_right_from_index(new_payload_index, ten, max_index_ex);
            for (size_t i = 0; i < ten; i++) {
                Payload<Traits>::swap(originalBlk.payload_list, old_payload_index + i,
                                                               exBlk.blk.payload_list, new_payload_index + i, age);
            }
            originalBlk.payload_list.shift_left_from_index(old_payload_index, ten, max_index_orig);
        }
        {
            // encoding the extended lslot index within the original block
            auto nLOI = last_one_index - (lslotIndexOriginalBlk - tillLSlotIdx + 1);
            originalBlk.bits.set_fast_one_reg(3, 3 * REGISTER_SIZE + nLOI, N, 0);
            originalBlk.bits.set(nLOI, true);
            originalBlk.bits.set(last_one_index, false);
        }
        return true;
    }
    static bool moveLSlotsToMakeSpaceDHT(Block<Traits> &originalBlk, Block<Traits> &targetBlk, const size_t lslotIndexOriginalBlk) {
        // only works for the last lslot in the current block
        const auto age = false;
        const auto tillLSlotIdx = getLSlotWithSuccessfulExpansionDHT(originalBlk, targetBlk, lslotIndexOriginalBlk);
        if (tillLSlotIdx == COUNT_SLOT)
            return false;
        const size_t last_one_index = COUNT_SLOT - 1 - originalBlk.bits.get_leading_zeros(originalBlk.bits.NUM_REGS - 1) + REGISTER_SIZE * (originalBlk.bits.NUM_REGS - 1);

        for (size_t curr = lslotIndexOriginalBlk; curr >= tillLSlotIdx; curr--) {
            const size_t lslot_start_index = originalBlk.get_lslot_start(curr);
            const size_t next_lslot_start_index = originalBlk.get_lslot_start(curr + 1);
            const size_t lslot_len = next_lslot_start_index - lslot_start_index;
            const size_t ten = originalBlk.get_ten(curr);

            // add new slot
            const auto actualIndex = 0;
            targetBlk.bits.shift_smart(1, actualIndex, REGISTER_SIZE);
            targetBlk.bits.set(actualIndex, false);

            // handle meta data
            targetBlk.set_ten(actualIndex, ten, N - 1);
            if (ten == 0)
                continue;

            size_t new_lslot_start_index = targetBlk.get_lslot_start(actualIndex);

            size_t old_payload_index = originalBlk.get_index_start_lslot(curr);

            targetBlk.bits.shift_smart(lslot_len, new_lslot_start_index, N - 1);
            targetBlk.bits.set_fast_two_reg(new_lslot_start_index, new_lslot_start_index + lslot_len,
                                            originalBlk.bits.range_fast_2(lslot_start_index, lslot_start_index + lslot_len));
            originalBlk.bits.set_fast_two_reg(lslot_start_index, lslot_start_index + lslot_len, 0);

            size_t new_payload_index = targetBlk.get_index_start_lslot(actualIndex);

            targetBlk.payload_list.shift_right_from_index(new_payload_index, ten);
            for (size_t i = 0; i < ten; i++) {
                Payload<Traits>::swap(originalBlk.payload_list, old_payload_index + i,
                                                               targetBlk.payload_list, new_payload_index + i, age);
            }
            originalBlk.payload_list.shift_left_from_index(old_payload_index, ten);
        }
        {
            originalBlk.bits.set(last_one_index - (lslotIndexOriginalBlk - tillLSlotIdx + 1), true);
            originalBlk.bits.set(last_one_index, false);
        }
        return true;
    }
    // todo move lslot back to original one
    WriteReturnInfo write(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint,
                                           SSDLog<Traits> &ssdLog,
                                           const size_t FP_index, const typename Traits::PAYLOAD_TYPE &payload,
                                           const size_t blkIdx, const size_t lslotBefore, const bool guarantee_update = false) {
        auto sizeAndLSlotsInfo = getNumberOfLSlotsAndSize(blkIdx);
        auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
        handleExtensionMetaData(blkIdx, lslotBefore, sizeAndLSlotsInfo);
        auto cp_fingerprint = fingerprint;
        Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
        return blk.write(cp_fingerprint, ssdLog, FP_index, payload, guarantee_update);
    }

#ifdef ENABLE_XDP
    WriteReturnInfo writeGI(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint,
                                           const size_t FP_index, const typename Traits::PAYLOAD_TYPE &payload,
                                           const size_t blkIdx, const size_t lslotBefore,
                                             XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp,
                            const bool guarantee_update = false) {
        auto sizeAndLSlotsInfo = getNumberOfLSlotsAndSize(blkIdx);
        auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
        handleExtensionMetaData(blkIdx, lslotBefore, sizeAndLSlotsInfo);
        auto cp_fingerprint = fingerprint;
        Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
        return blk.writeGIEx(cp_fingerprint, fingerprint, FP_index, payload, xdp, guarantee_update);
    }
#endif
    std::unique_ptr<RemoveReturnInfo> remove(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint,
                                             SSDLog<Traits> &ssdLog, const size_t FP_index, const size_t blkIdx,
                                             const size_t lslotBefore) {
        auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
        auto cp_fingerprint = fingerprint;
        Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
        return blk.remove(cp_fingerprint, ssdLog, FP_index);
    }
    void handleExtensionMetaData(const size_t blkIdx, const size_t lslotBefore, const pairStruct &p) {
        if (lslotBefore == p.ten) {  // only for testing purposes. in production never should happen
            auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
            lslotSizesBW.shift_smart(1, REGISTER_SIZE + p.tenBefore);
            if (lslotSizesBW.get(blkIdx)) {
                lslotSizesBW.set(REGISTER_SIZE + p.tenBefore, false);
            } else {
                assert(p.ten == 0);
                lslotSizesBW.set(blkIdx, true);
                lslotSizesBW.set(REGISTER_SIZE + p.tenBefore, true);
            }
            // create new empty lslot
            blk.bits.shift_smart(1, actualIndex, REGISTER_SIZE);
            blk.bits.set(actualIndex, false);
        } else if (lslotBefore > p.ten) {
            throw std::invalid_argument("should not be here in extension block");
        }
    }

    std::optional<typename Traits::ENTRY_TYPE> read(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, const SSDLog<Traits> &ssdLog,
                                     const size_t FP_index, const size_t blkIdx, const size_t lslotBefore) {
        auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
        auto cp_fingerprint = fingerprint;
        Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
        return blk.read(cp_fingerprint, ssdLog, FP_index);
    }
    auto read_payload(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint,
                                     const size_t FP_index, const size_t blkIdx, const size_t lslotBefore) {
        auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
        auto cp_fingerprint = fingerprint;
        Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
        return blk.read_payload(cp_fingerprint, FP_index);
    }
    std::optional<typename Traits::ENTRY_TYPE> readDHT(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, const SSDLog<Traits> &ssdLog,
                                                      const size_t FP_index, const size_t blkIdx, const size_t lslotBefore) {
        auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
        auto cp_fingerprint = fingerprint;
        Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
        return blk.readDHT(cp_fingerprint, ssdLog, FP_index);
    }

    auto get_index(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, const size_t FP_index,
                                                           const size_t blkIdx, const size_t lslotBefore) {
        auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
        auto cp_fingerprint = fingerprint;
        Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
        return blk.get_index(cp_fingerprint, FP_index);
    }

    auto get_index_withoutHTAndCase2(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, const size_t FP_index,
                                                           const size_t blkIdx, const size_t lslotBefore) {
        auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
        auto cp_fingerprint = fingerprint;
        Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
        return blk.get_index_withoutHT(cp_fingerprint, FP_index);
    }
    auto get_index_dht(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, const size_t FP_index,
                                      const size_t blkIdx, const size_t lslotBefore) {
        auto actualIndex = calculatePhysicalLSlotIndex(blkIdx, lslotBefore);
        auto cp_fingerprint = fingerprint;
        Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
        return blk.get_index_dht(cp_fingerprint, FP_index);
    }
    void print() {
        std::cout << "Extended Block:\n";
        std::cout << "\tMetadata:\n\t";
        lslotSizesBW.printBitset();
        std::cout << "\tBlock:\n\t";
        blk.print();
    }
};
