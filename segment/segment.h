#include <thread>
#include "../block/block.h"
#include "../config/config.h"
#include "../extension_block/extension_block.h"

#define CALCULATE_NEW_BLOCK_IDX(oldBlkIdx, oldLslotIdx) \
    ((oldBlkIdx / 2) + (COUNT_SLOT / 2) * (oldLslotIdx % 2))
#define CALCULATE_NEW_LSLOT_IDX(oldLslotIdx, firstBit) \
    ((oldLslotIdx / 2) + (COUNT_SLOT / 2) * firstBit)


#ifdef ENABLE_XDP
// Forward‐declare the template so we can hold pointers to it
template <
  typename TraitsGI, 
  typename TraitsLI, 
  typename TraitsLIBuffer
>
class XDP;
#endif


template <typename Traits = DefaultTraits>
class Segment {
    typedef typename Traits::PAYLOAD_TYPE PAYLOAD_TYPE;
    typedef typename Traits::KEY_TYPE KEY_TYPE;
    typedef typename Traits::VALUE_TYPE VALUE_TYPE;
    typedef typename Traits::ENTRY_TYPE ENTRY_TYPE;
   public:
    std::unique_ptr<Block<Traits>[]> blockList;

    using PtrsExBlksType = std::conditional_t<
        Traits::DHT_EVERYTHING,
        std::array<std::unique_ptr<Block<Traits>>, COUNT_SLOT>,
        std::nullptr_t>;

    std::conditional_t<
        Traits::DHT_EVERYTHING,
        PtrsExBlksType,
        std::nullptr_t> ptrsExBlks;

    auto getBLock(const size_t idx) {
        return &blockList[idx];
    }
    std::unique_ptr<ExtensionBlock<Traits>[]> extensionBlockList;
    std::mutex segmentMutex;
    size_t FP_index;
    explicit Segment(const size_t FP_index_) : blockList(std::make_unique<Block<Traits>[]>(COUNT_SLOT)), extensionBlockList(std::make_unique<ExtensionBlock<Traits>[]>(Traits::SEGMENT_EXTENSION_BLOCK_SIZE)), FP_index(FP_index_) {
        if constexpr (Traits::DHT_EVERYTHING) {
            ptrsExBlks = PtrsExBlksType{}; // Initializes the array
        } else {
            ptrsExBlks = nullptr;          // Only store a null pointer
        }
    }

    std::shared_ptr<Segment<TraitsLI>> replicate() {
        auto seg = std::make_shared<Segment<TraitsLI>>(FP_index);
        for (size_t i = 0; i < COUNT_SLOT; ++i) {
            seg->blockList[i].bits = blockList[i].bits.replicateTrieStore();
            assert(seg->blockList[i].bits.getInputString() == blockList[i].bits.getInputString());
        }
        if constexpr (!Traits::DHT_EVERYTHING) {
            for (size_t i = 0; i < Traits::SEGMENT_EXTENSION_BLOCK_SIZE; ++i) {
                seg->extensionBlockList[i].blk.bits = extensionBlockList[i].blk.bits.replicateTrieStore();
                assert(seg->extensionBlockList[i].blk.bits.getInputString() == extensionBlockList[i].blk.bits.getInputString());
                seg->extensionBlockList[i].lslotSizesBW = extensionBlockList[i].lslotSizesBW.replicateTrieStore();
                assert(seg->extensionBlockList[i].lslotSizesBW.getInputString() == extensionBlockList[i].lslotSizesBW.getInputString());
            }
        }
        return seg;
    }
    bool write(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint,
                                                     SSDLog<Traits> &ssdLog,
                                                     const PAYLOAD_TYPE &payload,
                                                     const bool guarantee_update = false) {
    SEGMENT_WRITE_START:
        size_t blkIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS * 2, FP_index - COUNT_SLOT_BITS);
        auto blk = &blockList[blkIdx];

        WriteReturnInfo write_return_info = blk->write(fingerprint, ssdLog, FP_index, payload, guarantee_update);
        const size_t lslotIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);

        // handle different outcomes
        auto res = true;
        switch (write_return_info.rs) {
            case WriteReturnStatusSuccessful: {
                break;
            }
            case WriteReturnStatusNotEnoughPayloadSpace:
            case WriteReturnStatusNotEnoughBlockSpace: {
                // std::cout << "1\n";
                size_t lastLslot = blk->get_last_lslot_idx();
                bool status;
                if constexpr (!Traits::DHT_EVERYTHING) {
                    status = ExtensionBlock<Traits>::moveLSlotsToMakeSpace(*blk, lastLslot, blkIdx, extensionBlockList.get());
                } else {
                    if (!ptrsExBlks[blkIdx]) {
                        ptrsExBlks[blkIdx] = std::make_unique<Block<Traits>>();
                    }
                    status = ExtensionBlock<Traits>::moveLSlotsToMakeSpaceDHT(*blk, *ptrsExBlks[blkIdx], lastLslot);
                }
                if (!status) {
                    res = false;
                    break;
                }
                res = write(fingerprint, ssdLog, payload);
                break;
            }
            case WriteReturnStatusLslotExtended: {
                // std::cout << "2\n";
                if constexpr (!Traits::DHT_EVERYTHING) {
                    const size_t lslotBefore = ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx);
                    const auto exBlkIdx = ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, lslotIdx);
                    auto exBlk = &extensionBlockList[exBlkIdx];
                    const WriteReturnInfo exInfo = exBlk->write(fingerprint, ssdLog, FP_index, payload, blkIdx, lslotBefore, guarantee_update);
                    if (exInfo.rs == WriteReturnStatusSuccessful) {
                        break;
                    } else if (guarantee_update) {
                        throw std::invalid_argument("not a valid update - not enough space - in segment - cond lslot extended");
                    }

                } else {
                    size_t actualIndex = lslotIdx - blk->get_block_info().firstExtendedLSlot;
                    auto cp_fingerprint = fingerprint;
                    Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
                    const WriteReturnInfo res_tmp = ptrsExBlks[blkIdx]->write(cp_fingerprint, ssdLog, FP_index, payload);
                    if (res_tmp.rs == WriteReturnStatusSuccessful)
                        break;
                }
                // chnage ten in the orignial block
                // or not :)
                res = false;
                break;
            }
            default:
                throw std::invalid_argument("should not be here, switch case in segment didn't handle all possibilities!\n");
        }
        return res;
    }

#ifdef ENABLE_XDP
    bool writeGI(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint,
                                                     const PAYLOAD_TYPE &payload,
                                                     XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp,
                                                     const bool guarantee_update = false) {
    SEGMENT_WRITE_START:
        size_t blkIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS * 2, FP_index - COUNT_SLOT_BITS);
        auto blk = &blockList[blkIdx];

        WriteReturnInfo write_return_info = blk->writeGI(fingerprint, FP_index, payload, xdp, guarantee_update);
        const size_t lslotIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);

        // handle different outcomes
        auto res = true;
        switch (write_return_info.rs) {
            case WriteReturnStatusSuccessful: {
                break;
            }
            case WriteReturnStatusNotEnoughPayloadSpace:
            case WriteReturnStatusNotEnoughBlockSpace: {
                // std::cout << "1\n";
                size_t lastLslot = blk->get_last_lslot_idx();
                bool status;
                if constexpr (!Traits::DHT_EVERYTHING) {
                    status = ExtensionBlock<Traits>::moveLSlotsToMakeSpace(*blk, lastLslot, blkIdx, extensionBlockList.get());
                } else {
                    if (!ptrsExBlks[blkIdx]) {
                        ptrsExBlks[blkIdx] = std::make_unique<Block<Traits>>();
                    }
                    status = ExtensionBlock<Traits>::moveLSlotsToMakeSpaceDHT(*blk, *ptrsExBlks[blkIdx], lastLslot);
                }
                if (!status) {
                    res = false;
                    break;
                }
                res = writeGI(fingerprint, payload, xdp, guarantee_update);
                break;
            }
            case WriteReturnStatusLslotExtended: {
                // std::cout << "2\n";
                if constexpr (!Traits::DHT_EVERYTHING) {
                    const size_t lslotBefore = ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx);
                    const auto exBlkIdx = ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, lslotIdx);
                    auto exBlk = &extensionBlockList[exBlkIdx];
                    const WriteReturnInfo exInfo = exBlk->writeGI(fingerprint, FP_index, payload, blkIdx, lslotBefore, xdp, guarantee_update);
                    if (exInfo.rs == WriteReturnStatusSuccessful)
                        break;
                    else if (guarantee_update)
                        throw std::invalid_argument("not a valid update - not enough space - in segment - cond lslot extended");
                } else
                    throw std::invalid_argument("not implemented for DHT_EVERYTHING");
                res = false;
                break;
            }
            default:
                throw std::invalid_argument("should not be here, switch case in segment didn't handle all possibilities!\n");
        }
        return res;
    }
#endif
    bool remove(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, SSDLog<Traits> &ssdLog) {
        size_t blkIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS * 2, FP_index - COUNT_SLOT_BITS);
        auto blk = &blockList[blkIdx];

        auto blkInfo = blk->remove(fingerprint, ssdLog, FP_index);
        const size_t lslotIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);

        // handle different outcomes
        auto res = true;
        switch (blkInfo->rs) {
            case RemoveReturnStatusSuccessful: {
                break;
            }
            case WriteReturnStatusLslotExtended: {
                // std::cout << "2\n";
                const size_t lslotBefore = ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx);
                const auto exBlkIdx = ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, lslotIdx);
                auto exBlk = &extensionBlockList[exBlkIdx];
                const auto exInfo = exBlk->remove(fingerprint, ssdLog, FP_index, blkIdx, lslotBefore);
                if (exInfo->rs == RemoveReturnStatusSuccessful) {
                    break;
                }
                res = false;
                break;
            }
            default:
                throw std::invalid_argument("should not be here, switch case in segment didn't handle all possibilities!\n");
        }
        return res;
    }
    std::optional<ENTRY_TYPE> read(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, const SSDLog<Traits> &ssdLog) const {
        const size_t blkIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS * 2, FP_index - COUNT_SLOT_BITS);
        auto blk = &blockList[blkIdx];
        const BlockInfo blkInfo = blk->get_block_info();
        const size_t lslotIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        const auto exBlkIdx = ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, lslotIdx);
        const auto exBlk = &extensionBlockList[exBlkIdx];

        std::optional<ENTRY_TYPE> res;
        if constexpr (Traits::DHT_EVERYTHING) {
             if (blkInfo.firstExtendedLSlot > lslotIdx) {
                res = blk->readDHT(fingerprint, ssdLog, FP_index);
            } else {
                size_t actualIndex = lslotIdx - blk->get_block_info().firstExtendedLSlot;
                auto cp_fingerprint_1 = fingerprint;
                Block<Traits>::setLSlotIndexInFP(cp_fingerprint_1, blk->get_block_info().firstExtendedLSlot - 1, FP_index);
                volatile auto r = blk->readDHT(cp_fingerprint_1, ssdLog, FP_index, true);
                auto cp_fingerprint = fingerprint;
                Block<Traits>::setLSlotIndexInFP(cp_fingerprint, actualIndex, FP_index);
                return ptrsExBlks[blkIdx]->readDHT(cp_fingerprint, ssdLog, FP_index);
            }
        } else {
            if (blkInfo.firstExtendedLSlot > lslotIdx) {
                if (Traits::READ_OFF_STRATEGY == 0)
                    res = blk->read(fingerprint, ssdLog, FP_index);
                if (Traits::READ_OFF_STRATEGY == 20) {
                    res = blk->readDHT(fingerprint, ssdLog, FP_index);
                }
            } else {
                if (Traits::READ_OFF_STRATEGY == 0)
                    res = exBlk->read(fingerprint, ssdLog, FP_index, blkIdx, ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx));
                if (Traits::READ_OFF_STRATEGY == 20) {
                    res = exBlk->readDHT(fingerprint, ssdLog, FP_index, blkIdx, ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx));
                }
            }
        }
        return res;
    }
    size_t read_payload(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint) const {
        const size_t blkIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS * 2, FP_index - COUNT_SLOT_BITS);
        auto blk = &blockList[blkIdx];
        const BlockInfo blkInfo = blk->get_block_info();
        const size_t lslotIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        const auto exBlkIdx = ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, lslotIdx);
        const auto exBlk = &extensionBlockList[exBlkIdx];

        if constexpr (Traits::DHT_EVERYTHING) {
            throw std::invalid_argument("DHT everything not supproted for this!\n"); 
        } else {
            if (blkInfo.firstExtendedLSlot > lslotIdx) {
                if (Traits::READ_OFF_STRATEGY == 0)
                    return blk->read_payload(fingerprint, FP_index);
                if (Traits::READ_OFF_STRATEGY == 20) {
                    throw std::invalid_argument("dht expand not supproted for this!\n");
                }
            } else {
                if (Traits::READ_OFF_STRATEGY == 0)
                    return exBlk->read_payload(fingerprint, FP_index, blkIdx, ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx));
                if (Traits::READ_OFF_STRATEGY == 20) {
                    throw std::invalid_argument("dht expand not supproted for this!\n");
                }
            }
        }
    }
    auto readOffset(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint) const {
        const size_t blkIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS * 2, FP_index - COUNT_SLOT_BITS);
        auto blk = &blockList[blkIdx];
        const BlockInfo blkInfo = blk->get_block_info();
        const size_t lslotIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        const auto exBlkIdx = ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, lslotIdx);
        const auto exBlk = &extensionBlockList[exBlkIdx];

        std::unique_ptr<ENTRY_TYPE> res;
        if (blkInfo.firstExtendedLSlot > lslotIdx) {
            if (Traits::READ_OFF_STRATEGY == 0)
                return blk->get_index(fingerprint, FP_index);
            else if (Traits::READ_OFF_STRATEGY == 2)
                return blk->get_index_withoutHT(fingerprint, FP_index);
            else if (Traits::READ_OFF_STRATEGY == 20) {
                return blk->get_index_dht(fingerprint, FP_index);
            } else
                throw std::invalid_argument("why here?");
        } else {
            if (Traits::READ_OFF_STRATEGY == 0)
                return exBlk->get_index(fingerprint, FP_index, blkIdx, ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx));
            else if (Traits::READ_OFF_STRATEGY == 2)
                return exBlk->get_index_withoutHTAndCase2(fingerprint, FP_index, blkIdx, ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx));
            else if (Traits::READ_OFF_STRATEGY == 20) {
                return exBlk->get_index_dht(fingerprint, FP_index, blkIdx,
                                            ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx));
            } else
                throw std::invalid_argument("why here in extended block?");
        }
    }

    auto readTen(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint) const {
        const size_t blkIdx = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS * 2, FP_index - COUNT_SLOT_BITS);
        auto blk = &blockList[blkIdx];
        const auto slot_index = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        return blk->get_ten(slot_index);
    }

    static auto count_bits_needed(const ExpandedBlock &pBlock, size_t slotIdx) {
        size_t needed_bits = COUNT_SLOT - slotIdx + COUNT_SLOT;  // number of bits for extension + number of bits for first bitmap
        size_t ten_sum = 0;
        for (size_t lslotIdx = 0; lslotIdx <= slotIdx; lslotIdx++) {
            auto const &lslot = pBlock.lslots[lslotIdx];
            needed_bits += lslot.ten + lslot.firstInvalidIndex;
            ten_sum += lslot.ten;
        }
        return std::make_pair(needed_bits, ten_sum);
    }

    auto get_expanded_seg(SSDLog<Traits> &ssdLog) {
        auto exp_seg1 = std::make_unique<ExpandedSegment>();
        auto exp_seg2 = std::make_unique<ExpandedSegment>();
        //        std::cout << sizeof(*exp_seg1) << " sizes" << std::endl;
        for (size_t i = 0; i < COUNT_SLOT; ++i) {
            expand(ssdLog, *exp_seg1, *exp_seg2, i);
        }
        // exp_seg1->print();
        // exp_seg2->print();
        // std::cout << exp_seg1->get_count() << "  " << exp_seg2->get_count() << std::endl;
        return std::make_pair(std::move(exp_seg1), std::move(exp_seg2));
    }

    std::pair<std::shared_ptr<Segment>, std::shared_ptr<Segment>> expand(SSDLog<Traits> &ssdLog) {
        if constexpr (Traits::DHT_EVERYTHING) {
            throw std::invalid_argument("not implemented for DHT_EVERYTHING");
        }
        auto seg1 = std::make_shared<Segment>(FP_index + 1);
        auto seg2 = std::make_shared<Segment>(FP_index + 1);

        auto [exp_seg1, exp_seg2] = get_expanded_seg(ssdLog);
        // blockList[0].print();
        fill_segment(*seg1, *exp_seg1);
        fill_segment(*seg2, *exp_seg2);
        return std::make_pair(seg1, seg2);
    }
#ifdef ENABLE_XDP
    auto get_expanded_seg_gi(XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp,
                            BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, size_t segmentCountLog) {
        auto exp_seg1 = std::make_unique<ExpandedSegment>();
        auto exp_seg2 = std::make_unique<ExpandedSegment>();
        //        std::cout << sizeof(*exp_seg1) << " sizes" << std::endl;
        for (size_t i = 0; i < COUNT_SLOT; ++i) {
            expand_gi(*exp_seg1, *exp_seg2, i, xdp, fingerprint, segmentCountLog);
        }
        // exp_seg1->print();
        // exp_seg2->print();
        // std::cout << exp_seg1->get_count() << "  " << exp_seg2->get_count() << std::endl;
        return std::make_pair(std::move(exp_seg1), std::move(exp_seg2));
    }

    std::pair<std::shared_ptr<Segment>, std::shared_ptr<Segment>> expand_gi(XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp,
                                                                            BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, 
                                                                            size_t segmentCountLog) {
        if constexpr (Traits::DHT_EVERYTHING) {
            throw std::invalid_argument("not implemented for DHT_EVERYTHING");
        }
        auto seg1 = std::make_shared<Segment>(FP_index + 1);
        auto seg2 = std::make_shared<Segment>(FP_index + 1);

        auto [exp_seg1, exp_seg2] = get_expanded_seg_gi(xdp, fingerprint, segmentCountLog);
        // blockList[0].print();
        fill_segment(*seg1, *exp_seg1);
        fill_segment(*seg2, *exp_seg2);
        return std::make_pair(seg1, seg2);
    }
#endif

    auto get_org_blk_payload_start_and_ten(const ExpandedBlock &exp_blk, const size_t j) const {
        const auto &lslot = exp_blk.lslots[j];

        size_t payload_start_idx = lslot.payload_start;
        const auto &payload_blk = lslot.wasExtended
                                      ? extensionBlockList[ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(lslot.orgBlock, lslot.orgLSlot)].blk
                                      : blockList[lslot.orgBlock];

        return std::make_tuple(payload_start_idx, lslot.ten, &payload_blk);
    }

    inline static auto set_ten_exp_blk(const ExpandedBlock &exp_blk, BitsetWrapper<N> &bw) {
        size_t index = REGISTER_SIZE - 1;
        for (size_t i = 0; i < COUNT_SLOT; i++) {
            const auto exp_lslot = exp_blk.lslots[i];
            index += exp_lslot.ten;
            bw.set(i, exp_lslot.ten > 0);
            if (exp_lslot.ten > 0)
                bw.set(index, true);
        }
        return index;
    }

    inline static auto set_index_struc_exp_blk(const ExpandedBlock &exp_blk, BitsetWrapper<N> &bw, size_t starting_index) {
        for (auto &exp_lslot : exp_blk.lslots) {
            if (exp_lslot.ten > 1) {
                auto prev_index = starting_index;
                starting_index += exp_lslot.firstInvalidIndex;
                if (starting_index >= N)
                    throw std::invalid_argument("not enough space in the block to store the lslot");
                bw.set_fast_two_reg(prev_index, starting_index, exp_lslot.bw.bitset[0]);
            }
        }
        return starting_index;
    }

    void fill_segment(Segment &segment, const ExpandedSegment &exp_segment) const {
        // method for expansion
        for (size_t blockIdx = 0; blockIdx < COUNT_SLOT; ++blockIdx) {
            const ExpandedBlock &exp_blk = exp_segment.blocks[blockIdx];
            const size_t max_lslot_index = find_lslot_before_extension(exp_blk);

            // in the orgBlk
            auto &blk_target = segment.blockList[blockIdx];
            auto index = set_ten_exp_blk(exp_blk, blk_target.bits) + 1;
            auto payload_idx_without_extension = 0;
            for (size_t j = 0; j <= max_lslot_index; j++) {
                auto [payload_start_idx, ten, payload_blk] = get_org_blk_payload_start_and_ten(exp_blk, j);
                for (size_t k = 0; k < ten; k++) {
                    Payload<Traits>::swap(payload_blk->payload_list, payload_start_idx++,
                                          blk_target.payload_list, payload_idx_without_extension++, true);
                }
            }
            set_index_struc_exp_blk(exp_blk, blk_target.bits, index);
            for (size_t lastIdx = N - 1; lastIdx > N - (COUNT_SLOT - max_lslot_index); --lastIdx) {
                blk_target.bits.set(lastIdx, false);
            }
            // I think i can remove next line
            blk_target.bits.set(N - 1, false);                        // reset the end
            blk_target.bits.set(N - (COUNT_SLOT - max_lslot_index), true);  // set overflowing lslots

            for (size_t j = COUNT_SLOT - 1; j > max_lslot_index; j--) {
                const auto exBlkIdx = ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(blockIdx, j);
                const auto lslotBefore = ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(j);
                auto [_payload_start_idx, _ten, payload_blk] = get_org_blk_payload_start_and_ten(exp_blk, j);
                segment.extensionBlockList[exBlkIdx].moveLslotHere(*payload_blk, lslotBefore,
                                                                   exp_blk.lslots[j], blockIdx);
            }
        }
    }
    static size_t find_lslot_before_extension(const ExpandedBlock &exp_blk) {
        size_t lslot_num;
        for (lslot_num = COUNT_SLOT - 1; lslot_num > 0; --lslot_num) {
            const auto [bits_needed, payload_size_needed] = count_bits_needed(exp_blk, lslot_num);
            if (bits_needed < N && payload_size_needed <= Traits::PAYLOADS_LENGTH)
                break;
        }
        return lslot_num;
    }

#ifdef ENABLE_XDP
    void expand_gi(ExpandedSegment &s1, ExpandedSegment &s2,
                   size_t blkIdx, XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp,
                   BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, size_t segmentCountLog) {
        auto &blk = blockList[blkIdx];
        const BlockInfo blkInfo = blk.get_block_info();

        auto fp2 = fingerprint;
        fp2.set_fast_one_reg(0, FP_index - 12, FP_index - 6, blkIdx);

        ExpandedSegment &newSeg = blkIdx % 2 == 0 ? s1 : s2;
        for (size_t lslotIdx = 0; lslotIdx < COUNT_SLOT; ++lslotIdx) {
            fp2.set_fast_one_reg(0, FP_index - 6, FP_index, lslotIdx);
            const auto extendedOldBlkIdx = ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, lslotIdx);
            auto &exBlk = extensionBlockList[extendedOldBlkIdx];
            const size_t newBlkIdx = CALCULATE_NEW_BLOCK_IDX(blkIdx, lslotIdx);
            const size_t newLSlotIdx0 = CALCULATE_NEW_LSLOT_IDX(lslotIdx, 0);
            const size_t newLSlotIdx1 = CALCULATE_NEW_LSLOT_IDX(lslotIdx, 1);

            size_t physical_lslot_idx;
            Block<Traits> &physical_block = blkInfo.firstExtendedLSlot > lslotIdx ? blk : exBlk.blk;
            bool wasExtended = false;
            if (blkInfo.firstExtendedLSlot > lslotIdx) {
                physical_lslot_idx = lslotIdx;
            } else {
                physical_lslot_idx = exBlk.calculatePhysicalLSlotIndex(blkIdx, ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx));
                wasExtended = true;
            }
            size_t start_lslot_index = physical_block.get_lslot_start(physical_lslot_idx);
            auto [payload_start_index, tenOld] = physical_block.getTenBeforeAndTen(physical_lslot_idx);
            BST<N> bst(tenOld, start_lslot_index, FP_index);
            bst.createBST(physical_block.bits);
            auto &exp_lslot0 = newSeg.blocks[newBlkIdx].lslots[newLSlotIdx0];
            auto &exp_lslot1 = newSeg.blocks[newBlkIdx].lslots[newLSlotIdx1];
            if (tenOld == 0) {
                exp_lslot0.set(0, blkIdx, lslotIdx, Traits::PAYLOADS_LENGTH, wasExtended, bst.getBitRepWrapper());
                exp_lslot1.set(0, blkIdx, lslotIdx, Traits::PAYLOADS_LENGTH, wasExtended, bst.getBitRepWrapper());
            } else {
                const size_t count_zero_start = physical_block.get_count_zero_start_gi(bst, payload_start_index, xdp, fp2);
                BST<REGISTER_SIZE> bst_new0(count_zero_start, N, FP_index + 1);
                BST<REGISTER_SIZE> bst_new1(tenOld - count_zero_start, N, FP_index + 1);
                if (count_zero_start < tenOld && count_zero_start > 0) {
                    bst_new0.root = std::move(bst.root->left);
                    bst_new1.root = std::move(bst.root->right);
                } else if (count_zero_start == 0) {
                    bst_new1.root = std::move(bst.root);
                } else if (count_zero_start == tenOld) {
                    bst_new0.root = std::move(bst.root);
                }
                exp_lslot0.set(count_zero_start, blkIdx, lslotIdx, payload_start_index, wasExtended, bst_new0.getBitRepWrapper());
                exp_lslot1.set(tenOld - count_zero_start, blkIdx, lslotIdx, payload_start_index + count_zero_start, wasExtended, bst_new1.getBitRepWrapper());
            }
        }
    }
#endif
    void expand(SSDLog<Traits> &ssdLog, ExpandedSegment &s1, ExpandedSegment &s2, size_t blkIdx) {
        auto &blk = blockList[blkIdx];
        const BlockInfo blkInfo = blk.get_block_info();

        ExpandedSegment &newSeg = blkIdx % 2 == 0 ? s1 : s2;
        for (size_t lslotIdx = 0; lslotIdx < COUNT_SLOT; ++lslotIdx) {
            const auto extendedOldBlkIdx = ExtensionBlock<Traits>::CALCULATE_EXTENDED_BLOCK_INDEX(blkIdx, lslotIdx);
            auto &exBlk = extensionBlockList[extendedOldBlkIdx];
            const size_t newBlkIdx = CALCULATE_NEW_BLOCK_IDX(blkIdx, lslotIdx);
            const size_t newLSlotIdx0 = CALCULATE_NEW_LSLOT_IDX(lslotIdx, 0);
            const size_t newLSlotIdx1 = CALCULATE_NEW_LSLOT_IDX(lslotIdx, 1);

            size_t physical_lslot_idx;
            Block<Traits> &physical_block = blkInfo.firstExtendedLSlot > lslotIdx ? blk : exBlk.blk;
            bool wasExtended = false;
            if (blkInfo.firstExtendedLSlot > lslotIdx) {
                physical_lslot_idx = lslotIdx;
            } else {
                physical_lslot_idx = exBlk.calculatePhysicalLSlotIndex(blkIdx, ExtensionBlock<Traits>::CALCULATE_LSLOT_BEFORE(lslotIdx));
                wasExtended = true;
            }
            size_t start_lslot_index = physical_block.get_lslot_start(physical_lslot_idx);
            auto [payload_start_index, tenOld] = physical_block.getTenBeforeAndTen(physical_lslot_idx);
            BST<N> bst(tenOld, start_lslot_index, FP_index);
            bst.createBST(physical_block.bits);
            auto &exp_lslot0 = newSeg.blocks[newBlkIdx].lslots[newLSlotIdx0];
            auto &exp_lslot1 = newSeg.blocks[newBlkIdx].lslots[newLSlotIdx1];
            if (tenOld == 0) {
                exp_lslot0.set(0, blkIdx, lslotIdx, Traits::PAYLOADS_LENGTH, wasExtended, bst.getBitRepWrapper());
                exp_lslot1.set(0, blkIdx, lslotIdx, Traits::PAYLOADS_LENGTH, wasExtended, bst.getBitRepWrapper());
            } else {
                const size_t count_zero_start = physical_block.get_count_zero_start(bst, ssdLog, payload_start_index);
                BST<REGISTER_SIZE> bst_new0(count_zero_start, N, FP_index + 1);
                BST<REGISTER_SIZE> bst_new1(tenOld - count_zero_start, N, FP_index + 1);
                if (count_zero_start < tenOld && count_zero_start > 0) {
                    bst_new0.root = std::move(bst.root->left);
                    bst_new1.root = std::move(bst.root->right);
                } else if (count_zero_start == 0) {
                    bst_new1.root = std::move(bst.root);
                } else if (count_zero_start == tenOld) {
                    bst_new0.root = std::move(bst.root);
                }
                exp_lslot0.set(count_zero_start, blkIdx, lslotIdx, payload_start_index, wasExtended, bst_new0.getBitRepWrapper());
                exp_lslot1.set(tenOld - count_zero_start, blkIdx, lslotIdx, payload_start_index + count_zero_start, wasExtended, bst_new1.getBitRepWrapper());
            }
        }
    }

    size_t get_ten_all() const {
        size_t sum = 0;
        for (auto i = 0; i < COUNT_SLOT; i++) {
            if (const auto a = blockList[i].get_max_index(); a >= 0)
                sum += static_cast<size_t>(a + 1);
        }
        if constexpr (!Traits::DHT_EVERYTHING) {
            for (auto i = 0; i < Traits::SEGMENT_EXTENSION_BLOCK_SIZE; i++) {
                if (const auto a = extensionBlockList[i].blk.get_max_index(); a >= 0)
                    sum += static_cast<size_t>(a + 1);
            }
        }
        if constexpr (Traits::DHT_EVERYTHING) {
            for (auto i = 0; i < COUNT_SLOT; i++) {
                if (ptrsExBlks[i]) {
                    sum += ptrsExBlks[i]->get_max_index() + 1;
                }
            }
        }
        return sum;
    }

    void print() const {
        if constexpr (!Traits::DHT_EVERYTHING) {
            std::cout << "printing segment\n";
            // std::cout << "counter:" << counter << "\n";
            for (auto i = 0; i < COUNT_SLOT; i++) {
                std::cout << i << '\t';
                blockList[i].print();
            }
            for (auto i = 0; i < Traits::SEGMENT_EXTENSION_BLOCK_SIZE; i++) {
                std::cout << i << '\t';
                extensionBlockList[i].print();
            }
        }
        if constexpr (Traits::DHT_EVERYTHING) {
            std::cout << "DHT is on:\n";
            for (size_t i = 0; i < COUNT_SLOT; i++) {
                std::cout << "Block: " << i << std::endl;
                blockList[i].print();
                if (ptrsExBlks[i]) {
                    std::cout << "\tExtension Block: \t";
                    ptrsExBlks[i]->print();
                }
            }
        }
    }
    size_t get_uniq_blks() const {
        size_t unique_blks;
        if constexpr (Traits::DHT_EVERYTHING) {
            unique_blks = COUNT_SLOT;
            for (size_t i = 0; i < COUNT_SLOT; i++) {
                if (ptrsExBlks[i]) {
                    unique_blks += 1;
                }
            }
        } else {
            throw std::invalid_argument("not implemented");
        }
        return unique_blks;
    }
    size_t get_memory() const {
        size_t memory;
        if constexpr (Traits::DHT_EVERYTHING) {
            memory = N * COUNT_SLOT;
            for (size_t i = 0; i < COUNT_SLOT; i++) {
                if (ptrsExBlks[i]) {
                    memory += N;
                }
            }
        } else {
            throw std::invalid_argument("not implemented");
        }
        return memory;
    }

    float get_average_age() {
        float sum = 0;
        float count = 0;
        for (size_t i = 0; i < COUNT_SLOT; i++) {
            sum += blockList[i].get_average_age();
            count++;
        }
        if constexpr (!Traits::DHT_EVERYTHING) {
            for (size_t i = 0; i < Traits::SEGMENT_EXTENSION_BLOCK_SIZE; i++) {
                sum += extensionBlockList[i].blk.get_average_age();
                count++;
            }
        }
        if (count == 0) {
            return 0;
        }
        return sum / count;
    }

    void printExtension() const {
        std::cout << "printing extension part of the segment\n";
        for (auto i = 0; i < Traits::SEGMENT_EXTENSION_BLOCK_SIZE; i++) {
            std::cout << i << '\t';
            extensionBlockList[i].print();
        }
    }

};
