#pragma once
#include <cstddef>  // for size_t
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

#include "../BST/BST.h"
#include "../SSDLog/SSDLog.h"
#include "../bitset_wrapper/bitset_wrapper.h"
#include "../config/config.h"
#include "../hashing/hashing.h"
#include "../hashtable/hashtable.h"
#include "../payload/payload.h"

#ifdef ENABLE_XDP
template <
  typename TraitsGI, 
  typename TraitsLI, 
  typename TraitsLIBuffer
>
class XDP;
#endif
#define CALCULATE_LAST_AVAILABLE_INDEX(firstExtendedLSlot) \
    ((N) - (COUNT_SLOT) + (firstExtendedLSlot)-1)

struct TenInfo {
    uint8_t tenBefore, ten;
};

struct BlockInfo {
    bool     isExtended{false};
    uint8_t  firstExtendedLSlot{COUNT_SLOT}, remainingBits{64}, remainingPayload{UINT8_MAX};
};


enum WriteReturnStatus {
    WriteReturnStatusSuccessful,
    WriteReturnStatusLslotExtended,
    WriteReturnStatusNotEnoughBlockSpace,
    WriteReturnStatusNotEnoughPayloadSpace,
};

enum RemoveReturnStatus {
    RemoveReturnStatusSuccessful,
    RemoveReturnStatusLslotExtended,
};

struct WriteReturnInfo {
    BlockInfo blockInfo{};
    uint8_t least_space_needed{0};
    WriteReturnStatus rs{WriteReturnStatusSuccessful};
};

struct RemoveReturnInfo {
    size_t least_space_needed{0};
    RemoveReturnStatus rs{RemoveReturnStatusSuccessful};
    std::unique_ptr<BlockInfo> blockInfo{nullptr};
};

template <typename Traits = DefaultTraits>
class Block {
   private:
    typedef typename Traits::PAYLOAD_TYPE PAYLOAD_TYPE;
    typedef typename Traits::KEY_TYPE KEY_TYPE;
    typedef typename Traits::VALUE_TYPE VALUE_TYPE;
    typedef typename Traits::ENTRY_TYPE ENTRY_TYPE;
    bool walk_over_trie(size_t &index, const int depth, int &ten_left) const {
        int diff_depth = 1;
        // by default assume left and right are true meaning Leaf Nodes
        bool left = true, right = true;
        if (ten_left != 2) {
            left = bits.get(index++);
            right = bits.get(index++);
        }
        bits.count_contiguous(index, diff_depth);
        if (left && right) {
            ten_left -= 2;
            return true;
        }
        if (left != right) {
            ten_left--;
            return walk_over_trie(index, depth + diff_depth, ten_left);
        }
        return walk_over_trie(index, depth + diff_depth, ten_left) &&
               walk_over_trie(index, depth + diff_depth, ten_left);
    }

    size_t walk_over_trie_payload(size_t &index, const int depth,
                                  int &ten_left,
                                  BitsetWrapper<FINGERPRINT_SIZE> &payload,
                                  const size_t &FP_index, size_t &total_ten) {
        int diff_depth = 1;
        // by default assume left and right are true meaning Leaf Nodes
        bool left = true, right = true;
        if (ten_left != 2) {
            left = bits.get(index++);
            right = bits.get(index++);
        }
        bits.count_contiguous(index, diff_depth);
        const bool is_right = payload.get(depth + diff_depth, FP_index);
        // both left and right Leaf Nodes
        if (left && right) {
            return is_right ? total_ten - ten_left + 1 : total_ten - ten_left;
        }
        // Left Internal Node and Right Leaf Node
        if (!left && right) {
            ten_left--;
            total_ten--;
            if (is_right) {
                walk_over_trie(index, depth + diff_depth, ten_left);
                return total_ten - ten_left;
            }
            return walk_over_trie_payload(index, depth + diff_depth, ten_left, payload,
                                          FP_index, total_ten);
        }
        // Left Leaf Node and Right Internal Node
        if (left) {
            if (is_right) {
                ten_left--;
                return walk_over_trie_payload(index, depth + diff_depth, ten_left,
                                              payload, FP_index, total_ten);
            }
            return total_ten - ten_left;
        }
        if (is_right) {
            walk_over_trie(index, depth + diff_depth, ten_left);
            return walk_over_trie_payload(index, depth + diff_depth, ten_left, payload,
                                          FP_index, total_ten);
        }
        return walk_over_trie_payload(index, depth + diff_depth, ten_left, payload,
                                      FP_index, total_ten);
    }

    inline int get_max_index_based_on_remaining_payloads(int rem_payload) const {
        int res = static_cast<int>(Traits::PAYLOADS_LENGTH) - 1 - rem_payload;
        return res;
    }

   public:
    BitsetWrapper<N> bits;
    Payload<Traits> payload_list;
    explicit Block() {
        bits.set(N - 1, true);
    }
    inline static void setLSlotIndexInFP(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, size_t newIndex, size_t FP_index) {
        fingerprint.set_fast_one_reg(0, FP_index - REGISTER_SIZE_BITS, FP_index, newIndex);
    }
    inline int64_t get_max_index() const {
        size_t first_extended_lslot = COUNT_SLOT - bits.get_leading_zeros(bits.NUM_REGS - 1);
        size_t reg_bitmap_rank = bits.rank(first_extended_lslot);
        if (reg_bitmap_rank == 0)
            return -1;
        int64_t offset = bits.select(reg_bitmap_rank, COUNT_SLOT / REGISTER_SIZE);
        return offset;
    }
    inline static bool compareTwoFP(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint1, BitsetWrapper<FINGERPRINT_SIZE> &fingerprint2, size_t FP_index) {
        const bool cond1 = fingerprint1.range_fast_one_reg(0, FP_index, REGISTER_SIZE) == fingerprint2.range_fast_one_reg(0, FP_index, REGISTER_SIZE);
        const bool cond2 = fingerprint1.bitset[1] == fingerprint2.bitset[1];
        return cond1 && cond2;
    }
    inline TenInfo getTenBeforeAndTen(size_t lslotIdx) const {
        const auto rank1 = bits.rank(lslotIdx);
        if (rank1 == 0) {
            return TenInfo{0, bits.get(lslotIdx) ? bits.select(1, 1) + 1 : 0};
        }
        if (bits.get(lslotIdx)) {
            auto [fst, snd] = bits.select_two(rank1, rank1 + 1, 1);
            return TenInfo{static_cast<uint8_t>(fst + 1), static_cast<uint8_t>(snd - fst)};
        }
        return TenInfo{bits.select(rank1, 1) + 1, 0};
    }
    // return start_index_lslot, offset
    std::pair<size_t, size_t> get_index(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, size_t FP_index) {
        size_t ret;
        const auto slot_index = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        size_t at_least_one_till_slot;
        int prev_ten_end;
        if (!bits.get(slot_index)) {
            auto first_one = bits.get_first_one_before_slow(slot_index);
            at_least_one_till_slot = bits.rank(first_one + 1);  // including the lslot first_one itself
            if (at_least_one_till_slot)
                prev_ten_end = bits.select(at_least_one_till_slot, COUNT_SLOT / REGISTER_SIZE);
            else
                prev_ten_end = -1;
            return std::make_pair(prev_ten_end + 1, 0);  // +1 for new entry
        }
        at_least_one_till_slot = bits.rank(slot_index);
        if (at_least_one_till_slot)
            prev_ten_end = bits.select(at_least_one_till_slot, COUNT_SLOT / REGISTER_SIZE);
        else
            prev_ten_end = -1;
        const size_t ten_end = bits.select(at_least_one_till_slot + 1, COUNT_SLOT / REGISTER_SIZE);
        auto total_ten = static_cast<size_t>(static_cast<int>(ten_end) - prev_ten_end);
        auto ten_left = static_cast<int>(total_ten);
        if (ten_left == 1) {
                ret = 0;
        } else {
            const size_t lslot_start =
                (bits.rank(REGISTER_SIZE) << 1) +
                ((prev_ten_end + 1 - at_least_one_till_slot) << 1);
            const auto lslot_start_index_pair = bits.select_two(lslot_start, lslot_start + (ten_left * 2 - 3));
            const auto signature = bits.range_fast_2(lslot_start_index_pair.first + 1, lslot_start_index_pair.second + 1);
            const auto hash_val = ht1.hash_function(static_cast<uint16_t>(signature));

            const auto node = ht1.table[hash_val];
            if (static_cast<std::uint64_t>(node.key) == signature) {
//                const auto important_bits = static_cast<uint64_t>(_pext_u64(fingerprint.bitset[0], node.value << FP_index));
                const auto important_bits = static_cast<uint64_t>(_pext_u64(fingerprint.bitset[0], static_cast<uint64_t>(node.value) << FP_index));
                const auto low_bit = important_bits * 3;
                ret = (node.indices >> low_bit) & (0b111);
            } else {
                auto new_total_ten = total_ten;
                size_t lslot_start_index = lslot_start_index_pair.first + 1;
                ret = walk_over_trie_payload(lslot_start_index, -1, ten_left, fingerprint,
                                             FP_index, new_total_ten);
            }
        }
        return std::make_pair(prev_ten_end + 1, ret);
    }
    std::pair<size_t, size_t> get_index_dht(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, size_t FP_index) {
        const auto slot_index = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        size_t payload_idx = 0;
        size_t offset = 0;

        size_t bitmapPtr = 0;
        size_t sizePtr = COUNT_SLOT;
        size_t dtsPtr = bits.select(2 * bits.rank(COUNT_SLOT)) + 1;

        for (size_t sltPtr = 0; sltPtr <= slot_index; sltPtr++) {
            if (bits.get(bitmapPtr++)) {
                int size = 1;
                bits.count_contiguous(sizePtr, size);
                if (sltPtr < slot_index) {
                    // TODO: FIX
                    if (Traits::DHT_EVERYTHING && size + payload_idx >= COUNT_SLOT) {
                        return std::make_pair(payload_idx, 0);
                    }
                    payload_idx += size;
                    if (size > 1) {
                        walk_over_trie(dtsPtr, -1, size);
                        dtsPtr++;
                    }
                } else {
                    size_t total_ten = size;
                    int tmp_ten = size;
                    auto depth = -1;
                    if (size > 1)
                        offset = walk_over_trie_payload(dtsPtr, depth, tmp_ten, fingerprint, FP_index, total_ten);
                    else
                        offset = 0;
                }
            }
        }

        return std::make_pair(payload_idx, offset);
    }
    std::pair<size_t, size_t> get_index_withoutHT(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, size_t FP_index) {
        size_t ret;
        // const auto start = std::chrono::high_resolution_clock::now();
        const auto slot_index = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        size_t at_least_one_till_slot;
        int prev_ten_end;
        if (!bits.get(slot_index)) {
            auto first_one = bits.get_first_one_before_slow(slot_index);
            at_least_one_till_slot = bits.rank(first_one + 1);  // including the lslot first_one itself
            if (at_least_one_till_slot)
                prev_ten_end = bits.select(at_least_one_till_slot, COUNT_SLOT / REGISTER_SIZE);
            else
                prev_ten_end = -1;
            return std::make_pair(prev_ten_end + 1, 0);  // +1 for new entry
        }
        at_least_one_till_slot = bits.rank(slot_index);
        if (at_least_one_till_slot)
            prev_ten_end = bits.select(at_least_one_till_slot, COUNT_SLOT / REGISTER_SIZE);
        else
            prev_ten_end = -1;
        const size_t ten_end = bits.select(at_least_one_till_slot + 1, COUNT_SLOT / REGISTER_SIZE);
        auto total_ten = static_cast<size_t>(static_cast<int>(ten_end) - prev_ten_end);
        auto ten_left = static_cast<int>(total_ten);
        switch (ten_left) {
            case 1:
                ret = 0;
                break;
            default: {
                const size_t lslot_start =
                        (bits.rank(REGISTER_SIZE) << 1) +
                        ((prev_ten_end + 1 - at_least_one_till_slot) << 1);
                const auto lslot_start_index_pair = bits.select_two(lslot_start, lslot_start + (ten_left * 2 - 3));
                auto new_total_ten = total_ten;
                size_t lslot_start_index = lslot_start_index_pair.first + 1;
                ret = walk_over_trie_payload(lslot_start_index, -1, ten_left, fingerprint,
                                             FP_index, new_total_ten);
            }
        }
        return std::make_pair(prev_ten_end + 1, ret);
    }
    [[nodiscard]] size_t get_index_start_lslot(const size_t slot_index) const {
        size_t at_least_one_till_slot;
        int prev_ten_end;
        if (!bits.get(slot_index)) {
            auto first_one = bits.get_first_one_before_slow(slot_index);
            at_least_one_till_slot = bits.rank(first_one + 1);  // including the lslot first_one itself
            if (at_least_one_till_slot)
                prev_ten_end = bits.select(at_least_one_till_slot, COUNT_SLOT / REGISTER_SIZE);
            else
                prev_ten_end = -1;
            return prev_ten_end + 1;  // +1 for new entry
        }
        at_least_one_till_slot = bits.rank(slot_index);
        if (at_least_one_till_slot)
            prev_ten_end = bits.select(at_least_one_till_slot, COUNT_SLOT / REGISTER_SIZE);
        else
            prev_ten_end = -1;
        return prev_ten_end + 1;
    }

    [[nodiscard]] size_t get_index_start_lslot_dht(const size_t slot_index) const {
        size_t bitmapPtr = 0;
        size_t sizePtr = COUNT_SLOT;

        int size;
        for (size_t sltPtr = 0; sltPtr <= slot_index; sltPtr++) {
            if (bits.get(bitmapPtr++)) {
                size = 1;
                bits.count_contiguous(sizePtr, size);
            }
        }
        return sizePtr - REGISTER_SIZE;
    }
    // these next two should be inlined
    size_t get_ten(const size_t lslotIndex) {
        if (!bits.get(lslotIndex)) {
            return 0;
        }
        const size_t at_least_one_till_slot = bits.rank(lslotIndex);
        int prev_ten_end;
        if (at_least_one_till_slot)
            prev_ten_end = bits.select(at_least_one_till_slot, COUNT_SLOT / REGISTER_SIZE);
        else
            prev_ten_end = -1;
        const size_t ten_end = bits.select(at_least_one_till_slot + 1, COUNT_SLOT / REGISTER_SIZE);
        size_t total_ten = static_cast<int>(ten_end) - prev_ten_end;
        return total_ten;
    }
    // these next two should be inlined
    size_t get_ten_dht(const size_t slot_index) {
        size_t bitmapPtr = 0;
        size_t sizePtr = COUNT_SLOT;

        int size = 0;
        for (size_t sltPtr = 0; sltPtr <= slot_index; sltPtr++) {
            size = 0;
            if (bits.get(bitmapPtr++)) {
                size = 1;
                bits.count_contiguous(sizePtr, size);
            }
        }
        return size;
    }
    void set_ten(size_t lslotIdx, size_t newTen, size_t to) {
        auto tenInfo = getTenBeforeAndTen(lslotIdx);
        if (tenInfo.ten == newTen) return;
        bits.set(lslotIdx, newTen != 0);
        int steps = static_cast<int>(newTen) - static_cast<int>(tenInfo.ten);
        const size_t from = REGISTER_SIZE + tenInfo.tenBefore;
        bits.shift_smart(steps, from, to);
        if (tenInfo.ten == 0)
            bits.set(from + steps - 1, true);
    }
    [[nodiscard]] size_t get_lslot_start(size_t slot_index) const {
        int prev_ten_end;
        size_t at_least_one_till_slot = bits.rank(slot_index);
        if (at_least_one_till_slot)
            prev_ten_end = bits.select(at_least_one_till_slot, COUNT_SLOT / REGISTER_SIZE);
        else
            prev_ten_end = -1;
        const size_t lslot_start =
            (bits.rank(REGISTER_SIZE) << 1) +
            ((prev_ten_end + 1 - at_least_one_till_slot) << 1);

        return lslot_start ? bits.select(lslot_start) + 1 : 0;
    }
    auto get_slot_info(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, size_t FP_index, size_t slot_index) {
        // slot_index at least has one entry
        int prev_ten_end, ten_end;
        size_t current_slot_start_pos, next_slot_start_pos, matched_offset;
        size_t at_least_one_till_slot = bits.rank(slot_index);
        // unlikely 
        if (__builtin_expect(at_least_one_till_slot == 0, 0)) {
            ten_end = bits.select(at_least_one_till_slot + 1, 1);
            prev_ten_end = -1;
        } else {
            auto [left, right] = bits.select_two(at_least_one_till_slot, at_least_one_till_slot + 1, 1);
            ten_end = right;
            prev_ten_end = left;
        }
        const size_t total_ten = static_cast<size_t>(ten_end - prev_ten_end);
        int ten_left = static_cast<int>(total_ten);
        const size_t lslot_start = (bits.rank(REGISTER_SIZE) << 1) + ((prev_ten_end + 1 - at_least_one_till_slot) << 1);
        if (ten_left == 1) {
            current_slot_start_pos = bits.select(lslot_start) + 1;
            next_slot_start_pos = current_slot_start_pos;
            matched_offset = 0;
            return std::tuple(current_slot_start_pos, prev_ten_end + 1, matched_offset, current_slot_start_pos);
        }
        const size_t next_lslot_start = lslot_start + (ten_left * 2 - 3);
        const auto lslot_start_index_pair = bits.select_two(lslot_start, next_lslot_start);
        current_slot_start_pos = lslot_start_index_pair.first + 1;
        next_slot_start_pos = lslot_start_index_pair.second + 1; // remember to add 1 later
        const auto signature = bits.range_fast_2(current_slot_start_pos, next_slot_start_pos);
        const auto hash_val = ht1.hash_function(static_cast<uint16_t>(signature));
        const auto node = ht1.table[hash_val];
        if (static_cast<std::uint64_t>(node.key) == signature) {
            const auto important_bits = static_cast<uint64_t>(_pext_u64(fingerprint.bitset[0], static_cast<uint64_t>(node.value) << FP_index));
            const auto low_bit = important_bits * 3;
            matched_offset = (node.indices >> low_bit) & (0b111);
        } else {
            auto new_total_ten = total_ten;
            size_t lslot_start_index = lslot_start_index_pair.first + 1;
            matched_offset = walk_over_trie_payload(lslot_start_index, -1, ten_left, fingerprint,
                                         FP_index, new_total_ten);
        }
        return std::tuple(current_slot_start_pos, prev_ten_end + 1, matched_offset, next_slot_start_pos + 1);
        // current slot start pos, payload index of the first slot's payload, matched offset within, next slot start pos
    }
    [[nodiscard]] size_t get_lslot_start_dht(size_t slot_index) const {
        size_t bitmapPtr = 0;
        size_t sizePtr = COUNT_SLOT;
        size_t dtsPtr = bits.select(2 * bits.rank(COUNT_SLOT)) + 1;

        for (size_t sltPtr = 0; sltPtr < slot_index; sltPtr++) {
            if (bits.get(bitmapPtr++)) {
                int size = 1;
                bits.count_contiguous(sizePtr, size);
                if (size > 1) {
                    walk_over_trie(dtsPtr, -1, size);
                    dtsPtr++;
                }
            }
        }
        return dtsPtr;
    }

    [[nodiscard]] std::pair<size_t, size_t> get_lslot_start_dht_and_next(size_t slot_index) const {
        size_t bitmapPtr = 0;
        size_t sizePtr = COUNT_SLOT;
        size_t dtsPtr = bits.select(2 * bits.rank(COUNT_SLOT)) + 1;
        size_t prevDstPtr;

        for (size_t sltPtr = 0; sltPtr < slot_index + 1; sltPtr++) {
            if (bits.get(bitmapPtr++)) {
                int size = 1;
                prevDstPtr = dtsPtr;
                bits.count_contiguous(sizePtr, size);
                if (size > 1) {
                    walk_over_trie(dtsPtr, -1, size);
                    dtsPtr++;
                }
            }
        }
        return std::make_pair(prevDstPtr, dtsPtr);
    }
    void inputBits(const std::string &bitString) {
        bits = BitsetWrapper<N>(bitString);
    }

    std::optional<ENTRY_TYPE> readDHT(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, const SSDLog<Traits> &ssdLog, size_t FP_index, bool ignore = false) {
        const volatile auto payload_index_pair = get_index_dht(fingerprint, FP_index);
        auto payload_index = payload_index_pair.first + payload_index_pair.second;
        PAYLOAD_TYPE payload;
        if (ignore) {
            payload_index = get_max_index();
            payload = payload_list[payload_index];
        } 
        else
            payload = payload_list[payload_index];
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            auto res = payload_list.get_extra_bits_at(payload_index);
            auto chunked_fp = fingerprint.range_fast_one_reg(0, FP_index, FP_index + res.first);
            if (chunked_fp != res.second)
                return std::nullopt;
        }

        ENTRY_TYPE kv;
        ssdLog.read(payload, kv);

        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            auto new_fp = Hashing<Traits>::hash_digest(kv.key);
            payload_list.set_extra_bits_at(new_fp.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_index, 0);
        }
        return kv;
    }

    std::optional<ENTRY_TYPE> read(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, const SSDLog<Traits> &ssdLog, size_t FP_index) {
        auto [base, extra] = get_index(fingerprint, FP_index);
        const size_t payload_index = base + extra;
        if (static_cast<int64_t>(payload_index) > get_max_index())
            return std::nullopt;
        auto payload = payload_list[payload_index];
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            auto res = payload_list.get_extra_bits_at(payload_index);
            auto chunked_fp = fingerprint.range_fast_one_reg(0, FP_index, FP_index + res.first);
            if (chunked_fp != res.second)
                return std::nullopt;
        }
        ENTRY_TYPE kv;
        ssdLog.read(payload, kv);
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            auto new_fp = Hashing<Traits>::hash_digest(kv.key);
            payload_list.set_extra_bits_at(new_fp.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_index, 0);
        }
        return kv;
    }
    inline BlockInfo get_block_info() const {
        BlockInfo b;
        b.isExtended = !bits.get(N - 1);
        if (b.isExtended)
            b.firstExtendedLSlot = COUNT_SLOT - bits.get_leading_zeros(bits.NUM_REGS - 1);
        else 
            b.firstExtendedLSlot = COUNT_SLOT;
        b.remainingBits = bits.get_second_leading_zeros(bits.NUM_REGS - 1) - bits.get_leading_zeros(bits.NUM_REGS - 1) - 1;
        b.remainingPayload = Traits::PAYLOADS_LENGTH - get_max_index() - 1;
        return b;
    }

    // print block info
    void print_block_info() const {
        auto b = get_block_info();
        std::cout << "Block Info: " << std::endl;
        std::cout << "\tIs Extended: " << b->isExtended << std::endl;
        std::cout << "\tFirst Extended LSlot: " << b->firstExtendedLSlot << std::endl;
        std::cout << "\tRemaining Bits: " << b->remainingBits << std::endl;
        std::cout << "\tRemaining Payload: " << b->remainingPayload << std::endl;
    }

#ifdef ENABLE_XDP
    WriteReturnInfo writeGIEx(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint,
                              BitsetWrapper<FINGERPRINT_SIZE> &org_fingerprint,
                            const size_t FP_index, const PAYLOAD_TYPE &payload,
                            XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp,
                            const bool guarantee_update = false) {
        if constexpr (Traits::WRITE_STRATEGY != 0)
            throw std::invalid_argument("writeGI is not supported in this version");
        WriteReturnInfo info;
        auto slot_index = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        info.blockInfo = get_block_info();
        if (info.blockInfo.remainingBits <= Traits::EXTENSION_THRESHOLD && !guarantee_update) {
            // this should be first
            info.rs = WriteReturnStatusNotEnoughBlockSpace;
            return info;
        }
        if (!info.blockInfo.remainingPayload && !guarantee_update) {
            info.rs = WriteReturnStatusNotEnoughPayloadSpace;
            return info;
        }
        if (slot_index >= static_cast<int16_t>(info.blockInfo.firstExtendedLSlot)) {
            info.rs = WriteReturnStatusLslotExtended;
            return info;
        }
        size_t payload_idx;
        size_t ten = get_ten(slot_index);
        if (ten == 0) {
            payload_idx = get_index_start_lslot(slot_index);
            bits.set(slot_index, true);
            bits.shift_smart(1, REGISTER_SIZE + payload_idx, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set(REGISTER_SIZE + payload_idx, true);
        } else {
            size_t start_lslot_index, start_next_lslot_index;

            auto [current_slot_start_pos, starting_payload_offset, matched_offset, next_slot_start_pos] = get_slot_info(fingerprint, FP_index, slot_index);
            start_lslot_index = current_slot_start_pos;
            start_next_lslot_index = next_slot_start_pos;
            payload_idx = starting_payload_offset + matched_offset;

            BST<N> bst(ten, start_lslot_index, FP_index);
            bst.createBST(bits);
            auto payload_old = payload_list[payload_idx];
            size_t first_diff = REGISTER_SIZE;
            if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
                auto [valid_bits, extra_bit] = payload_list.get_extra_bits_at(payload_idx);
                auto temp_fp = fingerprint;
                temp_fp.shift_smart(-static_cast<int>(FP_index), 0);
                first_diff = __builtin_ctzll(extra_bit ^ temp_fp.bitset[0]) + FP_index;
                if (first_diff >= valid_bits + FP_index) {
                    first_diff = REGISTER_SIZE;
                }
            }
            if (first_diff == REGISTER_SIZE) {
                ENTRY_TYPE e;
                {
                    // TODO: fix this oracle
                    auto li_idx_oracle = payload_old;
                    std::optional<ENTRY_TYPE> e2 = xdp->performReadTaskIdx(org_fingerprint, li_idx_oracle);
                    if (!e2) {
                        throw std::runtime_error("Failed to read from SSDLog");
                    }
                    e.key = e2->key;
                    e.value = e2->value;
                }



                auto old_fp = Hashing<Traits>::hash_digest(e.key);
                if (compareTwoFP(old_fp, fingerprint, FP_index)) {
                    // update
                    goto PAYLOAD_SETTER_MAIN;
                }
                if constexpr (Traits::NUMBER_EXTRA_BITS > 1)
                    payload_list.set_extra_bits_at(old_fp.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_idx, 0);
                setLSlotIndexInFP(old_fp, slot_index, FP_index);  // needed to be updated for extended blocks
                first_diff = bst.get_first_diff_index(old_fp, fingerprint);
            }

            bst.insert(fingerprint, first_diff);
            auto rep = bst.getBitRepWrapper();
            const auto step = static_cast<int>(rep.firstInvalidIndex) - static_cast<int>(start_next_lslot_index - start_lslot_index);
            if (step + 1 > static_cast<int>(info.blockInfo.remainingBits) + 1) {
                info.rs = WriteReturnStatusNotEnoughBlockSpace;
                info.least_space_needed = step + 1;
                return info;
            }
            bits.shift_smart(step, start_lslot_index, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set_fast_two_reg(start_lslot_index, start_lslot_index + rep.firstInvalidIndex, rep.bw.bitset[0]);
            bits.shift_smart(1, REGISTER_SIZE + payload_idx, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set(REGISTER_SIZE + payload_idx, false);
            payload_idx = bst.getOffsetIdx(fingerprint) + starting_payload_offset;
        }
        payload_list.shift_right_from_index(payload_idx, 1, get_max_index_based_on_remaining_payloads(static_cast<int>(info.blockInfo.remainingPayload)));
        PAYLOAD_SETTER_MAIN:
        payload_list.set_payload_at(payload_idx, payload);
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1)
            payload_list.set_extra_bits_at(fingerprint.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_idx, 0);

        info.blockInfo = get_block_info();
        return info;
    }
    WriteReturnInfo writeGI(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint,
                                           const size_t FP_index, const PAYLOAD_TYPE &payload,
                                           XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp,
                                           const bool guarantee_update = false) {
        if constexpr (Traits::WRITE_STRATEGY != 0) 
            throw std::invalid_argument("writeGI is not supported in this version");
        WriteReturnInfo info;
        auto slot_index = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        info.blockInfo = get_block_info();
        if (info.blockInfo.remainingBits <= Traits::EXTENSION_THRESHOLD && !guarantee_update) {
            // this should be first
            info.rs = WriteReturnStatusNotEnoughBlockSpace;
            return info;
        }
        if (!info.blockInfo.remainingPayload && !guarantee_update) {
            info.rs = WriteReturnStatusNotEnoughPayloadSpace;
            return info;
        }
        if (slot_index >= static_cast<int16_t>(info.blockInfo.firstExtendedLSlot)) {
            info.rs = WriteReturnStatusLslotExtended;
            return info;
        }
        size_t payload_idx;
        size_t ten = get_ten(slot_index);
        if (ten == 0) {
            payload_idx = get_index_start_lslot(slot_index);
            bits.set(slot_index, true);
            bits.shift_smart(1, REGISTER_SIZE + payload_idx, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set(REGISTER_SIZE + payload_idx, true);
        } else {
            size_t start_lslot_index, start_next_lslot_index;

            auto [current_slot_start_pos, starting_payload_offset, matched_offset, next_slot_start_pos] = get_slot_info(fingerprint, FP_index, slot_index);
            start_lslot_index = current_slot_start_pos;
            start_next_lslot_index = next_slot_start_pos;
            payload_idx = starting_payload_offset + matched_offset;
            
            BST<N> bst(ten, start_lslot_index, FP_index);
            bst.createBST(bits);
            auto payload_old = payload_list[payload_idx];
            size_t first_diff = REGISTER_SIZE;
            if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
                auto [valid_bits, extra_bit] = payload_list.get_extra_bits_at(payload_idx);
                auto temp_fp = fingerprint;
                temp_fp.shift_smart(-static_cast<int>(FP_index), 0);
                first_diff = __builtin_ctzll(extra_bit ^ temp_fp.bitset[0]) + FP_index;
                if (first_diff >= valid_bits + FP_index) {
                    first_diff = REGISTER_SIZE;
                }
            }
            if (first_diff == REGISTER_SIZE) {
                ENTRY_TYPE e;
                {
                    // TODO: fix this oracle
                    auto li_idx_oracle = payload_old;
                    std::optional<ENTRY_TYPE> e2 = xdp->performReadTaskIdx(fingerprint, li_idx_oracle);
                    if (!e2) {
                        throw std::runtime_error("Failed to read from SSDLog");
                    }
                    e.key = e2->key;
                    e.value = e2->value;
                }
                


                auto old_fp = Hashing<Traits>::hash_digest(e.key);
                if (compareTwoFP(old_fp, fingerprint, FP_index)) {
                    // update
                    goto PAYLOAD_SETTER_MAIN;
                }
                if constexpr (Traits::NUMBER_EXTRA_BITS > 1)
                    payload_list.set_extra_bits_at(old_fp.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_idx, 0);
                setLSlotIndexInFP(old_fp, slot_index, FP_index);  // needed to be updated for extended blocks
                first_diff = bst.get_first_diff_index(old_fp, fingerprint);
            }

            bst.insert(fingerprint, first_diff);
            auto rep = bst.getBitRepWrapper();
            const auto step = static_cast<int>(rep.firstInvalidIndex) - static_cast<int>(start_next_lslot_index - start_lslot_index);
            if (step + 1 > static_cast<int>(info.blockInfo.remainingBits) + 1) {
                info.rs = WriteReturnStatusNotEnoughBlockSpace;
                info.least_space_needed = step + 1;
                return info;
            }
            bits.shift_smart(step, start_lslot_index, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set_fast_two_reg(start_lslot_index, start_lslot_index + rep.firstInvalidIndex, rep.bw.bitset[0]);
            bits.shift_smart(1, REGISTER_SIZE + payload_idx, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set(REGISTER_SIZE + payload_idx, false);
            payload_idx = bst.getOffsetIdx(fingerprint) + starting_payload_offset;
        }
        payload_list.shift_right_from_index(payload_idx, 1, get_max_index_based_on_remaining_payloads(static_cast<int>(info.blockInfo.remainingPayload)));
        PAYLOAD_SETTER_MAIN:
        payload_list.set_payload_at(payload_idx, payload);
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1)
            payload_list.set_extra_bits_at(fingerprint.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_idx, 0);

        info.blockInfo = get_block_info();
        return info;
    }
#endif
    WriteReturnInfo write(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, SSDLog<Traits> &ssdLog,
                                           const size_t FP_index, const PAYLOAD_TYPE &payload,
                                           const bool guarantee_update = false) {
        if constexpr (Traits::WRITE_STRATEGY != 0) 
            return write_old(fingerprint, ssdLog, FP_index, payload, guarantee_update);
        WriteReturnInfo info;
        auto slot_index = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        info.blockInfo = get_block_info();
        if (info.blockInfo.remainingBits <= Traits::EXTENSION_THRESHOLD && !guarantee_update) {
            // this should be first
            info.rs = WriteReturnStatusNotEnoughBlockSpace;
            return info;
        }
        if (!info.blockInfo.remainingPayload && !guarantee_update) {
            info.rs = WriteReturnStatusNotEnoughPayloadSpace;
            return info;
        }
        if (slot_index >= static_cast<int16_t>(info.blockInfo.firstExtendedLSlot)) {
            info.rs = WriteReturnStatusLslotExtended;
            return info;
        }
        size_t payload_idx;
        size_t ten = get_ten(slot_index);
        if (ten == 0) {
            payload_idx = get_index_start_lslot(slot_index);
            bits.set(slot_index, true);
            bits.shift_smart(1, REGISTER_SIZE + payload_idx, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set(REGISTER_SIZE + payload_idx, true);
        } else {
            size_t start_lslot_index, start_next_lslot_index;

            auto [current_slot_start_pos, starting_payload_offset, matched_offset, next_slot_start_pos] = get_slot_info(fingerprint, FP_index, slot_index);
            start_lslot_index = current_slot_start_pos;
            start_next_lslot_index = next_slot_start_pos;
            payload_idx = starting_payload_offset + matched_offset;
            
            BST<N> bst(ten, start_lslot_index, FP_index);
            bst.createBST(bits);
            auto payload_old = payload_list[payload_idx];
            size_t first_diff = REGISTER_SIZE;
            if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
                auto [valid_bits, extra_bit] = payload_list.get_extra_bits_at(payload_idx);
                auto temp_fp = fingerprint;
                temp_fp.shift_smart(-static_cast<int>(FP_index), 0);
                first_diff = __builtin_ctzll(extra_bit ^ temp_fp.bitset[0]) + FP_index;
                if (first_diff >= valid_bits + FP_index) {
                    first_diff = REGISTER_SIZE;
                }
            }
            if (first_diff == REGISTER_SIZE) {
                ENTRY_TYPE e;
                ssdLog.read(payload_old, e);
                auto old_fp = Hashing<Traits>::hash_digest(e.key);
                if (compareTwoFP(old_fp, fingerprint, FP_index)) {
                    // update
                    goto PAYLOAD_SETTER_MAIN;
                }
                if constexpr (Traits::NUMBER_EXTRA_BITS > 1)
                    payload_list.set_extra_bits_at(old_fp.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_idx, 0);
                setLSlotIndexInFP(old_fp, slot_index, FP_index);  // needed to be updated for extended blocks
                first_diff = bst.get_first_diff_index(old_fp, fingerprint);
            }

            bst.insert(fingerprint, first_diff);
            auto rep = bst.getBitRepWrapper();
            const auto step = static_cast<int>(rep.firstInvalidIndex) - static_cast<int>(start_next_lslot_index - start_lslot_index);
            if (step + 1 > static_cast<int>(info.blockInfo.remainingBits) + 1) {
                info.rs = WriteReturnStatusNotEnoughBlockSpace;
                info.least_space_needed = step + 1;
                return info;
            }
            bits.shift_smart(step, start_lslot_index, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set_fast_two_reg(start_lslot_index, start_lslot_index + rep.firstInvalidIndex, rep.bw.bitset[0]);
            bits.shift_smart(1, REGISTER_SIZE + payload_idx, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set(REGISTER_SIZE + payload_idx, false);
            payload_idx = bst.getOffsetIdx(fingerprint) + starting_payload_offset;
        }
        payload_list.shift_right_from_index(payload_idx, 1, get_max_index_based_on_remaining_payloads(static_cast<int>(info.blockInfo.remainingPayload)));
        PAYLOAD_SETTER_MAIN:
        payload_list.set_payload_at(payload_idx, payload);
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1)
            payload_list.set_extra_bits_at(fingerprint.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_idx, 0);

        info.blockInfo = get_block_info();
        return info;
    }

    // return false if overflows
    WriteReturnInfo write_old(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint,
                                           SSDLog<Traits> &ssdLog,
                                           const size_t FP_index, const PAYLOAD_TYPE &payload,
                                           const bool guarantee_update = false) {
        WriteReturnInfo info;
        auto slot_index = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);

        size_t ten;
        if constexpr (Traits::WRITE_STRATEGY == 0) {
            ten = get_ten(slot_index);
        } else if constexpr (Traits::WRITE_STRATEGY == 20) {
            ten = get_ten_dht(slot_index);
        }
        // TODO: potentially this can be optimized
        info.blockInfo = get_block_info();
        if (info.blockInfo.remainingBits <= Traits::EXTENSION_THRESHOLD && !guarantee_update) {
            // this should be first
            info.rs = WriteReturnStatusNotEnoughBlockSpace;
            return info;
        }
        if (!info.blockInfo.remainingPayload && !guarantee_update) {
            info.rs = WriteReturnStatusNotEnoughPayloadSpace;
            return info;
        }
        if (slot_index >= static_cast<int16_t>(info.blockInfo.firstExtendedLSlot)) {
            info.rs = WriteReturnStatusLslotExtended;
            return info;
        }
        size_t payload_idx;
        if (ten == 0) {
            if constexpr (Traits::WRITE_STRATEGY == 0) {
                payload_idx = get_index_start_lslot(slot_index);
            } else if constexpr (Traits::WRITE_STRATEGY == 20) {
                payload_idx = get_index_start_lslot_dht(slot_index);
            }
            bits.set(slot_index, true);
            bits.shift_smart(1, REGISTER_SIZE + payload_idx, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set(REGISTER_SIZE + payload_idx, true);
        } else {
            std::pair<size_t, size_t> payload_idx_pair;
            if constexpr (Traits::WRITE_STRATEGY == 0) {
                payload_idx_pair = get_index(fingerprint, FP_index);
                payload_idx = payload_idx_pair.first + payload_idx_pair.second;
            } else if constexpr (Traits::WRITE_STRATEGY == 20) {
                payload_idx_pair = get_index_dht(fingerprint, FP_index);
                payload_idx = payload_idx_pair.first + payload_idx_pair.second;
            }
            // shift the tenancy area
            size_t start_lslot_index, start_next_lslot_index;
            if constexpr (Traits::WRITE_STRATEGY == 0) {
                start_lslot_index = get_lslot_start(slot_index);
                start_next_lslot_index = get_lslot_start(slot_index + 1);
            } else if constexpr (Traits::WRITE_STRATEGY == 20) {
                auto [start_lslot_index_pair, start_next_lslot_index_pair] = get_lslot_start_dht_and_next(slot_index);
                start_lslot_index = start_lslot_index_pair;
                start_next_lslot_index = start_next_lslot_index_pair;
            }

            BST<N> bst(ten, start_lslot_index, FP_index);
            bst.createBST(bits);
            auto payload_old = payload_list[payload_idx];
            size_t first_diff = REGISTER_SIZE;
            if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
                auto [valid_bits, extra_bit] = payload_list.get_extra_bits_at(payload_idx);
                auto temp_fp = fingerprint;
                temp_fp.shift_smart(-static_cast<int>(FP_index), 0);
                first_diff = __builtin_ctzll(extra_bit ^ temp_fp.bitset[0]) + FP_index;
                if (first_diff >= valid_bits + FP_index) {
                    first_diff = REGISTER_SIZE;
                }
            }
            if (first_diff == REGISTER_SIZE) {
                ENTRY_TYPE e;
                ssdLog.read(payload_old, e);
                auto old_fp = Hashing<Traits>::hash_digest(e.key);
                if (compareTwoFP(old_fp, fingerprint, FP_index)) {
                    // update
                    goto PAYLOAD_SETTER_OLD;
                }
                if constexpr (Traits::NUMBER_EXTRA_BITS > 1)
                    payload_list.set_extra_bits_at(old_fp.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_idx, 0);
                setLSlotIndexInFP(old_fp, slot_index, FP_index);  // needed to be updated for extended blocks
                first_diff = bst.get_first_diff_index(old_fp, fingerprint);
            }

            bst.insert(fingerprint, first_diff);
            auto rep = bst.getBitRepWrapper();
            const auto step = static_cast<int>(rep.firstInvalidIndex) - static_cast<int>(start_next_lslot_index - start_lslot_index);
            if (step + 1 > static_cast<int>(info.blockInfo.remainingBits) + 1) {
                info.rs = WriteReturnStatusNotEnoughBlockSpace;
                info.least_space_needed = step + 1;
                return info;
            }
            bits.shift_smart(step, start_lslot_index, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set_fast_two_reg(start_lslot_index, start_lslot_index + rep.firstInvalidIndex, rep.bw.bitset[0]);
            bits.shift_smart(1, REGISTER_SIZE + payload_idx, CALCULATE_LAST_AVAILABLE_INDEX(info.blockInfo.firstExtendedLSlot));
            bits.set(REGISTER_SIZE + payload_idx, false);
            payload_idx = bst.getOffsetIdx(fingerprint) + payload_idx_pair.first;
        }
        payload_list.shift_right_from_index(payload_idx, 1, get_max_index_based_on_remaining_payloads(static_cast<int>(info.blockInfo.remainingPayload)));
    PAYLOAD_SETTER_OLD:
        payload_list.set_payload_at(payload_idx, payload);
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1)
            payload_list.set_extra_bits_at(fingerprint.range_fast_one_reg(0, FP_index, REGISTER_SIZE), payload_idx, 0);

        info.blockInfo = get_block_info();
        return info;
    }
    std::unique_ptr<RemoveReturnInfo> remove(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, SSDLog<Traits> &ssdLog, size_t FP_index) {
        auto info = std::make_unique<RemoveReturnInfo>();  // Initialize info using std::make_unique
        auto slot_index = fingerprint.range_fast_one_reg(0, FP_index - COUNT_SLOT_BITS, FP_index);
        auto ten = get_ten(slot_index);
        BlockInfo blkInfoP = get_block_info();
        info->blockInfo = std::make_unique<BlockInfo>(blkInfoP);
        if (slot_index >= static_cast<int16_t>(info->blockInfo->firstExtendedLSlot)) {
            info->rs = RemoveReturnStatusLslotExtended;
            return info;
        }
        size_t payload_idx;
        if (!ten)
            throw std::invalid_argument("not a valid delete - key did not exist - tenancy 0");
        if (ten == 1) {
            payload_idx = get_index_start_lslot(slot_index);
            bits.set(slot_index, false);
            bits.shift_smart(-1, REGISTER_SIZE + payload_idx, CALCULATE_LAST_AVAILABLE_INDEX(info->blockInfo->firstExtendedLSlot));
        } else {
            const auto payload_idx_pair = get_index(fingerprint, FP_index);
            payload_idx = payload_idx_pair.first + payload_idx_pair.second;

            const auto start_lslot_index = get_lslot_start(slot_index);
            const auto start_next_lslot_index = get_lslot_start(slot_index + 1);
            BST<N> bst(ten, start_lslot_index, FP_index);
            bst.createBST(bits);
            auto payload_old = payload_list.get_payload_at(payload_idx);
            ENTRY_TYPE e;
            ssdLog.read(payload_old, e);
            auto old_fp = Hashing<Traits>::hash_digest(e.key);
            if (const auto temp = old_fp ^ fingerprint; temp.bitset[0] >= (1 << FP_index) || temp.bitset[1] != 0) {
                old_fp.printBitset();
                fingerprint.printBitset();
                throw std::invalid_argument("not a valid delete - key did not exist");
            }

            bst.remove(fingerprint);
            auto rep = bst.getBitRepWrapper();
            // cout << rep.firstInvalidIndex  << " haha\n";
            const auto step = static_cast<int>(rep.firstInvalidIndex) - static_cast<int>(start_next_lslot_index - start_lslot_index);
            bits.shift_smart(step, start_lslot_index, CALCULATE_LAST_AVAILABLE_INDEX(info->blockInfo->firstExtendedLSlot));
            bits.set_fast_two_reg(start_lslot_index, start_lslot_index + rep.firstInvalidIndex, rep.bw.bitset[0]);
            bits.shift_smart(-1, REGISTER_SIZE + payload_idx_pair.first, CALCULATE_LAST_AVAILABLE_INDEX(info->blockInfo->firstExtendedLSlot));
        }
        payload_list.shift_left_from_index(payload_idx);
        BlockInfo b = get_block_info();
        // make unique 
        info->blockInfo = std::make_unique<BlockInfo>(b);
        return info;
    }
    void print() {
        std::cout << "payload size:\t" << static_cast<int>(get_max_index() + 1) << "\t-\t";
        bits.printBitset();
    }

    std::string getLSlotString(SSDLog<Traits> &ssdLog, size_t lslot_idx) {
        const auto ten = get_ten(lslot_idx);
        const auto payload_start = get_index_start_lslot(lslot_idx);

        std::string ret;
        for (size_t i = payload_start; i < payload_start + ten; i++) {
            ENTRY_TYPE e;
            ssdLog.read(payload_list.get_payload_at(i), e);
            if (!ret.empty()) {
                ret += ' ';
            }
            ret += std::to_string(e.key);
        }
        return ret;
    }

    void printLSlot(SSDLog<Traits> &ssdLog, size_t lslot_idx) {
        std::cout << "print lslot " << lslot_idx << std::endl;
        auto ten = get_ten(lslot_idx);
        if (!ten)
            std::cout << "empty\n";
        std::cout << getLSlotString(ssdLog, lslot_idx) << std::endl;
    }

    size_t get_count_zero_start(BST<N> &bst, SSDLog<Traits> &ssdLog, const size_t payload_start_idx) {
        const auto fp_index = bst.getFPIndex();
        size_t zero_count;
        if (!bst.getTenSize()) {  // ten 0
            zero_count = 0;
        } else if (bst.root == nullptr) {  // ten 1
            zero_count = get_first_bit_fingerprint(ssdLog, payload_start_idx, fp_index);
        } else if (bst.root->index == (int)fp_index) {  // ten > 1
            zero_count = bst.getTen(bst.root->left);
        } else {  // ten > 1 but first node's index is > 0
            zero_count = get_first_bit_fingerprint(ssdLog, payload_start_idx, fp_index) ? bst.getTenSize() : 0;
        }
        return zero_count;
    }

#ifdef ENABLE_XDP
    size_t get_count_zero_start_gi(BST<N> &bst, const size_t payload_start_idx, XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp,
                                   BitsetWrapper<FINGERPRINT_SIZE> fp_artificial) {
        const auto fp_index = bst.getFPIndex();
        size_t zero_count;
        if (!bst.getTenSize()) {  // ten 0
            zero_count = 0;
        } else if (bst.root == nullptr) {  // ten 1
            zero_count = get_first_bit_fingerprint_gi(payload_start_idx, fp_index, xdp, fp_artificial);
        } else if (bst.root->index == (int)fp_index) {  // ten > 1
            zero_count = bst.getTen(bst.root->left);
        } else {  // ten > 1 but first node's index is > 0
            zero_count = get_first_bit_fingerprint_gi(payload_start_idx, fp_index, xdp, fp_artificial) ? bst.getTenSize() : 0;
        }
        return zero_count;
    }
#endif
    size_t get_first_bit_fingerprint(SSDLog<Traits> &ssdLog, const size_t payload_start_idx, size_t fp_index) {
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            auto [valid_bits, extra_bits] = payload_list.get_extra_bits_at(payload_start_idx);
            if (valid_bits) {
                if (extra_bits % 2 == 0)
                    return 1;
                else
                    return 0;
            }
        }
        auto payload = payload_list.get_payload_at(payload_start_idx);
        ENTRY_TYPE kv;
        ssdLog.read(payload, kv);
        auto fp = Hashing<Traits>::hash_digest(kv.key);
        auto bit = fp.get(fp_index);
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            auto extra = fp.range_fast_one_reg(0, fp_index, REGISTER_SIZE);
            payload_list.set_extra_bits_at(extra, payload_start_idx, 0);
        }
        return bit ? 0 : 1;
    }

#ifdef ENABLE_XDP
    size_t get_first_bit_fingerprint_gi(const size_t payload_start_idx, size_t fp_index,
                                        XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp, BitsetWrapper<FINGERPRINT_SIZE> fp_artificial) {
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            auto [valid_bits, extra_bits] = payload_list.get_extra_bits_at(payload_start_idx);
            if (valid_bits) {
                if (extra_bits % 2 == 0)
                    return 1;
                else
                    return 0;
            }
        }
        auto payload = payload_list.get_payload_at(payload_start_idx);
        ENTRY_TYPE kv;
        bool bit;
        {
            auto li_idx_oracle = payload;
            std::optional<ENTRY_TYPE> e2 = xdp->performReadTaskIdx(fp_artificial, li_idx_oracle);
            if (!e2) {
                throw std::runtime_error("Failed to read from SSDLog");
            }
            auto fp2 = Hashing<Traits>::hash_digest(e2->key);
            auto bit2 = fp2.get(fp_index);

            bit = bit2;
        }
//        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
//            auto extra = fp.range_fast_one_reg(0, fp_index, REGISTER_SIZE);
//            payload_list.set_extra_bits_at(extra, payload_start_idx, 0);
//        }
        return bit ? 0 : 1;
    }

#endif
    [[nodiscard]] size_t get_last_lslot_idx() const {
        return COUNT_SLOT - bits.get_leading_zeros(bits.NUM_REGS - 1) - 1;
    }
    size_t get_ten_all_lslots(size_t first_extended_lslot = COUNT_SLOT) {
        size_t total_ten = 0;
        std::cout << "Printing Tens " << first_extended_lslot << std::endl;
        for (size_t i = 0; i < first_extended_lslot; i++) {
            const auto tmp = get_ten_dht(i);
            std::cout << "lslot " << i << " ten " << tmp << std::endl;
            total_ten += tmp;
        }
        return total_ten;
    }

    size_t read_payload(BitsetWrapper<FINGERPRINT_SIZE> &fingerprint, size_t FP_index) {
        const auto payload_index_pair = get_index(fingerprint, FP_index);
        const auto payload_index = payload_index_pair.first + payload_index_pair.second;
        if (static_cast<int64_t>(payload_index) > get_max_index())
            return SIZE_MAX;
        auto payload = payload_list[payload_index];
        return payload;
    }

    float get_average_age() {
        size_t total_ten = get_max_index() + 1;
        size_t total_age = 0;
        for (int i = 0; i <= get_max_index(); i++) {
            auto age = payload_list.get_age_at(i);
            total_age += age;
        }
        if (total_ten == 0)
            return 0;
        return (float) total_age / (float) total_ten;
    }
};
