#pragma once
#include <cassert>
#include <cstdint>
#include <iostream>
#include <immintrin.h>
#include <stdexcept>
#include <array>

#include "../bitset_wrapper/bitset_wrapper.h"
#include "../config/config.h"

template <typename Traits = DefaultTraits>
class Payload {
   private:
    typedef typename Traits::KEY_TYPE KEY_TYPE;
    typedef typename Traits::VALUE_TYPE VALUE_TYPE;
    typedef typename Traits::ENTRY_TYPE ENTRY_TYPE;

    using FixedArray = std::array<typename Traits::PAYLOAD_TYPE, Traits::PAYLOADS_LENGTH>;
    static constexpr size_t SIZE_VAR_LEN_BITSET = COUNT_SLOT * 2;
    using VarArray = BitsetWrapper<SIZE_VAR_LEN_BITSET>; 
    using PayloadStorage = std::conditional_t<
                                Traits::VAR_LEN_PAYLOAD,
                                VarArray,
                                FixedArray
                           >;
    PayloadStorage payloads;

   public:
    void set_init_page_of_block(uint32_t page) {
        if constexpr (Traits::VAR_LEN_PAYLOAD) {
            payloads.set_fast_one_reg(1, 32, 64, page); 
        } else {
            throw std::runtime_error("set_init_page_of_block is not supported for fixed length payloads");
        }
    }

    uint32_t get_init_page_of_block() const {
        if constexpr (Traits::VAR_LEN_PAYLOAD) {
            auto page = payloads.range_fast_one_reg(1, 32, 64);
            if (page == 0xFFFFFFFF) {
                throw std::runtime_error("initPageOfBlock is not set");
            }
            return page;
        } else {
            throw std::runtime_error("get_init_page_of_block is not supported for fixed length payloads");
        }
    }

    Payload() {
        if constexpr (Traits::VAR_LEN_PAYLOAD) {
            set_init_page_of_block(0xFFFFFFFF); // Initialize to an invalid page
        } 
    }

    inline int64_t get_payload_at(size_t index) const {
        if constexpr (Traits::VAR_LEN_PAYLOAD) {
            auto initPageOfBlock = get_init_page_of_block();
            auto select_int_res = payloads.select(index + 1);
            auto relative_idx = select_int_res - index;
            if (relative_idx == 0) 
                return initPageOfBlock + select_int_res;
            else {
                auto select_last0_before_res = (~payloads).select(relative_idx);
                auto offset_within_page = select_int_res - select_last0_before_res - 1; // because of the last 0 I add -1
                auto newPage = initPageOfBlock / (PAGE_SIZE / sizeof(ENTRY_TYPE)) + relative_idx;
                auto offset = newPage * (PAGE_SIZE / sizeof(ENTRY_TYPE)) + offset_within_page;
                return offset;
            }
        } else {
            if constexpr (Traits::NUMBER_EXTRA_BITS <= 1) {
                return static_cast<uint64_t>(payloads[index]);
            } else {
                constexpr size_t EB = Traits::NUMBER_EXTRA_BITS;
                const auto mask = (typename Traits::PAYLOAD_TYPE(-1)) >> EB;
                return static_cast<uint64_t>(payloads[index] & mask);
            }
            return -1;
        }
    }

    inline int64_t operator[](size_t index) const {
        return get_payload_at(index);
    }

    inline bool set_payload_at(size_t index, typename Traits::PAYLOAD_TYPE val) {
        if constexpr (Traits::VAR_LEN_PAYLOAD) {
            size_t from;
            if (val == 0) throw std::invalid_argument("Payload value cannot be 0 in var len setting.");
            if (index == 0) from = 0; 
            else from = payloads.select(index) + 1;
            payloads.shift_smart(val, from, SIZE_VAR_LEN_BITSET - 32);
            payloads.set_fast_two_reg(from, from + val, 1 << (val - 1)); 
            return true;
        } else {
            if (index < Traits::PAYLOADS_LENGTH) {
                if constexpr (Traits::NUMBER_EXTRA_BITS <= 1) {
                    payloads[index] = val;
                } else {
                    constexpr size_t EB = Traits::NUMBER_EXTRA_BITS;
                    const auto mask = (typename Traits::PAYLOAD_TYPE(-1)) >> EB;
                    payloads[index] = (payloads[index] & ~mask) | (val & mask);
                }
                return true;
            }
            throw std::out_of_range("Index out of range in Set Payload");
        }
    }

    inline static void swap(const Payload<Traits>& source, size_t src_idx, Payload<Traits>& dest, size_t dst_idx, bool age) {
        dest.set_payload_at(dst_idx, source[src_idx]);
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            auto exBit = source.get_extra_bits_at(src_idx);
            auto orgExBit = (1ull << exBit.first) | exBit.second;
            if (age && exBit.first > 0) orgExBit >>= 1;
            dest.set_extra_bits_at_raw(orgExBit, dst_idx);
        }
    }

    inline auto get_extra_bits_at(size_t index) const {
        if constexpr (Traits::NUMBER_EXTRA_BITS <= 1) return std::make_pair(0ul, 0ul);
        uint64_t val;
        if constexpr (Traits::VAR_LEN_PAYLOAD) {
            throw std::runtime_error("get_extra_bits_at not supported for variable length payloads");
        } else {
            constexpr size_t PAYLOAD_BITS = sizeof(typename Traits::PAYLOAD_TYPE) * 8;
            constexpr size_t EB = Traits::NUMBER_EXTRA_BITS;
            val = (payloads[index] >> (PAYLOAD_BITS - EB)) & ((1 << EB) - 1);
        }
        auto age = __builtin_clzll(val) - (64 - Traits::NUMBER_EXTRA_BITS);
        assert(Traits::NUMBER_EXTRA_BITS > age);
        auto valid_extra = (1 << (Traits::NUMBER_EXTRA_BITS - age - 1)) - 1;
        auto extra = val & valid_extra;
        return std::make_pair((Traits::NUMBER_EXTRA_BITS - age - 1), extra);
    }

    inline size_t get_age_at(size_t index) {
        if constexpr (Traits::NUMBER_EXTRA_BITS <= 1) return 0;
        uint64_t val;
        if constexpr (Traits::VAR_LEN_PAYLOAD) {
            throw std::runtime_error("get_age_at not supported for variable length payloads");
        } else {
            constexpr size_t PAYLOAD_BITS = sizeof(typename Traits::PAYLOAD_TYPE) * 8;
            constexpr size_t EB = Traits::NUMBER_EXTRA_BITS;
            val = (payloads[index] >> (PAYLOAD_BITS - EB)) & ((1 << EB) - 1);
        }
        auto age = __builtin_clzll(val) - (64 - Traits::NUMBER_EXTRA_BITS);
        assert(Traits::NUMBER_EXTRA_BITS > age);
        return age;
    }

    inline void set_extra_bits_at(size_t extra, size_t index, size_t age) {
        if constexpr (Traits::NUMBER_EXTRA_BITS <= 1) return;
        assert(Traits::NUMBER_EXTRA_BITS > age);
        auto valid_extra = (1 << (Traits::NUMBER_EXTRA_BITS - age - 1)) - 1;
        extra &= valid_extra;
        extra |= (valid_extra + 1);
        set_extra_bits_at_raw(extra, index);
    }

    inline void set_extra_bits_at_raw(size_t extra_raw, size_t index) {
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            constexpr size_t PAYLOAD_BITS = sizeof(typename Traits::PAYLOAD_TYPE) * 8;
            constexpr size_t EB = Traits::NUMBER_EXTRA_BITS;
            if constexpr (!Traits::VAR_LEN_PAYLOAD) {
                auto current = payloads[index];
                const auto mask = (typename Traits::PAYLOAD_TYPE(-1)) >> EB;
                payloads[index] = (current & mask) | 
                                 ((extra_raw & ((1 << EB) - 1)) << (PAYLOAD_BITS - EB));
            } else {
                throw std::runtime_error("set_extra_bits_at_raw not supported for variable length payloads");
            }
        }
    }

    inline void shift_right_from_index(size_t index, size_t steps = 1, size_t max_index = Traits::PAYLOADS_LENGTH - 2) {
        if constexpr (Traits::VAR_LEN_PAYLOAD) {
            throw std::runtime_error("Shift right is not supported for variable length payloads");
        } else {
            size_t num_elements = static_cast<size_t>(max_index - index) + 1;
            size_t max_num_elements = Traits::PAYLOADS_LENGTH - index - steps;
            if (num_elements > max_num_elements)
                num_elements = max_num_elements;
            memmove(payloads.data() + index + steps, payloads.data() + index,
                   num_elements * sizeof(typename Traits::PAYLOAD_TYPE));
        }
    }

    inline void shift_left_from_index(size_t index, size_t steps = 1, size_t max_index = Traits::PAYLOADS_LENGTH - 1) {
        if constexpr (Traits::VAR_LEN_PAYLOAD) {
            throw std::runtime_error("Shift left is not supported for variable length payloads");
        } else {
            for (size_t i = index; i < static_cast<size_t>(max_index + 1 - steps); i++) {
                payloads[i] = payloads[i + steps];
            }
        }
    }

    void printPayload(bool no_ext_bits = false) const {
        std::cout << "Payload List: ";
        for (auto i = 0; i < Traits::PAYLOADS_LENGTH; i++) {
            std::cout << i << ":" << get_payload_at(i) << " - ";
        }
        std::cout << std::endl;
        if (no_ext_bits) return;
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            std::cout << "extra bits: ";
            for (auto i = 0; i < Traits::PAYLOADS_LENGTH; i++) {
                auto ex = get_extra_bits_at(i);
                std::cout << "(" << ex.first << "," << ex.second << ") ";
            }
            std::cout << std::endl;
        }
    }
};
