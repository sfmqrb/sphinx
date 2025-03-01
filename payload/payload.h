#pragma once
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <stdexcept>

#include "../bitset_wrapper/bitset_wrapper.h"  // Include your BitsetWrapper implementation
#include "../config/config.h"

template <typename Traits = DefaultTraits>
class Payload {
   private:
    typedef typename Traits::KEY_TYPE KEY_TYPE;
    typedef typename Traits::VALUE_TYPE VALUE_TYPE;
    typedef typename Traits::ENTRY_TYPE ENTRY_TYPE;

    typename Traits::PAYLOAD_TYPE payloads[Traits::PAYLOADS_LENGTH];
    std::conditional_t<static_cast<bool>(Traits::NUMBER_EXTRA_BITS),
                       BitsetWrapper<((Traits::PAYLOADS_LENGTH * Traits::NUMBER_EXTRA_BITS + REGISTER_SIZE - 1) / REGISTER_SIZE) * REGISTER_SIZE>,
                       void*>
        extraBits;
    int64_t maxIndex{-1};

   public:
    constexpr bool has_space() const {
        return maxIndex < static_cast<int64_t>(Traits::PAYLOADS_LENGTH - 1);
    }

    constexpr auto get_max_index() const {
        return maxIndex;
    }

    typename Traits::PAYLOAD_TYPE get_payload_at(size_t index, bool ignore) const {
        if (static_cast<int64_t>(index) <= maxIndex) {
            return payloads[index];
        }
        if (!ignore) {
            std::cerr << "Index out of range in Payload" << std::endl;
            if (!Traits::DHT_EVERYTHING) {
                throw std::out_of_range("Index out of range in Payload for DHT_EVERYTHING off");
            }
        }
        return -1;
    }

    typename Traits::PAYLOAD_TYPE operator[](size_t index) const {
        return get_payload_at(index, false);
    }

    void printPayload() const {
        std::cout << "Payload List: ";
        for (auto i = 0; i <= maxIndex; i++) {
            std::cout << i << ":" << payloads[i] << " - ";
        }
        std::cout << std::endl;
        std::cout << "extra bits: ";
        extraBits.printBitset();
    }

    bool set_payload_at(size_t index, typename Traits::PAYLOAD_TYPE val) {
        if (index < Traits::PAYLOADS_LENGTH) {
            payloads[index] = val;
            maxIndex = std::max(maxIndex, static_cast<int64_t>(index));
            return true;
        }
        throw std::out_of_range("Index out of range in Set Payload");
    }

    static void swap(const Payload<Traits>& source, size_t src_idx, Payload<Traits>& dest, size_t dst_idx, bool age) {
        dest.set_payload_at(dst_idx, source[src_idx]);
        if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
            auto exBit = source.get_extra_bits_at(src_idx);
            auto orgExBit = (1ull << exBit.first) | exBit.second;
            if (age && exBit.first > 0)
                orgExBit >>= 1;
            dest.set_extra_bits_at_raw(orgExBit, dst_idx);
        }
    }

    auto get_extra_bits_at(size_t index) const {
        if constexpr (Traits::NUMBER_EXTRA_BITS <= 1)
            return std::make_pair(0ul, 0ul);
        auto val = static_cast<uint64_t>(extraBits.range_fast(index * Traits::NUMBER_EXTRA_BITS, (index + 1) * Traits::NUMBER_EXTRA_BITS));
        auto age = __builtin_clzll(val) - (REGISTER_SIZE - Traits::NUMBER_EXTRA_BITS);
        assert(Traits::NUMBER_EXTRA_BITS > age);
        auto valid_extra = (1 << (Traits::NUMBER_EXTRA_BITS - age - 1)) - 1;  // -1 for one bit to store age
        auto extra = val & valid_extra;
        return std::make_pair((Traits::NUMBER_EXTRA_BITS - age - 1), extra);  // return valid_bits_count and extra val
    }

    void set_extra_bits_at(size_t extra, size_t index, size_t age) {
        if constexpr (Traits::NUMBER_EXTRA_BITS <= 1)
            return;
        assert(Traits::NUMBER_EXTRA_BITS > age);
        auto valid_extra = (1 << (Traits::NUMBER_EXTRA_BITS - age - 1)) - 1;  // -1 for one bit to store age
        extra &= valid_extra;
        extra |= (valid_extra + 1);
        extraBits.set_fast_two_reg(index * Traits::NUMBER_EXTRA_BITS, (index + 1) * Traits::NUMBER_EXTRA_BITS, extra);
    }

    void set_extra_bits_at_raw(size_t extra_raw, size_t index) {
        extraBits.set_fast_two_reg(index * Traits::NUMBER_EXTRA_BITS, (index + 1) * Traits::NUMBER_EXTRA_BITS, extra_raw);
    }
    void shift_right_from_index(size_t index, size_t steps = 1) {
        if (index < Traits::PAYLOADS_LENGTH - 1 && maxIndex >= static_cast<int64_t>(index)) {
            for (int i = maxIndex + steps; i > static_cast<int>(index); i--) {
                payloads[i] = payloads[i - steps];
            }
            maxIndex = std::min(static_cast<int64_t>(maxIndex + steps), static_cast<int64_t>(Traits::PAYLOADS_LENGTH - 1));
            // if Reserve Bits are enabled, shift them as well
            if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
                auto bit_steps = static_cast<int>(steps * Traits::NUMBER_EXTRA_BITS);
                auto bit_start = static_cast<size_t>(index) * Traits::NUMBER_EXTRA_BITS;
                auto bit_end = (static_cast<size_t>(maxIndex) + 1) * Traits::NUMBER_EXTRA_BITS;
                if (bit_steps >= REGISTER_SIZE)
                    extraBits.deprecated_shift(bit_steps, bit_start, bit_end);
                else
                    extraBits.shift_smart(bit_steps, bit_start, bit_end);
            }
        }
    }

    void shift_left_from_index(size_t index, size_t steps = 1) {
        if (index < Traits::PAYLOADS_LENGTH - 1 && maxIndex >= static_cast<int64_t>(index)) {
            // if Reserve Bits are enabled, shift them as well
            if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
                auto bit_steps = static_cast<int>(steps * Traits::NUMBER_EXTRA_BITS);
                auto bit_start = index * Traits::NUMBER_EXTRA_BITS;
                auto bit_end = (maxIndex + 1) * Traits::NUMBER_EXTRA_BITS;
                if (bit_steps >= REGISTER_SIZE)
                    extraBits.deprecated_shift(bit_steps, bit_start, bit_end);
                else
                    extraBits.shift_smart(-bit_steps, bit_start, bit_end);
            }
            // then shift the payloads
            for (size_t i = index; i < static_cast<size_t>(maxIndex + 1 - static_cast<int>(steps)); i++) {
                payloads[i] = payloads[i + steps];
            }
            maxIndex -= steps;
        } else if (index == Traits::PAYLOADS_LENGTH - 1) {
            assert(steps == 1);
            assert(maxIndex > 0);
            maxIndex -= 1;
        }
    }
};
