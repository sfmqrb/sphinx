#pragma once
#include <x86intrin.h>  // For _pdep_u64 and _tzcnt_u64

#include <cstddef>
#include <cstdint>
#include <cstring>  // For std::memcmp
#include <iostream>
#include <stdexcept>
#include <utility>
#include <vector>

constexpr size_t count_bits(size_t n, size_t count = -1) {  // same as log2
    return n ? count_bits(n >> 1, count + 1) : count;
}

constexpr size_t REGISTER_SIZE = 64;
constexpr size_t REGISTER_SIZE_BITS = static_cast<size_t>(count_bits(REGISTER_SIZE));
// 128 here is a optimization it should be N

#define GET_INDEX(agg_index) ((agg_index) >> (REGISTER_SIZE_BITS))
#define GET_OFFSET(agg_index) ((agg_index) & (REGISTER_SIZE - 1))

inline uint64_t GET_ZERO_MSB(const size_t first_zero_index) {
    if (first_zero_index == 64)
        return ~0;
    return (1ull << first_zero_index) - 1;
}
inline uint64_t GET_ONE_MSB(const size_t first_one_index) {
    return ~GET_ZERO_MSB(first_one_index);
}

template <size_t N>
class BitsetWrapper {
   public:
    static constexpr size_t NUM_REGS = (N + REGISTER_SIZE - 1) / REGISTER_SIZE;

    uint64_t bitset[(N + REGISTER_SIZE - 1) / REGISTER_SIZE] = {};

    BitsetWrapper() = default;

    explicit BitsetWrapper(const std::string &binaryString) {
        setInputString(binaryString);
    }

    explicit BitsetWrapper(const std::vector<uint64_t>& vals, bool is_array) {
        if (vals.size() != NUM_REGS || !is_array) throw std::invalid_argument("not correct nums of args");
        for (size_t i = 0; i < NUM_REGS; ++i)
            this->bitset[i] = vals[i];
    }
    inline BitsetWrapper<N>& operator=(const BitsetWrapper<N>& other) {
        if (this != &other)
            std::memcpy(this->bitset, other.bitset, sizeof(bitset));
        return *this;
    }

    inline bool operator>(const BitsetWrapper& other) const {
        for (int i = 0; i < N; i++)
            if (get(i) != other.get(i))
                return get(i) > other.get(i);
        return false;
    }

    inline bool operator<(const BitsetWrapper& other) const {
        for (int i = 0; i < NUM_REGS; i++)
            if (bitset[i] != other.bitset[i])
                return bitset[i] < other.bitset[i];
        return false;
    }
    inline bool operator==(const BitsetWrapper &other) const {
        return std::memcmp(bitset, other.bitset, sizeof(bitset)) == 0;
    }
    inline bool operator!=(const BitsetWrapper &other) const {
        return !(*this == other);
    }
    inline BitsetWrapper operator^(const BitsetWrapper &other) const {
        BitsetWrapper result;
        for (size_t i = 0; i < NUM_REGS; i++)
            result.bitset[i] = this->bitset[i] ^ other.bitset[i];
        return result;
    }
    inline BitsetWrapper operator&(const BitsetWrapper &other) const {
        BitsetWrapper result;
        for (size_t i = 0; i < NUM_REGS; i++)
            result.bitset[i] = this->bitset[i] & other.bitset[i];
        return result;
    }
    inline BitsetWrapper operator~() const {
        BitsetWrapper result;
        for (size_t i = 0; i < NUM_REGS; i++)
            result.bitset[i] = ~this->bitset[i];
        return result;
    }
    void setInputString(const std::string &binaryString) {
        for (size_t i = 0; i < binaryString.size() && i < N; ++i) {
            if (binaryString[i] == '1') {
                set(i, true);
            } else if (binaryString[i] == '0') {
                set(i, false);
            } else {
                throw std::invalid_argument("Input string contains invalid characters (not 0 or 1).");
            }
        }
    }

    void setInputInt64(const int64_t repeating_number) {
        for (size_t j = 0; j < NUM_REGS; j++) {
            set_fast_one_reg(j, 0, REGISTER_SIZE, repeating_number);
        }
    }

    [[nodiscard]] std::string getInputString(size_t firstInvalidIndex = size()) const {
        std::string s;
        for (size_t i = 0; i < firstInvalidIndex; i++) {
            s += (get(i) ? '1' : '0');
        }
        return s;
    }

    inline static size_t __attribute__((always_inline)) size() {
        return N;
    }

    [[nodiscard]] inline size_t __attribute__((always_inline)) get_leading_zeros(size_t reg_idx) const {
        return __builtin_clzll(bitset[reg_idx]);
    }

    // need to be tested
    [[nodiscard]] inline size_t __attribute__((always_inline)) get_second_leading_zeros(size_t reg_idx) const {
        int64_t nVal = ~(1ULL << (REGISTER_SIZE - __builtin_clzll(bitset[reg_idx]) - 1)) & bitset[reg_idx];
        if (nVal == 0)
            return REGISTER_SIZE;
        return __builtin_clzll(nVal);
    }

    inline size_t __attribute__((always_inline)) get_trailing_zeros(size_t reg_idx) {
        return __builtin_ctzll(bitset[reg_idx]);
    }

    inline bool __attribute__((always_inline)) get(const size_t index, const size_t from = 0) const {
        const size_t agg_index = index + from;
        if (__builtin_expect(agg_index >= N, 0))
            throw std::invalid_argument("index out of bound 1");
        const size_t idx = GET_INDEX(agg_index), offset = GET_OFFSET(agg_index);
        return static_cast<bool>(bitset[idx] & 1ULL << offset);
    }
    inline int get_first_one_before_slow(const size_t index, const size_t from = 0) const {
        const size_t agg_index = index + from;
        if (__builtin_expect(agg_index >= N, 0))
            throw std::out_of_range("index out of bound 2");

        // Start from the given index and move backwards
        for (int i = static_cast<int>(agg_index); i > 0; --i) {
            if (get(i - 1)) {
                return i - 1;  // Found the first '1' before the given index
            }
        }
        // no 1s before agg_index
        return -1;
    }

    inline size_t range(const size_t index1, const size_t index2) const {
        if (__builtin_expect((index2 < index1 || index2 > N), 0))
            throw std::invalid_argument("Invalid index range");
        size_t result = 0;
        // could be faster
        for (size_t i = index1; i < index2; i++) {
            result |= (get(i) << (index2 - i - 1));
        }
        return result;
    }

    inline size_t range_fast(const size_t index1, const size_t index2) const {
        if (__builtin_expect((index2 < index1 || index2 > N), 0))
            throw std::invalid_argument("Invalid index range");
        const size_t idx1 = GET_INDEX(index1);
        const size_t offset1 = GET_OFFSET(index1);
        const size_t idx2 = GET_INDEX(index2);
        const size_t offset2 = GET_OFFSET(index2);
        if (idx1 == idx2) {
            return ((bitset[idx1] >> offset1) & ((1ULL << (offset2 - offset1)) - 1));
        } else if (idx1 + 1 == idx2) {
            return ((bitset[idx1] >> offset1) & ((1ULL << (REGISTER_SIZE - offset1)) - 1)) + ((bitset[idx2] & ((1ULL << offset2) - 1)) << (REGISTER_SIZE - offset1));
        }
        throw std::invalid_argument("should not be here in range fast");
    }

    inline auto __attribute__((always_inline)) range_fast_one_reg(const size_t index, const size_t offset1, const size_t offset2) const {
        auto x = bitset[index] >> offset1;
        auto y = (1ULL << (offset2 - offset1)) - 1;
        auto z = (x & y);
        return z;
    }

    inline auto __attribute__((always_inline)) range_fast_2(const size_t index1, const size_t index2) const {
        const size_t idx1 = GET_INDEX(index1);
        const size_t offset1 = GET_OFFSET(index1);
        const size_t idx2 = GET_INDEX(index2);
        const size_t offset2 = GET_OFFSET(index2);
        if (idx1 == idx2) {
            auto x = bitset[idx1] >> offset1;
            auto y = (1ULL << (offset2 - offset1)) - 1;
            auto z = (x & y);
            return z;
        } else if (idx1 + 1 == idx2) {
            return ((bitset[idx1] >> offset1) & ((1ULL << (REGISTER_SIZE - offset1)) - 1)) + ((bitset[idx2] & ((1ULL << offset2) - 1)) << (REGISTER_SIZE - offset1));
        }
        throw std::invalid_argument("should not be here in range 2");
    }

    inline bool __attribute__((always_inline)) set(const size_t index, bool value) {
        if (__builtin_expect((index >= size()), 0))
            throw std::invalid_argument("index out of bound set 2");
        const size_t idx = GET_INDEX(index), offset = GET_OFFSET(index);
        if (idx * REGISTER_SIZE + offset >= size()) {
            return false;
        }
        if (value)
            bitset[idx] |= 1ULL << offset;
        else
            bitset[idx] &= ~(1ULL << offset);
        return true;
    }
    inline void __attribute__((always_inline)) set_fast_one_reg(const size_t idx, const size_t offset_start, const size_t offset_after_end, const size_t value) {
        // idx2 exclusive
        size_t range_length = offset_after_end - offset_start;
        size_t range_mask = ((1UL << range_length) - 1) << offset_start;

        range_mask |= -(range_length == REGISTER_SIZE);

        bitset[idx] &= ~range_mask;
        bitset[idx] |= (value << offset_start) & range_mask;
    }

    inline void __attribute__((always_inline)) set_fast_two_reg(const size_t idx1, const size_t idx2, const size_t value) {
        // idx2 exclusive
        const size_t reg_i1 = GET_INDEX(idx1), offset1 = GET_OFFSET(idx1);
        const size_t reg_i2 = GET_INDEX(idx2), offset2 = GET_OFFSET(idx2);

        if (reg_i1 == reg_i2) {
            set_fast_one_reg(reg_i1, offset1, offset2, value);
        } else {
            set_fast_one_reg(reg_i1, offset1, REGISTER_SIZE, value);                   // Set bits in the first register
            set_fast_one_reg(reg_i2, 0, offset2, value >> (REGISTER_SIZE - offset1));  // Set bits in the second register
        }
    }
    void printBitset() const {
        for (size_t i = 0; i < size(); ++i) {
            std::cout << get(i);
            if (i % REGISTER_SIZE == REGISTER_SIZE - 1)
                std::cout << ' ';
        }
        std::cout << std::endl;
    }

    void printRange(size_t i, size_t j) {
        for (; i < j; ++i) {
            std::cout << get(i);
        }
        std::cout << std::endl;
    }

    size_t rank_dumb(const size_t pos) {
        size_t count = 0;
        for (size_t i = 0; i < pos; ++i) {
            if (get(i)) ++count;
        }
        return count;
    }

    size_t select_dumb(const size_t nth) {
        if (nth == 0) throw std::out_of_range("nth must be greater than 0.");
        size_t count = 0;
        size_t i;
        for (i = 0; i < N; ++i) {
            if (get(i)) ++count;
            if (count == nth) return i;
        }
        return size();
    }

    inline size_t __attribute__((always_inline)) rank(const size_t pos) const {
        // exclusive the end
        const size_t index = GET_INDEX(pos), offset = GET_OFFSET(pos);
        size_t count = 0;
        for (size_t i = 0; i < index; ++i) {
            count += __builtin_popcountll(bitset[i]);
        }
        uint64_t mask = (static_cast<uint64_t>(1) << offset) - 1;
        count += __builtin_popcountll(bitset[index] & mask);
        return count;
    }

    inline size_t __attribute__((always_inline)) select(const size_t nth, const size_t start_from_reg = 0) const {
        size_t count = 0;
        size_t blockPopCount = 0;
        size_t i;
        uint64_t block;
        for (i = start_from_reg; i < NUM_REGS && count + blockPopCount < nth; ++i) {
            count += blockPopCount;
            block = bitset[i];
            blockPopCount = __builtin_popcountll(block);
        }
        const uint64_t nthBitMask = static_cast<uint64_t>(1) << (nth - count - 1);  // Create a mask for the nth bit
        const uint64_t depositMask = _pdep_u64(nthBitMask, block);
        const size_t bitPosition = _tzcnt_u64(depositMask);  // Count trailing zeros to find the position
        return ((i - start_from_reg - 1) * REGISTER_SIZE) + bitPosition;
    }

    // at most in two adjacent registers
    inline std::pair<size_t, size_t> select_two(const size_t fromth, const size_t toth, const size_t start_from_reg = 0) const {
        size_t count = 0;
        size_t i1 = start_from_reg;
        uint64_t block = bitset[i1];
        uint64_t block_pop_count = __builtin_popcountll(block);

        while (i1 < NUM_REGS) {
            if (count + block_pop_count < fromth) {
                count += block_pop_count;
                block = bitset[++i1];
                block_pop_count = __builtin_popcountll(block);
                continue;
            }
            break;
        }

        const uint64_t nth_fromth = static_cast<uint64_t>(1 + (1 << (toth - fromth))) << (fromth - count - 1);
        const uint64_t deposit_fromth = _pdep_u64(nth_fromth, block);
        const size_t bit_pos_fromth = _tzcnt_u64(deposit_fromth);
        size_t bit_pos_toth = _lzcnt_u64(deposit_fromth);
        size_t diff = REGISTER_SIZE - 1 - bit_pos_toth - bit_pos_fromth;
        size_t fromth_return = ((i1 - start_from_reg) << REGISTER_SIZE_BITS) + bit_pos_fromth;
        size_t toth_return = fromth_return + diff;

        if (diff == 0) {  // very rare case aprox = 15/200
            count += block_pop_count;
            block = bitset[++i1];
            block_pop_count = __builtin_popcountll(block);
            const uint64_t nth_toth = static_cast<uint64_t>(1) << (toth - count - 1);
            const uint64_t deposit_toth = _pdep_u64(nth_toth, block);
            bit_pos_toth = _tzcnt_u64(deposit_toth);
            toth_return = ((i1 - start_from_reg) << REGISTER_SIZE_BITS) + bit_pos_toth;
        }

        return std::make_pair(fromth_return, toth_return);
    }

    inline std::pair<size_t, size_t> select2(const size_t nth, const size_t start_from_reg = 0) {
        if (__builtin_expect(nth == 0, 0))
            throw std::out_of_range("nth must be greater than 0.");

        size_t count = 0;
        for (size_t i = start_from_reg; i < (N + REGISTER_SIZE - 1) >> REGISTER_SIZE_BITS; ++i) {
            const uint64_t block = bitset[i];
            const size_t blockPopCount = __builtin_popcountll(block);

            if (count + blockPopCount < nth) {
                count += blockPopCount;
                continue;
            }

            const uint64_t nthBitMask = static_cast<uint64_t>(3) << (nth - count - 1);  // Create a mask for the nth bit
            const uint64_t depositMask = _pdep_u64(nthBitMask, block);
            const size_t bitPosition = _tzcnt_u64(depositMask);       // Count trailing zeros to find the position
            const size_t bitPosition_left = _lzcnt_u64(depositMask);  // Count trailing zeros to find the position
            size_t diff = REGISTER_SIZE - 1 - bitPosition_left - bitPosition;
            return std::make_pair(((i - start_from_reg) << REGISTER_SIZE_BITS) + bitPosition,
                                  ((i - start_from_reg) << REGISTER_SIZE_BITS) + bitPosition + diff);
        }
        return std::make_pair(size(), 0);
    }
    inline void count_contiguous(size_t &index, int &count) const {
        // stop *after* the first 0
        while (!get(index++)) {
            count++;
        }
    }

    inline void count_contiguous_until_false(size_t &index, int &count) const {
        // stop *after* the first 0
        while (get(index++)) {
            count++;
        }
    }

    inline void deprecated_shift(int64_t steps, int64_t from, int64_t to = size()) {
        // to exclusive / from inclusive
    #ifdef DEBUG
        if (!(to >= from && to <= size())) {
            throw std::invalid_argument("Invalid to/from range");
        }
    #endif
        if (steps == 0) return;

        if (steps > 0) {
            // Right shift
            for (auto i = to - 1; i >= from + steps; --i) {
                set(i, get(i - steps));
            }
            // Clear the bits that have been shifted out
            for (auto i = from; i < from + std::min(steps, to - from); ++i) {
                set(i, false);
            }
        } else {
            // Left shift
            int64_t abs_steps = -steps;
            for (auto i = from; i < to - abs_steps; ++i) {
                set(i, get(i + abs_steps));
            }
            for (auto i = to - std::min(abs_steps, to - from); i < to; ++i) {
                set(i, false);
            }
        }
    }
// some ideas
// remove steps sign so remove one branch
    inline void shift_smart(int steps, size_t from, size_t to = size()) {
        const size_t bitShift = std::abs(steps);
    #ifdef DEBUG
        if (!(to >= from && to <= size())) {
            throw std::invalid_argument("Invalid to/from range");
        }
        if (bitShift >= REGISTER_SIZE) {
            throw std::invalid_argument("Shift exceeds register size");
        }
    #endif
    
        if (steps == 0 || to == from) return;
    
        const uint64_t startIdx = GET_INDEX(from);
        const uint64_t endIdx = GET_INDEX(to - 1);
        const uint64_t startOffset = GET_OFFSET(from);
        const uint64_t endOffset = GET_OFFSET(to - 1);
        const uint64_t oneOnMSBEnd = GET_ONE_MSB(endOffset + 1);
        const uint64_t zeroOnMSBStart = GET_ZERO_MSB(startOffset);
        const uint64_t to2end = bitset[endIdx] & oneOnMSBEnd;
        const uint64_t start2from = bitset[startIdx] & zeroOnMSBStart;
    
        if (steps > 0) {
            // Mask and compute spill over for startIdx
            const uint64_t startMask = ~zeroOnMSBStart;
            uint64_t maskedStart = bitset[startIdx] & startMask;
    
            // Shift elements from end to start
            for (int64_t i = endIdx; i > static_cast<int64_t>(startIdx); --i) {
                uint64_t prev = (i == startIdx + 1) ? maskedStart : bitset[i - 1];
                bitset[i] = (bitset[i] << bitShift) | (prev >> (REGISTER_SIZE - bitShift));
            }
    
            // Apply shifts to startIdx and restore preserved bits
            bitset[startIdx] = (maskedStart << bitShift) | start2from;
            bitset[endIdx] = (bitset[endIdx] & ~oneOnMSBEnd) | to2end;
        } else {
            // Mask and compute spill over for endIdx
            const uint64_t endMask = ~oneOnMSBEnd;
            uint64_t maskedEnd = bitset[endIdx] & endMask;
    
            // Shift elements from start to end
            for (int64_t i = startIdx; i < static_cast<int64_t>(endIdx); ++i) {
                uint64_t next = (i == endIdx - 1) ? maskedEnd : bitset[i + 1];
                bitset[i] = (bitset[i] >> bitShift) | (next << (REGISTER_SIZE - bitShift));
            }
    
            // Apply shifts to endIdx and restore preserved bits
            bitset[endIdx] = (maskedEnd >> bitShift) | to2end;
            bitset[startIdx] = (bitset[startIdx] & ~zeroOnMSBStart) | start2from;
        }
    }

    BitsetWrapper<N> replicateTrieStore() const {
        BitsetWrapper<N> copy;
        std::memcpy(copy.bitset, this->bitset, sizeof(this->bitset));
        return copy;
    }
};
