#ifndef HASHTABLE_H
#define HASHTABLE_H

#include <cstdint>
#include <iostream>
#include <array>

const size_t size_h1 = 512;

extern size_t arr_h1;
extern std::int16_t signatures_h1[];
extern std::int16_t important_bits_h1[];
extern std::int32_t indices_h1[];

struct HashEntry {
    std::uint16_t key = 0;
    std::uint16_t value = 0;
    std::uint32_t indices = 0;  // can support up to 4 entries at each slot
};

template <std::int32_t TableSize, std::int32_t SHIFT1, std::int32_t SHIFT2, std::int32_t SHIFT3>
class HashTable {
public:
    // Using std::array for safety
    std::array<HashEntry, TableSize> table{};

    // Hash function based solely on bit-shift mixing
    inline static std::uint32_t hash_function(std::uint16_t h) {
        // Bitwise mixing (1, 11, 1 shifts)
        h ^= (h << SHIFT1) & 0xFFFF;
        h ^= (h >> SHIFT2) & 0xFFFF;
        h ^= (h << SHIFT3) & 0xFFFF;

        // Map to table range
        return h & (TableSize - 1);
    }

    // Constructor: initialize table entries to zeros
    HashTable() {
        for (auto& entry : table) {
            entry = {0, 0, 0};
        }
    }

    // Insert an entry (key, val, indices) into the table
    void insert(std::int16_t key, std::int16_t val, std::int32_t indices) {
        auto index = hash_function(static_cast<std::uint16_t>(key));
        table[index].key = static_cast<std::uint16_t>(key);
        table[index].value = static_cast<std::uint16_t>(val);
        table[index].indices = static_cast<std::uint32_t>(indices);
    }

    // Retrieve value by key (not optimized)
    [[nodiscard]] std::int16_t get_value(std::int16_t key) const {
        auto index = hash_function(static_cast<std::uint16_t>(key));
        if (table[index].key == static_cast<std::uint16_t>(key)) {
            return table[index].value;
        }
        return -1;  // If not found
    }

    // Print non-empty entries for debugging
    void print() const {
        for (size_t i = 0; i < TableSize; ++i) {
            const auto& entry = table[i];
            if (entry.key != 0) {
                std::cout << "Index: " << i
                          << ", Key: " << entry.key
                          << ", Value: " << entry.value
                          << ", Indices: " << entry.indices
                          << '\n';
            }
        }
    }
};

// Helper function to populate the hash table from arrays of keys, values, indices
template <std::int32_t TableSize, std::int32_t SHIFT1, std::int32_t SHIFT2, std::int32_t SHIFT3>
void generate_hashtable(
        HashTable<TableSize, SHIFT1, SHIFT2, SHIFT3>& ht,
        const std::int16_t keys[],
        const std::int16_t values[],
        const std::int32_t indices_list[],
        size_t size)
{
    for (size_t i = 0; i < size; ++i) {
        ht.insert(keys[i], values[i], indices_list[i]);
    }
}

// Declare an external instance of the hash table with (size=512, shifts=1,11,1)
extern HashTable<size_h1, 1, 11, 1> ht1;

#endif
