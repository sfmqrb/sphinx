#include <x86intrin.h>  // For _pdep_u64 and _tzcnt_u64

#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <random>

#include "hashtable.h"

TEST_CASE("HashTable: Access Speed", "[performance]") {
    generate_hashtable(ht1, signatures_h1, important_bits_h1, indices_h1, arr_h1);
    ht1.print();
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(1, 4900);
    const int KK = 10000;

    // Pre-generate random numbers
    std::vector<int> random_list(KK);
    for (int i = 0; i < KK; ++i) {
        random_list[i] = distrib(gen);
    }

    SECTION("Measure Access Time") {
        int sum = 0;
        int i;
        // Access hash table using pre-generated random indices
        auto start = std::chrono::high_resolution_clock::now();
        for (i = 0; i < KK; ++i) {
            int r_num = random_list[i];
            sum += ht1.get_value(r_num);
        }

        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        std::cout << "Total time for " << KK << " accesses: " << duration.count() << " nanoseconds" << std::endl;
        std::cout << "Average time per access: " << duration.count() / KK << " nanoseconds" << std::endl;
        std::cout << sum;
    }

    SECTION("Is it sound?") {
        CHECK(ht1.get_value(173) == 11);  // Measure the time it takes to get a key
        for (int i = 0; i < KK; ++i) {
            int r_num = distrib(gen) % arr_h1;
            CHECK(ht1.get_value(signatures_h1[r_num]) == important_bits_h1[r_num]);  // Measure the time it takes to get a key
        }
    }
}
