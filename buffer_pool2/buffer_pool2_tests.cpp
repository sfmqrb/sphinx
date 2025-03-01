#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <thread>
#include <cmath>
#include <random>
#include <vector>
#include "../config/config.h"
#include "buffer_pool2.h"

typedef typename DefaultTraits::PAYLOAD_TYPE PAYLOAD_TYPE;
typedef typename DefaultTraits::KEY_TYPE KEY_TYPE;
typedef typename DefaultTraits::VALUE_TYPE VALUE_TYPE;
typedef typename DefaultTraits::ENTRY_TYPE ENTRY_TYPE;


void run_mt_write_read_test(size_t capacity, size_t num_threads, size_t entries_per_thread) {
    LinearProbingHashTable<TestBP2Traits> bufferPool(capacity);
    std::atomic<size_t> failed_cnt = 0;

    // Pre-generate random numbers in the main thread
    size_t total_entries = num_threads * entries_per_thread;
    std::vector<uint64_t> keys(total_entries);
    std::random_device rd;
    std::mt19937_64 rng(std::chrono::system_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<uint64_t> dist(0, std::numeric_limits<uint64_t>::max());

    for (size_t i = 0; i < total_entries; ++i) {
        keys[i] = dist(rng);
    }

    auto insert_entries = [&](size_t thread_id) {
        size_t start_idx = thread_id * entries_per_thread;
        size_t end_idx = start_idx + entries_per_thread;

        for (size_t i = start_idx; i < end_idx; ++i) {
            auto key = BitsetWrapper<FINGERPRINT_SIZE>({keys[i], keys[i]}, true);
            auto result = bufferPool.put(key, static_cast<VALUE_TYPE>(keys[i]), keys[i]);
            if (!result) {
                failed_cnt += 1;
            }
        }
    };

    auto read_entries = [&](size_t thread_id) {
        size_t start_idx = thread_id * entries_per_thread;
        size_t end_idx = start_idx + entries_per_thread;

        for (size_t i = start_idx; i < end_idx; ++i) {
            auto key = BitsetWrapper<FINGERPRINT_SIZE>({keys[i], keys[i]}, true);
            auto result = bufferPool.get(key);
            if (result.has_value() && result.value().second != static_cast<VALUE_TYPE>(keys[i])) {
                std::cerr << "Mismatch: key = " << keys[i] << ", value = " << result.value().second << std::endl;
            }
        }
    };

    // Multithreaded Insert
    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back(insert_entries, t);
    }
    for (auto& thread : threads) {
        thread.join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto insert_per_entry = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / total_entries;

    std::cout << "%failed writes: " << (float)failed_cnt * 100 / total_entries << std::endl;
    std::cout << "insert per ent: " << insert_per_entry << " ns" << std::endl;

    // Multithreaded Read
    threads.clear();
    start = std::chrono::high_resolution_clock::now();
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back(read_entries, t);
    }
    for (auto& thread : threads) {
        thread.join();
    }
    end = std::chrono::high_resolution_clock::now();
    auto read_per_entry = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / total_entries;

    std::cout << "read per ent: " << read_per_entry << " ns" << std::endl;
}
TEST_CASE("Buffer Pool: Multithreaded Write and Read") {
    size_t capacity = (1 << 10) + 20;
    size_t num_threads = 64;
    size_t entries_per_thread = 500;
    run_mt_write_read_test(capacity, num_threads, entries_per_thread);
    capacity = (1 << 10) + 20;
    num_threads = 16;
    entries_per_thread = 8000;
    run_mt_write_read_test(capacity, num_threads, entries_per_thread);
    capacity = (1 << 10) + 20;
    num_threads = 1;
    entries_per_thread = 8000 * 16;
    run_mt_write_read_test(capacity, num_threads, entries_per_thread);
}

void run_multithreaded_test(size_t capacity, size_t num_threads, size_t entries_per_thread) {

    LinearProbingHashTable<TestBP2Traits> bufferPool(capacity);
    std::atomic<size_t> failed_cnt = 0;

    auto insert_entries = [&](size_t thread_id) {
        std::random_device rd;
        std::mt19937_64 rng(std::chrono::system_clock::now().time_since_epoch().count());
        std::uniform_int_distribution<uint64_t> dist(0, std::numeric_limits<uint64_t>::max());
        for (size_t i = 0; i < entries_per_thread; ++i) {
            auto x  = dist(rng);
            auto key = BitsetWrapper<FINGERPRINT_SIZE>({x, x}, true);
            auto result = bufferPool.put(key, static_cast<VALUE_TYPE>(x), 1);
            if (!result) {
                failed_cnt += 1;
            }
        }
    };

    // Multithreaded Insert
    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back(insert_entries, t);
    }
    for (auto& thread : threads) {
        thread.join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto insert_per_entry = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() / (num_threads * entries_per_thread);


//    bufferPool.printTable();
    std::cout << "%failed writes: " << (float)failed_cnt * (float)100 / (float) (num_threads * entries_per_thread) << std::endl;
    std::cout << "insert per ent: " << insert_per_entry << " ns" << std::endl;
    REQUIRE(bufferPool.getOccupied() == bufferPool.size_);
}

TEST_CASE("Buffer Pool: Multithreaded Concurrency Test Easier") {
    size_t capacity = (1 << 10) + 20;
    size_t num_threads = 16;
    size_t entries_per_thread = 8000;
    run_multithreaded_test(capacity, num_threads, entries_per_thread);
    capacity = (1 << 10) + 20;
    num_threads = 1;
    entries_per_thread = 8000 * 16;
    run_multithreaded_test(capacity, num_threads, entries_per_thread);
}
void run_latency_benchmark(size_t capacity) {
    LinearProbingHashTable<TestBP2Traits> bufferPool(capacity);

    size_t total_insert_latency_ns = 0;
    size_t total_lookup_latency_ns = 0;

    // Seed for random number generator
    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::uniform_int_distribution<uint64_t> dist(0, std::numeric_limits<uint64_t>::max());

    std::vector<BitsetWrapper<FINGERPRINT_SIZE>> keys;

    // Insert Benchmark
    std::cout << "Starting insert benchmark..." << std::endl;
    auto num_ins = capacity * 10;
    for (size_t i = 0; i < num_ins  ; ++i) {
        auto key = BitsetWrapper<FINGERPRINT_SIZE>({dist(rng), dist(rng)}, true);
        keys.push_back(key);

        auto start = std::chrono::high_resolution_clock::now();
        bufferPool.put(key, static_cast<VALUE_TYPE>(i * 10), 1);
        auto end = std::chrono::high_resolution_clock::now();

        total_insert_latency_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    double avg_insert_latency = static_cast<double>(total_insert_latency_ns) / num_ins;
    std::cout << "Average insert latency: " << avg_insert_latency << " ns" << std::endl;
//    bufferPool.printTable();

//    for (int i = 0; i < 1990; i++) {
//        bufferPool.evict();
//        std::cout << bufferPool.size_ << "  " << bufferPool.clock_hand_ << std::endl;
//    }
    // Lookup Benchmark
    std::cout << "Starting lookup benchmark..." << std::endl;
    auto num_read = capacity;
    for (size_t i = 0; i < num_read ; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        auto value = bufferPool.get(keys[i]);
        auto end = std::chrono::high_resolution_clock::now();

        // assert(value.has_value() && value.value() == static_cast<VALUE_TYPE>(i * 10));
        total_lookup_latency_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    double avg_lookup_latency = static_cast<double>(total_lookup_latency_ns) / num_read;
    std::cout << "Average lookup latency: " << avg_lookup_latency << " ns" << std::endl;
    std::cout << bufferPool.size_  << std::endl;
}

TEST_CASE("Buffer Pool: Single-Threaded Latency Test") {
    const size_t capacity = (1 << 15) + 10;
    run_latency_benchmark(capacity);
}

TEST_CASE("LinearProbingHashTable Basic Operations", "[LinearProbingHashTable]") {

    SECTION("Insert and Find Operations") {
        LinearProbingHashTable<TestBP2Traits> bufferPool(10);

        auto key1 = BitsetWrapper<FINGERPRINT_SIZE>({1ull << 63, 0}, true);
        auto key2 = BitsetWrapper<FINGERPRINT_SIZE>({1, 0}, true);
        auto key3 = BitsetWrapper<FINGERPRINT_SIZE>({2, 0}, true);

        bufferPool.put(key1, 100, 1);
        bufferPool.put(key2, 200, 2);
        bufferPool.put(key3, 300, 3);

        REQUIRE(bufferPool.get(key1).value().second == 100);
        REQUIRE(bufferPool.get(key2).value().second == 200);
        REQUIRE(bufferPool.get(key3).value().second == 300);

        bufferPool.printTable();
    }

    SECTION("test2") {
        LinearProbingHashTable<Test2BP2Traits> hashTable(10); // Small capacity to test eviction quickly

        auto fp1 = BitsetWrapper<FINGERPRINT_SIZE>({1, 0}, true);
        auto fp2 = BitsetWrapper<FINGERPRINT_SIZE>({2, 0}, true);
        auto fp3 = BitsetWrapper<FINGERPRINT_SIZE>({3, 0}, true);
        auto fp4 = BitsetWrapper<FINGERPRINT_SIZE>({4, 0}, true);
        // Insert entries

        // Insert entries and access some to set their reference bits
        hashTable.put(fp1, 100, 1); // Reference bit set to true
        hashTable.put(fp2, 200, 2); // Reference bit set to true
        hashTable.put(fp3, 300, 3); // Reference bit set to true


        // Access fp1 and fp2 to keep their reference bits set
        auto val1 = hashTable.get(fp1);
        auto val2 = hashTable.get(fp2);
        assert(val1.value().second == 100);
        assert(val2.value().second == 200);

        // The load factor is now 0.75, so the next insert will trigger eviction
        hashTable.put(fp4, 400, 4); // This should evict fp3 (since its reference bit is false)

        // Verify that fp3 has been evicted
        auto val = hashTable.get(fp1);
        assert(!val.has_value()); // fp3 should have been evicted

        // Verify that fp1, fp2, and fp4 are present
        val2 = hashTable.get(fp2);
        auto val3 = hashTable.get(fp3);
        auto val4 = hashTable.get(fp4);
        assert(val2.value().second == 200);
        assert(val3.value().second == 300);
        assert(val4.value().second == 400);
    }

    SECTION("harder") {
        const size_t capacity = 32800;
        const size_t num_entries = 16000;

        LinearProbingHashTable<TestBP2Traits> bufferPool(capacity);

        // Seed for random number generator
        std::random_device rd; // Seed from hardware (non-deterministic)
        std::mt19937_64 rng(rd()); // Random number generator engine (64-bit Mersenne Twister)

        // Define the distribution for uint64_t range
        std::uniform_int_distribution<uint64_t> dist(0, std::numeric_limits<uint64_t>::max());
        // Store fingerprints and their corresponding values
        std::vector<BitsetWrapper<FINGERPRINT_SIZE>> fingerprints;
        for (size_t i = 0; i < num_entries; ++i) {
            uint64_t random_fp = dist(rng);
            auto fp = BitsetWrapper<FINGERPRINT_SIZE>({random_fp, random_fp}, true);
            fingerprints.push_back(fp);
            bufferPool.put(fp, i, 1); // Store value `i` for each fingerprint
        }

//        bufferPool.printTable();
        // Verify that all entries are correctly stored and retrievable
        for (size_t i = 0; i < num_entries; ++i) {
            auto value = bufferPool.get(fingerprints[i]);
            assert(value.has_value() && value.value().second == i);
        }

        std::cout << "All " << num_entries << " entries were successfully inserted and retrieved.\n";
    }
}
TEST_CASE("LinearProbingHashTable Invalidate Operation", "[LinearProbingHashTable]") {
    SECTION("Invalidate and Verify Behavior") {
        LinearProbingHashTable<TestBP2Traits> hashTable(10);

        auto key1 = BitsetWrapper<FINGERPRINT_SIZE>({1ull << 63, 1}, true);
        auto key2 = BitsetWrapper<FINGERPRINT_SIZE>({2, 2}, true);
        auto key3 = BitsetWrapper<FINGERPRINT_SIZE>({3, 3}, true);

        // Insert entries into the hash table
        REQUIRE(hashTable.put(key1, 100, 1));
        REQUIRE(hashTable.put(key2, 200, 2));
        REQUIRE(hashTable.put(key3, 300, 3));

        // Verify entries can be retrieved
        REQUIRE(hashTable.get(key1).value().second == 100);
        REQUIRE(hashTable.get(key2).value().second == 200);
        REQUIRE(hashTable.get(key3).value().second == 300);

        // Invalidate an entry
        REQUIRE(hashTable.invalidate(key2)); // Invalidate key2
        REQUIRE(!hashTable.invalidate(BitsetWrapper<FINGERPRINT_SIZE>({0, 5}, true))); // Try to invalidate a non-existent key

        // Verify invalidated entry cannot be retrieved
        REQUIRE(!hashTable.get(key2).has_value());

        // Verify other entries are unaffected
        REQUIRE(hashTable.get(key1).value().second == 100);
        REQUIRE(hashTable.get(key3).value().second == 300);

        // Verify invalidation does not affect the hash table's size
        REQUIRE(hashTable.getOccupied() == 3); // All entries are still technically occupied
    }
}

TEST_CASE("LinearProbingHashTable Operation With Actual Payload as the type", "[LinearProbingHashTable]") {
    LinearProbingHashTable<TestBP2Fleck> hashTable(10);
    SECTION("Invalidate and Verify Behavior") {
        PAYLOAD_TYPE key1 = 0;
        PAYLOAD_TYPE key2 = 1;
        PAYLOAD_TYPE key3 = 2;


        auto fp1 = BitsetWrapper<FINGERPRINT_SIZE>({1ull << 63, 1}, true);
        auto fp2 = BitsetWrapper<FINGERPRINT_SIZE>({2, 2}, true);
        auto fp3 = BitsetWrapper<FINGERPRINT_SIZE>({3, 3}, true);

        PAYLOAD_TYPE non_existent_key = 3;
        // Insert entries into the hash table
        REQUIRE(hashTable.put(key1, 100, 1));
        REQUIRE(hashTable.put(key2, 200, 2));
        REQUIRE(hashTable.put(key3, 300, 3));

        // Verify entries can be retrieved
        REQUIRE(hashTable.get(key1).value().second == 100);
        REQUIRE(hashTable.get(key2).value().second == 200);
        REQUIRE(hashTable.get(key3).value().second == 300);

        // Invalidate an entry
        REQUIRE(hashTable.invalidate(key2)); // Invalidate key2
        REQUIRE(!hashTable.invalidate(non_existent_key)); // Try to invalidate a non-existent key

        // Verify invalidated entry cannot be retrieved
        REQUIRE(!hashTable.get(key2).has_value());

        // Verify other entries are unaffected
        REQUIRE(hashTable.get(key1).value().second == 100);
        REQUIRE(hashTable.get(key3).value().second == 300);

        // Verify invalidation does not affect the hash table's size
        REQUIRE(hashTable.getOccupied() == 3); // All entries are still technically occupied
    }


    SECTION("Eviction under Load") {
        auto dummy_fp = 1;
        REQUIRE(hashTable.put(0, 0, dummy_fp));
        REQUIRE(hashTable.put(1, 10, dummy_fp));
        REQUIRE(hashTable.put(2, 20, dummy_fp));
        REQUIRE(hashTable.put(3, 30, dummy_fp));
        REQUIRE(hashTable.put(4, 40, dummy_fp)); // Should fill the table
        REQUIRE(hashTable.put(5, 50, dummy_fp)); // Eviction required, but ensure behavior

        // Verify the entries are present or properly evicted
        REQUIRE(hashTable.get(1).has_value());
        REQUIRE_FALSE(hashTable.get(2).has_value());
        REQUIRE(hashTable.get(3).has_value());
        REQUIRE(hashTable.get(4).has_value());
        REQUIRE(hashTable.get(5).has_value());
    }

    SECTION("Collision Resolution") {
        auto dummy_fp = 1;
        // Insert keys that hash to the same index
        REQUIRE(hashTable.put(0, 100, dummy_fp));
        REQUIRE(hashTable.put(4, 200, dummy_fp));
        REQUIRE(hashTable.put(8, 300, dummy_fp)); // All keys map to the same hash bucket

        // Verify collision resolution
        REQUIRE(hashTable.get(0).value().second == 100);
        REQUIRE(hashTable.get(4).value().second == 200);
        REQUIRE(hashTable.get(8).value().second == 300);
    }


    SECTION("Invalidate Multiple Entries") {
        auto dummy_fp = 1;
        REQUIRE(hashTable.put(1, 10, dummy_fp));
        REQUIRE(hashTable.put(2, 20, dummy_fp));
        REQUIRE(hashTable.put(3, 30, dummy_fp));

        REQUIRE(hashTable.invalidate(1));
        REQUIRE(hashTable.invalidate(2));

        // Verify invalidated keys
        REQUIRE_FALSE(hashTable.get(1).has_value());
        REQUIRE_FALSE(hashTable.get(2).has_value());

        // Verify unaffected keys
        REQUIRE(hashTable.get(3).value().second == 30);
    }
}