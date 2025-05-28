#pragma once
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>
#include <cassert>

#include "../lib/ConQueue/concurrentqueue.h"
#include "../segment/segment.h"

using ELEMENT = std::function<void()>;
#ifdef ENABLE_XDP
// Forward‐declare the template so we can hold pointers to it
template <
  typename TraitsGI, 
  typename TraitsLI, 
  typename TraitsLIBuffer
>
class XDP;
#endif

template<typename Traits = DefaultTraits>
class Directory {
   private:
    // alias types from Traits
    using PAYLOAD_TYPE = typename Traits::PAYLOAD_TYPE;
    using KEY_TYPE     = typename Traits::KEY_TYPE;
    using ENTRY_TYPE   = typename Traits::ENTRY_TYPE;
    using VALUE_TYPE   = typename Traits::VALUE_TYPE;

    struct SegmentData {
        std::shared_ptr<Segment<Traits>> segment;
    };

    std::mutex g_segDataVecMutex;
    std::atomic<size_t> max_FP_index;

    // --- NEW MULTI-THREADING MEMBERS ---
    size_t maxThreads;
    std::vector<std::thread> threadPool;
    // Instead of one global queue, we have one queue per thread.
    std::vector<moodycamel::ConcurrentQueue<ELEMENT>> taskQueues;
    std::atomic<bool> stop{false};

    // --- WORKER FUNCTION ---
    // Each worker thread will check only its own queue.
    void worker(int threadId) {
        while (!stop || taskQueues[threadId].size_approx() > 0) {
            ELEMENT func;
            if (taskQueues[threadId].try_dequeue(func)) {
                func();
            } else {
                // Avoid busy spin
                std::this_thread::yield();
            }
        }
    }

    // --- EXISTING FUNCTIONS (unchanged) ---
    std::optional<ENTRY_TYPE> performReadTask(const KEY_TYPE key, const SSDLog<Traits>& ssdLog) const {
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        const auto segmentIndex = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        const auto segment_addr = (*segDataVec)[segmentIndex].segment;
#ifdef ENABLE_MT
        std::lock_guard<std::mutex> lock(segment_addr->segmentMutex);
#endif
        auto res = segment_addr->read(fingerprint, ssdLog);
        return res;
    }

    bool performWriteTask(const BitsetWrapper<FINGERPRINT_SIZE> fingerprint, SSDLog<Traits>& ssdLog,
                          const PAYLOAD_TYPE payload, bool guarantee_update=false) {
    PERFORM_WRITE_TASK_START:
        const auto segment_index = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        auto fp = fingerprint;
        const auto shared_ptr_seg = (*segDataVec)[segment_index].segment;
#ifdef ENABLE_MT
        std::unique_lock<std::mutex> lock(shared_ptr_seg->segmentMutex);
        const auto segment_index_double_check = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        const auto addr_segment_after_lock = (*segDataVec)[segment_index_double_check].segment.get();
        if (addr_segment_after_lock != shared_ptr_seg.get()) {
            lock.unlock();
            goto PERFORM_WRITE_TASK_START;
        }
#endif
        const auto res = shared_ptr_seg->write(fp, ssdLog, payload, guarantee_update);
        if (res) {
            return res;
        }
        // expansion logic
        {
            if constexpr (Traits::DHT_EVERYTHING) {
                return false;
            }
            if constexpr (!Traits::EXPAND) {
                std::cerr << "Expansion is disabled" << std::endl;
                return false;
            }
            auto [seg1ptr, seg2ptr] = shared_ptr_seg->expand(ssdLog);
            const size_t old_seg_bits = shared_ptr_seg->FP_index - 2 * COUNT_SLOT_BITS;
            const auto step = 1 << old_seg_bits;
            const auto real_segment_index = fingerprint.range_fast_one_reg(0, 0, shared_ptr_seg->FP_index - 2 * COUNT_SLOT_BITS);
            auto seg1idx = real_segment_index;
            auto seg2idx = seg1idx + step;
            if (max_FP_index == shared_ptr_seg->FP_index) {
#ifdef ENABLE_MT
                std::lock_guard<std::mutex> lock_seg_vec(g_segDataVecMutex);
#endif
                doubleSegmentDataVec();
            }
            const size_t diff = max_FP_index - shared_ptr_seg->FP_index;
            for (auto i = 0; i < (1 << (diff - 1)); i++) {
                {
                    if ((*segDataVec)[seg1idx].segment == shared_ptr_seg) {
                        (*segDataVec)[seg1idx].segment = seg1ptr;
                    } else {
                        // std::lock_guard<std::mutex> lockseg1(shared_ptr_seg->segmentMutex);
                        (*segDataVec)[seg1idx].segment = seg1ptr;
                    }
                }
                {
                    if ((*segDataVec)[seg2idx].segment == shared_ptr_seg) {
                        (*segDataVec)[seg2idx].segment = seg2ptr;
                    } else {
                        // std::lock_guard<std::mutex> lockseg2(shared_ptr_seg->segmentMutex);
                        (*segDataVec)[seg2idx].segment = seg2ptr;
                    }
                }

                seg1idx += 2 * step;
                seg2idx += 2 * step;
            }
        }
        // does not need this because expansion makes the segment invalid
        // lock.unlock();
        goto PERFORM_WRITE_TASK_START;
    }

#ifdef ENABLE_XDP
    bool performWriteTaskGI(const BitsetWrapper<FINGERPRINT_SIZE> fingerprint,
                          const PAYLOAD_TYPE payload, XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp, bool guarantee_update=false) {
    PERFORM_WRITE_TASK_START:
        const auto segment_index = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        auto fp = fingerprint;
        const auto shared_ptr_seg = (*segDataVec)[segment_index].segment;
#ifdef ENABLE_MT
        std::unique_lock<std::mutex> lock(shared_ptr_seg->segmentMutex);
        const auto segment_index_double_check = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        const auto addr_segment_after_lock = (*segDataVec)[segment_index_double_check].segment.get();
        if (addr_segment_after_lock != shared_ptr_seg.get()) {
            lock.unlock();
            goto PERFORM_WRITE_TASK_START;
        }
#endif
        const auto res = shared_ptr_seg->writeGI(fp, payload, xdp, guarantee_update);
        if (res) {
            return res;
        }
        // expansion logic
        {
            if constexpr (Traits::DHT_EVERYTHING) {
                return false;
            }
            if constexpr (!Traits::EXPAND) {
                std::cerr << "Expansion is disabled" << std::endl;
                return false;
            }
            size_t seg_cnt_log = segmentCountLog;
            BitsetWrapper<FINGERPRINT_SIZE> fp2;
            const size_t old_seg_bits = shared_ptr_seg->FP_index - 2 * COUNT_SLOT_BITS;
            const auto step = 1 << old_seg_bits;
            const auto real_segment_index = fingerprint.range_fast_one_reg(0, 0, shared_ptr_seg->FP_index - 2 * COUNT_SLOT_BITS);

            fp2.set_fast_one_reg(0, 0, shared_ptr_seg->FP_index - 2 * COUNT_SLOT_BITS, real_segment_index);
            auto [seg1ptr, seg2ptr] = shared_ptr_seg->expand_gi(xdp, fp2, seg_cnt_log);

            auto seg1idx = real_segment_index;
            auto seg2idx = seg1idx + step;
            if (max_FP_index == shared_ptr_seg->FP_index) {
#ifdef ENABLE_MT
                std::lock_guard<std::mutex> lock_seg_vec(g_segDataVecMutex);
#endif
                doubleSegmentDataVec();
            }
            const size_t diff = max_FP_index - shared_ptr_seg->FP_index;
            for (auto i = 0; i < (1 << (diff - 1)); i++) {
                {
                    if ((*segDataVec)[seg1idx].segment == shared_ptr_seg) {
                        (*segDataVec)[seg1idx].segment = seg1ptr;
                    } else {
                        // std::lock_guard<std::mutex> lockseg1(shared_ptr_seg->segmentMutex);
                        (*segDataVec)[seg1idx].segment = seg1ptr;
                    }
                }
                {
                    if ((*segDataVec)[seg2idx].segment == shared_ptr_seg) {
                        (*segDataVec)[seg2idx].segment = seg2ptr;
                    } else {
                        // std::lock_guard<std::mutex> lockseg2(shared_ptr_seg->segmentMutex);
                        (*segDataVec)[seg2idx].segment = seg2ptr;
                    }
                }

                seg1idx += 2 * step;
                seg2idx += 2 * step;
            }
        }
        // does not need this because expansion makes the segment invalid
        // lock.unlock();
        goto PERFORM_WRITE_TASK_START;
    }
#endif
    bool performRemoveTask(const BitsetWrapper<FINGERPRINT_SIZE> fingerprint, SSDLog<Traits>& ssdLog) {
    PERFORM_REMOVE_TASK_START:
        const auto segment_index = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        auto fp = fingerprint;
        const auto shared_ptr_seg = (*segDataVec)[segment_index].segment;
        std::unique_lock<std::mutex> lock(shared_ptr_seg->segmentMutex);
        const auto segment_index_double_check = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        const auto addr_segment_after_lock = (*segDataVec)[segment_index_double_check].segment.get();
        if (addr_segment_after_lock != shared_ptr_seg.get()) {
            lock.unlock();
            goto PERFORM_REMOVE_TASK_START;
        }
        const auto res = shared_ptr_seg->remove(fp, ssdLog);
        if (res) {
            return res;
        }
        throw std::invalid_argument("should not throw error in removal");
    }

   public:
    std::unique_ptr<std::vector<SegmentData>> segDataVec;
    std::atomic<size_t> segmentCountLog;

    auto getSegmentPtr(const size_t index) {
        return (*segDataVec)[index].segment;
    }

#ifdef ENABLE_XDP
    std::optional<ENTRY_TYPE> performReadTaskFP(BitsetWrapper<FINGERPRINT_SIZE> fingerprint,
                                                const SSDLog<Traits>& ssdLog) const {
        const auto segmentIndex = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        const auto segment_addr = (*segDataVec)[segmentIndex].segment;
#ifdef ENABLE_MT
        std::lock_guard<std::mutex> lock(segment_addr->segmentMutex);
#endif
        auto res = segment_addr->read(fingerprint, ssdLog);

        return res;
    }
#endif
    void doubleSegmentDataVec() {
        const size_t currentSize = (1 << segmentCountLog);
        auto newBuffer = std::make_unique<std::vector<SegmentData>>(*segDataVec);
        newBuffer->resize(currentSize * 2);
        segDataVec = std::move(newBuffer);
        for (size_t i = 0; i < currentSize; ++i) {
            (*segDataVec)[currentSize + i].segment = (*segDataVec)[i].segment;
        }
        ++max_FP_index;
        ++segmentCountLog;
    }

    // --- CONSTRUCTOR ---
    // Initialize segDataVec and also create a vector of task queues (one per thread)
    Directory(const size_t segmentCountLog_, const size_t maxThreads_)
        : maxThreads(maxThreads_) {
        segmentCountLog = segmentCountLog_;
        segDataVec = std::make_unique<std::vector<SegmentData>>(1 << segmentCountLog);
        max_FP_index = segmentCountLog_ + 2 * COUNT_SLOT_BITS;
        for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
            (*segDataVec)[i].segment = std::make_shared<Segment<Traits>>(max_FP_index);
        }
        // Create one task queue per thread.
        taskQueues.resize(maxThreads);
        for (size_t i = 0; i < maxThreads; ++i) {
            threadPool.emplace_back(&Directory::worker, this, static_cast<int>(i));
        }
        generate_hashtable(ht1, signatures_h1, important_bits_h1, indices_h1, arr_h1);
    }

    ~Directory() {
        stop = true;
        for (auto& thread : threadPool) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    bool isActive() {
        for (size_t i = 0; i < maxThreads; ++i) {
            if (taskQueues[i].size_approx() > 0)
                return true;
        }
        return false;
    }

    // --- NEW accessSegment ---
    // Now the main thread chooses the target thread's queue (by threadId)
    template <typename Func, typename... Args>
    auto accessSegment(int threadId, Func&& func, Args&&... args)
        -> std::future<typename std::invoke_result_t<Func, Args...>> {
        using ReturnType = typename std::invoke_result_t<Func, Args...>;
        // Wrap the function and arguments into a packaged task.
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<Func>(func), std::forward<Args>(args)...));
        auto future = task->get_future();
        // Enqueue into the queue for threadId.
        while (!taskQueues[threadId].try_enqueue([task] { (*task)(); })) {
            std::this_thread::yield();
        }
        return future;
    }

    // --- MODIFIED SEGMENT ACCESS FUNCTIONS ---
    // In each of these functions we compute the segment index from the key,
    // then choose the worker queue by (segmentIndex % maxThreads).

    bool writeSegmentSingleThread(const KEY_TYPE key, VALUE_TYPE val, SSDLog<Traits>& ssdLog,
                                  const PAYLOAD_TYPE payload) {
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        const auto result = performWriteTask(fingerprint, ssdLog, payload);
        return result;
    }
#ifdef ENABLE_XDP
    bool writeSegmentSingleThreadGI(const KEY_TYPE key, VALUE_TYPE val,
                                  const PAYLOAD_TYPE payload, XDP<TraitsGI, TraitsLI, TraitsLIBuffer> *xdp) {
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        const auto result = performWriteTaskGI(fingerprint, payload, xdp);
        return result;
    }
#endif

    bool updateSegmentSingleThread(const KEY_TYPE key, VALUE_TYPE val, SSDLog<Traits>& ssdLog,
                                  const PAYLOAD_TYPE payload) {
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        const auto result = performWriteTask(fingerprint, ssdLog, payload, true);
        return result;
    }
    bool removeSegmentSingleThread(const KEY_TYPE key, SSDLog<Traits>& ssdLog) {
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        const auto result = performRemoveTask(fingerprint, ssdLog);
        return result;
    }

    std::future<bool> writeSegment(const KEY_TYPE key, const VALUE_TYPE val, SSDLog<Traits>& ssdLog,
                                   const PAYLOAD_TYPE payload) {
#ifdef ENABLE_MT
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        size_t segmentIndex = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        int threadId = segmentIndex % maxThreads;  // choose the target thread
        auto writeTask = [this, key, val, &ssdLog, payload]() {
            auto fingerprint = Hashing<Traits>::hash_digest(key);
            return performWriteTask(fingerprint, ssdLog, payload);
        };
        return accessSegment(threadId, writeTask);
#else
        throw std::invalid_argument("Enable MT in the config file");
#endif
    }

    [[nodiscard]] size_t getQLen() const {
        size_t total = 0;
        for (const auto &q : taskQueues)
            total += q.size_approx();
        return total;
    }

    auto performOffsetReadTask(const KEY_TYPE key) const {
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        const auto segmentIndex = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        const std::shared_ptr<Segment<Traits>> segment_addr = (*segDataVec)[segmentIndex].segment;
        return segment_addr->readOffset(fingerprint);
    }

    auto performTestGetTen(const KEY_TYPE key) {
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        const auto segmentIndex = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        const std::shared_ptr<Segment<Traits>> segment_addr = (*segDataVec)[segmentIndex].segment;
        return segment_addr->readTen(fingerprint);
    }

    std::future<bool> removeSegment(const KEY_TYPE key, SSDLog<Traits>& ssdLog) {
#ifdef ENABLE_MT
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        size_t segmentIndex = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        int threadId = segmentIndex % maxThreads;
        auto removeTask = [this, fingerprint, &ssdLog]() {
            return performRemoveTask(fingerprint, ssdLog);
        };
        return accessSegment(threadId, removeTask);
#else
        throw std::invalid_argument("Enable MT in the config file");
#endif
    }

    [[nodiscard]] std::optional<ENTRY_TYPE> readSegmentSingleThread(KEY_TYPE key,
                                                                        const SSDLog<Traits>& ssdLog) const {
        return performReadTask(key, ssdLog);
    }
    auto readPayloadSegmentSingleThread(KEY_TYPE key) const {
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        const auto segmentIndex = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        const auto segment_addr = (*segDataVec)[segmentIndex].segment;
        return segment_addr->read_payload(fingerprint);
    }
    auto readPayloadSegmentSingleThread(BitsetWrapper<FINGERPRINT_SIZE> fingerprint) {
        const auto segmentIndex = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        const auto segment_addr = (*segDataVec)[segmentIndex].segment;
        return segment_addr->read_payload(fingerprint);
    }

    std::future<std::optional<ENTRY_TYPE>> readSegment(KEY_TYPE key, const SSDLog<Traits>& ssdLog) {
#ifdef ENABLE_MT
        auto fingerprint = Hashing<Traits>::hash_digest(key);
        size_t segmentIndex = fingerprint.range_fast_one_reg(0, 0, segmentCountLog);
        int threadId = segmentIndex % maxThreads;
        auto readTask = [this, key, &ssdLog]() {
            return performReadTask(key, ssdLog);
        };
        return accessSegment(threadId, readTask);
#else
        throw std::invalid_argument("Enable MT in the config file");
#endif
    }

    // For tasks that do not have a key (e.g. readRandom), you can choose a default thread (e.g. 0)
    std::future<bool> readRandom(PAYLOAD_TYPE p, const SSDLog<Traits>& ssdLog) {
#ifdef ENABLE_MT
        int threadId = p % maxThreads;
        auto readTask = [this, p, &ssdLog]() {
            ENTRY_TYPE kv;
            auto payload = p;
            return ssdLog.read(payload, kv);
        };
        return accessSegment(threadId, readTask);
#else
        throw std::invalid_argument("Enable MT in the config file");
#endif
    }

    void print() const {
        std::cout << "printing directory\n";
        for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
            std::cout << "segment " << i << std::endl;
            (*segDataVec)[i].segment->print();
        }
    }

    void print_segs_info() const {
        std::cout << "printing seg info\n";
        for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
            std::cout << "segment: " << i << '\t';
            std::cout << "ten all: " << (*segDataVec)[i].segment->get_ten_all() << '\t';
            std::cout << "count: " << (*segDataVec)[i].segment.use_count() << '\t';
            std::cout << "address: " << (*segDataVec)[i].segment.get() << '\t';
            std::cout << std::endl;
        }
    }

    [[nodiscard]] auto get_ten_all() const {
        double count = 0;
        for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
            count += static_cast<double>((*segDataVec)[i].segment->get_ten_all()) /
                     static_cast<double>((*segDataVec)[i].segment.use_count());
        }
        return static_cast<size_t>(count);
    }

    [[nodiscard]] auto get_num_uniq_segs() const {
        double count = 0;
        for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
            // std::cout << "segment: " << i << "\t" << (*segDataVec)[i].segment.use_count() << std::endl;
            count += 1.0 / static_cast<double>((*segDataVec)[i].segment.use_count());
        }
        // std::cout << "count: " << count << std::endl << std::endl;
        return static_cast<size_t>(count);
    }

    [[nodiscard]] auto get_count_unique_segs() const {
        double count = 0;
        for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
            count += 1.0 / static_cast<double>((*segDataVec)[i].segment.use_count());
        }
        return static_cast<size_t>(count);
    }

    auto get_memory_footprint(const size_t i) const {
        return get_memory_footprint_total() / static_cast<double>(i); 
    }
    auto get_memory_footprint_total() const {
        if constexpr (!Traits::DHT_EVERYTHING) {
            size_t ex_bits_per_block = 0;
            if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
                ex_bits_per_block = Traits::NUMBER_EXTRA_BITS * COUNT_SLOT;
            }
            auto unique_segs = get_count_unique_segs();
            auto expected_memory_footprint = unique_segs * (COUNT_SLOT + 3 * Traits::SEGMENT_EXTENSION_BLOCK_SIZE / 2) * (N + ex_bits_per_block);
            return static_cast<double>(expected_memory_footprint);
        } else {
            double whole = 0.0;
            for (size_t j = 0; j < (1 << segmentCountLog); ++j) {
                double mem = (*segDataVec)[j].segment->get_memory();
                mem *= 1.0 / static_cast<double>((*segDataVec)[j].segment.use_count());
                whole += mem;
            }
            return whole;
        }
    }
    auto get_memory_including_ptr() const {
        auto ptr_size = sizeof(typename Traits::PAYLOAD_TYPE) * 8;
        if constexpr (!Traits::DHT_EVERYTHING) {
            size_t ex_bits_per_block = 0;
            if constexpr (Traits::NUMBER_EXTRA_BITS > 1) {
                ex_bits_per_block = Traits::NUMBER_EXTRA_BITS * COUNT_SLOT;
            }
            auto unique_segs = get_count_unique_segs();
            auto expected_memory_footprint = unique_segs * (COUNT_SLOT + 3 * Traits::SEGMENT_EXTENSION_BLOCK_SIZE / 2) * (N + ex_bits_per_block);
            auto memory_footprint = static_cast<double>(expected_memory_footprint);
            size_t ptr_size_in_bits = 0;
            if constexpr (Traits::VAR_LEN_PAYLOAD) 
                ptr_size_in_bits = (COUNT_SLOT + Traits::SEGMENT_EXTENSION_BLOCK_SIZE) * unique_segs * 128;
            else
                ptr_size_in_bits = (COUNT_SLOT + Traits::SEGMENT_EXTENSION_BLOCK_SIZE) * unique_segs * (ptr_size * Traits::PAYLOADS_LENGTH);
            auto memory_footprint_with_ptr = (memory_footprint + ptr_size_in_bits);
            return memory_footprint_with_ptr;
        } else {
            double whole = 0.0;
            for (size_t j = 0; j < (1 << segmentCountLog); ++j) {
                double mem = (*segDataVec)[j].segment->get_memory();
                mem *= 1.0 / static_cast<double>((*segDataVec)[j].segment.use_count());
                whole += mem;
            }
            size_t ptr_size_in_bits = 0;
            if constexpr (Traits::VAR_LEN_PAYLOAD) 
                ptr_size_in_bits = (COUNT_SLOT + Traits::SEGMENT_EXTENSION_BLOCK_SIZE) * get_count_unique_segs() * 128;
            else
                ptr_size_in_bits = (COUNT_SLOT + Traits::SEGMENT_EXTENSION_BLOCK_SIZE) * get_count_unique_segs() * (ptr_size * Traits::PAYLOADS_LENGTH);
            whole += ptr_size_in_bits;
            return whole;
        }
    }

    auto get_load_factor(const size_t i) const {
        if constexpr (!Traits::DHT_EVERYTHING) {
            auto ten_all = get_ten_all();
            assert(i == ten_all);
            auto physical_ten = Traits::PAYLOADS_LENGTH * (COUNT_SLOT + Traits::SEGMENT_EXTENSION_BLOCK_SIZE) * get_count_unique_segs();
            return static_cast<double>(ten_all) / static_cast<double>(physical_ten);
        } else {
            double whole = 0.0;
            for (size_t j = 0; j < (1 << segmentCountLog); ++j) {
                double num_uniq_blks = (*segDataVec)[j].segment->get_uniq_blks();
                std::cout << "num_uniq_blks: " << num_uniq_blks << std::endl;
                num_uniq_blks *= 1.0 / static_cast<double>((*segDataVec)[j].segment.use_count());
                whole += num_uniq_blks;
            }
            return static_cast<double>(i) / (whole * Traits::PAYLOADS_LENGTH);
        }
    }

    void print_dir_info() const {
        std::cout << "Directory Information\n";
        std::cout << "\tsum all: " << get_ten_all() << std::endl;
        std::cout << "\tnumber of segments: " << (1 << segmentCountLog) << std::endl;
    }

    std::unique_ptr<Directory<TraitsLI>> replicate() {
        auto newDir = std::make_unique<Directory<TraitsLI>>(segmentCountLog, 1);
        for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
            (*newDir->segDataVec)[i].segment = nullptr;
        }

        for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
            if ((*newDir->segDataVec)[i].segment == nullptr) {
                (*newDir->segDataVec)[i].segment = (*segDataVec)[i].segment->replicate();
                for (size_t ii = i + 1; ii < (1 << segmentCountLog); ++ii) {
                    if ((*segDataVec)[ii].segment.get() == (*segDataVec)[i].segment.get()) {
                        (*newDir->segDataVec)[ii].segment = (*newDir->segDataVec)[i].segment;
                    }
                }
            }
        }
        // for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
        //     std::cout << "segment: " << i << "\t" << (*newDir->segDataVec)[i].segment << std::endl;
        //     std::cout << "segment: " << i << "\t" << (*segDataVec)[i].segment << std::endl;
        // }
        assert(get_num_uniq_segs() == newDir->get_num_uniq_segs());
        return newDir;
    }

    float get_average_age() {
        float sum = 0;
        float count = 0;
        for (size_t i = 0; i < (1 << segmentCountLog); ++i) {
            auto count_ref = (*segDataVec)[i].segment.use_count();
            sum += (*segDataVec)[i].segment->get_average_age() / count_ref;
            count += 1.0 / static_cast<float>(count_ref);
        }
        return sum / count;
    }
};
