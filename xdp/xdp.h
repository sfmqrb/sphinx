#pragma once
#include <cassert>
#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <atomic>
#include <future>
#include <utility>
#include <algorithm>

#include "../config/config.h"
#include "../directory/directory.h"
#include "SSDLog.h"

template <typename TraitsGI = DefaultTraits, typename TraitsLI = DefaultTraits,
          typename TraitsLIBuffer = DefaultTraits>
class XDP {
#ifdef ENABLE_XDP
  private:
    // --- sizes & paths ---
    size_t l_log_size_in_page = 40'000;
    const std::string ssdLogLIPath = "directory_l.txt";

    // --- global‐index (GI) ---
    std::unique_ptr<Directory<TraitsGI>> dirGI;

    // --- local‐indexes (LI) ---
    std::vector<std::unique_ptr<SSDLog<TraitsLI>>> logLIList;
    std::vector<std::unique_ptr<Directory<TraitsLI>>> dirLIList;

    // --- LI buffer ---
    std::unique_ptr<SSDLog<TraitsLIBuffer>> logLIBuffer;
    std::unique_ptr<Directory<TraitsLIBuffer>> dirLIBuffer;

    // -- boundaries of LI buffer based on the pointers 
    // -- not neccasary only for health check
    std::vector<size_t> boundaries;


    uint64_t max_li_entries;
    uint64_t li_counter;
    uint64_t max_li_entries_log;

    using ENTRY_TYPE = typename TraitsGI::ENTRY_TYPE;
    using KEY_TYPE = typename TraitsGI::KEY_TYPE;
    using VALUE_TYPE = typename TraitsGI::VALUE_TYPE;

  public:
    XDP(size_t _max_li_entries)
        : dirGI(std::make_unique<Directory<TraitsGI>>(0, 1)),
          logLIBuffer(std::make_unique<SSDLog<TraitsLIBuffer>>(
              ssdLogLIPath, l_log_size_in_page)),
          dirLIBuffer(std::make_unique<Directory<TraitsLIBuffer>>(0, 1)),
          max_li_entries(_max_li_entries), li_counter(0) {

        max_li_entries_log = std::ceil(std::log2(_max_li_entries));
    }

    void print_test() {
        std::cout << "calling print_test" << std::endl;
    }

    std::int64_t get_li_oracle(const KEY_TYPE key) {
        auto payload_gi = dirGI->readPayloadSegmentSingleThread(key);
        int64_t target_li_idx = static_cast<int64_t>(payload_gi);
        return target_li_idx;
    }

    std::optional<ENTRY_TYPE> performReadTask(const KEY_TYPE key) const {
        auto payload_gi = dirGI->readPayloadSegmentSingleThread(key);
        if (payload_gi == boundaries.size()) {
            auto entry = dirLIBuffer->readSegmentSingleThread(key, *logLIBuffer);
            return entry;
        } else {
            auto payload_li_stacked = dirLIList[payload_gi]->readPayloadSegmentSingleThread(key);
            ENTRY_TYPE kv;
            logLIList[payload_gi]->read(payload_li_stacked, kv);
            return kv;
        }
    }

    std::optional<ENTRY_TYPE> performReadTaskIdx(BitsetWrapper<FINGERPRINT_SIZE> fingerprint, 
                                                 uint32_t target_li_idx) {
        if (target_li_idx == boundaries.size()) {
            auto entry = dirLIBuffer->performReadTaskFP(fingerprint, *logLIBuffer);
            return entry;
        } else {
            auto payload_li_stacked = dirLIList[target_li_idx]->readPayloadSegmentSingleThread(fingerprint);
            ENTRY_TYPE kv;
            logLIList[target_li_idx]->read(payload_li_stacked, kv);
            return kv;
        }
    }
    bool performWriteTask(const KEY_TYPE key, VALUE_TYPE val) {
        auto payload_gi = boundaries.size();
        dirGI->writeSegmentSingleThreadGI(key, val, payload_gi, this);

        auto payload_li = logLIBuffer->write(key, val);
        dirLIBuffer->writeSegmentSingleThread(key, val, *logLIBuffer,
                                              payload_li);


        if (++li_counter >= max_li_entries) {
            boundaries.push_back(1);
            // replicate & flush…
            auto logLIFlush = std::make_unique<SSDLog<TraitsLI>>(
                ssdLogLIPath + std::to_string(logLIList.size()),
                l_log_size_in_page);
            auto dirLIFlush = dirLIBuffer->replicate();
            auto segSize = dirLIBuffer->segmentCountLog.load();
            for (size_t i = 0; i < (1u << segSize); ++i) {
                // if already replicated, skip
                bool skip = false;
                for (size_t ii = 0; ii < i; ++ii) {
                    if ((*dirLIBuffer->segDataVec)[i].segment.get() ==
                        (*dirLIBuffer->segDataVec)[ii].segment.get()) {
                        skip = true;
                    }
                }
                if (skip) {
                    continue;
                }
                // convert the payloads
                auto segDataFlush = (*dirLIFlush->segDataVec)[i];
                auto segDataLIBuffer = (*dirLIBuffer->segDataVec)[i];
                for (size_t j = 0; j < COUNT_SLOT; ++j) {
                    Payload<TraitsLIBuffer> &plOLD = segDataLIBuffer.segment->blockList[j].payload_list;
                    Payload<TraitsLI> &plNEW = segDataFlush.segment->blockList[j].payload_list;
                    auto old_pl_max_index = segDataLIBuffer.segment->blockList[j].get_max_index();
                    auto new_pl_max_index = segDataFlush.segment->blockList[j].get_max_index();
                    convertPayloadGIToLI(plOLD, plNEW, *logLIFlush, old_pl_max_index);
                    // check if the pointers are correctly set 
                    for (int k = 0; k <= new_pl_max_index; ++k) {
                        auto pl = plOLD.get_payload_at(k);
                        ENTRY_TYPE entry;
                        logLIBuffer->read(pl, entry);
                        auto res1 = dirLIBuffer->readSegmentSingleThread(entry.key, *logLIBuffer);


                        auto pl_tmp = dirLIFlush->readPayloadSegmentSingleThread(entry.key);
                        ENTRY_TYPE res2;
                        logLIFlush->read(pl_tmp, res2);
                        assert(entry.key == res1->key);
                        assert(entry.key == res2.key);
                    }
                }
                if constexpr (!TraitsGI::DHT_EVERYTHING) {
                    for (size_t j = 0; j < TraitsGI::SEGMENT_EXTENSION_BLOCK_SIZE; ++j) {
                        Payload<TraitsLIBuffer> &plOLD = segDataLIBuffer.segment->extensionBlockList[j].blk.payload_list;
                        Payload<TraitsLI> &plNEW = segDataFlush.segment->extensionBlockList[j].blk.payload_list;
                        auto old_pl_max_index = segDataLIBuffer.segment->extensionBlockList[j].blk.get_max_index();
                        convertPayloadGIToLI(plOLD, plNEW, *logLIFlush.get(), old_pl_max_index);
                    }
                } else {
                    throw std::invalid_argument("not implemented for DHT_EVERYTHING");
                }
            }

            // flush the log to dirLIList
            logLIList.push_back(std::move(logLIFlush));
            dirLIList.push_back(std::move(dirLIFlush));

            // reset LI‐buffer & directory
            logLIBuffer = std::make_unique<SSDLog<TraitsLIBuffer>>(ssdLogLIPath, l_log_size_in_page);
            dirLIBuffer = std::make_unique<Directory<TraitsLIBuffer>>(0, 1);
            li_counter = 0;
            // std::cout << "Flushed LI buffer to disk." << std::endl;
        }

        #ifdef DEBUG
        // if (li_counter % 1000 == 0) {
        //     std::cout << "Counter: " << li_counter << std::endl;
        //     std::cout << "Load Factor of Buffer: " << dirLIBuffer->get_load_factor(li_counter) << std::endl;
        // }
        #endif
        return true;
    }

    void convertPayloadGIToLI(Payload<TraitsLIBuffer> &plOLD,
                              Payload<TraitsLI> &plNEW,
                              SSDLog<TraitsLI> &logLIFlush,
                              int64_t old_max_index) {
        int prev_new_pl;
        for (int64_t k = 0; k <= old_max_index; ++k) {
            ENTRY_TYPE entry;
            auto pl = plOLD.get_payload_at(k);
            logLIBuffer->read(pl, entry);

            auto new_pl_exact = logLIFlush.write(entry.key, entry.value);
            auto new_pl = logLIFlush.get_page_address(new_pl_exact);
            if (k == 0) {
                prev_new_pl = new_pl;
                // set exact position with offset
                plNEW.set_init_page_of_block(new_pl_exact);
            }
            plNEW.set_payload_at(k, new_pl == prev_new_pl ? 1 : 2);
            prev_new_pl = new_pl;
        }
    }

    void printStatus() const {
        std::cout << "XDP Status:" << std::endl;
        std::cout << "Global Index:" << std::endl;
        std::cout << "\t ten all: " << dirGI->get_ten_all() << std::endl;
        std::cout << "Local Indexes:" << std::endl;
        for (const auto &dir : dirLIList) {
            std::cout << "\t ten all: " << dir->get_ten_all() << std::endl;
        }
        std::cout << "Buffer:" << std::endl;
        std::cout << "\t ten all: " << dirLIBuffer->get_ten_all() << std::endl;
    }

    void print() const {
        // …
    }

    std::vector<double> get_memory_footprint() const {
        // global index 
        auto mem_gi_footprint = dirGI->get_memory_including_ptr(boundaries.size());
        // local Indexes
        size_t local_index_footprint = 0;
        for (const auto &dir : dirLIList) {
            local_index_footprint += dir->get_memory_including_ptr();
        }
        // Buffer
        auto mem_buffer_footprint = dirLIBuffer->get_memory_including_ptr();
        mem_buffer_footprint += max_li_entries_log * sizeof(ENTRY_TYPE) * 8;

        // std::cout << "mem_gi_footprint: " << mem_gi_footprint << std::endl;
        // std::cout << "local_index_footprint: " << local_index_footprint << std::endl;
        // std::cout << "mem_buffer_footprint: " << mem_buffer_footprint << std::endl;

        return std::vector<double>({mem_gi_footprint, local_index_footprint, mem_buffer_footprint});
    }

    std::vector<double> get_memory_index_size() const {
        // global index 
        auto mem_gi_footprint = dirGI->get_memory_footprint_total();
        // local Indexes
        size_t local_index_footprint = 0;
        for (const auto &dir : dirLIList) {
            local_index_footprint += dir->get_memory_footprint_total();
        }
        // Buffer
        auto mem_buffer_footprint = dirLIBuffer->get_memory_footprint_total();
        return std::vector<double>({mem_gi_footprint, local_index_footprint, mem_buffer_footprint});
    }
        
    

    auto get_memory_footprint_per_entry(size_t i) const {
        auto memory = get_memory_footprint();
        auto sum = memory[0] + memory[1] + memory[2];
        return sum / static_cast<double>(i);
    }

    auto get_memory_index_size_per_entry(size_t i) const {
        auto memory = get_memory_index_size();
        auto sum = memory[0] + memory[1] + memory[2];
        return sum / static_cast<double>(i);
    }
#endif
};
