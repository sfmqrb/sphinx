#pragma once

#include <fcntl.h>      // For file control operations
#include <sys/stat.h>   // For file mode constants
#include <sys/types.h>  // For type definitions
#include <unistd.h>     // For read, write, close
#include <sys/mman.h>   // For madvise
#include <cerrno>       // For handling errno in case of errors

#include <cassert>
#include <cstdlib>      // For aligned_alloc and free
#include <cstring>      // For memcpy
#include <iostream>
#include <vector>
#include <cmath>
#include <atomic>
#include <string>
#include "../config/config.h"
#include "../buffer_pool2/buffer_pool2.h"

template<typename Traits = DefaultTraits>
class SSDLog {
   private:
    typedef typename Traits::PAYLOAD_TYPE PAYLOAD_TYPE;
    typedef typename Traits::KEY_TYPE KEY_TYPE;
    typedef typename Traits::VALUE_TYPE VALUE_TYPE;
    typedef typename Traits::ENTRY_TYPE ENTRY_TYPE;
#ifdef IN_MEMORY_FILE
    std::vector<char> inMemoryFile;
#endif
    int fd;
    std::string filename;
    size_t logSize;
    uint32_t firstValidPage{};
    uint32_t lastValidPage{};
    bool IS_OPTANE;
    std::vector<ENTRY_TYPE> writeBuffer;
    size_t entrySize;

    void flushBuffer() {
        if (writeBuffer.empty()) {
            return;
        }

        size_t bufferSize = writeBuffer.size() * sizeof(ENTRY_TYPE);
        size_t alignedSize = ((bufferSize + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
        assert(alignedSize == PAGE_SIZE);

        // Reuse a thread-local aligned buffer to avoid repeated allocation
        thread_local void* alignedBuffer = nullptr;
        if (alignedBuffer == nullptr) {
            if (posix_memalign(&alignedBuffer, PAGE_SIZE, alignedSize) != 0) {
                throw std::bad_alloc();  // Memory allocation failed
            }
        }

        std::memset(alignedBuffer, 0, alignedSize);
        std::memcpy(alignedBuffer, writeBuffer.data(), bufferSize);

        off_t offset = lastValidPage * PAGE_SIZE;
    #ifdef IN_MEMORY_FILE
        std::memcpy(&inMemoryFile[offset], alignedBuffer, alignedSize);
    #else
        ssize_t writtenBytes = pwrite(fd, alignedBuffer, alignedSize, offset);
        if (writtenBytes != static_cast<ssize_t>(alignedSize)) {
            throw std::runtime_error("Failed to write buffer to disk with Direct I/O");
        }
    #endif
        // Successfully written, update lastValidPage
        lastValidPage = (lastValidPage + alignedSize / PAGE_SIZE) % logSize;
        writeBuffer.clear();  // Clear the buffer after successful write
    }
    inline auto get_num_entries_per_page() const {
        return static_cast<int>(PAGE_SIZE / sizeof(ENTRY_TYPE));
    }

    inline auto get_log_num_entries_per_page() const {
        return static_cast<int>(ceil(log2(get_num_entries_per_page())));
    }
   public:
    mutable std::atomic<size_t> numQ{0};
    std::unique_ptr<LinearProbingHashTable<Traits>> BP;
    SSDLog(std::string fname, size_t size)
        : filename(std::move(fname)), logSize(size), entrySize(sizeof(ENTRY_TYPE)) {
        IS_OPTANE = filename.find("optane") != std::string::npos;
        firstValidPage = 0;
        lastValidPage = 0;
        BP = std::make_unique<LinearProbingHashTable<Traits>>(Traits::BUFFER_POOL_CAP);
    #ifdef IN_MEMORY_FILE
        inMemoryFile.resize(logSize * PAGE_SIZE);
    #else
        fd = open(filename.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_TRUNC, 0644); // with direct io to bypass cache
        // fd = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644); // without DIRECT_IO to use cache
        if (fd == -1) {
            throw std::runtime_error("Failed to open file with O_DIRECT flag");
        }
        posix_fallocate(fd, 0, logSize * PAGE_SIZE);
    #endif
    }

    ~SSDLog() {
        flushBuffer();
    #ifndef IN_MEMORY_FILE
        if (fd != -1) {
            close(fd);
        }
    #endif
    }

    PAYLOAD_TYPE write(KEY_TYPE key, VALUE_TYPE value) {
        const size_t pageIndex = lastValidPage;
        const size_t entryIndex = writeBuffer.size();
        writeBuffer.emplace_back(key, value);
        if ((writeBuffer.size() + 1) * entrySize > PAGE_SIZE) {  // one before page size is full
            flushBuffer();
        }
        const PAYLOAD_TYPE address = (pageIndex << get_log_num_entries_per_page()) + entryIndex;
    #ifdef ENABLE_BP_FOR_READ
        BP->put(address, value, key);
    #endif
        return address;
    }

    // bool shows if it successfully used the buffer pool
    bool read(PAYLOAD_TYPE address, ENTRY_TYPE& entry_type) const {
        numQ.fetch_add(1, std::memory_order_relaxed);
    #ifdef ENABLE_BP_FOR_READ
        auto bp_res = BP->get(address);
        if (bp_res.has_value()) {
            entry_type.key = bp_res.value().first;
            entry_type.value = bp_res.value().second;
            return true;
        }
    #endif
        const size_t pageIndex = address >> get_log_num_entries_per_page();
        const size_t entryIndex = address & ((1 << get_log_num_entries_per_page()) - 1);

        assert(pageIndex < logSize);
        assert(entryIndex < get_num_entries_per_page());
//        assert(isValid(pageIndex) || pageIndex == lastValidPage);

        ENTRY_TYPE entry;
        if (pageIndex != lastValidPage) {
            // If the filename contains "optane", read exactly one entry.
            if (IS_OPTANE) {
                const off_t offset = pageIndex * PAGE_SIZE + entryIndex * entrySize;
            #ifdef IN_MEMORY_FILE
                memcpy(&entry, &inMemoryFile[offset], entrySize);
            #else
                // Use a thread-local buffer to ensure alignment for Direct I/O.
                static thread_local void* aligned_buffer_optane = nullptr;
                if (aligned_buffer_optane == nullptr) {
                    if (posix_memalign(&aligned_buffer_optane, PAGE_SIZE, PAGE_SIZE) != 0) {
                        throw std::bad_alloc();
                    }
                }
                ssize_t result = pread(fd, aligned_buffer_optane, entrySize, offset);
                if (result != static_cast<ssize_t>(entrySize)) {
                    std::cerr << "Failed to read one entry from disk, expected " << entrySize << ", got " << result
                              << ". errno: " << strerror(errno) << '\n';
                    throw std::runtime_error("Failed to read one entry from disk");
                }
                memcpy(&entry, aligned_buffer_optane, entrySize);
            #endif
            } else {
                // Otherwise, use the original full-page read method.
                static thread_local void* aligned_buffer = nullptr;
                if (aligned_buffer == nullptr) {
                    if (posix_memalign(&aligned_buffer, PAGE_SIZE, PAGE_SIZE) != 0) {
                        throw std::bad_alloc();
                    }
                }
                const off_t offset = pageIndex * PAGE_SIZE;
            #ifdef IN_MEMORY_FILE
                memcpy(aligned_buffer, &inMemoryFile[offset], PAGE_SIZE);
            #else
                ssize_t result = pread(fd, aligned_buffer, PAGE_SIZE, offset);
                if (result != PAGE_SIZE) {
                    std::cerr << "Failed to read from disk, expected " << PAGE_SIZE << ", got " << result
                              << ". errno: " << strerror(errno) << '\n';
                    throw std::runtime_error("Failed to read from disk");
                }
            #endif
                memcpy(&entry, (char*)aligned_buffer + (entryIndex * entrySize), entrySize);
            }
        } else {
            assert(entryIndex < writeBuffer.size());
            entry = writeBuffer[entryIndex];
        }
        entry_type.key = entry.key;
        entry_type.value = entry.value;
    #ifdef ENABLE_BP_PUT_IN_READ
        BP->put(address, entry.value, entry.key);
    #endif
        return false;
    }

    bool isValid(size_t page) const {
        if (page >= logSize)
            return false;
        if (lastValidPage > firstValidPage)
            return firstValidPage <= page && page < lastValidPage;
        return page < lastValidPage || firstValidPage <= page;
    }

    void printLog() {
        std::cout << "Logged Entries:\n";
        void* buffer = nullptr;
        if (posix_memalign(&buffer, PAGE_SIZE, PAGE_SIZE) != 0) {
            throw std::bad_alloc();
        }

        for (size_t i = firstValidPage; i != lastValidPage; ++i) {
            i %= logSize;
            off_t offset = i * PAGE_SIZE;
        #ifdef IN_MEMORY_FILE
            std::memcpy(buffer, &inMemoryFile[offset], PAGE_SIZE);
        #else
            if (pread(fd, buffer, PAGE_SIZE, offset) != PAGE_SIZE) {
                free(buffer);
                throw std::runtime_error("Failed to read from disk");
            }
        #endif
            ENTRY_TYPE* entryPtr = static_cast<ENTRY_TYPE*>(buffer);
            size_t numEntries = PAGE_SIZE / entrySize;

            std::vector<ENTRY_TYPE> entries(entryPtr, entryPtr + numEntries);

            std::cout << "Page " << i << ":\n";
            for (size_t j = 0; j < numEntries; ++j) {
                std::cout << "(K: " << entries[j].key << ", V: " << entries[j].value << ')';
                if (j < numEntries - 1) std::cout << "\t";
            }
            std::cout << "\n";
        }

        free(buffer);

        std::cout << "Unflushed Entries in Buffer:\n";
        for (const auto& entry : writeBuffer) {
            std::cout << "Key: " << entry.key << ", Value: " << entry.value << " | ";
        }
        std::cout << "\n";
    }

};
