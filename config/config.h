#pragma once
#include <unistd.h>
#include <cstddef>
#include <cstdint>

#include "../bitset_wrapper/bitset_wrapper.h"

// ENABLE for in-memory benchmark
#define IN_MEMORY_FILE

// XDP ENABLE
#define ENABLE_XDP

// only for MT
#define ENABLE_MT

// DISABLE for read_latency benchmark and in memory benchmark
//#define ENABLE_BP_PUT_IN_READ

// disable for in-memory benchmark
// #define ENABLE_BP_FOR_READ

constexpr int N = 256;
constexpr int FINGERPRINT_SIZE = 128;
constexpr int COUNT_SLOT_BITS = 6;
constexpr int COUNT_SLOT = 1 << COUNT_SLOT_BITS;
constexpr int EXTENDED_BIT_WRAPPER_SIZE = 2 * COUNT_SLOT;
const size_t PAGE_SIZE = sysconf(_SC_PAGESIZE);

constexpr size_t BASE_EXP_INSERT_SIZE = 1ull << 20;
constexpr bool MAIN_BENCHMARK_ZIPF = false;

template <typename KeyType, typename ValueType>
struct EntryType {
    KeyType key;
    ValueType value;
    EntryType() = default;
    EntryType(const KeyType k, const ValueType v) : key(k), value(v) {}
};

struct DefaultTraits {
    typedef uint32_t PAYLOAD_TYPE;  // Type used for payload data.
    typedef int64_t KEY_TYPE;      // Type used for keys.
    typedef int64_t VALUE_TYPE;    // Type used for values.

    /* Number of extra bits allocated per entry. These bits help manage read-before-write operations,
     * accommodate expansion with almost no random storage read, and reduce the false positive rate 
     * for non-existent queries. */
    static constexpr size_t NUMBER_EXTRA_BITS = 8;

    /* The minimum number of bits remaining before an expansion is triggered.
     * When the remaining bits fall below this threshold, an expansion is initiated. */
    static constexpr int EXTENSION_THRESHOLD = 1;

    /* Number of bits allocated per entry. Lower values provide a better block load factor but may require 
     * more frequent extensions. Higher values enable faster parsing and increase the need for expansion. */
    static constexpr int BITS_PER_ENTRY = 4;

    /* Number of additional payload slots reserved to delay the need for expansion.
     * These slots act as a buffer, allowing more data to be stored in payloads section before an expansion is necessary. */
    static constexpr int SAFETY_PAYLOADS = 4;

    /* The number of extension blocks allocated per segment to accommodate potential extensions. */
    static constexpr int SEGMENT_EXTENSION_BLOCK_SIZE = 4;

    /* Internal type used for entries, combining key and value types.
     * This should be present at the end of all configuration settings. */
    using ENTRY_TYPE = EntryType<KEY_TYPE, VALUE_TYPE>;

    /* The computed length of payloads.
     * This value should be present at the end of all configuration settings. */
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;

    // buffer pool max trial before returingn no (first bp implementation)
    // only meaningful in highly multithreaded setting
    static constexpr size_t MAX_TRIALS = 100;

    // max dist to init pos (first bp implementation) - irrelevant to Fleck
    static constexpr size_t MAX_OCCUPIED_SEQ = 10;

    // max load factor of the buffer pool - disabled by default
    static constexpr float MAX_LF = 0;

    // number of slots each lock covers
    static constexpr size_t LOCK_LENGTH = 100;

    // batch eviction faster eviction than single eviction - disabled by default
    static constexpr bool BATCH_EVICTION = false;

    // Buffer pool Cap - disabled by default
    static constexpr size_t BUFFER_POOL_CAP = 0;

    // read offset method
    // 0: default and optimized
    // 1: no ht
    // 2: no ht and ten 2 handling
    // 20: DHT
    static constexpr size_t READ_OFF_STRATEGY = 0;

    // 0 fleck
    // 20 DHT
    static constexpr size_t WRITE_STRATEGY = 0;

    // for benchmarking purposes
    static constexpr bool USE_XXHASH = true;


    // read DHT
    static constexpr bool DHT_EVERYTHING = false;

    static constexpr bool IS_INFINI = false;

    static constexpr bool EXPAND = true;

    // to support the local index in XDP
    static constexpr bool VAR_LEN_PAYLOAD = false;

    // global index for partially sorting idea
    static constexpr bool GLOBAL_INDEX = false;
   
    // in memory file set 
    static constexpr bool IN_MEMORY = false;
};

struct TraitsLI : DefaultTraits {
    // the log resides in the SSD - uncomment this for in-memory
    // static constexpr bool IN_MEMORY = true;
    static constexpr bool VAR_LEN_PAYLOAD = true;
    static constexpr size_t NUMBER_EXTRA_BITS = 1; // disabled
};
struct TraitsLIBuffer : DefaultTraits {
    // the log resides in the memory for the buffer Sphinx
    static constexpr bool IN_MEMORY = true;
    static constexpr bool VAR_LEN_PAYLOAD = false;
    static constexpr size_t NUMBER_EXTRA_BITS = 1; // disabled
};
struct TraitsGI : DefaultTraits {
    // the log resides in the SSD - uncomment this for in-memory
    // static constexpr bool IN_MEMORY = true;
    typedef uint8_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr bool VAR_LEN_PAYLOAD = false;
    static constexpr size_t NUMBER_EXTRA_BITS = 1; // disabled
};
struct TestDefaultTraits : DefaultTraits {
    static constexpr bool USE_XXHASH = false;
};
struct TestDefaultTraitsXXHASH : DefaultTraits {
    static constexpr bool USE_XXHASH = true;
    static constexpr int SAFETY_PAYLOADS = 0;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;
};
struct TestDefaultTraitsDHT : DefaultTraits {
    static constexpr bool USE_XXHASH = false;
    static constexpr bool DHT_EVERYTHING = true;
    static constexpr int SAFETY_PAYLOADS = 0;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;
    static constexpr size_t READ_OFF_STRATEGY = 20;
    static constexpr size_t WRITE_STRATEGY = 20;
};
struct TestDefaultTraitsDHTXXHASH : DefaultTraits {
    static constexpr bool USE_XXHASH = true;
    static constexpr bool DHT_EVERYTHING = true;
    static constexpr int SAFETY_PAYLOADS = 0;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;
    static constexpr size_t READ_OFF_STRATEGY = 20;
    static constexpr size_t WRITE_STRATEGY = 20;
};
struct DefaultTraits2 : DefaultTraits {
    static constexpr int BITS_PER_ENTRY = 256;
    typedef int8_t KEY_TYPE;
    typedef int8_t VALUE_TYPE;
    using ENTRY_TYPE = EntryType<KEY_TYPE, VALUE_TYPE>;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;
};


struct DefaultTraitsTestBP : DefaultTraits {
    static constexpr size_t MAX_TRIALS = 20;
    static constexpr size_t MAX_OCCUPIED_SEQ = 5;
    static constexpr float MAX_LF = 0.8;
};

struct TestBPDeprecated : DefaultTraits {
    static constexpr size_t MAX_TRIALS = 400;
    static constexpr size_t MAX_OCCUPIED_SEQ = 50;
    static constexpr float MAX_LF = 0.8;
    static constexpr size_t LOCK_LENGTH = 100;
    static constexpr bool BATCH_EVICTION = true;
    static constexpr size_t BUFFER_POOL_CAP = 1ull << 30;

};

struct TestRSQF : DefaultTraits {
    static constexpr bool IS_INFINI = false;
};
struct TestInfini: DefaultTraits {
    static constexpr int IS_INFINI  = true;
};
struct TestRSQFInMem : TestRSQF {
    static constexpr bool IN_MEMORY = true;
};
struct TestInfiniInMem : TestInfini {
    static constexpr bool IN_MEMORY = true;
};

struct TestBP2Traits: DefaultTraits {
    typedef BitsetWrapper<FINGERPRINT_SIZE> PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr bool USE_XXHASH = false;
    static constexpr float MAX_LF = 0.5;
    static constexpr bool BATCH_EVICTION = true;
    static constexpr size_t LOCK_LENGTH = 100;
    static constexpr size_t BUFFER_POOL_CAP = 1ull << 30;
};

struct Test2BP2Traits: DefaultTraits {
    typedef BitsetWrapper<FINGERPRINT_SIZE> PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr bool USE_XXHASH = false;
    static constexpr float MAX_LF = 0.3;
    static constexpr bool BATCH_EVICTION = false;
    static constexpr size_t LOCK_LENGTH = 100;
    static constexpr size_t BUFFER_POOL_CAP = 1ull << 30;
};

struct TestBP2Fleck: DefaultTraits {
    static constexpr bool USE_XXHASH = true;
    static constexpr float MAX_LF = 0.5;
    static constexpr bool BATCH_EVICTION = false;
    static constexpr size_t LOCK_LENGTH = 100;
    static constexpr size_t BUFFER_POOL_CAP = 1ull << 30;
};

struct TestCPU: DefaultTraits {
    // don't use cache
    static constexpr float MAX_LF = 0;
    static constexpr bool BATCH_EVICTION = false;
    static constexpr size_t BUFFER_POOL_CAP = 0;

    // almost no overflows 128 as payload len
    static constexpr int SAFETY_PAYLOADS = 64;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;

    // disable filters
    static constexpr size_t NUMBER_EXTRA_BITS = 1; // disabled

    // read offset strategy
    static constexpr size_t READ_OFF_STRATEGY = 0;
};

struct TestCPUReadCost0: TestCPU {
    static constexpr size_t READ_OFF_STRATEGY = 0; // using 3.2
};

struct TestCPUReadCost2: TestCPU {
    static constexpr size_t READ_OFF_STRATEGY = 2; // without 3.2
};

struct TestCPUReadDHT: TestCPU {
    static constexpr size_t READ_OFF_STRATEGY = 20; // DHT
};

struct TestFleckInMemory: DefaultTraits {
    static constexpr int SAFETY_PAYLOADS = 0;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;

    // disable filters
    static constexpr size_t NUMBER_EXTRA_BITS = 1; // disabled

    // read offset strategy
    static constexpr size_t READ_OFF_STRATEGY = 0;

    // disable cache
    static constexpr float MAX_LF = 0;
    static constexpr bool BATCH_EVICTION = false;
    static constexpr size_t BUFFER_POOL_CAP = 0;
};

struct TestFleckInMemoryInMem : TestFleckInMemory {
    static constexpr bool IN_MEMORY = true; // in memory
};

struct TestFleckInMemoryExtraBits4P32 : TestFleckInMemory {
    typedef uint32_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr size_t NUMBER_EXTRA_BITS = 4;
};
struct TestFleckInMemoryExtraBits4P32InMem : TestFleckInMemoryExtraBits4P32 {
    static constexpr bool IN_MEMORY = true;
};
// extra bits tests
struct TestFleckInMemoryExtraBits6 : TestFleckInMemory {
    typedef uint64_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr size_t NUMBER_EXTRA_BITS = 6;
};
struct TestFleckInMemoryExtraBits4 : TestFleckInMemory {
    typedef uint64_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr size_t NUMBER_EXTRA_BITS = 4;
};
struct TestFleckInMemoryExtraBits2 : TestFleckInMemory {
    typedef uint64_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr size_t NUMBER_EXTRA_BITS = 2;
};
struct TestFleckInMemoryExtraBits1 : TestFleckInMemory {
    typedef uint64_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr size_t NUMBER_EXTRA_BITS = 1; // disabled
};

struct TestFleckInMemoryNoExpand : TestFleckInMemory {
    static constexpr bool EXPAND = false;
};
struct TestDHTInMemory: TestFleckInMemory{
    // read offset strategy
    static constexpr size_t READ_OFF_STRATEGY = 20;
    static constexpr size_t WRITE_STRATEGY = 20;
};
struct TestDHTInMemoryInMem: TestDHTInMemory {
    static constexpr bool IN_MEMORY = true; // in memory
};
struct TestRealDHTInMemory: TestFleckInMemory{
    // read offset strategy
    static constexpr size_t READ_OFF_STRATEGY = 20;
    static constexpr size_t WRITE_STRATEGY = 20;
    static constexpr bool DHT_EVERYTHING = true;
    static constexpr int SAFETY_PAYLOADS = 0;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;
};

struct TestRealDHTInMemoryInMem: TestRealDHTInMemory {
    static constexpr bool IN_MEMORY = true; // in memory
};

struct TestRead: DefaultTraits {
    static constexpr int SAFETY_PAYLOADS = 0;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;
    // disable filters
    static constexpr size_t NUMBER_EXTRA_BITS = 1; // disabled
    // read offset strategy
    static constexpr size_t READ_OFF_STRATEGY = 0;
};

struct TestBugWrite : DefaultTraits {
    // almost no overflows 128 as payload len
    static constexpr int SAFETY_PAYLOADS = 64;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;

    // read offset strategy
    static constexpr size_t READ_OFF_STRATEGY = 0;
};

struct TestMT: TestRead {
    static constexpr int SAFETY_PAYLOADS = 0;
    static constexpr int PAYLOADS_LENGTH = N / BITS_PER_ENTRY + SAFETY_PAYLOADS;
};


struct TestMT4ReserveBits: TestMT{
    static constexpr size_t NUMBER_EXTRA_BITS = 4;
};

struct TestRead20: TestRead {
    static constexpr float MAX_LF = 0.5;
    static constexpr bool BATCH_EVICTION = true;
    static constexpr size_t BUFFER_POOL_CAP = (BASE_EXP_INSERT_SIZE * 4 / 5);
};

struct TestRead10: TestRead {
    static constexpr float MAX_LF = 0.5;
    static constexpr bool BATCH_EVICTION = true;
    static constexpr size_t BUFFER_POOL_CAP = (BASE_EXP_INSERT_SIZE * 4 / 10);
};

struct TestRead5: TestRead {
    static constexpr float MAX_LF = 0.5;
    static constexpr bool BATCH_EVICTION = true;
    static constexpr size_t BUFFER_POOL_CAP = (BASE_EXP_INSERT_SIZE * 4 / 20);
};

struct TestRead20DHT: TestRead20 {
    static constexpr size_t READ_OFF_STRATEGY = 20; // DHT
};

struct TestRead10DHT: TestRead10 {
    static constexpr size_t READ_OFF_STRATEGY = 20; // DHT
};

struct TestRead5DHT: TestRead5 {
    static constexpr size_t READ_OFF_STRATEGY = 20; // DHT
};

struct TestPayloadUint8: DefaultTraits {
    typedef uint8_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr size_t NUMBER_EXTRA_BITS = 0;
};

struct TestPayloadUint16: DefaultTraits {
    typedef uint16_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr size_t NUMBER_EXTRA_BITS = 0; 
};

struct TestPayloadUint32: DefaultTraits {
    typedef uint32_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr size_t NUMBER_EXTRA_BITS = 0; 
};

struct TestPayloadUint64: DefaultTraits {
    typedef uint64_t PAYLOAD_TYPE;  // Type used for payload data.
    static constexpr size_t NUMBER_EXTRA_BITS = 0;
};

struct TestPayloadUint8ExtraBits: DefaultTraits {
    typedef uint8_t PAYLOAD_TYPE;  // Type used for payload data.
};

struct TestPayloadUint16ExtraBits: DefaultTraits {
    typedef uint16_t PAYLOAD_TYPE;  // Type used for payload data.
};

struct TestPayloadUint32ExtraBits: DefaultTraits {
    typedef uint32_t PAYLOAD_TYPE;  // Type used for payload data.
};

struct TestPayloadUint64ExtraBits: DefaultTraits {
    typedef uint64_t PAYLOAD_TYPE;  // Type used for payload data.
};
