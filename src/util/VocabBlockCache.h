// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_VOCABBLOCKCACHE_H
#define QLEVER_SRC_UTIL_VOCABBLOCKCACHE_H

#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "util/HashMap.h"

namespace ad_utility::vocab {

// Userspace block cache for the disk reads of `VocabularyOnDisk`.
//
// Motivation: with `O_DIRECT` vocabulary reads (ad-freiburg/qlever#3480) the
// kernel page cache no longer deduplicates repeated reads of the same disk
// blocks. This cache restores that reuse in userspace.
//
// The vocabulary index files are IMMUTABLE after the index build: they are
// written once and only ever read afterwards. A cached block can therefore
// never go stale, so no invalidation or write-back logic is needed (there is
// deliberately not even an API to invalidate entries).
//
// Design:
// * Fixed pool of `blockSize()` slots (4 KiB by default, a multiple of 4 KiB),
//   each aligned for `O_DIRECT` via `posix_memalign`.
// * Tag index keyed by `(device, inode, block number)`; the device+inode pair
//   identifies the underlying file, so entries from different vocabularies
//   never alias.
// * Clock-hand eviction: each slot carries a reference bit that `lookup` (and
//   re-insertion of the same key) sets and eviction clears, so frequently
//   reused blocks survive over merely inserted ones.
// * Per-thread sharding: each thread owns its shard (see
//   `threadLocalVocabBlockCache`), so the lookup path takes no locks.
//
// Each instance is single-threaded (no internal synchronization); sharing one
// instance across threads requires external synchronization.
class VocabBlockCache {
 public:
  // The default size of one cache slot in bytes. Matches the common
  // filesystem block and `O_DIRECT` alignment granularity. Every block size
  // must be a positive multiple of it.
  static constexpr size_t kDefaultBlockSize = 4096;

  // Create a cache with `numBlocks` slots of `blockSize` bytes. `0` blocks
  // means "no storage": `lookup` always misses and `insert` is a no-op.
  explicit VocabBlockCache(size_t numBlocks = 0,
                           size_t blockSize = kDefaultBlockSize);
  ~VocabBlockCache();

  VocabBlockCache(const VocabBlockCache&) = delete;
  VocabBlockCache& operator=(const VocabBlockCache&) = delete;
  VocabBlockCache(VocabBlockCache&& other) noexcept;
  VocabBlockCache& operator=(VocabBlockCache&& other) noexcept;

  // The number of block slots (capacity, not occupancy).
  size_t capacity() const { return slots_.size(); }
  // The size of one block (slot) in bytes.
  size_t blockSize() const { return blockSize_; }
  // The number of currently occupied slots.
  size_t size() const;
  // Drop all entries and resize to `numBlocks` slots of `blockSize` bytes.
  void resize(size_t numBlocks, size_t blockSize = kDefaultBlockSize);

  // If block `blockNo` of the file identified by `(dev, ino)` is cached,
  // return a pointer to its `blockSize()` bytes (valid until the next `insert`
  // or `resize` on this instance) and set its clock reference bit. Return
  // `nullptr` on a miss.
  const char* lookup(dev_t dev, ino_t ino, uint64_t blockNo);
  // Store `blockSize()` bytes from `data` under the given key, overwriting any
  // previous entry for the same key. If the cache is full, evict the block
  // designated by the clock hand. No-op when `capacity() == 0`.
  void insert(dev_t dev, ino_t ino, uint64_t blockNo, const char* data);

  // Cumulative statistics (for tests and monitoring).
  uint64_t numHits() const { return numHits_; }
  uint64_t numMisses() const { return numMisses_; }
  uint64_t numEvictions() const { return numEvictions_; }

 private:
  struct Slot {
    dev_t dev_ = 0;
    ino_t ino_ = 0;
    uint64_t blockNo_ = 0;
    bool occupied_ = false;
    // Clock reference bit: set by `lookup` and by overwrite-insert of an
    // existing key, cleared by eviction. Fresh inserts start cleared.
    bool referenced_ = false;
    // Points into `storage_`, `nullptr` when `capacity() == 0`.
    char* data_ = nullptr;
  };

  // The key of a cached block.
  struct Key {
    dev_t dev_;
    ino_t ino_;
    uint64_t blockNo_;
    bool operator==(const Key& other) const {
      return dev_ == other.dev_ && ino_ == other.ino_ &&
             blockNo_ == other.blockNo_;
    }
    template <typename H>
    friend H AbslHashValue(H h, const Key& key) {
      return H::combine(std::move(h), key.dev_, key.ino_, key.blockNo_);
    }
  };

  // Find the slot holding `key`, or `nullptr` if absent (hash lookup in
  // `slotOfKey_`; the cache can hold millions of blocks).
  Slot* findSlot(dev_t dev, ino_t ino, uint64_t blockNo);
  // Free `storage_` and reset all members.
  void clear();

  // Deleter for the `posix_memalign`-allocated `storage_` chunk below (plain
  // `unique_ptr<char[]>` would release with `delete[]`, which must not be
  // paired with `posix_memalign`).
  struct FreeDeleter {
    void operator()(void* p) const noexcept { ::free(p); }
  };

  std::vector<Slot> slots_;
  // The slot index of every occupied slot, by key.
  ad_utility::HashMap<Key, size_t> slotOfKey_;
  // Single aligned chunk of `capacity() * blockSize()` bytes (or `nullptr`
  // when `capacity() == 0`). Exclusive ownership: acquisition happens only
  // in `resize`, release only in `clear` (called by the destructor and the
  // move assignment), so the lifetime needs no manual `::free` calls.
  std::unique_ptr<char[], FreeDeleter> storage_;
  size_t blockSize_ = kDefaultBlockSize;
  // Clock-hand position for eviction.
  size_t hand_ = 0;
  uint64_t numHits_ = 0;
  uint64_t numMisses_ = 0;
  uint64_t numEvictions_ = 0;
};

// Return the calling thread's cache shard, resizing it to `numBlocks` slots of
// `blockSize` bytes first if needed (resizing drops all cached content). Each
// thread gets its own shard, so concurrent lookups from different threads take
// no locks.
VocabBlockCache& threadLocalVocabBlockCache(
    size_t numBlocks, size_t blockSize = VocabBlockCache::kDefaultBlockSize);

// Process-wide counters of the block cache in front of the `O_DIRECT`
// vocabulary reads (see `BatchReadOptions::blockCacheNumBlocks`): requests
// served from a cached block, requests that joined a read of their block that
// was still in flight for the same batch, block reads issued because the block
// was neither cached nor in flight, blocks inserted after such a read, and
// cached blocks evicted to make room. Monotonic; callers log differences (e.g.
// per export).
struct VocabBlockCacheCounters {
  std::atomic<uint64_t> hits_{0};
  std::atomic<uint64_t> inFlightHits_{0};
  std::atomic<uint64_t> misses_{0};
  std::atomic<uint64_t> inserts_{0};
  std::atomic<uint64_t> evictions_{0};
};
inline VocabBlockCacheCounters vocabBlockCacheCounters;

}  // namespace ad_utility::vocab

#endif  // QLEVER_SRC_UTIL_VOCABBLOCKCACHE_H
