// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABBLOCKCACHE_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABBLOCKCACHE_H

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <vector>

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
// * Fixed pool of `kBlockSize` (4 KiB) slots, each aligned for `O_DIRECT` via
//   `posix_memalign`.
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
  // The size of one cache slot in bytes. Matches the common filesystem block
  // and `O_DIRECT` alignment granularity.
  static constexpr size_t kBlockSize = 4096;

  // Create a cache with `numBlocks` slots. `0` means "no storage": `lookup`
  // always misses and `insert` is a no-op.
  explicit VocabBlockCache(size_t numBlocks = 0);
  ~VocabBlockCache();

  VocabBlockCache(const VocabBlockCache&) = delete;
  VocabBlockCache& operator=(const VocabBlockCache&) = delete;
  VocabBlockCache(VocabBlockCache&& other) noexcept;
  VocabBlockCache& operator=(VocabBlockCache&& other) noexcept;

  // The number of block slots (capacity, not occupancy).
  size_t capacity() const { return slots_.size(); }
  // The number of currently occupied slots.
  size_t size() const;
  // Drop all entries and resize to `numBlocks` slots.
  void resize(size_t numBlocks);

  // If block `blockNo` of the file identified by `(dev, ino)` is cached,
  // return a pointer to its `kBlockSize` bytes (valid until the next `insert`
  // or `resize` on this instance) and set its clock reference bit. Return
  // `nullptr` on a miss.
  const char* lookup(dev_t dev, ino_t ino, uint64_t blockNo);
  // Store `kBlockSize` bytes from `data` under the given key, overwriting any
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

  // Find the slot holding `key`, or `nullptr` if absent. Linear scan: the
  // capacity is small in practice (tens of blocks, see the
  // `vocab-block-cache-size` runtime parameter), so a hash index would only
  // add overhead. Revisit if profiling ever shows this lookup as hot.
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
  // Single aligned chunk of `capacity() * kBlockSize` bytes (or `nullptr`
  // when `capacity() == 0`). Exclusive ownership: acquisition happens only
  // in `resize`, release only in `clear` (called by the destructor and the
  // move assignment), so the lifetime needs no manual `::free` calls.
  std::unique_ptr<char[], FreeDeleter> storage_;
  // Clock-hand position for eviction.
  size_t hand_ = 0;
  uint64_t numHits_ = 0;
  uint64_t numMisses_ = 0;
  uint64_t numEvictions_ = 0;
};

// Return the calling thread's cache shard, resizing it to `numBlocks` slots
// first if needed (resizing drops all cached content). Each thread gets its
// own shard, so concurrent lookups from different threads take no locks.
VocabBlockCache& threadLocalVocabBlockCache(size_t numBlocks);

}  // namespace ad_utility::vocab

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABBLOCKCACHE_H
