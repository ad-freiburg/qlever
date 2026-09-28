// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/VocabBlockCache.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>

#include "util/Exception.h"

namespace ad_utility::vocab {

// _____________________________________________________________________________
VocabBlockCache::VocabBlockCache(size_t numBlocks, size_t blockSize) {
  resize(numBlocks, blockSize);
}

// _____________________________________________________________________________
VocabBlockCache::~VocabBlockCache() { clear(); }

// _____________________________________________________________________________
VocabBlockCache::VocabBlockCache(VocabBlockCache&& other) noexcept
    : slots_{std::move(other.slots_)},
      slotOfKey_{std::move(other.slotOfKey_)},
      storage_{std::exchange(other.storage_, nullptr)},
      blockSize_{other.blockSize_},
      hand_{std::exchange(other.hand_, 0)},
      numHits_{std::exchange(other.numHits_, 0)},
      numMisses_{std::exchange(other.numMisses_, 0)},
      numEvictions_{std::exchange(other.numEvictions_, 0)} {
  // The moved-from `other` transferred its storage above; its slots are only
  // ever destroyed, cleared, or reassigned afterwards, never dereferenced.
}

// _____________________________________________________________________________
VocabBlockCache& VocabBlockCache::operator=(VocabBlockCache&& other) noexcept {
  if (this != &other) {
    clear();
    slots_ = std::move(other.slots_);
    slotOfKey_ = std::move(other.slotOfKey_);
    storage_ = std::exchange(other.storage_, nullptr);
    blockSize_ = other.blockSize_;
    hand_ = std::exchange(other.hand_, 0);
    numHits_ = std::exchange(other.numHits_, 0);
    numMisses_ = std::exchange(other.numMisses_, 0);
    numEvictions_ = std::exchange(other.numEvictions_, 0);
  }
  return *this;
}

// _____________________________________________________________________________
size_t VocabBlockCache::size() const { return slotOfKey_.size(); }

// _____________________________________________________________________________
void VocabBlockCache::resize(size_t numBlocks, size_t blockSize) {
  if (blockSize == 0 || blockSize % kDefaultBlockSize != 0) {
    AD_THROW("vocabulary block cache block size " + std::to_string(blockSize) +
             " is not a positive multiple of " +
             std::to_string(kDefaultBlockSize));
  }
  // Guard the allocation-size multiplication: an unchecked wrap would
  // allocate an undersized chunk and corrupt the heap on the first `insert`.
  if (numBlocks > SIZE_MAX / blockSize) {
    AD_THROW("vocabulary block cache size " + std::to_string(numBlocks) +
             " blocks would overflow the allocation size");
  }
  // Build the new state in locals first and only commit once every
  // potentially throwing step succeeded, so a failed allocation keeps the
  // previous content untouched (strong guarantee).
  void* raw = nullptr;
  if (numBlocks > 0 &&
      ::posix_memalign(&raw, kDefaultBlockSize, numBlocks * blockSize) != 0) {
    AD_THROW("Failed to allocate " + std::to_string(numBlocks) + " blocks of " +
             std::to_string(blockSize) +
             " bytes for the vocabulary block cache");
  }
  std::unique_ptr<char[], FreeDeleter> newStorage{static_cast<char*>(raw)};
  std::vector<Slot> newSlots;
  newSlots.resize(numBlocks);
  for (size_t i = 0; i < numBlocks; ++i) {
    newSlots[i].data_ = newStorage.get() + i * blockSize;
  }
  clear();
  storage_ = std::move(newStorage);
  slots_ = std::move(newSlots);
  blockSize_ = blockSize;
  slotOfKey_.reserve(numBlocks);
  hand_ = 0;
}

// _____________________________________________________________________________
VocabBlockCache::Slot* VocabBlockCache::findSlot(dev_t dev, ino_t ino,
                                                 uint64_t blockNo) {
  auto it = slotOfKey_.find(Key{dev, ino, blockNo});
  return it == slotOfKey_.end() ? nullptr : &slots_[it->second];
}

// _____________________________________________________________________________
const char* VocabBlockCache::lookup(dev_t dev, ino_t ino, uint64_t blockNo) {
  Slot* slot = findSlot(dev, ino, blockNo);
  if (slot == nullptr) {
    ++numMisses_;
    return nullptr;
  }
  slot->referenced_ = true;
  ++numHits_;
  return slot->data_;
}

// _____________________________________________________________________________
void VocabBlockCache::insert(dev_t dev, ino_t ino, uint64_t blockNo,
                             const char* data) {
  if (slots_.empty()) {
    return;
  }
  // Overwrite the existing entry for an immutable file re-read.
  if (Slot* slot = findSlot(dev, ino, blockNo)) {
    std::memcpy(slot->data_, data, blockSize_);
    slot->referenced_ = true;
    return;
  }
  // Clock-hand eviction: give referenced slots a second chance.
  while (true) {
    Slot& slot = slots_[hand_];
    if (!slot.occupied_) {
      break;
    }
    if (!slot.referenced_) {
      break;
    }
    slot.referenced_ = false;
    hand_ = (hand_ + 1) % slots_.size();
  }
  Slot& slot = slots_[hand_];
  if (slot.occupied_) {
    ++numEvictions_;
    vocabBlockCacheCounters.evictions_.fetch_add(1, std::memory_order_relaxed);
    slotOfKey_.erase(Key{slot.dev_, slot.ino_, slot.blockNo_});
  }
  slotOfKey_[Key{dev, ino, blockNo}] = hand_;
  slot.dev_ = dev;
  slot.ino_ = ino;
  slot.blockNo_ = blockNo;
  slot.occupied_ = true;
  // Fresh inserts start unreferenced: only `lookup` (and overwrite-insert of
  // an existing key) sets the bit, so an explicitly re-referenced block wins
  // the next eviction sweep over a merely inserted one.
  slot.referenced_ = false;
  std::memcpy(slot.data_, data, blockSize_);
  hand_ = (hand_ + 1) % slots_.size();
}

// _____________________________________________________________________________
void VocabBlockCache::clear() {
  storage_.reset();
  slots_.clear();
  slots_.shrink_to_fit();
  slotOfKey_.clear();
  hand_ = 0;
}

// _____________________________________________________________________________
VocabBlockCache& threadLocalVocabBlockCache(size_t numBlocks,
                                            size_t blockSize) {
  thread_local VocabBlockCache cache;
  if (cache.capacity() != numBlocks || cache.blockSize() != blockSize) {
    cache.resize(numBlocks, blockSize);
  }
  return cache;
}

}  // namespace ad_utility::vocab
