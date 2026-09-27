// Copyright 2016 - 2026 The QLever Authors, in particular:
//
// 2016 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARYONDISK_H
#define QLEVER_SRC_INDEX_VOCABULARYONDISK_H

#include <atomic>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "index/vocabulary/VocabularyBinarySearchMixin.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Algorithm.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/IoUringManager.h"
#include "util/Iterators.h"
#include "util/Serializer/Serializer.h"
#include "util/ThreadSafeQueue.h"

// On-disk vocabulary of strings. Each entry is a pair of <ID, String>. The IDs
// are ascending, but not (necessarily) contiguous. If the strings are sorted,
// then binary search for a string can be performed.
class VocabularyOnDisk : public VocabularyBinarySearchMixin<VocabularyOnDisk> {
 private:
  // The offset of a word in the underlying file.
  using Offset = uint64_t;
  // The file in which the words are stored.
  ad_utility::File file_;

  // The file in which the offsets of the words are stored. It contains one
  // `Offset` per word, plus a final offset that marks the end of the last
  // word, followed by an `MmapVectorMetaData` trailer at the end of the file
  // that records the number of offsets. The number of words is therefore the
  // number of stored offsets minus one.
  ad_utility::File offsetsFile_;

  // The number of words stored in the vocabulary.
  size_t size_ = 0;

  // Pool persistent `BatchManagerBase` instances for `lookupBatch` as the
  // fallback for threads that do not own a ring (see `threadRingBudget_`
  // below). Such a thread pops a manager, runs both read phases of one
  // `lookupBatch` call through it, and returns it.
  mutable std::unique_ptr<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchManagerBase>>>
      ioManagers_;

  // Share per-vocabulary state with the thread-local rings (see
  // `threadLocalManager`). Use shared ownership to keep `VocabularyOnDisk`
  // movable, and let the thread-local rings hold only weak references, so
  // entries of a destroyed (or reopened) vocabulary expire and are pruned.
  // Keep this non-null; only a moved-from vocabulary has a null budget, and it
  // must not be used for lookups anyway (its `ioManagers_` is null as well).
  struct ThreadRingBudget {
    std::atomic<size_t> numOwnedRings_{0};
    // Store the initial `io_uring` preference set by `open()`. Each thread
    // loads it once when it creates its owned ring, so a failed
    // `io_uring_queue_init` degrades only that thread to the synchronous
    // fallback. Make it atomic, so a thread that reads the value stored by
    // `open()` also sees the state initialized before it.
    std::atomic<bool> preferIoUring_{true};
  };
  mutable std::shared_ptr<ThreadRingBudget> threadRingBudget_{
      std::make_shared<ThreadRingBudget>()};

  // This suffix is appended to the filename of the main file, in order to get
  // the name for the file in which IDs and offsets are stored.
  static constexpr std::string_view offsetSuffix_ = ".offsets";

 public:
  // A helper class that is used to build a vocabulary word by word.
  // Each call to `operator()` adds the next word to the vocabulary.
  // At the end, the `finish()` method can be called. Note that `finish`
  // is also implicitly called by the destructor, but doing so implicitly
  // releases resources earlier and is cleaner in case of exceptions.
  class WordWriter : public WordWriterBase {
   private:
    ad_utility::File file_;
    ad_utility::File offsetsFile_;
    uint64_t currentOffset_ = 0;
    uint64_t numWords_ = 0;

   public:
    // Constructor, used by `VocabularyOnDisk::wordWriter`.
    explicit WordWriter(const std::string& filename);
    // Add the next word to the vocabulary and return its index.
    uint64_t operator()(std::string_view word, bool isExternalDummy) override;

    ~WordWriter() override;

   private:
    // Finish the writing. After this no more calls to `operator()` are allowed.
    void finishImpl() override;
  };

  // The words are stored under the base filename itself, the IDs and offsets
  // in an additional file (see `offsetSuffix_`).
  static FileSuffixes fileSuffixes() {
    return {"", std::string{offsetSuffix_}};
  }

  // Open the vocabulary from file. It must have been previously written to
  // this file via a `WordWriter`. `preferIoUring` selects the backend of the
  // pooled managers and is the initial preference each thread copies when it
  // creates its owned ring (see `threadLocalManager`); `false` forces the
  // synchronous `pread` fallback everywhere, which is also what the tests use
  // to cover that backend. Calling `open` again discards all rings that
  // threads own for this vocabulary, so the new preference applies to every
  // later `lookupBatch`. Like all other members set here, `open` must not run
  // concurrently with lookups on the same vocabulary.
  void open(const std::string& filename, bool preferIoUring = true);

  // Return the number of threads that currently own a ring for this
  // vocabulary (at most `NUM_VOCAB_BATCH_IO_MANAGERS`).
  size_t numOwnedRingsForTesting() const;

  // Return the word that is stored at the index. Throw an exception if `idx >=
  // size`.
  std::string operator[](uint64_t idx) const;

  // Efficient iteration over all words in the vocabulary, in order, yielded as
  // `IndexAndWord`s (the word as a `string_view` together with its index).
  // Internally the words are read in batches, each produced by two large
  // sequential reads (offsets and word data). This is much faster than looking
  // up the words one at a time via `operator[]`, which performs two small
  // `pread`s and allocates a string per word. A batch is bounded both in the
  // number of words and in the number of bytes of word data it holds (but
  // always contains at least one word, even if that word alone exceeds the byte
  // limit).
  VocabularyScanRange scanAll() const;

  //____________________________________________________________________________
  VocabBatchLookupResult lookupBatch(ql::span<const size_t> indices) const;

  //____________________________________________________________________________
  VocabLookupOutput lookupBatchesStreamed(
      VocabLookupInput rangeOfIndexBatches) const;

  // Get the number of words in the vocabulary.
  size_t size() const { return size_; }

  // Default constructor for an empty vocabulary.
  VocabularyOnDisk() = default;

  // `VocabularyOnDisk` is movable, but not copyable.
  VocabularyOnDisk(VocabularyOnDisk&&) noexcept = default;
  VocabularyOnDisk& operator=(VocabularyOnDisk&&) noexcept = default;

  // The offset of a word in `file_` and its size in number of bytes.
  struct OffsetAndSize {
    uint64_t offset_;
    uint64_t size_;
  };

  // The `Accessor` for the `IteratorForAccessOperator` class below.
  struct Accessor {
    template <typename Voc>
    constexpr auto operator()(const Voc& vocabulary, uint64_t index) const {
      return vocabulary[index];
    }
  };
  // Const random access iterators, implemented via the
  // `IteratorForAccessOperator` template.
  using const_iterator =
      ad_utility::IteratorForAccessOperator<VocabularyOnDisk, Accessor>;
  const_iterator begin() const { return {this, 0}; }
  const_iterator end() const { return {this, size()}; }

  // Convert an iterator to the corresponding `WordAndIndex`. Needed for the
  // Mixin base class
  WordAndIndex iteratorToWordAndIndex(const_iterator it) const {
    if (it == end()) {
      return WordAndIndex::end();
    } else {
      return {*it, static_cast<uint64_t>(it - begin())};
    }
  }

  // Generic serialization support.
  AD_SERIALIZE_FRIEND_FUNCTION(VocabularyOnDisk) {
    (void)serializer;
    (void)arg;
    throw std::runtime_error(
        "Generic serialization is not implemented for VocabularyOnDisk.");
  }

 private:
  // Get the `OffsetAndSize` for the element with the `idx`. Return
  // `std::nullopt` if `idx` is not contained in the vocabulary.
  OffsetAndSize getOffsetAndSize(uint64_t idx) const;

  // Helper for `scanAll`: return a lazy input range that reads the word offsets
  // from the `.offsets` file in batches of at most
  // `VOCABULARY_SCAN_MAX_WORDS_PER_BATCH` words. Each element is a span over
  // the offsets of one batch, with one trailing entry marking the end of the
  // last word.
  auto readOffsetsInBatches() const;

  // Helper for `scanAll`: given the `offsets` of a single chunk of words (a
  // span over the chunk's offsets, with one trailing entry marking the end of
  // the last word), return a lazy input range that yields each word of the
  // chunk as a `string_view`, reading the word data from disk in sub-batches of
  // at most `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH` bytes. The return type is
  // deduced, so this function can only be used within `VocabularyOnDisk.cpp`.
  auto chunkToWords(ql::span<const uint64_t> offsets) const;

  // A word's start offset and the start offset of the following word (which
  // marks the end of the word), stored contiguously in the `.offsets` file.
  struct OffsetPair {
    uint64_t offset_;
    uint64_t nextOffset_;
  };

  // Return the calling thread's exclusively owned ring for this vocabulary,
  // or `nullptr` if `NUM_VOCAB_BATCH_IO_MANAGERS` other threads already own
  // one (then use the shared `ioManagers_` pool). Create the ring on first use
  // via `makeBatchManager` and destroy it at thread teardown. Drive it only
  // from the calling thread, which needs no lock on its I/O path.
  ad_utility::BatchManagerBase* threadLocalManager() const;

  // Run both read phases of one `lookupBatch` call through `manager`.
  VocabBatchLookupResult lookupBatchVia(ad_utility::BatchManagerBase& manager,
                                        ql::span<const size_t> indices) const;

  // Phase 1 of `lookupBatch`: for each requested index, read its `OffsetPair`
  // (16 bytes) from the `.offsets` file in a single batched read via `manager`.
  std::vector<OffsetPair> readOffsetPairs(ad_utility::BatchManagerBase& manager,
                                          ql::span<const size_t> indices) const;

  // Phase 2 of `lookupBatch`: given the `offsetPairs` from phase 1, read the
  // string data from `file_` into one contiguous buffer in a single batched
  // read via `manager`, and return it as a `VocabBatchLookupResult`.
  VocabBatchLookupResult readStrings(
      ad_utility::BatchManagerBase& manager,
      ql::span<const OffsetPair> offsetPairs) const;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARYONDISK_H
