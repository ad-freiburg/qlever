// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022        Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H

#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <range/v3/view/enumerate.hpp>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/CompactStringVector.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Iterators.h"
#include "util/TransparentFunctors.h"
#include "util/TypeTraits.h"
#include "util/Views.h"

// _____________________________________________________________________________
// Frozen owner of a batch's `string_view`s. Builders allocate and write, then
// move the populated views and the backing storage into a derived class;
// the result is observed as a `VocabBatchLookupResult` holding a
// `shared_ptr<const VocabBatchStorage>` with no mutators. `viewSpan()` is not
// virtual: it always returns this object's `views_`, which only a builder may
// fill via a derived constructor.
class VocabBatchStorage {
  std::vector<std::string_view> views_;

 protected:
  explicit VocabBatchStorage(std::vector<std::string_view> views)
      : views_{std::move(views)} {}

  // Assign the views during derived-class construction, after the backing
  // storage member has been moved into place (views must always point at the
  // final storage location, never at a moved-from temporary).
  void setViews(std::vector<std::string_view> views) {
    views_ = std::move(views);
  }

 public:
  virtual ~VocabBatchStorage() = default;
  VocabBatchStorage(const VocabBatchStorage&) = delete;
  VocabBatchStorage& operator=(const VocabBatchStorage&) = delete;
  VocabBatchStorage(VocabBatchStorage&&) = delete;
  VocabBatchStorage& operator=(VocabBatchStorage&&) = delete;

  [[nodiscard]] ql::span<const std::string_view> viewSpan() const noexcept {
    return views_;
  }
};

// Shared ownership of frozen batch storage. Assemblers retain these tokens
// for every child whose views they copy.
using VocabBatchOwner = std::shared_ptr<const VocabBatchStorage>;

class MultiSourceVocabBatchAssembler;

// _____________________________________________________________________________
// Batch lookup result: views are always `storage_->viewSpan()`. There is no
// constructor that takes an owner and a span independently. Copies share the
// storage. A view stays valid as long as any copy of the result lives, with
// one documented exception: a view that the producing vocabulary placed via
// `MultiSourceVocabBatchAssembler::assignUnownedViewAtPosition` points into
// that vocabulary's own memory and is valid as long as the vocabulary lives.
class VocabBatchLookupResult {
 private:
  VocabBatchOwner storage_{};
  ql::span<const std::string_view> span_{};

  // Assemblers take over `storage_` of the sub-batches they re-arrange, to
  // keep the bytes behind the re-arranged views alive.
  friend class MultiSourceVocabBatchAssembler;

 public:
  VocabBatchLookupResult() = default;

  // No custom cleanup: `storage_` owns the bytes via `shared_ptr` and
  // `span_` is a non-owning view. Declared explicitly because this class
  // manages shared ownership through the moves below (Sonar cpp:S3624).
  ~VocabBatchLookupResult() = default;

  explicit VocabBatchLookupResult(VocabBatchOwner storage)
      : storage_{std::move(storage)},
        span_{storage_ ? storage_->viewSpan()
                       : ql::span<const std::string_view>{}} {}

  // Moves reset the source span, so a moved-from result is empty (rather than
  // a null owner paired with a stale view into the moved-to storage).
  VocabBatchLookupResult(VocabBatchLookupResult&& other) noexcept
      : storage_{std::move(other.storage_)}, span_{other.span_} {
    other.span_ = {};
  }
  VocabBatchLookupResult& operator=(VocabBatchLookupResult&& other) noexcept {
    if (this != &other) {
      storage_ = std::move(other.storage_);
      span_ = other.span_;
      other.span_ = {};
    }
    return *this;
  }

  // Copies are cheap and safe (`shared_ptr` + span share ownership of the
  // frozen storage), so they stay available for future SplitVocabulary/merge
  // code; declared explicitly so the user-declared moves above do not leave
  // them only implicitly deleted.
  VocabBatchLookupResult(const VocabBatchLookupResult&) = default;
  VocabBatchLookupResult& operator=(const VocabBatchLookupResult&) = default;

  // Provide the container and range interface.
  [[nodiscard]] size_t size() const noexcept { return span_.size(); }
  [[nodiscard]] bool empty() const noexcept { return span_.empty(); }
  [[nodiscard]] std::string_view operator[](size_t index) const {
    AD_CONTRACT_CHECK(index < span_.size());
    return span_[index];
  }
  [[nodiscard]] auto begin() const noexcept { return span_.begin(); }
  [[nodiscard]] auto end() const noexcept { return span_.end(); }
  [[nodiscard]] const std::string_view* data() const noexcept {
    return span_.data();
  }

  // Stored views and backing bytes are not exposed for mutation. Callers may
  // inspect the immutable range.
};

// Type-erased input range of batches (each batch consists of a vector of
// indices into the underlying Vocabulary, specifying which terms' string
// representations need to be read from the underlying Vocabulary).
using VocabLookupInput = ad_utility::InputRangeTypeErased<std::vector<size_t>>;

// Type-erased output range of batch-lookup results (which are the string
// representations of the terms specified by `VocabLookupInput` to be read).
using VocabLookupOutput =
    ad_utility::InputRangeTypeErased<VocabBatchLookupResult>;

// _____________________________________________________________________________
// Represent a strong, self-contained batch-lookup result backed by a
// contiguous character buffer (used for reading fixed-size chunks from disk).
// Keep all storage private.
class ContiguousVocabBatchBuilder;

class ContiguousVocabBatchLookupData : public VocabBatchStorage {
 public:
  class Passkey {
   private:
    friend class ContiguousVocabBatchBuilder;
    explicit Passkey() = default;
  };

  ContiguousVocabBatchLookupData(Passkey, std::vector<char> buffer,
                                 std::vector<std::string_view> views)
      : VocabBatchStorage(std::move(views)), buffer_{std::move(buffer)} {}

  static VocabBatchLookupResult asResult(
      std::shared_ptr<ContiguousVocabBatchLookupData> self) {
    return VocabBatchLookupResult{
        std::shared_ptr<const VocabBatchStorage>{std::move(self)}};
  }

 private:
  std::vector<char> buffer_;
};

// _____________________________________________________________________________
// Builder for a contiguous batch lookup result. Allocates single contiguous
// memory for all requested word sizes, generates direct destination targets for
// asynchronous I/O (e.g. io_uring), and pre-computes the string_view for each
// word at its fixed offset before any I/O happens.
class ContiguousVocabBatchBuilder {
 private:
  std::vector<char> buffer_;
  std::vector<std::string_view> views_;
  std::vector<size_t> targetOffsets_;

 public:
  // Copying would duplicate `views_` pointers that refer to this builder's own
  // `buffer_`, leaving the copy with pointers into foreign storage. Moving is
  // safe: `std::vector` moves transfer the heap buffer, so the pointers keep
  // referring to the moved (now owned) storage.
  ContiguousVocabBatchBuilder(const ContiguousVocabBatchBuilder&) = delete;
  ContiguousVocabBatchBuilder& operator=(const ContiguousVocabBatchBuilder&) =
      delete;
  ContiguousVocabBatchBuilder(ContiguousVocabBatchBuilder&&) = default;
  ContiguousVocabBatchBuilder& operator=(ContiguousVocabBatchBuilder&&) =
      default;

  explicit ContiguousVocabBatchBuilder(ql::span<const size_t> wordSizes) {
    AD_CONTRACT_CHECK(!wordSizes.empty());
    size_t totalBytes = 0;
    for (size_t size : wordSizes) {
      AD_CONTRACT_CHECK(size <= SIZE_MAX - totalBytes);
      totalBytes += size;
    }
    buffer_.resize(totalBytes == 0 ? 1 : totalBytes);
    views_.reserve(wordSizes.size());
    targetOffsets_.reserve(wordSizes.size());

    size_t offset = 0;
    for (size_t size : wordSizes) {
      char* target = buffer_.data() + offset;
      targetOffsets_.push_back(offset);
      views_.emplace_back(target, size);
      offset += size;
    }
  }

  // Return an owning pointer array with one destination per requested word.
  // The array can safely outlive this builder's metadata. Its pointers remain
  // valid after `finalize()` because the result takes ownership of `buffer_`.
  // They become invalid when that result is destroyed. Multiple zero-sized
  // words may share an address, but no bytes are written through those entries.
  [[nodiscard]] std::vector<char*> targets() {
    std::vector<char*> result;
    result.reserve(targetOffsets_.size());
    for (size_t offset : targetOffsets_) {
      result.push_back(buffer_.data() + offset);
    }
    return result;
  }

  [[nodiscard]] VocabBatchLookupResult finalize() && {
    AD_CONTRACT_CHECK(!views_.empty());
    auto data = std::make_shared<ContiguousVocabBatchLookupData>(
        ContiguousVocabBatchLookupData::Passkey{}, std::move(buffer_),
        std::move(views_));
    return ContiguousVocabBatchLookupData::asResult(std::move(data));
  }
};

// _____________________________________________________________________________
// PMR `memory_resource` that charges an `AllocatorWithLimit`. Copies of the
// allocator share the same `MemoryLimitTracker` as query `IdTable`s.
class AllocatorAsMemoryResource : public ql::pmr::memory_resource {
  ad_utility::AllocatorWithLimit<std::byte> alloc_;

 public:
  explicit AllocatorAsMemoryResource(
      ad_utility::AllocatorWithLimit<std::byte> alloc)
      : alloc_{std::move(alloc)} {}

 protected:
  // The alignment argument is intentionally ignored: this resource only serves
  // `char` allocations from the arena builders, for which any alignment
  // suffices, and the underlying `AllocatorWithLimit` has no alignment
  // concept (it counts bytes).
  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    (void)alignment;
    return alloc_.allocate(bytes);
  }
  void do_deallocate(void* p, std::size_t bytes,
                     std::size_t alignment) override {
    (void)alignment;
    alloc_.deallocate(static_cast<std::byte*>(p), bytes);
  }
  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

// Strong, self-contained batch-lookup result backed by a PMR monotonic buffer
// resource. `upstream_` (if set) must outlive `buffer_` so deallocation can
// still charge the memory tracker; members are destroyed in reverse order.
class ArenaVocabBatchBuilder;

class PmrVocabBatchLookupData : public VocabBatchStorage {
 public:
  class Passkey {
   private:
    friend class ArenaVocabBatchBuilder;
    explicit Passkey() = default;
  };

  PmrVocabBatchLookupData(
      Passkey, std::unique_ptr<ql::pmr::memory_resource> upstream,
      std::unique_ptr<ql::pmr::monotonic_buffer_resource> buffer,
      std::vector<std::string_view> views)
      : VocabBatchStorage(std::move(views)),
        upstream_{std::move(upstream)},
        buffer_{std::move(buffer)} {}

  static VocabBatchLookupResult asResult(
      std::shared_ptr<PmrVocabBatchLookupData> self) {
    return VocabBatchLookupResult{
        std::shared_ptr<const VocabBatchStorage>{std::move(self)}};
  }

 private:
  std::unique_ptr<ql::pmr::memory_resource> upstream_;
  std::unique_ptr<ql::pmr::monotonic_buffer_resource> buffer_;
};

// _____________________________________________________________________________
// Strong, self-contained batch-lookup result backed by owning std::strings.
class StringVectorVocabBatchLookupData : public VocabBatchStorage {
 private:
  std::vector<std::string> words_;

  static std::vector<std::string_view> viewsInto(
      ql::span<const std::string> words) {
    return ::ranges::to_vector(
        words | ql::views::transform(ad_utility::staticCast<std::string_view>));
  }

 public:
  explicit StringVectorVocabBatchLookupData(std::vector<std::string> words)
      : VocabBatchStorage({}), words_{std::move(words)} {
    // Build the views from `words_` after the move, so they always point at
    // the final owner. Views built from the parameter beforehand would rely
    // on the vector move stealing (not relocating) the string objects, which
    // breaks for short-string (SSO) buffers if the strings are ever moved
    // element-wise instead.
    setViews(viewsInto(words_));
  }

  static VocabBatchLookupResult asResult(
      std::shared_ptr<StringVectorVocabBatchLookupData> self) {
    return VocabBatchLookupResult{
        std::shared_ptr<const VocabBatchStorage>{std::move(self)}};
  }

  // Return the batch-lookup result for the owning `words` (one word per
  // looked-up index, in order). This is the common pattern "store the
  // materialized words, then point one view at each of them" in one call;
  // callers never touch the buffer or the views. Use it only when the words
  // really have to be materialized as `std::string`s; words that are decoded
  // into caller-provided memory go into an `ArenaVocabBatchBuilder` instead,
  // and words that already live in storage that outlives the result need no
  // copy at all (see `MultiSourceVocabBatchAssembler`).
  static VocabBatchLookupResult fromWords(std::vector<std::string> words) {
    AD_CONTRACT_CHECK(!words.empty());
    return asResult(
        std::make_shared<StringVectorVocabBatchLookupData>(std::move(words)));
  }
};

// _____________________________________________________________________________
// Strong, self-contained batch-lookup result that owns multiple independent
// storage owners.
class MultiOwnerVocabBatchLookupData : public VocabBatchStorage {
 public:
  class Passkey {
   private:
    friend class MultiSourceVocabBatchAssembler;
    explicit Passkey() = default;
  };

  MultiOwnerVocabBatchLookupData(Passkey, std::vector<VocabBatchOwner> owners,
                                 std::vector<std::string_view> views)
      : VocabBatchStorage(std::move(views)), owners_{std::move(owners)} {}

  static VocabBatchLookupResult asResult(
      std::shared_ptr<MultiOwnerVocabBatchLookupData> self) {
    return VocabBatchLookupResult{
        std::shared_ptr<const VocabBatchStorage>{std::move(self)}};
  }

 private:
  std::vector<VocabBatchOwner> owners_;
};

// _____________________________________________________________________________
// A single entry yielded by a vocabulary's `scanAll`: a word together with its
// index in the vocabulary. For most vocabularies the indices are simply
// `0, 1, 2, ...`, but e.g. a `SplitVocabulary` yields the (non-contiguous)
// marker-encoded indices that its `operator[]` expects.
//
// IMPORTANT: `word_` is in general a view into a buffer that is reused when
// the range is advanced (e.g. for the on-disk and compressed vocabularies).
// It is therefore only valid until the next element is pulled from the range;
// consume each entry (or copy the word) before advancing.
struct IndexAndWord {
  uint64_t index_;
  std::string_view word_;
};

// A type-erased input range vocabularies can use for `scanAll()`, that yields
// all words of the vocabulary in order, together with their index.
using VocabularyScanRange = ad_utility::InputRangeTypeErased<IndexAndWord>;

// _____________________________________________________________________________
// Decompress a single word into `destination` using `decompress(span)`.
// `destination` must hold exactly the first `bound` bytes passed to
// `decompress`. A `bound` of 0 returns an empty view without calling
// `decompress`. Returns a string_view to the decompressed word.
template <typename DecompressFunc>
std::string_view decompressIntoSpan(ql::span<char> destination, size_t bound,
                                    DecompressFunc&& decompress) {
  if (bound == 0) {
    return "";
  }
  AD_CORRECTNESS_CHECK(destination.size() >= bound);
  const size_t bytesWritten =
      decompress(ql::span<char>{destination.data(), bound});
  AD_CORRECTNESS_CHECK(bytesWritten <= bound);
  return bytesWritten == 0 ? ""
                           : std::string_view{destination.data(), bytesWritten};
}

// _____________________________________________________________________________
// Builder for a PMR arena-backed `VocabBatchLookupResult`.
// Owns the arena and, when constructed with `AllocatorWithLimit`, the
// memory-limit tracker. Vocabularies only append decompressed bytes.
class ArenaVocabBatchBuilder {
 private:
  std::unique_ptr<ql::pmr::memory_resource> upstream_;
  std::unique_ptr<ql::pmr::monotonic_buffer_resource> buffer_;
  std::vector<std::string_view> views_;

  void initBuffer(ql::pmr::memory_resource& resource) {
    buffer_ = std::make_unique<ql::pmr::monotonic_buffer_resource>(&resource);
  }

 public:
  // Default PMR resource (tests and vocabs that do not see a query
  // allocator).
  explicit ArenaVocabBatchBuilder(size_t expectedSize) {
    AD_CONTRACT_CHECK(expectedSize > 0);
    views_.reserve(expectedSize);
    initBuffer(*ql::pmr::get_default_resource());
  }

  // Charge `allocator`'s shared memory tracker (same object as the Index /
  // query `AllocatorWithLimit`).
  explicit ArenaVocabBatchBuilder(
      size_t expectedSize,
      const ad_utility::AllocatorWithLimit<Id>& allocator) {
    AD_CONTRACT_CHECK(expectedSize > 0);
    views_.reserve(expectedSize);
    upstream_ =
        std::make_unique<AllocatorAsMemoryResource>(allocator.as<std::byte>());
    initBuffer(*upstream_);
  }

  // Allocate storage inside the arena for `bound` bytes, invoke
  // `decompress(destinationSpan)` to write the bytes (up to `bound`), and
  // register the view.
  template <typename DecompressFunc>
  void appendDecompressedWord(size_t bound, DecompressFunc&& decompress) {
    if (bound == 0) {
      views_.emplace_back("");
      return;
    }

    ql::pmr::polymorphic_allocator<char> allocator{buffer_.get()};
    char* mem = allocator.allocate(bound);
    views_.push_back(
        decompressIntoSpan(ql::span<char>{mem, bound}, bound,
                           std::forward<DecompressFunc>(decompress)));
  }

  // Allocate storage inside the arena and copy the given word into it.
  void appendWord(std::string_view word) {
    if (word.empty()) {
      views_.emplace_back("");
      return;
    }
    ql::pmr::polymorphic_allocator<char> allocator{buffer_.get()};
    char* mem = allocator.allocate(word.size());
    ql::ranges::copy(word, mem);
    views_.emplace_back(mem, word.size());
  }

  // ___________________________________________________________________________
  // Finalize and return the immutable batch result.
  [[nodiscard]] VocabBatchLookupResult finalize() && {
    AD_CONTRACT_CHECK(!views_.empty());
    auto data = std::make_shared<PmrVocabBatchLookupData>(
        PmrVocabBatchLookupData::Passkey{}, std::move(upstream_),
        std::move(buffer_), std::move(views_));
    return PmrVocabBatchLookupData::asResult(std::move(data));
  }
};

// _____________________________________________________________________________
// Construct a PMR arena-backed `VocabBatchLookupResult` by copying words into a
// monotonic buffer arena.
inline VocabBatchLookupResult makePmrVocabBatchLookupResult(
    ql::span<const std::string_view> words) {
  AD_CONTRACT_CHECK(!words.empty());
  ArenaVocabBatchBuilder builder(words.size());
  for (std::string_view word : words) {
    builder.appendWord(word);
  }
  return std::move(builder).finalize();
}

inline VocabBatchLookupResult makePmrVocabBatchLookupResult(
    std::initializer_list<std::string_view> words) {
  return makePmrVocabBatchLookupResult(
      ql::span<const std::string_view>{words.begin(), words.size()});
}

// _____________________________________________________________________________
// Assemble one `VocabBatchLookupResult` from words that come from several
// sources, without copying any word bytes. Every output position is filled
// exactly once, by one of two means:
//
// 1. `scatterSubBatchResultAtPositions`: the views of a batch result that was
//    received from another vocabulary are re-arranged into the given output
//    positions, and that batch's storage is kept alive by the assembled
//    result (shared ownership, no copy of the words).
// 2. `assignUnownedViewAtPosition`: a view into storage that is NOT owned by
//    the result, typically the words of an in-memory vocabulary. See that
//    function for the lifetime contract.
//
// The assembler checks that no position is filled twice and that all
// positions are filled before `finalizeVocabBatchLookupResult` hands out the
// result.
class MultiSourceVocabBatchAssembler {
 private:
  std::vector<std::string_view> assembledWordViews_;
  std::vector<bool> slotFilledTracking_;
  std::vector<VocabBatchOwner> storageOwners_;

  // Fill one output position (shared by the two public placement functions,
  // which differ only in how the storage behind `word` is kept alive).
  void assignWordAtPosition(size_t resultPosition, std::string_view word) {
    AD_CORRECTNESS_CHECK(resultPosition < assembledWordViews_.size());
    AD_CORRECTNESS_CHECK(!slotFilledTracking_[resultPosition]);
    slotFilledTracking_[resultPosition] = true;
    assembledWordViews_[resultPosition] = word;
  }

 public:
  // ___________________________________________________________________________
  explicit MultiSourceVocabBatchAssembler(size_t totalExpectedWords)
      : assembledWordViews_(totalExpectedWords),
        slotFilledTracking_(totalExpectedWords, false) {
    // Fail fast like every other factory in this file: finalization requires
    // a non-empty view list, so an empty assembler could never succeed.
    AD_CONTRACT_CHECK(totalExpectedWords > 0);
    AD_CORRECTNESS_CHECK(assembledWordViews_.size() ==
                         slotFilledTracking_.size());
  }

  // ___________________________________________________________________________
  // Place the `i`-th word of `subBatchResult` at output position
  // `resultPositions[i]` and keep the storage of `subBatchResult` alive for
  // as long as the assembled result lives. Only the views are re-arranged;
  // the word bytes stay where they are. Taken by value so that a caller that
  // no longer needs the batch can move it in without touching the reference
  // count.
  void scatterSubBatchResultAtPositions(
      VocabBatchLookupResult subBatchResult,
      ql::span<const size_t> resultPositions) {
    AD_CONTRACT_CHECK(subBatchResult.size() == resultPositions.size());

    for (auto [resultPosition, word] :
         ::ranges::views::zip(resultPositions, subBatchResult)) {
      assignWordAtPosition(resultPosition, word);
    }
    // Steal the owner of the (by-value) argument instead of copying it.
    if (subBatchResult.storage_ != nullptr) {
      storageOwners_.push_back(std::move(subBatchResult.storage_));
    }
  }

  // ___________________________________________________________________________
  // Place `word` at output position `resultPosition` WITHOUT taking any
  // ownership of the bytes it points to.
  //
  // Lifetime contract: the bytes of `word` must stay valid and unchanged for
  // as long as the assembled `VocabBatchLookupResult` (or any copy of it) is
  // used. This holds for the words of a vocabulary that is kept in memory for
  // its whole lifetime, as long as the vocabulary outlives the result, which
  // is the case for lookups during query processing (the index, and thus its
  // vocabularies, outlives every query). It does NOT hold for temporaries,
  // for scratch buffers that are reused, or for the words of another batch
  // result; use `scatterSubBatchResultAtPositions` for those.
  void assignUnownedViewAtPosition(size_t resultPosition,
                                   std::string_view word) {
    assignWordAtPosition(resultPosition, word);
  }

  // ___________________________________________________________________________
  // Finalize the assembled batch and return it as a `VocabBatchLookupResult`
  // that co-owns the storage of all scattered sub-batches (can be called only
  // once).
  [[nodiscard]] VocabBatchLookupResult finalizeVocabBatchLookupResult() && {
    AD_CORRECTNESS_CHECK(assembledWordViews_.size() ==
                         slotFilledTracking_.size());
    AD_CORRECTNESS_CHECK(!assembledWordViews_.empty());
    // `storageOwners_` is empty iff all words were placed via
    // `assignUnownedViewAtPosition`, which is valid.
    AD_CORRECTNESS_CHECK(ql::ranges::all_of(
        slotFilledTracking_, [](bool isFilled) { return isFilled; }));

    auto multiOwnerData = std::make_shared<MultiOwnerVocabBatchLookupData>(
        MultiOwnerVocabBatchLookupData::Passkey{}, std::move(storageOwners_),
        std::move(assembledWordViews_));
    return MultiOwnerVocabBatchLookupData::asResult(std::move(multiOwnerData));
  }
};

// _____________________________________________________________________________
// Paired lookup data for one vocabulary marker: for each position `i` in the
// arrays, `underlyingIndices[i]` is the index to look up, and
// `resultPositions[i]` is where the result goes in the final output. The
// arrays are always kept in sync (same size).
class MarkerIndicesAndPositions {
 private:
  std::vector<size_t> underlyingIndices_;
  std::vector<size_t> resultPositions_;

 public:
  // ___________________________________________________________________________
  // Pre-allocate capacity for both paired vectors, preserving their 1:1
  // correspondence.
  void reserve(size_t capacity) {
    underlyingIndices_.reserve(capacity);
    resultPositions_.reserve(capacity);
    AD_CORRECTNESS_CHECK(underlyingIndices_.size() == resultPositions_.size());
  }

  // ___________________________________________________________________________
  // Add a (`underlyingIndex`, `resultPosition`) pair.
  void addPair(size_t underlyingIndex, size_t resultPosition) {
    underlyingIndices_.push_back(underlyingIndex);
    resultPositions_.push_back(resultPosition);
    AD_CORRECTNESS_CHECK(underlyingIndices_.size() == resultPositions_.size());
  }

  // ___________________________________________________________________________
  // Return the span of underlying vocabulary indices to look up. Element `i`
  // corresponds to position `i` in `getResultPositions()`.
  [[nodiscard]] ql::span<const size_t> getUnderlyingIndices() const noexcept {
    return underlyingIndices_;
  }

  // ___________________________________________________________________________
  // Return the span of target output positions. Element `i` corresponds to
  // underlying index `i` in `getUnderlyingIndices()`.
  [[nodiscard]] ql::span<const size_t> getResultPositions() const noexcept {
    return resultPositions_;
  }

  // ___________________________________________________________________________
  [[nodiscard]] bool empty() const noexcept {
    return underlyingIndices_.empty();
  }

  // ___________________________________________________________________________
  [[nodiscard]] size_t size() const noexcept {
    return underlyingIndices_.size();
  }
};

// _____________________________________________________________________________
// Paired lookup data for each of the `NumVocabs` underlying vocabularies,
// indexed by the marker that identifies the vocabulary.
template <size_t NumVocabs>
using IndicesAndPositionsByMarker =
    std::array<MarkerIndicesAndPositions, NumVocabs>;

// _____________________________________________________________________________
// Partition marked indices into paired (`underlyingIndex`, `resultPosition`)
// lists per marker. For each input index, `getMarkerAndVocabIndex` extracts
// the marker that identifies the underlying vocabulary and the unmarked vocab
// index; the index is paired with its position in the input, and both are
// grouped by marker.
template <size_t NumVocabs, typename GetMarkerAndVocabIndex>
IndicesAndPositionsByMarker<NumVocabs> partitionMarkerIndicesAndPositions(
    ql::span<const size_t> indices,
    GetMarkerAndVocabIndex getMarkerAndVocabIndex) {
  IndicesAndPositionsByMarker<NumVocabs> out;
  for (const auto& [resultPosition, markedIndex] :
       ::ranges::views::enumerate(indices)) {
    auto [marker, underlyingIndex] = getMarkerAndVocabIndex(markedIndex);
    AD_CORRECTNESS_CHECK(marker < NumVocabs);
    out[marker].addPair(underlyingIndex, resultPosition);
  }
  return out;
}

// _____________________________________________________________________________
// Batch lookup results for each underlying vocabulary, indexed by vocabulary
// marker. Stores results only from vocabularies with lookup indices in this
// batch. The single-release invariant guarantees that each slot is consumed at
// most once.
template <size_t NumVocabs>
class MarkerBatchLookups {
 private:
  std::array<std::optional<VocabBatchLookupResult>, NumVocabs> results_{};

 public:
  // Create an empty set of per-marker lookup-result slots to be populated
  // before single-release consumption.
  MarkerBatchLookups() = default;

  // Return the lookup result for the given vocabulary marker.
  std::optional<VocabBatchLookupResult>& operator[](size_t marker) {
    AD_CORRECTNESS_CHECK(marker < NumVocabs);
    return results_[marker];
  }
  const std::optional<VocabBatchLookupResult>& operator[](size_t marker) const {
    AD_CORRECTNESS_CHECK(marker < NumVocabs);
    return results_[marker];
  }

  // Release the lookup result for the given vocabulary marker exactly once.
  VocabBatchLookupResult release(size_t marker) {
    AD_CORRECTNESS_CHECK(marker < NumVocabs);
    AD_CORRECTNESS_CHECK(results_[marker].has_value());
    auto result = std::move(results_[marker].value());
    results_[marker].reset();
    return result;
  }
};

// _____________________________________________________________________________
// Merge per-vocabulary lookup batches into a single combined
// `VocabBatchLookupResult` where each word is at the position of its original
// input index.
template <size_t NumVocabs, typename ReleaseLookupResultForMarker>
VocabBatchLookupResult mergeMarkerBatchesInInputOrder(
    const IndicesAndPositionsByMarker<NumVocabs>& markerIndicesAndPositions,
    ReleaseLookupResultForMarker releaseLookupResult) {
  size_t totalPositions = 0;
  for (const auto& markerIndices : markerIndicesAndPositions) {
    AD_CONTRACT_CHECK(markerIndices.size() <= SIZE_MAX - totalPositions);
    totalPositions += markerIndices.size();
  }
  MultiSourceVocabBatchAssembler assembler(totalPositions);

  for (const auto& [vocabMarker, markerIndices] :
       ::ranges::views::enumerate(markerIndicesAndPositions)) {
    if (markerIndices.empty()) {
      continue;
    }
    auto lookupResult = releaseLookupResult(vocabMarker);
    assembler.scatterSubBatchResultAtPositions(
        std::move(lookupResult), markerIndices.getResultPositions());
  }
  return std::move(assembler).finalizeVocabBatchLookupResult();
}

// _____________________________________________________________________________
// Convenient overload accepting `MarkerBatchLookups`.
template <size_t NumVocabs>
VocabBatchLookupResult mergeMarkerBatchesInInputOrder(
    MarkerBatchLookups<NumVocabs> markerLookups,
    const IndicesAndPositionsByMarker<NumVocabs>& markerIndicesAndPositions) {
  return mergeMarkerBatchesInInputOrder(markerIndicesAndPositions,
                                        [&markerLookups](size_t marker) {
                                          return markerLookups.release(marker);
                                        });
}

// _____________________________________________________________________________
// Generic sequential fallback implementations of the batch-lookup interface,
// used by all vocabularies that do not provide a specialized (e.g. io_uring)
// implementation. They simply loop over the indices and issue the ordinary
// single-word `operator[]` lookups one after another.
namespace ad_utility::vocabulary {
// Return the placeholder that is reported for a vocabulary index that is not
// contained in a vocabulary with "holes" (see `VocabularyInMemoryBinSearch`).
// This happens when such a vocabulary was created by excluding some of the
// entries of a larger vocabulary, but an `Id` that refers to an excluded entry
// is still looked up.
inline std::string placeholderForMissingVocabIndex(uint64_t index) {
  return absl::StrCat("<qlever-excluded-vocab-entry-", index, ">");
}

namespace detail {
// The implementation of `replaceOptionalByPlaceholderOnExport` below. The
// primary template covers all vocabularies that don't declare the
// corresponding member, the partial specialization those that do.
template <typename Vocab, typename = void>
struct ReplaceOptionalByPlaceholderOnExportImpl : std::false_type {};

template <typename Vocab>
struct ReplaceOptionalByPlaceholderOnExportImpl<
    Vocab, std::void_t<decltype(Vocab::replaceOptionalByPlaceholderOnExport)>>
    : std::bool_constant<Vocab::replaceOptionalByPlaceholderOnExport> {};
}  // namespace detail

// Whether the `Vocab` has opted in to reporting a word that it doesn't contain
// (that is, its `operator[]` returns `std::nullopt`) as
// `placeholderForMissingVocabIndex` when the words are exported, instead of
// throwing. A vocabulary opts in by declaring
// `static constexpr bool replaceOptionalByPlaceholderOnExport = true;`. The
// default is `false`, because silently reporting a word that is not the one
// that was asked for is only correct for vocabularies that are deliberately
// created with holes (see `VocabularyInMemoryBinSearch`).
template <typename Vocab>
constexpr bool replaceOptionalByPlaceholderOnExport =
    detail::ReplaceOptionalByPlaceholderOnExportImpl<Vocab>::value;

// Return `vocab[index]` as a `std::string`. If the `operator[]` of `vocab`
// returns a `std::optional` (which is the case for vocabularies with holes, see
// `VocabularyInMemoryBinSearch`) that is `std::nullopt`, then return
// `placeholderForMissingVocabIndex(index)` if the `vocab` has opted in to this
// behavior via `replaceOptionalByPlaceholderOnExport` (see above), and throw
// otherwise.
template <typename Vocab>
std::string wordAsStringOrPlaceholder(const Vocab& vocab, uint64_t index) {
  decltype(auto) word = vocab[index];
  if constexpr (ad_utility::similarToInstantiation<decltype(word),
                                                   std::optional>) {
    if (!word.has_value()) {
      if constexpr (replaceOptionalByPlaceholderOnExport<Vocab>) {
        return placeholderForMissingVocabIndex(index);
      } else {
        AD_THROW(absl::StrCat(
            "The index ", index,
            " is not contained in the vocabulary. If the vocabulary is "
            "deliberately built with such holes, then it has to declare "
            "`static constexpr bool replaceOptionalByPlaceholderOnExport = "
            "true;` to report a placeholder for the missing word instead."));
      }
    }
    return std::string{word.value()};
  } else {
    return std::string{std::move(word)};
  }
}

// The implementation of `getPositionOfWord` (see `VocabularyConstraints.h`)
// for a vocabulary with "holes" (see `VocabularyInMemoryBinSearch`): binary
// search for the `word` and return the range of vocabulary indices at which it
// is stored, or the empty range at the index at which it would be stored if it
// is not contained. Note that the "one past the end" index has to be passed in
// as `endIndex` and must not be `vocab.size()`: because of the holes, the
// largest vocabulary index that is contained is in general much larger than
// the number of words, so using `vocab.size()` would report a word that sorts
// after all contained words as if it sorted somewhere in the middle.
template <typename Vocab, typename InternalStringType, typename Comparator>
std::pair<uint64_t, uint64_t> getPositionOfWordInVocabWithHoles(
    const Vocab& vocab, const InternalStringType& word, Comparator comparator,
    uint64_t endIndex) {
  return vocab.lower_bound(word, std::move(comparator))
      .positionOfWord(word)
      .value_or(std::pair<uint64_t, uint64_t>{endIndex, endIndex});
}

// _____________________________________________________________________________
// Sequential fallback for `lookupBatch`: look up each index individually via
// `vocab[idx]`, returning one `string_view` per index. Works for any vocabulary
// whose `operator[]` yields something convertible to `std::string`, or a
// `std::optional` thereof (see `wordAsStringOrPlaceholder`).
template <typename Vocab>
VocabBatchLookupResult sequentialLookupBatch(const Vocab& vocab,
                                             ql::span<const size_t> indices) {
  AD_CONTRACT_CHECK(!indices.empty());
  // Construct each word as an owning `std::string` directly in the result's
  // `std::vector<std::string>` buffer; `fromWords` then points one view at
  // each of them. Words that the vocabulary returns as (optional) views are
  // copied straight into their slot: building a temporary `std::string` per
  // word (via `wordAsStringOrPlaceholder`) and moving it into the vector costs
  // about 30 extra instructions per word (callgrind, 4,096-word batch from a
  // `VocabularyInMemoryBinSearch`). Only a missing word (a "hole", see
  // `wordAsStringOrPlaceholder`) takes that path.
  std::vector<std::string> words;
  words.reserve(indices.size());
  for (size_t index : indices) {
    decltype(auto) word = vocab[index];
    if constexpr (ad_utility::similarToInstantiation<decltype(word),
                                                     std::optional>) {
      if (word.has_value()) {
        words.emplace_back(word.value());
      } else {
        words.push_back(wordAsStringOrPlaceholder(vocab, index));
      }
    } else {
      words.emplace_back(std::move(word));
    }
  }
  return StringVectorVocabBatchLookupData::fromWords(std::move(words));
}

// _____________________________________________________________________________
// Streamed version of `lookupBatch`: lazily apply `vocab.lookupBatch` for the
// passed `vocab` to each batch of the (type-erased) input range.
// The referenced `vocab` must outlive the returned range.
template <typename Vocab>
VocabLookupOutput lookupBatchesStreamed(const Vocab& vocab,
                                        VocabLookupInput input) {
  return VocabLookupOutput{ad_utility::OwningView{std::move(input)} |
                           ql::views::transform([&vocab](const auto& indices) {
                             return vocab.lookupBatch(indices);
                           })};
}

}  // namespace ad_utility::vocabulary

// _____________________________________________________________________________
// A word and its index in the vocabulary from which it was obtained. Also
// contains a special state `end()` which can be queried by the `isEnd()`
// function. This can be used to represent words that are larger than the
// largest word in the vocabulary, similar to a typical `end()` iterator.
class WordAndIndex {
 private:
  std::optional<std::pair<std::string, uint64_t>> wordAndIndex_;
  // See the documentation for `previousIndex()` below.
  std::optional<uint64_t> previousIndex_ = std::nullopt;

 public:
  // ___________________________________________________________________________
  // Query for the special `end` semantics.
  bool isEnd() const { return !wordAndIndex_.has_value(); }

  // ___________________________________________________________________________
  // Return the word. Throws if `isEnd() == true`.
  const std::string& word() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().first;
  }

  // ___________________________________________________________________________
  // Return the index. Throws if `isEnd() == true`.
  uint64_t index() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().second;
  }

  // ___________________________________________________________________________
  uint64_t indexOrDefault(uint64_t defaultValue) const {
    return isEnd() ? defaultValue : index();
  }

  // ___________________________________________________________________________
  // The next valid index before `index()`. If `nullopt` either no
  // such index exists (because `index()` is already the first valid index),
  // or the `previousIndex_` simply wasn't set. This member is currently used to
  // communicate between the `VocabularyInMemoryBinSearch` and the
  // `InternalExternalVocabulary`.
  std::optional<uint64_t>& previousIndex() { return previousIndex_; }

  // ___________________________________________________________________________
  // Assuming this object holds a `lower_bound` result, check whether the word
  // is stored at this position and return an upper bound accordingly.
  template <typename T>
  std::optional<std::pair<uint64_t, uint64_t>> positionOfWord(
      const T& wordToCheck) {
    if (isEnd()) {
      return std::nullopt;
    }
    auto lower = index();
    auto upper = word() == wordToCheck ? lower + 1 : lower;
    return std::pair<uint64_t, uint64_t>{lower, upper};
  }

  // ___________________________________________________________________________
  // The default constructor creates a `WordAndIndex` with `isEnd() == true`.
  WordAndIndex() = default;

  // ___________________________________________________________________________
  // Explicit factory function for the end state.
  static WordAndIndex end() { return {}; }

  // ___________________________________________________________________________
  // Constructors for the ordinary non-end case.
  WordAndIndex(std::string word, uint64_t index)
      : wordAndIndex_{std::in_place, std::move(word), index} {}
  WordAndIndex(std::string_view word, uint64_t index)
      : wordAndIndex_{std::in_place, std::string{word}, index} {}
};

// The suffixes that have to be appended to the base filename of a vocabulary
// in order to obtain the names of all the files that the vocabulary consists
// of. These are exactly the files that its `WordWriter` writes and that its
// `open` reads. The suffix of the file that is stored under the base filename
// itself (which most vocabularies have) is the empty string.
//
// Every vocabulary declares its suffixes via a
// `static FileSuffixes fileSuffixes()`. A vocabulary that is composed of other
// vocabularies composes its suffixes from theirs via
// `addFileSuffixesWithPrefix` below.
using FileSuffixes = std::vector<std::string>;

// Append the `suffixes` of an underlying vocabulary to `out`, where that
// vocabulary is stored under the base filename of the composing vocabulary plus
// the given `prefix`.
inline void addFileSuffixesWithPrefix(FileSuffixes& out,
                                      std::string_view prefix,
                                      const FileSuffixes& suffixes) {
  for (const std::string& suffix : suffixes) {
    out.push_back(absl::StrCat(prefix, suffix));
  }
}

// A common base class for the `WordWriter` types of different vocabulary
// implementations. It has to be called for each of the words (in the correct
// order).
class WordWriterBase {
 private:
  ad_utility::ThrowInDestructorIfSafe throwIfSafe_;
  std::string readableName_;
  std::atomic_bool finishWasCalled_ = false;

 public:
  // ___________________________________________________________________________
  // Write the next word. The `isExternal` flag is ignored for all the
  // vocabulary implementations but the `VocabularyInternalExternal`. Return the
  // index that was assigned to the word.
  virtual uint64_t operator()(std::string_view word, bool isExternal) = 0;

  // ___________________________________________________________________________
  // Destructor. If `finish` hasn't been called, an exception is thrown if it is
  // safe to do so. Derived classes have to make sure that their destructors
  // call `finish` if necessary. Note: It is unfortunately not possible to call
  // the virtual function `finish` directly from this base class destructor, as
  // at that point the derived class is already destroyed.
  virtual ~WordWriterBase() noexcept(false) {
    using namespace std::string_view_literals;
    if (!finishWasCalled_) {
      throwIfSafe_(
          []() {
            throw std::runtime_error{
                "WordWriterBase::finish was not called before the destructor."};
          },
          "this can happen when `finish` was not called before destroying a"
          " `WordWriter` that inherits from `WordWriterBase`. This is either a"
          " bug, or it can happen when an exception was thrown in the"
          " constructor of the subclass."sv);
    }
  }

  // ___________________________________________________________________________
  // Calling this function will signal that the last word has been pushed.
  // Implementations might e.g. flush all buffers to disk and close underlying
  // files. After calling `finish`, no more calls to `operator()` are allowed.
  // The destructor also calls `finish` if it wasn't called manually.
  virtual void finish() final {
    if (finishWasCalled_.exchange(true)) {
      return;
    }
    finishImpl();
  }

  // ___________________________________________________________________________
  bool finishWasCalled() const { return finishWasCalled_; }

  // ___________________________________________________________________________
  // Access to a `readableName` of the vocabulary that is written. Some
  // implementations use it to customize log messages.
  virtual std::string& readableName() { return readableName_; }

 private:
  // ___________________________________________________________________________
  // The base classes have to implement the actual logic for `finish` here.
  virtual void finishImpl() = 0;
};

// The parallel writers of a vocabulary. A `WordWriterBase` (see above) is fed
// one word at a time by a single thread. A `BlockWriterBase` is fed blocks of
// consecutive words and splits the writing of a block into two steps:
//
// 1. The `prepare` step runs on any thread and concurrently for different
// blocks (the compression of the words, the geometry info of WKT literals).
//
// 2. The append step (the `AppendBlock` that `prepare` returns) appends the
// prepared block to the files. It runs on a single thread, in the order of the
// blocks.
//
// NOTE: This lets the vocabulary merger (see `index/VocabularyMerger.h`, in a
// follow-up PR) do the expensive per-word work of the writing on all its
// threads.

// A block of consecutive words of a vocabulary, the unit in which a
// `BlockWriterBase` works: the words, whether each word is to be externalized
// (see `WordWriterBase::operator()`), and the position of the first word in the
// vocabulary. The concatenated bytes of the words and their offsets (see
// `data()` and `offsets()`) are directly what `VocabularyOnDisk` writes to
// disk.
struct WordBlock {
  CompactVectorOfStrings<char> words_;
  std::vector<bool> isExternal_;
  uint64_t firstPosition_ = 0;
  // The precomputed payload of the words, `payloadSize_` bytes per word (see
  // `BlockWriterBase::precomputePayload`), empty if the vocabulary has none.
  std::string payload_;
  size_t payloadSize_ = 0;

  // Append a word, with its precomputed `payload` (of `payloadSize_` bytes,
  // empty if the vocabulary has none).
  void push(std::string_view word, bool isExternal,
            std::string_view payload = {}) {
    AD_CORRECTNESS_CHECK(payload.size() == payloadSize_);
    words_.push_back(word);
    isExternal_.push_back(isExternal);
    payload_.append(payload);
  }

  // Append a word whose payload (`payloadSize_` bytes) is not computed yet,
  // and return the bytes of its payload inside this block, for the caller to
  // compute it into (see `BlockWriterBase::precomputePayload`). The returned
  // span is only valid until the next `push` or `pushWithPayloadSlot`.
  ql::span<char> pushWithPayloadSlot(std::string_view word, bool isExternal) {
    words_.push_back(word);
    isExternal_.push_back(isExternal);
    size_t offset = payload_.size();
    payload_.resize(offset + payloadSize_);
    return {payload_.data() + offset, payloadSize_};
  }

  // The payload of the `i`-th word.
  std::string_view payload(size_t i) const {
    return std::string_view{payload_}.substr(i * payloadSize_, payloadSize_);
  }

  size_t numWords() const { return isExternal_.size(); }
  bool empty() const { return numWords() == 0; }

  // The `i`-th word of the block.
  std::string_view word(size_t i) const { return words_[i]; }

  // The words of the block as views into `words_`.
  std::vector<std::string_view> words() const {
    return {words_.begin(), words_.end()};
  }

  // The concatenated bytes of all the words.
  ql::span<const char> data() const { return words_.dataSpan(); }

  // The offset of each word in `data()`, plus the end of the last word. Empty
  // if the block is empty.
  ql::span<const uint64_t> offsets() const { return words_.offsetsSpan(); }
};

// The append step of a block that `BlockWriterBase::prepare` returns, see the
// comment above. It has to be called exactly once.
using AppendBlock = absl::AnyInvocable<void() &&>;

// The default number of words per block for a vocabulary that has no
// requirement of its own (see `BlockWriterBase::blockSize`).
inline constexpr size_t DEFAULT_WORDS_PER_VOCABULARY_BLOCK = 100'000;

// The block-wise counterpart of a `WordWriterBase`, see the comment above. The
// blocks have to be appended in the order of their positions, and every block
// but the last has to consist of exactly `blockSize()` words, with the first
// block starting at position `0`.
class BlockWriterBase {
 private:
  std::string readableName_;
  std::atomic_bool finishWasCalled_ = false;

 public:
  // NOTE: The destructor is `noexcept(false)` like the one of
  // `WordWriterBase`, because the block writers hold word writers. It is
  // deliberately not `= default`, which GCC 8 rejects (it deletes a defaulted
  // destructor whose exception specification differs from the implicit one).
  virtual ~BlockWriterBase() noexcept(false) {}

  // The number of words per block that this vocabulary requires, for example
  // because it creates one codebook per block. A vocabulary without such a
  // requirement returns `DEFAULT_WORDS_PER_VOCABULARY_BLOCK`.
  virtual size_t blockSize() const = 0;

  // The index of the `word` at the given `position` (the number of words
  // before it), which for most vocabularies is the position itself. This is
  // what the corresponding `WordWriterBase::operator()` would return. This
  // function must be thread-safe (for all overrides), because it may be called
  // from any thread.
  virtual uint64_t indexOf(uint64_t position,
                           [[maybe_unused]] std::string_view word) const {
    return position;
  }

  // The number of bytes that `precomputePayload` produces per word, `0` if the
  // vocabulary precomputes nothing.
  virtual size_t precomputedPayloadSize() const { return 0; }

  // Compute the payload of a `word` (see `precomputedPayloadSize`) into
  // `payload`, which has that many bytes. This is the expensive per-word work
  // of a vocabulary (the geometry info of a WKT literal), which the caller can
  // do wherever it has the words and idle threads; the block then carries the
  // payload (see `WordBlock::payload_`), and `prepare` uses it instead of
  // computing it. Never called if the size is `0`. This function must be
  // thread-safe (for all overrides), because it may be called from any thread.
  virtual void precomputePayload(
      [[maybe_unused]] std::string_view word,
      [[maybe_unused]] ql::span<char> payload) const {}

  // Do the part of the writing of the `block` that does not depend on any
  // other block. The block carries the precomputed payload of its words if
  // `precomputedPayloadSize()` is not `0`. This function must be thread-safe
  // (for all overrides), because it may be called from any thread and
  // concurrently for different blocks. Return the step that appends the
  // prepared block to the vocabulary, which has to be called from a single
  // thread, in the order of the blocks, and before `finish()`.
  virtual AppendBlock prepare(WordBlock block) = 0;

  // Signal that the last block has been appended (flush and close the files).
  // No further block may be prepared or appended afterwards.
  void finish() {
    if (finishWasCalled_.exchange(true)) {
      return;
    }
    finishImpl();
  }
  bool finishWasCalled() const { return finishWasCalled_; }

  // A readable name of the vocabulary that is written, for log messages.
  std::string& readableName() { return readableName_; }

 private:
  virtual void finishImpl() = 0;
};

// The writer of a vocabulary that may consist of several sub-vocabularies (see
// `SplitVocabulary`), each of which is written by a `BlockWriterBase` of its
// own. The user first asks for the sub-vocabulary of a word, then for the index
// that the word gets at a given position in that sub-vocabulary, and writes
// the words of each sub-vocabulary in blocks via its block writer.
class ParallelWordWriterBase {
 private:
  std::atomic_bool finishWasCalled_ = false;

 public:
  // NOTE: Not `= default`, see `~BlockWriterBase`.
  virtual ~ParallelWordWriterBase() noexcept(false) {}

  // The number of sub-vocabularies, at least one.
  virtual uint8_t numSubVocabularies() const = 0;

  // The sub-vocabulary to which the `word` belongs. This function must be
  // thread-safe (for all overrides), because it may be called from any thread.
  virtual uint8_t subVocabularyOf(std::string_view word) const = 0;

  // The index in the whole vocabulary of the `word` at the given `position` in
  // the sub-vocabulary `sub`. This function must be thread-safe (for all
  // overrides), because it may be called from any thread.
  virtual uint64_t indexOf(uint8_t sub, uint64_t position,
                           std::string_view word) const = 0;

  // The block writer of the sub-vocabulary `sub`.
  virtual BlockWriterBase& blockWriter(uint8_t sub) = 0;
  const BlockWriterBase& blockWriter(uint8_t sub) const {
    return const_cast<ParallelWordWriterBase*>(this)->blockWriter(sub);
  }

  // Finish all the block writers.
  void finish() {
    if (finishWasCalled_.exchange(true)) {
      return;
    }
    for (uint8_t sub = 0; sub < numSubVocabularies(); ++sub) {
      blockWriter(sub).finish();
    }
  }

  // The readable name of the (first) sub-vocabulary, see
  // `BlockWriterBase::readableName`.
  std::string& readableName() { return blockWriter(0).readableName(); }
};

// The `ParallelWordWriterBase` of a vocabulary with a single sub-vocabulary.
class SingleVocabularyParallelWriter : public ParallelWordWriterBase {
 private:
  std::unique_ptr<BlockWriterBase> blockWriter_;

 public:
  explicit SingleVocabularyParallelWriter(
      std::unique_ptr<BlockWriterBase> blockWriter)
      : blockWriter_{std::move(blockWriter)} {
    AD_CONTRACT_CHECK(blockWriter_ != nullptr);
  }
  uint8_t numSubVocabularies() const override { return 1; }
  uint8_t subVocabularyOf(std::string_view) const override { return 0; }
  uint64_t indexOf(uint8_t sub, uint64_t position,
                   std::string_view word) const override {
    AD_CORRECTNESS_CHECK(sub == 0);
    return blockWriter_->indexOf(position, word);
  }
  BlockWriterBase& blockWriter(uint8_t sub) override {
    AD_CORRECTNESS_CHECK(sub == 0);
    return *blockWriter_;
  }
};

namespace detail {
// Whether `T` has a member function `finish()`, see `BlockWriterFromCallback`.
template <typename T, typename = void>
struct HasFinish : std::false_type {};
template <typename T>
struct HasFinish<T, std::void_t<decltype(std::declval<T&>().finish())>>
    : std::true_type {};
}  // namespace detail

// A `BlockWriterBase` for a vocabulary that has no block writer of its own:
// the words of a block are handed one by one to the `callback`, which has the
// signature of `WordWriterBase::operator()` and which has to return the
// position of the word (the block writer checks that). If the callback has a
// `finish()`, it is called by `finishImpl`. The `prepare` step does nothing,
// so all the work of such a writer happens in the append step.
template <typename Callback>
class BlockWriterFromCallback : public BlockWriterBase {
 private:
  Callback callback_;

 public:
  explicit BlockWriterFromCallback(Callback callback)
      : callback_{std::move(callback)} {}

  // Direct access to the callback.
  Callback& callback() { return callback_; }

  size_t blockSize() const override {
    return DEFAULT_WORDS_PER_VOCABULARY_BLOCK;
  }

  AppendBlock prepare(WordBlock block) override {
    return [this, block = std::move(block)]() {
      for (size_t i = 0; i < block.numWords(); ++i) {
        uint64_t position = std::invoke(
            callback_, block.word(i), static_cast<bool>(block.isExternal_[i]));
        AD_CORRECTNESS_CHECK(position == block.firstPosition_ + i);
      }
    };
  }

 private:
  void finishImpl() override {
    if constexpr (detail::HasFinish<Callback>::value) {
      callback_.finish();
    }
  }
};

// The callback for `BlockWriterFromCallback` that writes into a
// `WordWriterBase` (which it owns).
class WordWriterCallback {
 private:
  std::unique_ptr<WordWriterBase> writer_;

 public:
  explicit WordWriterCallback(std::unique_ptr<WordWriterBase> writer)
      : writer_{std::move(writer)} {
    AD_CONTRACT_CHECK(writer_ != nullptr);
  }
  uint64_t operator()(std::string_view word, bool isExternal) {
    return (*writer_)(word, isExternal);
  }
  void finish() { writer_->finish(); }
};

// Make the `BlockWriterBase` that writes word by word into the given
// `WordWriterBase`, for the vocabularies without a block writer of their own.
inline std::unique_ptr<BlockWriterBase> makeBlockWriterFromWordWriter(
    std::unique_ptr<WordWriterBase> writer) {
  return std::make_unique<BlockWriterFromCallback<WordWriterCallback>>(
      WordWriterCallback{std::move(writer)});
}

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H
