// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Iterators.h"
#include "util/TransparentFunctors.h"
#include "util/Views.h"

// The result type for a batch of vocabulary lookups.
using VocabBatchLookupResult = std::shared_ptr<ql::span<std::string_view>>;

// Type-erased input range of batches (each batch consists of a vector of
// indices into the underlying Vocabulary, specifying which terms' string
// representations need to be read from the underlying Vocabulary).
using VocabLookupInput = ad_utility::InputRangeTypeErased<std::vector<size_t>>;

// Type-erased output range of batch-lookup results (which are the string
// representations of the terms specified by `VocabLookupInput` to be read).
using VocabLookupOutput =
    ad_utility::InputRangeTypeErased<VocabBatchLookupResult>;

// Base class for a vocabulary batch-lookup result, shared by the different
// vocabulary implementations. Owns the materialized string data (`buffer()`,
// whose concrete type `BufferType` depends on the implementation) and one
// `string_view` per looked-up term (`views()`, each pointing into `buffer()`).
// The vocabulary implementation that performs the lookup fills `buffer()` and
// `views()`, then calls `asResult()` to hand out a `VocabBatchLookupResult`
// that keeps this object (and thus the storage the views point into) alive for
// as long as the `VocabBatchLookupResult` is used.
//
// NOTE: Use `finalize()` after filling `views` to set up the span, then use
// `asResult()` to get a `VocabBatchLookupResult` via aliasing shared_ptr.
template <typename BufferType>
class VocabLookupDataCommonBase {
 public:
  // Mutable access to the buffer that holds the materialized string data, for
  // the producer to fill before calling `asResult`.
  BufferType& buffer() { return buffer_; }

  // Mutable access to the views (one `string_view` per looked-up index, each
  // pointing into `buffer()`), for the producer to fill before calling
  // `asResult`.
  std::vector<std::string_view>& views() { return views_; }

  // Convert a filled lookup-data object into the public result type
  // `VocabBatchLookupResult`. `self` must be the owning shared_ptr of the
  // object to convert. The returned aliasing shared_ptr exposes only the span
  // over `views()`, but keeps the whole object (and thus the
  // `buffer()`/`views()` that the span points into) alive as long as the result
  // lives.
  static VocabBatchLookupResult asResult(
      std::shared_ptr<VocabLookupDataCommonBase> self) {
    self->finalize();
    auto* spanPtr = &self->span_;
    return std::shared_ptr<ql::span<std::string_view>>(std::move(self),
                                                       spanPtr);
  }

 private:
  // Buffer for the materialized string data (used by disk-based vocabularies).
  BufferType buffer_;

  // One `string_view` per looked-up index, each pointing into `buffer_`.
  std::vector<std::string_view> views_;

  // The span over `views_`, populated by `finalize()` and exposed by
  // `asResult()`.
  ql::span<std::string_view> span_;

  // Set up `span_` over `views_`. Call after `views_` is fully filled; do not
  // modify `views_` afterward, as `span_` would be invalidated.
  void finalize() { span_ = ql::span<std::string_view>{views_}; }
};

// A vocabulary batch-lookup result whose total size is known up front, so all
// strings can be materialized into a single contiguous `buffer()` in one go
// (e.g. reading a contiguous byte range from a disk-based vocabulary). Because
// the `views()` point into that one `std::vector<char>`, the buffer must not be
// grown after the views are created: a reallocation would move the bytes and
// invalidate every existing `string_view`. Use `PmrVocabBatchLookupData`
// instead when words are produced incrementally with unknown sizes.
struct VocabBatchLookupData : VocabLookupDataCommonBase<std::vector<char>> {};

// A vocabulary batch-lookup result when words are produced incrementally with
// sizes not known in advance (e.g. `CompressedVocabulary::lookupBatch`). A
// single string buffer as in `VocabBatchLookupData` is unsuitable, as appending
// would reallocate it and invalidate existing string_view's. Each word is
// instead allocated from a monotonic_buffer_resource, giving pointer-stable
// allocations. Exposed as a `VocabBatchLookupResult` via `asResult()`.
using BufferType = std::unique_ptr<ql::pmr::monotonic_buffer_resource>;
struct PmrVocabBatchLookupData : VocabLookupDataCommonBase<BufferType> {};

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

// A vocabulary batch-lookup result whose words are already materialized as
// owning `std::string`s. The words are moved into the
// `std::vector<std::string>` buffer and the `views()` point at those strings.
struct StringVectorVocabBatchLookupData
    : VocabLookupDataCommonBase<std::vector<std::string>> {};

// Construct a result from owning strings and expose views into their storage.
inline VocabBatchLookupResult makeStringVectorVocabBatchLookupResult(
    std::vector<std::string> words) {
  auto data = std::make_shared<StringVectorVocabBatchLookupData>();
  data->buffer() = std::move(words);
  data->views() = ::ranges::to_vector(
      data->buffer() |
      ql::views::transform(ad_utility::staticCast<std::string_view>));
  return StringVectorVocabBatchLookupData::asResult(std::move(data));
}

// Construct a PMR-backed result and expose views into its monotonic allocator.
// `views` must all point into `buffer`, else we get UB.
inline VocabBatchLookupResult makePmrVocabBatchLookupResult(
    std::unique_ptr<ql::pmr::monotonic_buffer_resource> buffer,
    std::vector<std::string_view> views) {
  auto data = std::make_shared<PmrVocabBatchLookupData>();
  data->buffer() = std::move(buffer);
  data->views() = std::move(views);
  return PmrVocabBatchLookupData::asResult(std::move(data));
}

// Type-erased smart pointer holding whatever keeps word storage alive. Used
// to store child `VocabBatchLookupResult`s or references to vocabulary state
// (e.g., shared ownership of a vocabulary's in-memory word storage).
// See the usage below.
using VocabBatchOwner = std::shared_ptr<const void>;

// `VocabBatchLookupResult` that owns multiple independent storage sources.
// Stores a list of `VocabBatchOwner`s that back the `string_view`s. Because
// every view is backed by an owner stored here, the result is self-contained:
// no view can dangle, and callers don't need to manage external lifetimes.
struct MultiOwnerVocabBatchLookupData
    : VocabLookupDataCommonBase<std::vector<VocabBatchOwner>> {};

// Scatter string_views from `result` into `viewsInInputOrder` at positions
// given by `resultPositions`, and keep `result` in `owners` to retain storage.
// Called multiple times to merge multiple `VocabBatchLookupResult`s into a
// single combined `VocabBatchLookupResult` via `keepAliveVocabBatch()`.
inline void scatterVocabBatchLookupResult(
    VocabBatchLookupResult result, ql::span<const size_t> resultPositions,
    ql::span<std::string_view> viewsInInputOrder,
    std::vector<VocabBatchOwner>& owners) {
  AD_CONTRACT_CHECK(result != nullptr,
                    "scatterVocabBatchLookupResult called with a null child "
                    "batch; lookupBatch must never return null");
  AD_CONTRACT_CHECK(result->size() == resultPositions.size(),
                    "each decompressed word of the child batch needs exactly "
                    "one target position");
  std::vector<bool> written(viewsInInputOrder.size());
  for (auto [resultPosition, word] :
       ::ranges::views::zip(resultPositions, *result)) {
    AD_CORRECTNESS_CHECK(resultPosition < viewsInInputOrder.size(),
                         "child batch position ", resultPosition,
                         " points outside the assembled result; the "
                         "resultPositions arrays are inconsistent with the "
                         "requested batch size");
    AD_CORRECTNESS_CHECK(!written[resultPosition], "result position ",
                         resultPosition,
                         " is written twice; two child batches claim the same "
                         "output slot, so their positions overlap");
    written[resultPosition] = true;
    viewsInInputOrder[resultPosition] = word;
  }
  // Note: this function is called once per child batch; each call writes only
  // its own positions. Completeness across calls (every position written) is
  // the caller's contract, enforced by `keepAliveVocabBatch`'s non-empty
  // checks and the per-call double-write guard above.
  owners.push_back(std::move(result));
}

// Create a `VocabBatchLookupResult` for the given `words`. The result will
// additionally keep the `owners` alive. Only call this if the storage for the
// `words` is managed by the `owners`; see `scatterVocabBatchLookupResult()` for
// an example.
//
// TODO<ms2144>: This API takes independent owner and view lists, so the
// lifetime link is a call-site convention rather than a structural type. A
// later redesign could replace it with a builder or an owned-view capability
// type so slots are only filled together with their storage.
inline VocabBatchLookupResult keepAliveVocabBatch(
    std::vector<VocabBatchOwner> owners, std::vector<std::string_view> words) {
  AD_CONTRACT_CHECK(!owners.empty(),
                    "without owners, the returned views would dangle; the "
                    "owners keep the backing storage alive");
  AD_CONTRACT_CHECK(!words.empty(),
                    "an empty word batch would produce an empty result; use "
                    "an empty `VocabBatchLookupResult` instead");
  auto data = std::make_shared<MultiOwnerVocabBatchLookupData>();
  data->buffer() = std::move(owners);
  data->views() = std::move(words);
  return MultiOwnerVocabBatchLookupData::asResult(std::move(data));
}

// Generic sequential fallback implementations of the batch-lookup interface,
// used by all vocabularies that do not provide a specialized (e.g. io_uring)
// implementation. They simply loop over the indices and issue the ordinary
// single-word `operator[]` lookups one after another.
namespace ad_utility::vocabulary {

// Sequential fallback for `lookupBatch`: look up each index individually via
// `vocab[idx]`, returning one `string_view` per index. Works for any vocabulary
// whose `operator[]` yields something convertible to `std::string`.
template <typename Vocab>
VocabBatchLookupResult sequentialLookupBatch(const Vocab& vocab,
                                             ql::span<const size_t> indices) {
  AD_CONTRACT_CHECK(!indices.empty());
  // Materialize the words as owning `std::string`s and move them into the
  // result's `std::vector<std::string>` buffer. The views then point at those
  // strings; no byte copying into a contiguous buffer is needed. Building the
  // views after the move is safe: moving the vector does not relocate the
  // contained strings.

  std::vector<std::string> words = ::ranges::to<std::vector<std::string>>(
      indices | ql::views::transform(
                    [&vocab](size_t idx) { return std::string{vocab[idx]}; }));

  return makeStringVectorVocabBatchLookupResult(std::move(words));
}

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
  // Query for the special `end` semantics.
  bool isEnd() const { return !wordAndIndex_.has_value(); }

  // Return the word. Throws if `isEnd() == true`.
  const std::string& word() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().first;
  }

  // Return the index. Throws if `isEnd() == true`.
  uint64_t index() const {
    AD_CONTRACT_CHECK(wordAndIndex_.has_value());
    return wordAndIndex_.value().second;
  }

  // _______________________________________________________
  uint64_t indexOrDefault(uint64_t defaultValue) const {
    return isEnd() ? defaultValue : index();
  }

  // The next valid index before `index()`. If `nullopt` either no
  // such index exists (because `index()` is already the first valid index),
  // or the `previousIndex_` simply wasn't set. This member is currently used to
  // communicate between the `VocabularyInMemoryBinSearch` and the
  // `InternalExternalVocabulary`.
  std::optional<uint64_t>& previousIndex() { return previousIndex_; }

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

  // The default constructor creates a `WordAndIndex` with `isEnd() == true`.
  WordAndIndex() = default;

  // Explicit factory function for the end state.
  static WordAndIndex end() { return {}; }

  // Constructors for the ordinary non-end case.
  WordAndIndex(std::string word, uint64_t index)
      : wordAndIndex_{std::in_place, std::move(word), index} {}
  WordAndIndex(std::string_view word, uint64_t index)
      : wordAndIndex_{std::in_place, std::string{word}, index} {}
};

// A common base class for the `WordWriter` types of different vocabulary
// implementations. It has to be called for each of the words (in the correct
// order).
class WordWriterBase {
 private:
  ad_utility::ThrowInDestructorIfSafe throwIfSafe_;
  std::string readableName_;
  std::atomic_bool finishWasCalled_ = false;

 public:
  // Write the next word. The `isExternal` flag is ignored for all the
  // vocabulary implementations but the `VocabularyInternalExternal`. Return the
  // index that was assigned to the word.
  virtual uint64_t operator()(std::string_view word, bool isExternal) = 0;

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

  bool finishWasCalled() const { return finishWasCalled_; }

  // Access to a `readableName` of the vocabulary that is written. Some
  // implementations use it to customize log messages.
  virtual std::string& readableName() { return readableName_; }

 private:
  // The base classes have to implement the actual logic for `finish` here.
  virtual void finishImpl() = 0;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H
