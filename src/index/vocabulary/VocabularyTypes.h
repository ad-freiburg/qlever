//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

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

#include "backports/memory_resource.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Iterators.h"

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

// The result type of every vocabulary's `scanAll`: a type-erased input range
// that yields all words of the vocabulary in order, one `std::string_view` at
// a time.
using VocabularyScanRange = ad_utility::InputRangeTypeErased<std::string_view>;

namespace ad_utility::vocabulary {

// The number of words that the batched `scanAll` implementations read per
// underlying access. Reading in batches instead of word by word is what makes
// `scanAll` much faster than a plain loop over `operator[]`.
constexpr size_t scanAllBatchSize = 1024;

// Generic fallback implementation of `scanAll` for vocabularies without a
// specialized (batched) one: simply enumerate the words `[0, size())` via the
// ordinary single-word `operator[]`. If `operator[]` already yields a
// `std::string_view` (into the vocabulary's own storage, which outlives the
// range), it is forwarded without copying; otherwise (an owning `std::string`)
// the word is kept alive in a buffer.
template <typename Vocab>
auto scanAllViaOperatorBracket(const Vocab* vocab) {
  return InputRangeFromGetCallable{
      [vocab, nextIdx = uint64_t{0},
       buffer = std::string{}]() mutable -> std::optional<std::string_view> {
        if (nextIdx >= vocab->size()) {
          return std::nullopt;
        }
        decltype(auto) word = (*vocab)[nextIdx++];
        if constexpr (std::is_same_v<std::decay_t<decltype(word)>,
                                     std::string_view>) {
          return word;
        } else {
          buffer = std::move(word);
          return std::string_view{buffer};
        }
      }};
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
  };

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
