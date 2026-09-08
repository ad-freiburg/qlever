//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYTYPES_H

#include <absl/strings/str_cat.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/memory_resource.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Iterators.h"
#include "util/TransparentFunctors.h"
#include "util/TypeTraits.h"
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

// Sequential fallback for `lookupBatch`: look up each index individually via
// `vocab[idx]`, returning one `string_view` per index. Works for any vocabulary
// whose `operator[]` yields something convertible to `std::string`, or a
// `std::optional` thereof (see `wordAsStringOrPlaceholder`).
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
      indices | ql::views::transform([&vocab](size_t idx) {
        return wordAsStringOrPlaceholder(vocab, idx);
      }));

  auto data = std::make_shared<StringVectorVocabBatchLookupData>();
  data->buffer() = std::move(words);
  data->views() = ::ranges::to_vector(
      data->buffer() |
      ql::views::transform(ad_utility::staticCast<std::string_view>));

  return StringVectorVocabBatchLookupData::asResult(std::move(data));
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
