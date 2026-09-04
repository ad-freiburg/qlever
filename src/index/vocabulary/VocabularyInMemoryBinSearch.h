// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYINMEMORYBINSEARCH_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYINMEMORYBINSEARCH_H

#include <array>
#include <string>
#include <string_view>
#include <variant>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "index/vocabulary/VocabularyBinarySearchMixin.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Algorithm.h"
#include "util/CompactStringVector.h"
#include "util/Exception.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"

// A vocabulary that stores all words in memory. The vocabulary supports
// "holes", meaning that the indices of the contained words don't have to be
// contiguous (but ascending). All accesses are implemented using binary search.
class VocabularyInMemoryBinSearch
    : public VocabularyBinarySearchMixin<VocabularyInMemoryBinSearch> {
 public:
  using CharType = char;
  using StringView = std::basic_string_view<CharType>;
  using String = std::basic_string<CharType>;
  using Words = CompactVectorOfStrings<CharType>;
  using Indices = std::vector<uint64_t>;
  using IndicesView = ql::span<const uint64_t>;

  // This suffix is appended to the base filename in order to get the name of
  // the file in which the (because of the holes, explicit) indices of the words
  // are stored. The words themselves are stored under the base filename itself.
  static constexpr std::string_view idsSuffix = ".ids";

  // The holes of this vocabulary are deliberate: such a vocabulary is created
  // by excluding some of the entries of a larger vocabulary, and is used in
  // settings where looking up an excluded entry must not throw. Exporting a
  // word that is not contained therefore yields
  // `ad_utility::vocabulary::placeholderForMissingVocabIndex` instead of an
  // exception (see `VocabularyTypes.h`).
  static constexpr bool replaceOptionalByPlaceholderOnExport = true;

 private:
  // The actual storage. The indices are stored either as an owned vector
  // (after `open()`, or after reading from a regular, non-zero-copy
  // serializer), or as a non-owning view into externally-owned memory (after
  // `fromZeroCopyDeserializer`).
  Words words_;
  std::variant<Indices, IndicesView> indices_;

 public:
  // Construct an empty vocabulary
  VocabularyInMemoryBinSearch() = default;

  // Vocabularies are movable (but not copyable).
  VocabularyInMemoryBinSearch& operator=(
      VocabularyInMemoryBinSearch&&) noexcept = default;
  VocabularyInMemoryBinSearch(VocabularyInMemoryBinSearch&&) noexcept = default;

  // Build a vocabulary as a non-owning, zero-copy view directly into the
  // buffer of `serializer`, which must support zero-copy deserialization (see
  // `ZeroCopyReadSerializer` in `util/Serializer/Serializer.h`). The returned
  // vocabulary is only valid as long as the memory backing `serializer`'s
  // buffer is valid and unchanged. The layout read here exactly matches the one
  // written by the generic serialization function below.
  CPP_template(typename S)(
      requires ad_utility::serialization::ZeroCopyReadSerializer<
          S>) static VocabularyInMemoryBinSearch
      fromZeroCopyDeserializer(S& serializer) {
    VocabularyInMemoryBinSearch result;
    result.words_ = Words::fromZeroCopyDeserializer(serializer);
    result.indices_ =
        ad_utility::serialization::zeroCopyDeserializeToSpan<uint64_t>(
            serializer);
    return result;
  }

  // Const access to the indices, no matter whether they are currently owned or
  // only viewed.
  IndicesView indices() const;

  // Read the vocabulary from a file. The file must have been created using a
  // `WordWriter`.
  void open(const std::string& fileName);

  // Return the total number of words
  [[nodiscard]] size_t size() const {
    AD_CORRECTNESS_CHECK(indices().size() == words_.size());
    return words_.size();
  }

  // Return the position (i.e. the offset into the words) of the word with the
  // given vocabulary `index`, or `std::nullopt` if `index` is not contained in
  // this vocabulary (which can happen because of the "holes", see above).
  std::optional<size_t> positionOfIndex(uint64_t index) const;

  // Return the vocabulary index of the word at the given `position`. The
  // `position` must be smaller than `size()`.
  uint64_t indexAtPosition(size_t position) const;

  // Return the word at the given `position` (i.e. the offset into the words,
  // which because of the holes is in general different from the vocabulary
  // index, see `positionOfIndex`). The `position` must be smaller than
  // `size()`.
  std::string_view wordAtPosition(size_t position) const;

  // Return the vocabulary index one past the largest index that is contained
  // in this vocabulary, or `0` if the vocabulary is empty. Because of the
  // holes, this is in general much larger than `size()`.
  uint64_t endIndex() const;

  // Return the range of vocabulary indices at which `word` is stored, or the
  // empty range at the index at which it would be stored if it is not
  // contained. This vocabulary needs a special implementation of this function
  // (see `HasSpecialGetPositionOfWord` in `VocabularyConstraints.h`), because
  // the generic implementation would use `size()` as the "one past the end"
  // index, which is wrong in the presence of holes (see `endIndex`).
  template <typename InternalStringType, typename Comparator>
  std::pair<uint64_t, uint64_t> getPositionOfWord(
      const InternalStringType& word, Comparator comparator) const {
    return ad_utility::vocabulary::getPositionOfWordInVocabWithHoles(
        *this, word, std::move(comparator), endIndex());
  }

  // Return the word with index `index`. If this index is not part of the
  // vocabulary, return `std::nullopt`.
  std::optional<std::string_view> operator[](uint64_t index) const;

  // Iterate over all words of the vocabulary in order, together with their
  // (because of the holes, not necessarily contiguous) vocabulary index.
  auto scanAll() const {
    return ::ranges::views::zip(indices(), words_) |
           ql::views::transform([](const auto& indexAndWord) {
             const auto& [index, word] = indexAndWord;
             return IndexAndWord{index, word};
           });
  }

  //____________________________________________________________________________
  VocabBatchLookupResult lookupBatch(ql::span<const size_t> indices) const {
    return ad_utility::vocabulary::sequentialLookupBatch(*this, indices);
  }

  //____________________________________________________________________________
  VocabLookupOutput lookupBatchesStreamed(VocabLookupInput input) const {
    return ad_utility::vocabulary::lookupBatchesStreamed(*this,
                                                         std::move(input));
  }

  // Convert an iterator to a `WordAndIndex`. Required for the mixin.
  WordAndIndex iteratorToWordAndIndex(ql::ranges::iterator_t<Words> it) const;

  // A helper type that can be used to directly write a vocabulary to disk
  // word-by-word, without having to materialize it in RAM first.
  struct WordWriter {
    typename Words::Writer writer_;
    using OffsetWriter = ad_utility::serialization::VectorIncrementalSerializer<
        uint64_t, ad_utility::serialization::FileWriteSerializer>;
    OffsetWriter offsetWriter_;
    std::optional<uint64_t> lastIndex_ = std::nullopt;
    // Construct a `WordWriter` that will write to the given `filename`.
    explicit WordWriter(const std::string& filename);
    // Add the given `word` with the given `idx`. The `idx` must be greater than
    // all previous indices.
    uint64_t operator()(std::string_view word, uint64_t idx);

    // Finish writing and dump all contents that still reside in buffers to
    // disk.
    void finish();

    // The suffixes of the files that this `WordWriter` writes. This class does
    // not inherit from `WordWriterBase` (see `makeDiskWriterPtr` below), but
    // mirrors its `fileSuffixes` interface.
    static ql::span<const std::string_view> fileSuffixes() {
      return fileSuffixes_;
    }

   private:
    static constexpr std::array<std::string_view, 2> fileSuffixes_{"",
                                                                   idsSuffix};
  };

  // A vocabulary with holes cannot be written via the `WordWriterBase`
  // interface (which cannot express the explicit indices), so this function
  // always throws. Use the nested `WordWriter` above instead.
  [[noreturn]] static std::unique_ptr<WordWriterBase> makeDiskWriterPtr(
      const std::string& filename);

  // Clear the vocabulary.
  void close();

  // Const access to the underlying words.
  auto begin() const { return words_.begin(); }
  auto end() const { return words_.end(); }

  // Generic serialization support. Note: Reading always produces a vocabulary
  // that owns its indices; use `fromZeroCopyDeserializer` (see above) to obtain
  // a non-owning, zero-copy view.
  AD_SERIALIZE_FRIEND_FUNCTION(VocabularyInMemoryBinSearch) {
    serializer | arg.words_;
    if constexpr (ad_utility::serialization::WriteSerializer<S>) {
      serializer << arg.indices();
    } else {
      auto& indices = arg.indices_.template emplace<Indices>();
      serializer | indices;
    }
  }

 private:
  // Access the owned indices. Throws (via `std::get`) if this vocabulary
  // currently only views its indices, which is a programming error (a
  // zero-copy view is read-only).
  Indices& ownedIndices() { return std::get<Indices>(indices_); }
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYINMEMORYBINSEARCH_H
