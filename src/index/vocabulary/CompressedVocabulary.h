//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_COMPRESSEDVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_COMPRESSEDVOCABULARY_H

#include "backports/algorithm.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary/CompressionWrappers.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/PrefixHeuristic.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/FsstCompressor.h"
#include "util/InputRangeUtils.h"
#include "util/OverloadCallOperator.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/TaskQueue.h"

namespace detail {

template <typename Vocabulary, typename Iterator>
CPP_requires(IterableVocabulary_,
             requires(const Vocabulary& vocabulary,
                      const Iterator& it)(it - vocabulary.begin()));

template <typename Vocabulary, typename Iterator>
CPP_concept IterableVocabulary =
    CPP_requires_ref(IterableVocabulary_, Vocabulary, Iterator);

}  // namespace detail

// A vocabulary in which compression is performed using a customizable
// compression algorithm, with one dictionary per `NumWordsPerBlock` many words
// (default 1 million).
CPP_template(typename UnderlyingVocabulary,
             typename CompressionWrapper =
                 ad_utility::vocabulary::FsstSquaredCompressionWrapper,
             size_t NumWordsPerBlock = 1UL << 20)(
    requires ad_utility::vocabulary::CompressionWrapper<
        CompressionWrapper>) class CompressedVocabulary {
 private:
  UnderlyingVocabulary underlyingVocabulary_;
  CompressionWrapper compressionWrapper_;

  // Whether the underlying vocabulary has "holes", meaning that not every index
  // in `[0, endIndex())` has a word associated with it. This currently is the
  // case for exactly one vocabulary, which we therefore check explicitly. Such
  // a vocabulary needs a special handling in several places (see the uses of
  // this constant below), in particular the vocabulary index of a word is in
  // general different from the position (i.e. the offset into the words) at
  // which the word is stored, and has to be translated via `positionOfIndex`.
  static constexpr bool underlyingHasHoles =
      std::is_same_v<UnderlyingVocabulary, VocabularyInMemoryBinSearch>;

  // As a safeguard for the future: A vocabulary with holes has to report a
  // placeholder (and not throw) for the indices that are holes, see
  // `operator[]` below.
  static_assert(!underlyingHasHoles ||
                ad_utility::vocabulary::replaceOptionalByPlaceholderOnExport<
                    UnderlyingVocabulary>);

 public:
  // Two files are stored, one for the words (which is the base filename of the
  // `UnderlyingVocabulary`) and one for the codebooks. These suffixes are
  // appended to the base filename of this vocabulary to obtain their names.
  static constexpr std::string_view wordsSuffix = ".words";
  static constexpr std::string_view decodersSuffix = ".codebooks";

  // The vocabulary is initialized using the `open()` method, the default
  // constructor leads to an empty vocabulary.
  CompressedVocabulary() = default;

  // Build a `CompressedVocabulary` as a (partially) non-owning, zero-copy view
  // directly into the buffer of `serializer`. The words are deserialized
  // zero-copy by delegating to the `UnderlyingVocabulary` (which therefore has
  // to support zero-copy deserialization itself, see
  // `SupportsZeroCopyDeserialization`). The `decoders` are small, so they are
  // simply read (and thus copied) as usual. The layout read here exactly
  // matches the one written by the generic serialization function below. The
  // returned vocabulary is only valid as long as the memory backing
  // `serializer`'s buffer is valid and unchanged.
  // Note: This has to use `CPP_template_2` (and not `CPP_template`), because
  // the enclosing class is itself constrained via `CPP_template`.
  CPP_template_2(typename S)(
      requires ad_utility::serialization::SupportsZeroCopyDeserialization<
          UnderlyingVocabulary, S>) static CompressedVocabulary
      fromZeroCopyDeserializer(S& serializer) {
    CompressedVocabulary result;
    result.underlyingVocabulary_ =
        UnderlyingVocabulary::fromZeroCopyDeserializer(serializer);
    std::vector<typename CompressionWrapper::Decoder> decoders;
    serializer | decoders;
    result.compressionWrapper_ = CompressionWrapper{{std::move(decoders)}};
    return result;
  }

  // Get the uncompressed word at the given index. If the underlying vocabulary
  // has holes (see `VocabularyInMemoryBinSearch`) and `idx` is one of those
  // holes, return a placeholder (see `placeholderForMissingVocabIndex`).
  std::string operator[](uint64_t idx) const {
    if constexpr (underlyingHasHoles) {
      // NOTE: We deliberately translate the index to a position (which
      // requires a binary search) exactly once, and then use that position
      // both to look up the word and to select the decoder. Using
      // `underlyingVocabulary_[idx]` together with `getDecoderIdx(idx)` would
      // perform the very same binary search twice.
      auto position = underlyingVocabulary_.positionOfIndex(idx);
      if (!position.has_value()) {
        return ad_utility::vocabulary::placeholderForMissingVocabIndex(idx);
      }
      return compressionWrapper_.decompress(
          underlyingVocabulary_.wordAtPosition(position.value()),
          getDecoderIdxFromPosition(position.value()));
    } else {
      decltype(auto) word = underlyingVocabulary_[idx];
      // As a safeguard for the future: only a vocabulary that deliberately has
      // holes (see `underlyingHasHoles` and
      // `replaceOptionalByPlaceholderOnExport` in `VocabularyTypes.h`) may
      // report a missing word via an empty optional instead of throwing, and
      // such a vocabulary is handled by the branch above.
      static_assert(
          !ad_utility::similarToInstantiation<decltype(word), std::optional>);
      return compressionWrapper_.decompress(toStringView(word),
                                            getDecoderIdx(idx));
    }
  }

  // Wrap the underlying vocabulary's `scanAll` (which reads the compressed
  // words in batches) and decompress each word. `scanAll()` is expected to
  // yield `IndexAndWord` elements, so we have to apply a transformation at the
  // end.
  auto scanAll() const {
    // NOTE: The correct decoder is selected by the position of the word, which
    // for a vocabulary with holes is different from its vocabulary index. As
    // the scan yields the words in order, we can simply count the yielded
    // elements instead of translating each index to its position (which would
    // require a binary search per word).
    return ad_utility::CachingTransformInputRange(
        underlyingVocabulary_.scanAll(),
        [this, buffer = std::string{},
         position = size_t{0}](const IndexAndWord& compressed) mutable {
          const auto& [index, word] = compressed;
          buffer = compressionWrapper_.decompress(
              word, getDecoderIdxFromPosition(position));
          ++position;
          return IndexAndWord{index, buffer};
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

  [[nodiscard]] uint64_t size() const { return underlyingVocabulary_.size(); }

  // Return the vocabulary index one past the largest index that is contained
  // in this vocabulary. For an underlying vocabulary with holes (see
  // `VocabularyInMemoryBinSearch`) this is in general much larger than
  // `size()`, for all other vocabularies it is exactly `size()`.
  uint64_t endIndex() const {
    if constexpr (underlyingHasHoles) {
      return underlyingVocabulary_.endIndex();
    } else {
      return size();
    }
  }

  // Return the range of vocabulary indices at which `word` is stored, or the
  // empty range at the index at which it would be stored if it is not
  // contained. This is only needed (and, see `HasSpecialGetPositionOfWord` in
  // `VocabularyConstraints.h`, only used) if the underlying vocabulary has
  // holes, for which the generic implementation would use the wrong "one past
  // the end" index (see `endIndex`). The `static_assert` below enforces that
  // this function is never instantiated for any other underlying vocabulary
  // (for which the generic implementation is correct and cheaper).
  template <typename InternalStringType, typename Comparator>
  std::pair<uint64_t, uint64_t> getPositionOfWord(
      const InternalStringType& word, Comparator comparator) const {
    static_assert(underlyingHasHoles,
                  "`CompressedVocabulary::getPositionOfWord` is only intended "
                  "for an underlying vocabulary with holes, see "
                  "`HasSpecialGetPositionOfWord` in `VocabularyConstraints.h`");
    return ad_utility::vocabulary::getPositionOfWordInVocabWithHoles(
        *this, word, std::move(comparator), endIndex());
  }

  // From a `comparator` that can compare two strings, make a new comparator,
  // that can compare a string and an `iterator` by decompressing the word that
  // the iterator points to. The returned comparator is symmetric, meaning that
  // the iterator can either be the left or the right argument.
  template <typename StringType, typename Comparator>
  auto makeSymmetricComparator(Comparator comparator = Comparator{}) const {
    auto pred1 = [comparator, self = this](const StringType& el,
                                           const auto& it) {
      return comparator(el, self->decompressFromIterator(it));
    };
    auto pred2 = [comparator, self = this](const auto& it,
                                           const StringType& el) {
      return comparator(self->decompressFromIterator(it), el);
    };
    return ad_utility::OverloadCallOperator{pred1, pred2};
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`. Only works correctly if the
  /// `words_` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto actualComparator =
        makeSymmetricComparator<InternalStringType>(comparator);

    auto wordAndIndex =
        underlyingVocabulary_.lower_bound_iterator(word, actualComparator);
    return convertWordAndIndexFromUnderlyingVocab(wordAndIndex);
  }

  /// Return a `WordAndIndex` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `words_`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto actualComparator =
        makeSymmetricComparator<InternalStringType>(comparator);
    auto wordAndIndex =
        underlyingVocabulary_.upper_bound_iterator(word, actualComparator);
    return convertWordAndIndexFromUnderlyingVocab(wordAndIndex);
  }

  /// Open the underlying vocabulary from a file. The vocabulary must have been
  /// created by using the `WordWriter` of this class, which is a
  /// `DiskWriterWithExplicitIndices` if the underlying vocabulary has holes
  /// (see `VocabularyInMemoryBinSearch`) and a
  /// `DiskWriterFromUncompressedWords` otherwise.
  void open(const std::string& filename) {
    underlyingVocabulary_.open(absl::StrCat(filename, wordsSuffix));
    ad_utility::serialization::FileReadSerializer decoderReader(
        absl::StrCat(filename, decodersSuffix));
    std::vector<typename CompressionWrapper::Decoder> decoders;
    decoderReader >> decoders;
    compressionWrapper_ = CompressionWrapper{{std::move(decoders)}};
    AD_CORRECTNESS_CHECK((size() == 0) || (getDecoderIdxFromPosition(size()) <=
                                           compressionWrapper_.numDecoders()));
  }

  /// Allows the incremental writing of the words to disk. Uses `WordWriter` of
  /// the underlying vocabulary.
  //
  // NOTE: This class is a template with the `UnderlyingVocab` defaulted to the
  // `UnderlyingVocabulary` of the enclosing class (it is never instantiated
  // with anything else). This is necessary because its member functions don't
  // compile for an underlying vocabulary with holes (see
  // `VocabularyInMemoryBinSearch`), the `WordWriter` of which requires an
  // explicit index for each word. We therefore have to defer the instantiation
  // of this class until its actual usage, which for a vocabulary with holes
  // never happens (`DiskWriterWithExplicitIndices` is used instead).
  template <typename UnderlyingVocab = UnderlyingVocabulary>
  class DiskWriterFromUncompressedWords : public WordWriterBase {
   private:
    std::vector<std::string> wordBuffer_;
    std::vector<bool> isExternalBuffer_;
    std::vector<typename CompressionWrapper::Decoder> decoders_;
    typename UnderlyingVocab::WordWriter underlyingWriter_;
    std::string filenameDecoders_;
    ad_utility::MemorySize uncompressedSize_ = bytes(0);
    ad_utility::MemorySize compressedSize_ = bytes(0);
    size_t numBlocks_ = 0u;
    size_t numBlocksLargerWhenCompressed_ = 0u;
    ad_utility::data_structures::OrderedThreadSafeQueue<std::function<void()>>
        writeQueue_{5};
    ad_utility::JThread writeThread_{[this] {
      while (auto opt = writeQueue_.pop()) {
        opt.value()();
      }
    }};
    std::atomic<size_t> queueIndex_ = 0;
    ad_utility::TaskQueue<false> compressQueue_{10, 10};
    uint64_t counter_ = 0;

   public:
    /// Constructor. The `filename` is the base filename of the vocabulary; the
    /// names of the two files that are actually written are derived from it via
    /// `wordsSuffix` and `decodersSuffix`.
    explicit DiskWriterFromUncompressedWords(const std::string& filename)
        : underlyingWriter_{absl::StrCat(filename, wordsSuffix)},
          filenameDecoders_{absl::StrCat(filename, decodersSuffix)} {}

    /// Compress the `uncompressedWord` and write it to disk.
    uint64_t operator()(std::string_view uncompressedWord,
                        bool isExternal) override {
      wordBuffer_.emplace_back(uncompressedWord);
      isExternalBuffer_.push_back(isExternal);
      if (wordBuffer_.size() == NumWordsPerBlock) {
        finishBlock();
      }
      return counter_++;
    }

    ~DiskWriterFromUncompressedWords() override {
      if (!finishWasCalled()) {
        ad_utility::terminateIfThrows([this]() { this->finish(); },
                                      "Calling `finish` from the destructor of "
                                      "`DiskWriterFromUncompressedWords`");
      }
    }

   private:
    /// Dump all the words that still might be contained in intermediate buffers
    /// to the underlying file and close the file. After calls to `finish()` no
    /// more words can be pushed. `finish()` is implicitly also called by the
    /// destructor.
    void finishImpl() override {
      finishBlock();
      compressQueue_.finish();
      writeQueue_.finish();
      AD_CORRECTNESS_CHECK(writeThread_.joinable());
      writeThread_.join();
      underlyingWriter_.finish();
      ad_utility::serialization::FileWriteSerializer decoderWriter(
          filenameDecoders_);
      decoderWriter << decoders_;
      auto compressionRatio =
          (100ULL * std::max(compressedSize_.getBytes(), size_t(1))) /
          std::max(uncompressedSize_.getBytes(), size_t(1));
      std::string nameString =
          readableName().empty() ? std::string{"vocabulary"} : readableName();
      AD_LOG_INFO << "Finished writing compressed " << nameString
                  << ", size = " << compressedSize_
                  << " [uncompressed = " << uncompressedSize_
                  << ", ratio = " << compressionRatio << "%]" << std::endl;
      if (numBlocksLargerWhenCompressed_ > 0) {
        AD_LOG_WARN
            << "Number of blocks made larger by the compression instead "
               "of smaller: "
            << numBlocksLargerWhenCompressed_ << " of " << numBlocks_
            << std::endl;
      }
    }

   public:
    DiskWriterFromUncompressedWords(const DiskWriterFromUncompressedWords&) =
        delete;
    DiskWriterFromUncompressedWords& operator=(
        const DiskWriterFromUncompressedWords&) = delete;

   private:
    // Compress a complete block and write it to the underlying vocabulary.
    void finishBlock() {
      if (wordBuffer_.empty()) {
        return;
      }

      static constexpr auto getSize = [](const auto& words) {
        return std::accumulate(
            words.begin(), words.end(), bytes(0),
            [](auto x, std::string_view v) { return x + bytes(v.size()); });
      };
      auto uncompressedSize = getSize(wordBuffer_);
      uncompressedSize_ += uncompressedSize;

      auto compressAndWrite = [uncompressedSize, words = std::move(wordBuffer_),
                               this, idx = queueIndex_++,
                               isExternalBuffer =
                                   std::move(isExternalBuffer_)]() mutable {
        auto bulkResult = CompressionWrapper::compressAll(words);
        writeQueue_.push(std::pair{
            idx, [uncompressedSize, bulkResult = std::move(bulkResult), this,
                  isExternalBuffer = std::move(isExternalBuffer)]() {
              auto& [buffer, views, decoder] = bulkResult;
              auto compressedSize = getSize(views);
              compressedSize_ += compressedSize;
              ++numBlocks_;
              numBlocksLargerWhenCompressed_ +=
                  static_cast<size_t>(compressedSize > uncompressedSize);
              size_t i = 0;
              for (auto& word : views) {
                if constexpr (std::is_invocable_v<decltype(underlyingWriter_),
                                                  decltype(word), bool>) {
                  underlyingWriter_(word, isExternalBuffer.at(i));
                  ++i;
                } else {
                  underlyingWriter_(word);
                }
              }
              decoders_.emplace_back(decoder);
            }});
      };
      compressQueue_.push(std::move(compressAndWrite));
      wordBuffer_.clear();
      isExternalBuffer_.clear();
    }
  };
  // A writer for a `CompressedVocabulary` whose `UnderlyingVocabulary` supports
  // "holes" (see `VocabularyInMemoryBinSearch`) and therefore requires an
  // explicit index for each word. Such a vocabulary cannot be written via the
  // `WordWriterBase` interface (which cannot express those indices), so this
  // class deliberately does not derive from `WordWriterBase`, but mirrors the
  // interface of `VocabularyInMemoryBinSearch::WordWriter`. It is deliberately
  // kept simple and synchronous (no thread pool or task queue), because the
  // filtered vocabularies that it builds are small for all current use cases.
  class DiskWriterWithExplicitIndices {
   private:
    std::vector<std::string> wordBuffer_;
    std::vector<uint64_t> indexBuffer_;
    std::vector<typename CompressionWrapper::Decoder> decoders_;
    typename UnderlyingVocabulary::WordWriter underlyingWriter_;
    std::string filenameDecoders_;
    bool finishWasCalled_ = false;
    // The index of the word that was added last, `nullopt` if no word has been
    // added yet.
    std::optional<uint64_t> lastIndex_ = std::nullopt;

   public:
    // Constructor. The `filename` is the base filename of the vocabulary; the
    // names of the two files that are actually written are derived from it via
    // `wordsSuffix` and `decodersSuffix`.
    explicit DiskWriterWithExplicitIndices(const std::string& filename)
        : underlyingWriter_{absl::StrCat(filename, wordsSuffix)},
          filenameDecoders_{absl::StrCat(filename, decodersSuffix)} {}

    // This type can neither be copied nor moved (the user-declared destructor
    // below suppresses the implicit move operations). It is always used
    // directly at the place where it is created.
    DiskWriterWithExplicitIndices(const DiskWriterWithExplicitIndices&) =
        delete;
    DiskWriterWithExplicitIndices& operator=(
        const DiskWriterWithExplicitIndices&) = delete;

    // Destructor. Call `finish` if that hasn't happened yet.
    ~DiskWriterWithExplicitIndices() {
      ad_utility::terminateIfThrows([this]() { this->finish(); },
                                    "Calling `finish` from the destructor of "
                                    "`DiskWriterWithExplicitIndices`");
    }

    // Add the `uncompressedWord` with the given vocabulary index `idx`, which
    // must be greater than all previous indices. Return `idx`.
    uint64_t operator()(std::string_view uncompressedWord, uint64_t idx) {
      AD_CONTRACT_CHECK(!finishWasCalled_);
      // Check the invariant eagerly here, and not only in the underlying
      // writer (which only sees the words once a full block is written), such
      // that a violation is reported at the call site that caused it.
      AD_CONTRACT_CHECK(!lastIndex_.has_value() || lastIndex_.value() < idx,
                        "The indices of a vocabulary with holes have to be "
                        "added in strictly ascending order");
      lastIndex_ = idx;
      wordBuffer_.emplace_back(uncompressedWord);
      indexBuffer_.push_back(idx);
      if (wordBuffer_.size() == NumWordsPerBlock) {
        finishBlock();
      }
      return idx;
    }

    // Write the last (partial) block and the decoders to disk. Calling this
    // function multiple times has no additional effect.
    void finish() {
      if (finishWasCalled_) {
        return;
      }
      finishWasCalled_ = true;
      finishBlock();
      underlyingWriter_.finish();
      ad_utility::serialization::FileWriteSerializer decoderWriter(
          filenameDecoders_);
      decoderWriter << decoders_;
    }

   private:
    // Compress the words that are currently buffered and write them, together
    // with their explicit indices, to the underlying vocabulary.
    void finishBlock() {
      if (wordBuffer_.empty()) {
        return;
      }
      auto bulkResult = CompressionWrapper::compressAll(wordBuffer_);
      // NOTE: The `buffer` owns the memory that the `views` point into, so it
      // has to be kept alive until all the words have been written.
      auto& [buffer, views, decoder] = bulkResult;
      (void)buffer;
      AD_CORRECTNESS_CHECK(views.size() == indexBuffer_.size());
      for (const auto& [word, index] :
           ::ranges::views::zip(views, indexBuffer_)) {
        underlyingWriter_(word, index);
      }
      decoders_.emplace_back(std::move(decoder));
      wordBuffer_.clear();
      indexBuffer_.clear();
    }
  };

  // The `WordWriter` for an underlying vocabulary with holes has to take an
  // explicit index for each word.
  using WordWriter =
      std::conditional_t<underlyingHasHoles, DiskWriterWithExplicitIndices,
                         DiskWriterFromUncompressedWords<>>;

  // The files of the underlying vocabulary, which is stored under the base
  // filename plus `wordsSuffix`, plus the file for the codebooks.
  static FileSuffixes fileSuffixes() {
    FileSuffixes suffixes;
    addFileSuffixesWithPrefix(suffixes, wordsSuffix,
                              UnderlyingVocabulary::fileSuffixes());
    suffixes.emplace_back(decodersSuffix);
    return suffixes;
  }

  // Return a `unique_ptr<DiskWriterFromUncompressedWords>` that can be used to
  // create the vocabulary. For an underlying vocabulary with holes this throws,
  // because such a vocabulary requires an explicit index for each word (see
  // `DiskWriterWithExplicitIndices`).
  // NOTE: The return type is the concrete writer type (and not
  // `std::unique_ptr<WordWriterBase>`), because some callers (e.g.
  // `GeoVocabulary::WordWriter`) store the result as such.
  static std::unique_ptr<DiskWriterFromUncompressedWords<>> makeDiskWriterPtr(
      const std::string& filename) {
    if constexpr (underlyingHasHoles) {
      (void)filename;
      AD_THROW(
          "A vocabulary with holes cannot be built word by word, because the "
          "`WordWriterBase` interface cannot express the explicit indices. "
          "Such a vocabulary can only be created by filtering an existing "
          "vocabulary.");
    } else {
      return std::make_unique<DiskWriterFromUncompressedWords<>>(filename);
    }
  }

  // Access to the underlying vocabulary.
  UnderlyingVocabulary& getUnderlyingVocabulary() {
    return underlyingVocabulary_;
  }
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return underlyingVocabulary_;
  }

  void close() { underlyingVocabulary_.close(); }

  // Generic serialization support.
  AD_SERIALIZE_FRIEND_FUNCTION(CompressedVocabulary) {
    serializer | arg.underlyingVocabulary_;
    if constexpr (ad_utility::serialization::WriteSerializer<S>) {
      // Serialize the decoders.
      const auto& decoders = arg.compressionWrapper_.getDecoders();
      serializer | decoders;
    } else {
      // Deserialize the decoders.
      std::vector<typename CompressionWrapper::Decoder> decoders;
      serializer | decoders;
      arg.compressionWrapper_ = CompressionWrapper{{std::move(decoders)}};
    }
  }

 private:
  // Get the correct decoder for the word at the given position. One decoder is
  // created per `NumWordsPerBlock` words that are pushed to the `WordWriter`,
  // so `position` has to be the position of the word in exactly that sequence
  // of pushed words. Note that this is a different quantity for each of the
  // underlying vocabularies: for a `VocabularyInMemory` it is simply the
  // vocabulary index; for a `VocabularyInternalExternal` it is the *global*
  // vocabulary index (all words are pushed, also those that additionally end up
  // in the internal vocabulary); and for a vocabulary with holes (see
  // `VocabularyInMemoryBinSearch`) it is the offset into the words, which
  // because of the holes is smaller than the vocabulary index.
  size_t getDecoderIdxFromPosition(size_t position) const {
    return position / NumWordsPerBlock;
  }

  // Get the correct decoder for the given vocabulary index `idx`. For a
  // vocabulary with holes (see `VocabularyInMemoryBinSearch`) the index has to
  // be translated to a position first (see `getDecoderIdxFromPosition`).
  size_t getDecoderIdx(size_t idx) const {
    if constexpr (underlyingHasHoles) {
      // NOTE: If `idx` is one of the holes, then there is no word to
      // decompress at all, and no caller ever asks for a decoder in that case
      // (`operator[]` returns the placeholder directly), so the value of the
      // fallback is irrelevant.
      return getDecoderIdxFromPosition(
          underlyingVocabulary_.positionOfIndex(idx).value_or(0));
    } else {
      return getDecoderIdxFromPosition(idx);
    }
  }

  // Decompress the word that `it` points to. `it` is an iterator into the
  // underlying vocabulary.
  template <typename It>
  auto decompressFromIterator(It it) const {
    auto position = [&]() {
      if constexpr (detail::IterableVocabulary<UnderlyingVocabulary, It>) {
        return it - underlyingVocabulary_.begin();
      } else {
        return underlyingVocabulary_.iteratorToIndex(it);
      }
    }();
    return compressionWrapper_.decompress(toStringView(*it),
                                          getDecoderIdxFromPosition(position));
  }

  // ____________________________________________________
  static constexpr ad_utility::MemorySize bytes(size_t numBytes) {
    return ad_utility::MemorySize::bytes(numBytes);
  }

  // _________________________________________________________________
  template <typename T>
  static std::string_view toStringView(const T& el) {
    if constexpr (ranges::convertible_to<T, std::string_view>) {
      return el;
    } else if constexpr (ad_utility::isInstantiation<T, std::optional>) {
      return toStringView(el.value());
    } else {
      // WordAndIndex
      return el.word_.value();
    }
  }

  // Convert a `WordAndIndex` from the underlying vocabulary by decompressing
  // the word.
  WordAndIndex convertWordAndIndexFromUnderlyingVocab(
      const WordAndIndex& wordAndIndex) const {
    if (wordAndIndex.isEnd()) {
      return wordAndIndex;
    }
    auto decompressedWord = compressionWrapper_.decompress(
        wordAndIndex.word(), getDecoderIdx(wordAndIndex.index()));
    return {std::move(decompressedWord), wordAndIndex.index()};
  }
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_COMPRESSEDVOCABULARY_H
