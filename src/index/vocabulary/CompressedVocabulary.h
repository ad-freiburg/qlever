//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "index/ConstantsIndexBuilding.h"
#include "index/PrefixHeuristic.h"
#include "index/vocabulary/CompressionWrappers.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/FsstCompressor.h"
#include "util/OverloadCallOperator.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializePair.h"
#include "util/TaskQueue.h"

// A vocabulary in which compression is performed using a customizable
// compression algorithm, with one dictionary per `NumWordsPerBlock` many words
// (default 1 million).
template <typename UnderlyingVocabulary,
          ad_utility::vocabulary::CompressionWrapper CompressionWrapper =
              ad_utility::vocabulary::FsstSquaredCompressionWrapper,
          size_t NumWordsPerBlock = 1UL << 20>
class CompressedVocabulary {
 private:
  UnderlyingVocabulary underlyingVocabulary_;
  CompressionWrapper compressionWrapper_;
  // We need to store two files, one for the words and one for the codebooks.
  static constexpr std::string_view wordsSuffix = ".words";
  static constexpr std::string_view decodersSuffix = ".codebooks";

 public:
  // The vocabulary is initialized using the `open()` method, the default
  // constructor leads to an empty vocabulary.
  CompressedVocabulary() = default;

  // Get the uncompressed word at the given index.
  std::string operator[](uint64_t idx) const {
    return compressionWrapper_.decompress(
        toStringView(underlyingVocabulary_[idx]), getDecoderIdx(idx));
  }

  [[nodiscard]] uint64_t size() const { return underlyingVocabulary_.size(); }

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
  /// created by using a `DiskWriterFromUncompressedWords`.
  void open(const std::string& filename) {
    underlyingVocabulary_.open(absl::StrCat(filename, wordsSuffix));
    ad_utility::serialization::FileReadSerializer decoderReader(
        absl::StrCat(filename, decodersSuffix));
    std::vector<typename CompressionWrapper::Decoder> decoders;
    decoderReader >> decoders;
    compressionWrapper_ = CompressionWrapper{{std::move(decoders)}};
    AD_CORRECTNESS_CHECK((size() == 0) || (getDecoderIdx(size()) <=
                                           compressionWrapper_.numDecoders()));
  }

  /// Allows the incremental writing of the words to disk. Uses `WordWriter` of
  /// the underlying vocabulary.
  class DiskWriterFromUncompressedWords {
   private:
    std::vector<std::string> wordBuffer_;
    std::vector<bool> isExternalBuffer_;
    std::vector<typename CompressionWrapper::Decoder> decoders_;
    typename UnderlyingVocabulary::WordWriter underlyingWriter_;
    std::string filenameDecoders_;
    std::string readableName_ = "";
    bool isFinished_ = false;
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

   public:
    /// Constructor.
    explicit DiskWriterFromUncompressedWords(
        const std::string& filenameWords, const std::string& filenameDecoders)
        : underlyingWriter_{filenameWords},
          filenameDecoders_{filenameDecoders} {}

    /// Compress the `uncompressedWord` and write it to disk.
    void operator()(std::string_view uncompressedWord,
                    bool isExternal = false) {
      AD_CORRECTNESS_CHECK(!isFinished_);
      wordBuffer_.emplace_back(uncompressedWord);
      isExternalBuffer_.push_back(isExternal);
      if (wordBuffer_.size() == NumWordsPerBlock) {
        finishBlock();
      }
    }

    /// Dump all the words that still might be contained in intermediate buffers
    /// to the underlying file and close the file. After calls to `finish()` no
    /// more words can be pushed. `finish()` is implicitly also called by the
    /// destructor.
    void finish() {
      if (std::exchange(isFinished_, true)) {
        return;
      }
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
          readableName_.empty() ? std::string{"vocabulary"} : readableName_;
      LOG(INFO) << "Finished writing compressed " << nameString
                << ", size = " << compressedSize_
                << " [uncompressed = " << uncompressedSize_
                << ", ratio = " << compressionRatio << "%]" << std::endl;
      if (numBlocksLargerWhenCompressed_ > 0) {
        LOG(WARN) << "Number of blocks made larger by the compression instead "
                     "of smaller: "
                  << numBlocksLargerWhenCompressed_ << " of " << numBlocks_
                  << std::endl;
      }
    }

    // Access the human-readable description for the vocabulary to be written
    // that will be used in log message.
    std::string& readableName() { return readableName_; }

    // Call `finish`, does nothing if `finish` has been manually called.
    ~DiskWriterFromUncompressedWords() noexcept {
      ad_utility::terminateIfThrows(
          [this]() { this->finish(); },
          "The destructor of a DiskWriter of a `CompressedVocabulary`");
    }
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
                if constexpr (requires() { underlyingWriter_(word, false); }) {
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
  using WordWriter = DiskWriterFromUncompressedWords;

  // Return a `DiskWriter` that can be used to create the vocabulary.
  DiskWriterFromUncompressedWords makeDiskWriter(
      const std::string& filename) const {
    return DiskWriterFromUncompressedWords{
        absl::StrCat(filename, wordsSuffix),
        absl::StrCat(filename, decodersSuffix)};
  }
  /// Initialize the vocabulary from the given `words`.
  // TODO<joka921> This can be a generic Mixin...
  void build(const std::vector<std::string>& words,
             const std::string& filename) {
    WordWriter writer = makeDiskWriter(filename);
    for (const auto& word : words) {
      writer(word);
    }
    writer.finish();
    open(filename);
  }

  // Access to the underlying vocabulary.
  UnderlyingVocabulary& getUnderlyingVocabulary() {
    return underlyingVocabulary_;
  }
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return underlyingVocabulary_;
  }

  void close() { underlyingVocabulary_.close(); }

 private:
  // Get the correct decoder for the given `idx`.
  size_t getDecoderIdx(size_t idx) const { return idx / NumWordsPerBlock; }

  // Decompress the word that `it` points to. `it` is an iterator into the
  // underlying vocabulary.
  auto decompressFromIterator(auto it) const {
    auto idx = [&]() {
      if constexpr (requires() { it - underlyingVocabulary_.begin(); }) {
        return it - underlyingVocabulary_.begin();
      } else {
        return underlyingVocabulary_.iteratorToIndex(it);
      }
    }();
    return compressionWrapper_.decompress(toStringView(*it),
                                          getDecoderIdx(idx));
  };

  // ____________________________________________________
  static constexpr ad_utility::MemorySize bytes(size_t numBytes) {
    return ad_utility::MemorySize::bytes(numBytes);
  };

  // _________________________________________________________________
  template <typename T>
  static std::string_view toStringView(const T& el) {
    if constexpr (std::convertible_to<T, std::string_view>) {
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
