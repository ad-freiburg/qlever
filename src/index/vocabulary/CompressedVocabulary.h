//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDVOCABULARY_H
#define QLEVER_COMPRESSEDVOCABULARY_H

#include "index/ConstantsIndexBuilding.h"
#include "index/PrefixHeuristic.h"
#include "index/vocabulary/CompressionWrappers.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/FsstCompressor.h"
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
  [[nodiscard]] uint64_t getHighestId() const {
    return underlyingVocabulary_.getHighestId();
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto actualComparator = [self = this, &comparator](const auto& it,
                                                       const auto& b) {
      return comparator(self->decompressFromIterator(it), b);
    };
    auto underlyingResult =
        underlyingVocabulary_.lower_bound_iterator(word, actualComparator);
    WordAndIndex result;
    result._index = underlyingResult._index;
    if (underlyingResult._word.has_value()) {
      result._word = compressionWrapper_.decompress(
          underlyingResult._word.value(), getDecoderIdx(result._index));
    }
    return result;
  }

  /// Return a `WordAndIndex` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `_words`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto actualComparator = [this, &comparator](const auto& a, const auto& it) {
      return comparator(a, this->decompressFromIterator(it));
    };
    auto underlyingResult =
        underlyingVocabulary_.upper_bound_iterator(word, actualComparator);
    // TODO:: make this a private helper function.
    WordAndIndex result;
    result._index = underlyingResult._index;
    if (underlyingResult._word.has_value()) {
      result._word = compressionWrapper_.decompress(
          underlyingResult._word.value(), getDecoderIdx(result._index));
    }
    return result;
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
    std::vector<typename CompressionWrapper::Decoder> decoders_;
    typename UnderlyingVocabulary::WordWriter underlyingWriter_;
    std::string filenameDecoders_;
    bool isFinished_ = false;
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
    void operator()(std::string_view uncompressedWord) {
      AD_CORRECTNESS_CHECK(!isFinished_);
      wordBuffer_.emplace_back(uncompressedWord);
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
    }

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

      auto compressAndWrite = [words = std::move(wordBuffer_), this,
                               idx = queueIndex_++]() {
        auto bulkResult = CompressionWrapper::compressAll(words);
        writeQueue_.push(
            std::pair{idx, [bulkResult = std::move(bulkResult), this]() {
                        auto& [buffer, views, decoder] = bulkResult;
                        for (auto& word : views) {
                          underlyingWriter_(word);
                        }
                        decoders_.emplace_back(decoder);
                      }});
      };
      compressQueue_.push(std::move(compressAndWrite));
      wordBuffer_.clear();
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
    auto idx = it - underlyingVocabulary_.begin();
    return compressionWrapper_.decompress(toStringView(*it),
                                          getDecoderIdx(idx));
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
      return el._word.value();
    }
  }
};

#endif  // QLEVER_COMPRESSEDVOCABULARY_H
