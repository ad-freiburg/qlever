//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDVOCABULARY_H
#define QLEVER_COMPRESSEDVOCABULARY_H

#include "index/ConstantsIndexBuilding.h"
#include "index/PrefixHeuristic.h"
#include "index/vocabulary/PrefixCompressor.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/FsstCompressor.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializePair.h"
#include "util/TaskQueue.h"

/// TODO<joka921> Currently the settings of the compressor are not directly
/// serialized but have to be manually stored and initialized.

/// TODO<joka921> Make "Compressor" and "Vocabulary" a concept.

/// A vocabulary in which compression is performed per word via the `Compressor`
template <typename UnderlyingVocabulary, typename Compressor>
class CompressedVocabulary {
 private:
  UnderlyingVocabulary _underlyingVocabulary;
  Compressor _compressor;

 public:
  explicit CompressedVocabulary(Compressor compressor = Compressor())
      : _compressor{std::move(compressor)} {}

  auto operator[](uint64_t id) const {
    return _compressor.decompress(_underlyingVocabulary[id]);
  }

  [[nodiscard]] uint64_t size() const { return _underlyingVocabulary.size(); }
  [[nodiscard]] uint64_t getHighestId() const {
    return _underlyingVocabulary.getHighestId();
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    auto actualComparator = [this, &comparator](const auto& a, const auto& b) {
      return comparator(_compressor.decompress(a), b);
    };
    auto underlyingResult =
        _underlyingVocabulary.lower_bound(word, actualComparator);
    WordAndIndex result;
    result._index = underlyingResult._index;
    if (underlyingResult._word.has_value()) {
      result._word = _compressor.decompress(underlyingResult._word.value());
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
    auto actualComparator = [this, &comparator](const auto& a, const auto& b) {
      return comparator(a, _compressor.decompress(b));
    };
    auto underlyingResult =
        _underlyingVocabulary.upper_bound(word, actualComparator);
    // TODO:: make this a private helper function.
    WordAndIndex result;
    result._index = underlyingResult._index;
    if (underlyingResult._word.has_value()) {
      result._word = _compressor.decompress(underlyingResult._word.value());
    }
    return result;
  }

  /// Open the underlying vocabulary from a file. The vocabulary must have been
  /// created by using a `DiskWriterFromUncompressedWords`. Note that the
  /// settings of the `compressor` are not stored in the file, but currently
  /// have to be manually stored a set via the constructor.
  void open(const std::string& filename) {
    _underlyingVocabulary.open(filename);
  }

  /// Allows the incremental writing of the words to disk. Uses `WordWriter` of
  /// the underlying vocabulary.
  class DiskWriterFromUncompressedWords {
   private:
    const Compressor& _compressor;
    typename UnderlyingVocabulary::WordWriter _underlyingWriter;

   public:
    /// Constructor
    explicit DiskWriterFromUncompressedWords(const Compressor& compressor,
                                             const std::string& filename)
        : _compressor{compressor}, _underlyingWriter{filename} {}

    /// Compress the `uncompressedWord` and write it to disk.
    void push(std::string_view uncompressedWord) {
      auto compressedWord = _compressor.compress(uncompressedWord);
      _underlyingWriter.push(compressedWord.data(), compressedWord.size());
    }

    /// After calls to `finish()` no more wrods can be pushed.
    /// `finish()` is implicitly also called by the destructor.
    void finish() { _underlyingWriter.finish(); }
  };

  DiskWriterFromUncompressedWords makeDiskWriter(const std::string& filename) {
    return DiskWriterFromUncompressedWords{_compressor, filename};
  }

  UnderlyingVocabulary& getUnderlyingVocabulary() {
    return _underlyingVocabulary;
  }
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return _underlyingVocabulary;
  }
  Compressor& getCompressor() { return _compressor; }
  const Compressor& getCompressor() const { return _compressor; }

  void close() { _underlyingVocabulary.close(); }

  void build(std::vector<std::string> words) {
    for (auto& word : words) {
      word = _compressor.compress(word);
    }
    _underlyingVocabulary.build(words);
  }
};

template <typename DecoderT>
struct CompressionMixin {
  using Decoder = DecoderT;
  std::vector<Decoder> decoders_;
  std::string decompress(std::string_view compressed,
                         size_t decoderIndex) const {
    return decoders_.at(decoderIndex).decompress(compressed);
  }
  size_t numDecoders() const { return decoders_.size(); }
};

struct FsstCompressionWrapper : CompressionMixin<FsstDecoder> {
  using Strings = std::vector<std::string>;
  static FsstEncoder::BulkResult compressAll(const Strings& strings) {
    return FsstEncoder::compressAll(strings);
  }
};

struct PrefixCompressionDecoder : CompressionMixin<PrefixCompressor> {
  using Strings = std::vector<std::string>;
  using BulkResult = std::tuple<bool, std::vector<std::string>, Decoder>;

  static BulkResult compressAll(const Strings& strings) {
    PrefixCompressor compressor;
    auto stringsCopy = strings;
    std::ranges::sort(stringsCopy);
    auto prefixes =
        calculatePrefixes(stringsCopy, NUM_COMPRESSION_PREFIXES, 1, true);
    compressor.buildCodebook(prefixes);
    Strings compressedStrings;
    for (const auto& string : strings) {
      compressedStrings.push_back(compressor.compress(string));
    }
    return {true, std::move(compressedStrings), std::move(compressor)};
  }
};

struct SuffixCompressionDecoder : CompressionMixin<PrefixCompressor> {
  // TODO<joka921> If this is beneficial,
  // add functionality to NOT always physically reverse the strings.
  using Decoder = PrefixCompressor;
  using Strings = std::vector<std::string>;
  using BulkResult = std::tuple<bool, std::vector<std::string>, Decoder>;

  static BulkResult compressAll(Strings strings) {
    std::ranges::for_each(strings, std::ranges::reverse);
    PrefixCompressor compressor;
    auto stringsCopy = strings;
    std::ranges::sort(stringsCopy);
    auto prefixes =
        calculatePrefixes(stringsCopy, NUM_COMPRESSION_PREFIXES, 1, true);
    compressor.buildCodebook(prefixes);
    Strings compressedStrings;
    for (const auto& string : strings) {
      compressedStrings.push_back(compressor.compress(string));
    }
    std::ranges::for_each(compressedStrings, std::ranges::reverse);
    return {true, std::move(compressedStrings), std::move(compressor)};
  }
  std::string decompress(std::string compressed, size_t decoderIndex) const {
    std::ranges::reverse(compressed);
    auto result = decoders_.at(decoderIndex).decompress(compressed);
    std::ranges::reverse(result);
    return result;
  }
};

struct PrefixSuffixFsstDecodderImpl
    : public std::tuple<PrefixCompressor, PrefixCompressor, FsstDecoder> {
  using Base = std::tuple<PrefixCompressor, PrefixCompressor, FsstDecoder>;
  std::string decompress(std::string_view compressed) const {
    const auto& [prefixDecoder, suffixDecoder, fsstDecoder] =
        static_cast<const Base&>(*this);
    auto result = fsstDecoder.decompress(compressed);
    std::ranges::reverse(result);
    result = suffixDecoder.decompress(result);
    std::ranges::reverse(result);
    result = prefixDecoder.decompress(result);
    return result;
  }
  const auto& base() const { return static_cast<const Base&>(*this); }
  auto& base() { return static_cast<Base&>(*this); }
  AD_SERIALIZE_FRIEND_FUNCTION(PrefixSuffixFsstDecodderImpl) {
    serializer | arg.base();
  }
};
struct PrefixSuffixFsstDecodder
    : CompressionMixin<PrefixSuffixFsstDecodderImpl> {
  using Strings = std::vector<std::string>;
  using BulkResult =
      std::tuple<std::string, std::vector<std::string_view>, Decoder>;

  static BulkResult compressAll(const Strings& strings) {
    auto [unused, prefixCompressed, prefixDecoder] =
        PrefixCompressionDecoder::compressAll(strings);
    auto [unused2, suffixCompressed, suffixDecoder] =
        SuffixCompressionDecoder::compressAll(prefixCompressed);
    auto [buffer, fsstCompressed, fsstDecoder] =
        FsstCompressionWrapper::compressAll(suffixCompressed);
    return {std::move(buffer),
            std::move(fsstCompressed),
            {{std::move(prefixDecoder), std::move(suffixDecoder),
              std::move(fsstDecoder)}}};
  }
};

// A vocabulary in which compression is performed using the `FSST` algorithm,
// with one dictionary per `numWordsPerBlock` many words (currently 1 million).
// The interface is currently designed to work with the `VocabularyOnDisk`.
template <typename UnderlyingVocabulary,
          typename CompressionWrapper = PrefixSuffixFsstDecodder>
class FSSTCompressedVocabulary {
 private:
  // The number of words that share the same decoder.
  static constexpr size_t numWordsPerBlock = 1UL << 20;
  UnderlyingVocabulary underlyingVocabulary_;
  CompressionWrapper compressionWrapper_;
  // We need to store two files, one for the words and one for the decoders.
  static constexpr std::string_view wordsSuffix = ".words";
  static constexpr std::string_view decodersSuffix = ".codebooks";

 public:
  // The vocabulary is initialized using the `open()` method, the default
  // constructor leads to an empty vocabulary.
  FSSTCompressedVocabulary() = default;

  // Get the uncompressed word at the given index.
  std::string operator[](uint64_t idx) const {
    return compressionWrapper_.decompress(underlyingVocabulary_[idx].value(),
                                          getDecoderIdx(idx));
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
    typename UnderlyingVocabulary::WordWriter _underlyingWriter;
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
        : _underlyingWriter{filenameWords},
          filenameDecoders_{filenameDecoders} {}

    /// Compress the `uncompressedWord` and write it to disk.
    void operator()(std::string_view uncompressedWord) {
      AD_CORRECTNESS_CHECK(!isFinished_);
      wordBuffer_.emplace_back(uncompressedWord);
      if (wordBuffer_.size() == numWordsPerBlock) {
        finishBlock();
      }
    }

    /// After calls to `finish()` no more words can be pushed.
    /// `finish()` is implicitly also called by the destructor.
    void finish() {
      if (std::exchange(isFinished_, true)) {
        return;
      }
      finishBlock();
      compressQueue_.finish();
      writeQueue_.finish();
      AD_CORRECTNESS_CHECK(writeThread_.joinable());
      writeThread_.join();
      _underlyingWriter.finish();
      ad_utility::serialization::FileWriteSerializer decoderWriter(
          filenameDecoders_);
      decoderWriter << decoders_;
    }

    // Call `finish`, does nothing if `finish` has been manually called.
    ~DiskWriterFromUncompressedWords() { finish(); }

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
                          _underlyingWriter(word);
                        }
                        decoders_.emplace_back(decoder);
                      }});
      };
      compressQueue_.push(std::move(compressAndWrite));
      wordBuffer_.clear();
    }
  };

  // Return a `DiskWriter` that can be used to create the vocabulary.
  DiskWriterFromUncompressedWords makeDiskWriter(
      const std::string& filename) const {
    return DiskWriterFromUncompressedWords{
        absl::StrCat(filename, wordsSuffix),
        absl::StrCat(filename, decodersSuffix)};
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
  size_t getDecoderIdx(size_t idx) const { return idx / numWordsPerBlock; }

  // Decompress the word that `it` points to. `it` is an iterator into the
  // underlying vocabulary.
  auto decompressFromIterator(auto it) const {
    auto idx = it - underlyingVocabulary_.begin();
    return compressionWrapper_.decompress((*it)._word.value(),
                                          getDecoderIdx(idx));
  };
};

#endif  // QLEVER_COMPRESSEDVOCABULARY_H
