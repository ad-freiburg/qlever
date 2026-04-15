// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_TENSORDATAVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_TENSORDATAVOCABULARY_H

#include <cstdint>
#include <memory>
#include <string>

#include "index/vocabulary/VocabularyTypes.h"
#include "rdfTypes/TensorData.h"
#include "util/ExceptionHandling.h"
#include "util/MmapVector.h"
#include "util/File.h"
#include "util/Serializer/Serializer.h"

// A `TensorDataVocabulary` holds tensor data literals. In contrast to the
// regular vocabulary classes it does not only store the strings. Instead it
// stores both preprocessed and original forms of its input words. Preprocessing
// includes parsing the tensor data and computing a faiss index for accelerated
// similarity search. Note: A `TensorDataVocabulary` is only suitable for tensor
// data literals, therefore it should be used as part of a `SplitVocabulary`.
constexpr uint64_t TENSOR_VOCAB_VERSION = 1;
template <typename UnderlyingVocabulary>
class TensorDataVocabulary {
 private:
  using TensorData = ad_utility::TensorData;
  using Offset = uint64_t;

  UnderlyingVocabulary literals_;
  size_t numTensors_ = 0;

  // The file in which the parsed tensor data is stored.
  ad_utility::File tensorVocabFile_;
  // The file in which the offsets for the tensor data are stored.
  ad_utility::MmapVectorView<Offset> offsets_;

  // Filename suffix for tensor vocabulary file
  static constexpr std::string_view tensorVocabSuffix = ".tensors";
  static constexpr std::string_view tensorVocabOffsetsSuffix =
      ".tensors.offsets";

  // Offset for the header of the tensor vocabulary file
  static constexpr size_t tensorDataHeader =
      sizeof(TENSOR_VOCAB_VERSION);

 public:
  TensorDataVocabulary() = default;

  // Load the precomputed `TensorData` object for the literal with
  // the given index from disk. Return `std::nullopt` for invalid tensor data.
  std::optional<TensorData> getTensorData(uint64_t index) const;

  // Construct a filename for the tensor info file by appending a suffix to the
  // given filename.
  static std::string getTensorDataFilename(std::string_view filename) {
    return absl::StrCat(filename, tensorVocabSuffix);
  }

  // Construct a filename for the tensor offsets file by appending a suffix to
  // the given filename.
  static std::string getTensorDataOffsetsFilename(std::string_view filename) {
    return absl::StrCat(filename, tensorVocabOffsetsSuffix);
  }

  // Forward all the standard operations to the underlying literal vocabulary.
  // See there for more details.

  // ___________________________________________________________________________
  decltype(auto) operator[](uint64_t id) const { return literals_[id]; }

  // ___________________________________________________________________________
  [[nodiscard]] uint64_t size() const { return literals_.size(); }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return literals_.lower_bound(word, comparator);
  }

  // ___________________________________________________________________________
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return literals_.upper_bound(word, comparator);
  }

  // ___________________________________________________________________________
  UnderlyingVocabulary& getUnderlyingVocabulary() { return literals_; }

  // ___________________________________________________________________________
  const UnderlyingVocabulary& getUnderlyingVocabulary() const {
    return literals_;
  }

  // ___________________________________________________________________________
  void open(const std::string& filename);

  // Custom word writer, which precomputes and writes geometry info along with
  // the words.
  class WordWriter : public WordWriterBase {
   private:
    std::unique_ptr<typename UnderlyingVocabulary::WordWriter>
        underlyingWordWriter_;
    ad_utility::File tensorVocabFile_;
    size_t numInvalidTensors_ = 0;
    size_t currentOffset_ = sizeof(TENSOR_VOCAB_VERSION);
    // A vector to store the offsets for each tensor.
    ad_utility::MmapVector<Offset> offsets_;

   public:
    // Initialize the `tensorVocabFile_` by writing its header and open a word
    // writer on the underlying vocabulary.
    WordWriter(const UnderlyingVocabulary& vocabulary,
               const std::string& filename);

    // Add the next literal to the vocabulary, precompute additional information
    // using `GeometryInfo` and return the literal's new index.
    uint64_t operator()(std::string_view word, bool isExternal) override;

    // Finish the writing on the underlying writer and close the
    // `tensorVocabFile_` file handle. After this no more calls to `operator()`
    // are allowed.
    void finishImpl() override;

    ~WordWriter() override;
  };

  // ___________________________________________________________________________
  std::unique_ptr<WordWriter> makeDiskWriterPtr(
      const std::string& filename) const {
    return std::make_unique<WordWriter>(literals_, filename);
  }

  // ___________________________________________________________________________
  void close();

  // Generic serialization support.
  AD_SERIALIZE_FRIEND_FUNCTION(TensorDataVocabulary) {
    (void)serializer;
    (void)arg;
    throw std::runtime_error(
        "Generic serialization is not implemented for TensorDataVocabulary.");
  }
  struct OffsetAndSize {
    uint64_t offset_;
    uint64_t size_;
  };

 private:
  // Get the `OffsetAndSize` for the element with the `idx`. Return
  // `std::nullopt` if `idx` is not contained in the vocabulary.
  OffsetAndSize getOffsetAndSize(uint64_t idx) const;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_TENSORDATAVOCABULARY_H
