// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR (the
//      `GeoVocabulary` on which this class is modeled)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_TENSORDATAVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_TENSORDATAVOCABULARY_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "rdfTypes/TensorData.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/TypeTraits.h"
#include "util/serializer/Serializer.h"

// The version of the on-disk format of a `TensorDataVocabulary` (stored as the
// header of its `.tensors` file). NOTE: The format must stay compatible with
// indexes that were built by older versions, see the class comment.
constexpr uint64_t TENSOR_VOCAB_VERSION = 1;

namespace ad_utility::detail {
// A read-only memory mapping of a complete file. Move-only, unmaps on
// destruction. Reading from the mapping is thread-safe.
class ReadOnlyMappedFile {
 private:
  const uint8_t* data_ = nullptr;
  size_t size_ = 0;

 public:
  ReadOnlyMappedFile() = default;
  ReadOnlyMappedFile(const ReadOnlyMappedFile&) = delete;
  ReadOnlyMappedFile& operator=(const ReadOnlyMappedFile&) = delete;
  ReadOnlyMappedFile(ReadOnlyMappedFile&& other) noexcept
      : data_{std::exchange(other.data_, nullptr)},
        size_{std::exchange(other.size_, 0)} {}
  ReadOnlyMappedFile& operator=(ReadOnlyMappedFile&& other) noexcept {
    if (this != &other) {
      close();
      data_ = std::exchange(other.data_, nullptr);
      size_ = std::exchange(other.size_, 0);
    }
    return *this;
  }
  ~ReadOnlyMappedFile() { close(); }

  // Map the complete file with the given name. Throws if the file cannot be
  // opened or mapped. An empty file results in an empty mapping.
  void open(const std::string& filename);

  // Unmap the file (no-op if nothing is mapped).
  void close() noexcept;

  bool isOpen() const { return data_ != nullptr; }

  // The bytes of the file. Valid until `close` is called.
  ql::span<const uint8_t> bytes() const { return {data_, size_}; }
};
}  // namespace ad_utility::detail

// A `TensorDataVocabulary` holds tensor data literals (with the datatype
// `TENSOR_LITERAL` or `TENSOR_NUMERIC_LITERAL`). In contrast to the regular
// vocabulary classes it does not only store the strings. Instead it stores
// both the original literals (in the `UnderlyingVocabulary`) and the parsed
// `TensorData` in a binary form, such that a tensor can be accessed without
// re-parsing the literal.
//
// Files (relative to the base filename):
//   ""                  the files of the `UnderlyingVocabulary`
//   ".tensors"          header (`TENSOR_VOCAB_VERSION`, 8 bytes), followed by
//                       the serialized `TensorData` records of all words (see
//                       `TensorData::serialize`), in the order of the words.
//   ".tensors.offsets"  `size() + 1` values of type `uint64_t`: the offset of
//                       the record of each word in the `.tensors` file, plus
//                       the end of the last record. A record of size zero
//                       represents a literal that could not be parsed. The
//                       file has the layout of the (removed) legacy
//                       `ad_utility::MmapVector<uint64_t>`: the raw values,
//                       zero padding up to the page-rounded capacity, and an
//                       `MmapVectorMetaData` trailer (see `util/MmapVector.h`)
//                       from which the number of values is read.
//
// Both binary files are memory-mapped when the vocabulary is opened, so that
// reading a tensor is a direct memory access (see `getSerializedTensorData`).
//
// NOTE: A `TensorDataVocabulary` is only suitable for tensor data literals,
// therefore it should be used as part of a `SplitVocabulary` (see
// `SplitTensorVocabulary`).
template <typename UnderlyingVocabulary>
class TensorDataVocabulary {
 private:
  using TensorData = ad_utility::TensorData;
  using Offset = uint64_t;

  // The index of a word must be its position in the underlying vocabulary,
  // because the tensor records are stored by position. Add a vocabulary type
  // here only if that holds for it (it does not for the vocabularies with
  // "holes", see `VocabularyInMemoryBinSearch`).
  static_assert(
      ad_utility::SameAsAny<UnderlyingVocabulary, VocabularyInMemory,
                            CompressedVocabulary<VocabularyInternalExternal>>);

  // The underlying vocabulary, which stores the tensor literals as strings.
  UnderlyingVocabulary literals_;

  // The memory-mapped `.tensors` file (header + serialized records).
  ad_utility::detail::ReadOnlyMappedFile tensorDataFile_;
  // The memory-mapped `.tensors.offsets` file.
  ad_utility::detail::ReadOnlyMappedFile offsetsFile_;
  // A typed view on the `offsetsFile_` (`size() + 1` entries when opened).
  ql::span<const Offset> offsets_;

  // Filename suffixes for the tensor data and the offsets files.
  static constexpr std::string_view tensorVocabSuffix = ".tensors";
  static constexpr std::string_view tensorVocabOffsetsSuffix =
      ".tensors.offsets";

  // Size of the header of the `.tensors` file.
  static constexpr size_t tensorDataHeader = sizeof(TENSOR_VOCAB_VERSION);

 public:
  TensorDataVocabulary();

  // Load the precomputed `TensorData` object for the literal with the given
  // index. Return `std::nullopt` if the literal is not a valid tensor.
  // Thread-safe.
  std::optional<TensorData> getTensorData(uint64_t index) const;

  // Zero-copy access to the serialized `TensorData` (see
  // `TensorData::serialize`) of the literal with the given index, or
  // `std::nullopt` if the literal is not a valid tensor. The returned span
  // points directly into the memory-mapped file and stays valid until `close`
  // is called. Thread-safe.
  std::optional<ql::span<const uint8_t>> getSerializedTensorData(
      uint64_t index) const;

  // Deserialize a record as returned by `getSerializedTensorData`. This is
  // equivalent to `TensorData::deserialize`, but works directly on the span
  // without an intermediate copy. Throws on a malformed record.
  static TensorData deserializeTensorData(ql::span<const uint8_t> record);

  // Construct a filename for the tensor data file by appending a suffix to the
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
  decltype(auto) operator[](uint64_t id) const {
    AD_CONTRACT_CHECK(id < size());
    return literals_[id];
  }

  //____________________________________________________________________________
  VocabBatchLookupResult lookupBatch(ql::span<const size_t> indices) const {
    return literals_.lookupBatch(indices);
  }

  //____________________________________________________________________________
  VocabLookupOutput lookupBatchesStreamed(VocabLookupInput input) const {
    return ad_utility::vocabulary::lookupBatchesStreamed(*this,
                                                         std::move(input));
  }

  // Iterate over all words together with their index (which is their
  // position).
  auto scanAll() const { return literals_.scanAll(); }

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

  // Open the underlying vocabulary and map the tensor files. Throws if the
  // version of the files is not `TENSOR_VOCAB_VERSION` or if the files are
  // inconsistent.
  void open(const std::string& filename);

  // Custom word writer, which parses each literal and writes the serialized
  // `TensorData` along with the words.
  class WordWriter : public WordWriterBase {
   private:
    std::unique_ptr<typename UnderlyingVocabulary::WordWriter>
        underlyingWordWriter_;
    // The `.tensors` and `.tensors.offsets` files, both written sequentially.
    ad_utility::File tensorDataFile_;
    ad_utility::File offsetsFile_;
    // The number of words written so far (the position of the next word).
    uint64_t numWords_ = 0;
    size_t numInvalidTensors_ = 0;
    // The offset of the next record in the `tensorDataFile_`.
    Offset currentOffset_ = tensorDataHeader;

   public:
    // Initialize the `tensorDataFile_` by writing its header and open a word
    // writer on the underlying vocabulary.
    WordWriter(const UnderlyingVocabulary& vocabulary,
               const std::string& filename);

    // Add the next literal to the vocabulary, parse it as `TensorData`, write
    // the serialized tensor (or an empty record if it is invalid) and return
    // the literal's new index.
    uint64_t operator()(std::string_view word, bool isExternal) override;

    // Finish the writing on the underlying writer and write the final offset
    // and close the files. After this no more calls to `operator()` are
    // allowed.
    void finishImpl() override;

    ~WordWriter() override;

   private:
    void writeOffset(Offset offset);
    // The number of offsets written so far.
    uint64_t numOffsets_ = 0;
  };

  // The files of the underlying vocabulary, which is stored under the base
  // filename itself, plus the tensor data and offsets files.
  static FileSuffixes fileSuffixes() {
    FileSuffixes suffixes = UnderlyingVocabulary::fileSuffixes();
    suffixes.push_back(std::string{tensorVocabSuffix});
    suffixes.push_back(std::string{tensorVocabOffsetsSuffix});
    return suffixes;
  }

  // ___________________________________________________________________________
  std::unique_ptr<WordWriter> makeDiskWriterPtr(
      const std::string& filename) const {
    return std::make_unique<WordWriter>(literals_, filename);
  }

  // The block writer (see `BlockWriterBase`). The records have a variable
  // size, so they cannot be precomputed as a fixed-size payload; the words
  // are written one by one by the `WordWriter` in the append step.
  std::unique_ptr<BlockWriterBase> makeBlockWriterPtr(
      const std::string& filename) const {
    return makeBlockWriterFromWordWriter(makeDiskWriterPtr(filename));
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
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_TENSORDATAVOCABULARY_H
