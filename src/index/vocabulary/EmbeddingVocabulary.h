// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_EMBEDDINGVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_EMBEDDINGVOCABULARY_H

#include <absl/strings/str_cat.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "index/vocabulary/VocabularyTypes.h"
#include "rdfTypes/EmbeddingVector.h"
#include "util/File.h"
#include "util/MmapVector.h"
#include "util/Serializer/Serializer.h"

// An `EmbeddingVocabulary` holds embedding-vector literals (datatype
// `emb:fp32Vector`). Like the `GeoVocabulary`, it stores both the original
// literal strings (in the underlying vocabulary, so they remain first-class,
// exportable RDF terms) and a precomputed, decoded form (here: the raw
// `float32` vectors) in a sidecar, so that query-time access is a cheap binary
// read instead of re-parsing a decimal string.
//
// In contrast to the `GeoVocabulary`, whose per-word `GeometryInfo` records are
// fixed-size, embedding vectors vary in length (different embedding types have
// different dimensions). The sidecar is therefore variable-length: an in-memory
// offset table (`VocabIndex -> byte offset`) plus an on-disk blob of
// concatenated `float32` written in vocabulary order. The blob is memory-mapped
// at query time, so `getEmbedding` borrows a zero-copy `span` into the mapping
// (the index outlives every access). This vocabulary is only suitable for
// embedding literals and should be used as part of a `SplitVocabulary`.
template <typename UnderlyingVocabulary>
class EmbeddingVocabulary {
 private:
  UnderlyingVocabulary literals_;

  // Read-only memory map of the `.embvec` blob (raw `float32` bytes; mapped as
  // `std::byte` since the float element type is applied on read). The trailing
  // `MmapVectorMetaData` written by the `WordWriter` lets `MmapVectorView` open
  // the blob directly.
  ad_utility::MmapVectorView<std::byte> mapping_;

  // In-memory offset table with `size() + 1` entries. `offsets_[i]` is the
  // start byte of vector `i` in the blob; `offsets_[size()]` is the total size,
  // so `length(i) = offsets_[i + 1] - offsets_[i]`.
  std::vector<uint64_t> offsets_;

  // Filename suffixes for the two sidecar files.
  static constexpr std::string_view dataSuffix = ".embvec";
  static constexpr std::string_view offsetsSuffix = ".embvec.idx";

  // Header of the offsets file: just the format version.
  static constexpr size_t offsetsHeader =
      sizeof(ad_utility::EMBEDDING_VECTOR_VERSION);

 public:
  EmbeddingVocabulary() = default;

  // Load the decoded `float32` vector for the literal with the given (local,
  // unmarked) index, as a zero-copy `span` into the memory map. Returns
  // `std::nullopt` for an out-of-range index.
  std::optional<ad_utility::MaybeOwnedVector> getEmbedding(
      uint64_t index) const;

  // Filenames for the sidecar files, derived from the underlying vocabulary's
  // filename.
  static std::string getDataFilename(std::string_view filename) {
    return absl::StrCat(filename, dataSuffix);
  }
  static std::string getOffsetsFilename(std::string_view filename) {
    return absl::StrCat(filename, offsetsSuffix);
  }

  // Forward all the standard operations to the underlying literal vocabulary.

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

  // Custom word writer that decodes each vector literal and writes its raw
  // `float32` bytes (plus an offset) to the sidecar.
  class WordWriter : public WordWriterBase {
   private:
    std::unique_ptr<typename UnderlyingVocabulary::WordWriter>
        underlyingWordWriter_;
    ad_utility::File dataFile_;
    std::string offsetsFilename_;
    // Accumulated offset table; starts as `{0}` and appends an end offset per
    // vector.
    std::vector<uint64_t> offsets_{0};

   public:
    // Open a writer on the underlying vocabulary and the sidecar data file.
    WordWriter(const UnderlyingVocabulary& vocabulary,
               const std::string& filename);

    // Decode the next vector literal, append its `float32` bytes to the
    // sidecar, and return the literal's new index. Throws on a malformed
    // literal.
    uint64_t operator()(std::string_view word, bool isExternal) override;

    // Finish the underlying writer and flush the data and offsets files.
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
  AD_SERIALIZE_FRIEND_FUNCTION(EmbeddingVocabulary) {
    (void)serializer;
    (void)arg;
    throw std::runtime_error(
        "Generic serialization is not implemented for EmbeddingVocabulary.");
  }
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_EMBEDDINGVOCABULARY_H
