// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/EmbeddingVocabulary.h"

#include <stdexcept>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "rdfTypes/EmbeddingVector.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/MmapVector.h"

// ____________________________________________________________________________
template <typename V>
void EmbeddingVocabulary<V>::open(const std::string& filename) {
  literals_.open(filename);

  // Read and check the header of the offsets file, then load the whole offset
  // table into memory.
  ad_utility::File offsetsFile{getOffsetsFilename(filename), "r"};
  std::decay_t<decltype(ad_utility::EMBEDDING_VECTOR_VERSION)> versionOfFile =
      0;
  offsetsFile.read(&versionOfFile, offsetsHeader, 0);
  if (versionOfFile != ad_utility::EMBEDDING_VECTOR_VERSION) {
    throw std::runtime_error(absl::StrCat(
        "The embedding vector version of ", getOffsetsFilename(filename),
        " is ", versionOfFile, ", which is incompatible with version ",
        ad_utility::EMBEDDING_VECTOR_VERSION,
        " as required by this version of QLever. Please rebuild your index."));
  }

  size_t numWords = literals_.size();
  offsets_.resize(numWords + 1);
  offsetsFile.read(offsets_.data(), (numWords + 1) * sizeof(uint64_t),
                   offsetsHeader);
  offsetsFile.close();

  // Memory-map the data blob. Skip the mapping for an empty bucket (no vectors
  // were written), where there is nothing to map and `getEmbedding` is never
  // called (no index routes to this sub-vocabulary).
  uint64_t numDataBytes = offsets_.empty() ? 0 : offsets_.back();
  if (numDataBytes > 0) {
    mapping_.open(getDataFilename(filename), ad_utility::AccessPattern::None);
  }
}

// ____________________________________________________________________________
template <typename V>
void EmbeddingVocabulary<V>::close() {
  literals_.close();
  mapping_.close();
  offsets_.clear();
}

// ____________________________________________________________________________
template <typename V>
EmbeddingVocabulary<V>::WordWriter::WordWriter(const V& vocabulary,
                                               const std::string& filename)
    : underlyingWordWriter_{vocabulary.makeDiskWriterPtr(filename)},
      dataFile_{getDataFilename(filename), "w"},
      offsetsFilename_{getOffsetsFilename(filename)} {}

// ____________________________________________________________________________
template <typename V>
uint64_t EmbeddingVocabulary<V>::WordWriter::operator()(std::string_view word,
                                                        bool isExternal) {
  // Store the literal string in the underlying vocabulary.
  uint64_t index = (*underlyingWordWriter_)(word, isExternal);

  // Decode the vector and append its raw `float32` bytes to the data file. A
  // malformed literal is a hard build error (strict).
  auto vec = ad_utility::parseFp32VectorLiteral(word);
  if (!vec.has_value()) {
    throw std::runtime_error(absl::StrCat(
        "Malformed embedding vector literal could not be parsed as a finite "
        "fp32 array: ",
        word));
  }
  dataFile_.write(vec->data(), vec->size() * sizeof(float));
  offsets_.push_back(offsets_.back() + vec->size() * sizeof(float));

  return index;
}

// ____________________________________________________________________________
template <typename V>
void EmbeddingVocabulary<V>::WordWriter::finishImpl() {
  underlyingWordWriter_->finish();

  // Append the `MmapVectorMetaData` trailer to the data blob so it can be
  // opened directly by `MmapVectorView<std::byte>` at query time. The element
  // type for the view is `std::byte`, so the element count, capacity, and byte
  // size all equal the total number of data bytes (`offsets_.back()`).
  uint64_t numDataBytes = offsets_.back();
  ad_utility::MmapVectorMetaData{numDataBytes, numDataBytes, numDataBytes}
      .writeToFile(dataFile_);
  dataFile_.close();

  // Write the offsets file: header (version) followed by the offset table.
  ad_utility::File offsetsFile{offsetsFilename_, "w"};
  offsetsFile.write(&ad_utility::EMBEDDING_VECTOR_VERSION,
                    sizeof(ad_utility::EMBEDDING_VECTOR_VERSION));
  offsetsFile.write(offsets_.data(), offsets_.size() * sizeof(uint64_t));
  offsetsFile.close();
}

// ____________________________________________________________________________
template <typename V>
EmbeddingVocabulary<V>::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows(
        [this]() { this->finish(); },
        "Calling `finish` from the destructor of `EmbeddingVocabulary`");
  }
}

// ____________________________________________________________________________
template <typename V>
std::optional<ad_utility::MaybeOwnedVector>
EmbeddingVocabulary<V>::getEmbedding(uint64_t index) const {
  if (index + 1 >= offsets_.size()) {
    return std::nullopt;
  }
  uint64_t start = offsets_[index];
  uint64_t numBytes = offsets_[index + 1] - start;
  size_t numFloats = numBytes / sizeof(float);
  // Reinterpret the mapped byte region as a `float` span (the fp32 decode step;
  // `start` is a multiple of `sizeof(float)` and the mapping base is suitably
  // aligned, so the reinterpret is well-defined). The span borrows from the
  // mapping, which lives for the index's lifetime.
  const float* base = reinterpret_cast<const float*>(mapping_.data() + start);
  return ad_utility::MaybeOwnedVector{ql::span<const float>{base, numFloats}};
}

// Explicit template instantiations
template class EmbeddingVocabulary<
    CompressedVocabulary<VocabularyInternalExternal>>;
template class EmbeddingVocabulary<VocabularyInMemory>;
