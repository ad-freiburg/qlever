// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/vocabulary/TensorDataVocabulary.h"

#include <stdexcept>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "rdfTypes/TensorData.h"
#include "util/Exception.h"

using ad_utility::TensorData;

template <typename V>
TensorDataVocabulary<V>::OffsetAndSize
TensorDataVocabulary<V>::getOffsetAndSize(uint64_t i) const {
  AD_CORRECTNESS_CHECK(i < size());
  const auto offset = offsets_[i];
  const auto nextOffset = offsets_[i + 1];
  return {offset, nextOffset - offset};
}
// ____________________________________________________________________________
template <typename V>
void TensorDataVocabulary<V>::open(const std::string& filename) {
  literals_.open(filename);

  tensorVocabFile_.open(getTensorDataFilename(filename).c_str(), "r");
  // Read header of `tensorVocabFile_` to determine version
  std::decay_t<decltype(TENSOR_VOCAB_VERSION)> versionOfFile = 0;
  tensorVocabFile_.read(&versionOfFile, tensorDataHeader, 0);

  // Check version of tensor info file
  if (versionOfFile != TENSOR_VOCAB_VERSION) {
    throw std::runtime_error(absl::StrCat(
        "The tensor vocab version of ", getTensorDataFilename(filename), " is ",
        versionOfFile, ", which is incompatible with version ",
        TENSOR_VOCAB_VERSION,
        " as required by this version of QLever. Please rebuild your index."));
  }

  offsets_.open(getTensorDataOffsetsFilename(filename).c_str());
};

// ____________________________________________________________________________
template <typename V>
void TensorDataVocabulary<V>::close() {
  literals_.close();
  tensorVocabFile_.close();
  offsets_.close();
}

// ____________________________________________________________________________
template <typename V>
TensorDataVocabulary<V>::WordWriter::WordWriter(const V& vocabulary,
                                                const std::string& filename)
    : underlyingWordWriter_{vocabulary.makeDiskWriterPtr(filename)},
      tensorVocabFile_{getTensorDataFilename(filename), "w"},
      offsets_{getTensorDataOffsetsFilename(filename),
               ad_utility::CreateTag{}} {
  // Initialize tensor info file with header
  tensorVocabFile_.write(&TENSOR_VOCAB_VERSION, tensorDataHeader);
};

// ____________________________________________________________________________
template <typename V>
uint64_t TensorDataVocabulary<V>::WordWriter::operator()(std::string_view word,
                                                         bool isExternal) {
  uint64_t index;

  // Store the Tensor literal as a string in the underlying vocabulary
  index = (*underlyingWordWriter_)(word, isExternal);

  // Precompute `TensorData` and write the `TensorData` to disk, or write a
  // zero buffer of the same size (indicating an invalid geometry). This is
  // required to ensure direct access by index is still possible on the file.
  offsets_.push_back(currentOffset_);
  auto tensor_data = TensorData::fromLiteral(word);
  if (tensor_data.has_value()) {
    auto buffer = tensor_data.value().serialize();
    currentOffset_ += tensorVocabFile_.write(buffer.data(), buffer.size());
  } else {
    ++numInvalidTensors_;
  }

  return index;
};

// ____________________________________________________________________________
template <typename V>
void TensorDataVocabulary<V>::WordWriter::finishImpl() {
  // `WordWriterBase` ensures that this is not called twice and we thus do not
  // try to close the file handle twice
  size_t numTensors = offsets_.size();
  offsets_.push_back(currentOffset_);
  offsets_.close();
  underlyingWordWriter_->finish();
  tensorVocabFile_.close();
  AD_LOG_INFO << "Number of Tensors parsed " << numTensors << std::endl;
  AD_LOG_INFO << "Number of invalid Tensors " << numInvalidTensors_
              << std::endl;

  if (numInvalidTensors_ > 0) {
    AD_LOG_WARN << "Tensor preprocessing skipped " << numInvalidTensors_
                << " invalid tensor literals during index construction."
                << (numInvalidTensors_ == 1 ? "" : "s") << std::endl;
  }
}

// ____________________________________________________________________________
template <typename V>
TensorDataVocabulary<V>::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows([this]() { this->finish(); },
                                  "Calling `finish` from the destructor of "
                                  "`TensorDataVocabulary`");
  }
}

// ____________________________________________________________________________
template <typename V>
std::optional<TensorData> TensorDataVocabulary<V>::getTensorData(
    uint64_t index) const {
  AD_CONTRACT_CHECK(index < size());
  // Allocate the required number of bytes
  auto offsetAndSize = getOffsetAndSize(index);
  if (offsetAndSize.size_ == 0) {
    // This record on disk represents an invalid tensor.
    return std::nullopt;
  }
  std::vector<uint8_t> buffer(offsetAndSize.size_);

  // Read into the buffer
  tensorVocabFile_.read(buffer.data(), buffer.size(), offsetAndSize.offset_);
  // Deserialize the buffer into a `TensorData` object and return it
  return TensorData::deserialize(buffer);
}

// Explicit template instantiations
template class TensorDataVocabulary<
    CompressedVocabulary<VocabularyInternalExternal>>;
template class TensorDataVocabulary<VocabularyInMemory>;
