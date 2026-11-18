// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR (the
//      `GeoVocabulary` on which this class is modeled)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/TensorDataVocabulary.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyConstraints.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "rdfTypes/TensorData.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/MmapVector.h"

using ad_utility::TensorData;

namespace ad_utility::detail {
// ____________________________________________________________________________
void ReadOnlyMappedFile::open(const std::string& filename) {
  close();
  int fd = ::open(filename.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    throw std::runtime_error(absl::StrCat("Could not open file \"", filename,
                                          "\" for reading: ",
                                          std::strerror(errno)));
  }
  // Close the file descriptor in all cases, the mapping stays valid.
  absl::Cleanup closeFd = [fd]() { ::close(fd); };
  struct stat st {};
  if (::fstat(fd, &st) != 0) {
    throw std::runtime_error(absl::StrCat("Could not stat file \"", filename,
                                          "\": ", std::strerror(errno)));
  }
  size_ = static_cast<size_t>(st.st_size);
  if (size_ == 0) {
    // `mmap` does not support empty mappings. Use a non-null dummy pointer
    // so that `isOpen()` is true.
    static constexpr uint8_t emptyDummy = 0;
    data_ = &emptyDummy;
    return;
  }
  void* ptr = ::mmap(nullptr, size_, PROT_READ, MAP_SHARED, fd, 0);
  if (ptr == MAP_FAILED) {
    size_ = 0;
    throw std::runtime_error(absl::StrCat("Could not memory-map file \"",
                                          filename,
                                          "\": ", std::strerror(errno)));
  }
  // NOTE: No `madvise`: the tensors are read both in random order (single
  // lookups) and sequentially (e.g. when building a nearest-neighbor index
  // over all tensors), so the default kernel readahead is kept.
  data_ = static_cast<const uint8_t*>(ptr);
}

// ____________________________________________________________________________
void ReadOnlyMappedFile::close() noexcept {
  if (data_ != nullptr && size_ > 0) {
    ::munmap(const_cast<uint8_t*>(data_), size_);
  }
  data_ = nullptr;
  size_ = 0;
}
}  // namespace ad_utility::detail

// ____________________________________________________________________________
template <typename V>
TensorDataVocabulary<V>::TensorDataVocabulary() {
  // The index of a word is its position in the underlying vocabulary, which
  // requires contiguous positions and a plain `getPositionOfWord` (see the
  // `static_assert` in the class).
  static_assert(!HasSpecialGetPositionOfWord<V>);
}

// ____________________________________________________________________________
template <typename V>
void TensorDataVocabulary<V>::open(const std::string& filename) {
  close();
  literals_.open(filename);

  const auto dataFilename = getTensorDataFilename(filename);
  tensorDataFile_.open(dataFilename);
  auto data = tensorDataFile_.bytes();

  // Read the header of the tensor data file to determine the version.
  std::decay_t<decltype(TENSOR_VOCAB_VERSION)> versionOfFile = 0;
  if (data.size() < tensorDataHeader) {
    throw std::runtime_error(absl::StrCat(
        "The tensor data file ", dataFilename,
        " is too small to contain a header. Please rebuild your index."));
  }
  std::memcpy(&versionOfFile, data.data(), tensorDataHeader);
  if (versionOfFile != TENSOR_VOCAB_VERSION) {
    throw std::runtime_error(absl::StrCat(
        "The tensor vocab version of ", dataFilename, " is ", versionOfFile,
        ", which is incompatible with version ", TENSOR_VOCAB_VERSION,
        " as required by this version of QLever. Please rebuild your index."));
  }

  // The offsets file has the layout of a legacy `MmapVector<uint64_t>` (see
  // the class comment): the number of offsets is read from the trailer.
  const auto offsetsFilename = getTensorDataOffsetsFilename(filename);
  uint64_t numOffsets = 0;
  {
    ad_utility::File offsetsFileForTrailer{offsetsFilename, "r"};
    numOffsets =
        ad_utility::MmapVectorMetaData::readFromFile(offsetsFileForTrailer)
            .size_;
  }
  offsetsFile_.open(offsetsFilename);
  auto offsetBytes = offsetsFile_.bytes();
  const size_t expectedNumOffsets = size() + 1;
  if (numOffsets != expectedNumOffsets ||
      offsetBytes.size() < numOffsets * sizeof(Offset) +
                               ad_utility::MmapVectorMetaData::numBytes) {
    throw std::runtime_error(absl::StrCat(
        "The tensor offsets file ", offsetsFilename, " contains ", numOffsets,
        " offsets (with a file size of ", offsetBytes.size(), " bytes), but ",
        expectedNumOffsets, " offsets were expected for ", size(),
        " tensor literals. Please rebuild your index."));
  }
  // NOTE: The mapping is page-aligned, so the cast is properly aligned.
  offsets_ = ql::span<const Offset>{
      reinterpret_cast<const Offset*>(offsetBytes.data()), numOffsets};
  if (offsets_.front() != tensorDataHeader ||
      offsets_.back() != data.size()) {
    throw std::runtime_error(absl::StrCat(
        "The tensor offsets file ", offsetsFilename,
        " is inconsistent with the tensor data file ", dataFilename,
        ". Please rebuild your index."));
  }
}

// ____________________________________________________________________________
template <typename V>
void TensorDataVocabulary<V>::close() {
  literals_.close();
  offsets_ = {};
  offsetsFile_.close();
  tensorDataFile_.close();
}

// ____________________________________________________________________________
template <typename V>
std::optional<ql::span<const uint8_t>>
TensorDataVocabulary<V>::getSerializedTensorData(uint64_t index) const {
  AD_CONTRACT_CHECK(index < size());
  AD_CORRECTNESS_CHECK(index + 1 < offsets_.size());
  const Offset begin = offsets_[index];
  const Offset end = offsets_[index + 1];
  AD_CORRECTNESS_CHECK(begin <= end);
  if (begin == end) {
    // An empty record represents a literal that is not a valid tensor.
    return std::nullopt;
  }
  return tensorDataFile_.bytes().subspan(begin, end - begin);
}

// ____________________________________________________________________________
template <typename V>
TensorData TensorDataVocabulary<V>::deserializeTensorData(
    ql::span<const uint8_t> record) {
  // See `TensorData::serialize` for the format:
  // [dtype (1 byte)][number of dimensions (8 bytes)]
  // [shape (8 bytes per dimension)][tensor data (4 bytes per float)]
  constexpr size_t fixedHeader = 1 + sizeof(uint64_t);
  if (record.size() < fixedHeader) {
    throw std::runtime_error{"Buffer too small to deserialize TensorData"};
  }
  auto dtype = static_cast<TensorData::DType>(record[0]);
  uint64_t numDimensions;
  std::memcpy(&numDimensions, record.data() + 1, sizeof(numDimensions));
  if (numDimensions > (record.size() - fixedHeader) / sizeof(uint64_t)) {
    throw std::runtime_error{
        "Buffer too small to deserialize TensorData shape"};
  }
  const size_t dataStart = fixedHeader + numDimensions * sizeof(uint64_t);
  std::vector<size_t> shape(numDimensions);
  for (size_t i = 0; i < numDimensions; ++i) {
    uint64_t dim;
    std::memcpy(&dim, record.data() + fixedHeader + i * sizeof(uint64_t),
                sizeof(dim));
    shape[i] = static_cast<size_t>(dim);
  }
  const size_t dataSize = record.size() - dataStart;
  if (dataSize % sizeof(float) != 0) {
    throw std::runtime_error{"Buffer size is not a multiple of float size"};
  }
  std::vector<float> values(dataSize / sizeof(float));
  if (dataSize > 0) {
    std::memcpy(values.data(), record.data() + dataStart, dataSize);
  }
  return TensorData{std::move(values), std::move(shape), dtype};
}

// ____________________________________________________________________________
template <typename V>
std::optional<TensorData> TensorDataVocabulary<V>::getTensorData(
    uint64_t index) const {
  auto record = getSerializedTensorData(index);
  if (!record.has_value()) {
    return std::nullopt;
  }
  return deserializeTensorData(record.value());
}

// ____________________________________________________________________________
template <typename V>
TensorDataVocabulary<V>::WordWriter::WordWriter(const V& vocabulary,
                                                const std::string& filename)
    : underlyingWordWriter_{vocabulary.makeDiskWriterPtr(filename)},
      tensorDataFile_{getTensorDataFilename(filename), "w"},
      offsetsFile_{getTensorDataOffsetsFilename(filename), "w"} {
  // Initialize the tensor data file with its header.
  AD_CORRECTNESS_CHECK(tensorDataFile_.write(&TENSOR_VOCAB_VERSION,
                                             tensorDataHeader) ==
                       tensorDataHeader);
}

// ____________________________________________________________________________
template <typename V>
void TensorDataVocabulary<V>::WordWriter::writeOffset(Offset offset) {
  AD_CORRECTNESS_CHECK(offsetsFile_.write(&offset, sizeof(offset)) ==
                       sizeof(offset));
  ++numOffsets_;
}

// ____________________________________________________________________________
template <typename V>
uint64_t TensorDataVocabulary<V>::WordWriter::operator()(std::string_view word,
                                                         bool isExternal) {
  // Store the tensor literal as a string in the underlying vocabulary.
  uint64_t index = (*underlyingWordWriter_)(word, isExternal);
  // The records are stored by position, see the class comment.
  AD_CORRECTNESS_CHECK(index == numWords_);
  ++numWords_;

  // Parse the `TensorData` and write its serialized form, or an empty record
  // for a literal that cannot be parsed. Either way, there is one offset per
  // word, so that direct access by index is possible.
  writeOffset(currentOffset_);
  std::optional<TensorData> tensorData;
  try {
    tensorData = TensorData::fromLiteral(word);
  } catch (const std::exception&) {
    tensorData = std::nullopt;
  }
  if (tensorData.has_value()) {
    auto buffer = tensorData.value().serialize();
    AD_CORRECTNESS_CHECK(!buffer.empty());
    AD_CORRECTNESS_CHECK(tensorDataFile_.write(buffer.data(), buffer.size()) ==
                         buffer.size());
    currentOffset_ += buffer.size();
  } else {
    ++numInvalidTensors_;
  }
  return index;
}

// ____________________________________________________________________________
template <typename V>
void TensorDataVocabulary<V>::WordWriter::finishImpl() {
  // `WordWriterBase` ensures that this is not called twice and we thus do not
  // try to close the file handles twice.
  writeOffset(currentOffset_);
  // Complete the offsets file to the layout of a legacy
  // `MmapVector<uint64_t>`, which the format of this vocabulary has always
  // used: zero padding up to the capacity (at least 100 elements, with the
  // byte size rounded up to the next multiple of the page size, exactly as
  // `MmapVector` did), followed by the `MmapVectorMetaData` trailer.
  static constexpr uint64_t minCapacity = 100;
  const uint64_t pageSize = static_cast<uint64_t>(getpagesize());
  const uint64_t dataBytes = numOffsets_ * sizeof(Offset);
  uint64_t byteSize = std::max(numOffsets_, minCapacity) * sizeof(Offset);
  byteSize = (byteSize / pageSize + 1) * pageSize;
  const std::vector<char> padding(byteSize - dataBytes, 0);
  AD_CORRECTNESS_CHECK(offsetsFile_.write(padding.data(), padding.size()) ==
                       padding.size());
  ad_utility::MmapVectorMetaData{numOffsets_, byteSize / sizeof(Offset),
                                 byteSize}
      .writeToFile(offsetsFile_);
  offsetsFile_.close();
  tensorDataFile_.close();
  underlyingWordWriter_->finish();
  AD_LOG_INFO << "Number of tensor literals: " << numWords_ << std::endl;

  if (numInvalidTensors_ > 0) {
    AD_LOG_WARN << "Tensor preprocessing skipped " << numInvalidTensors_
                << " invalid tensor literal"
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

// Explicit template instantiations
template class TensorDataVocabulary<
    CompressedVocabulary<VocabularyInternalExternal>>;
template class TensorDataVocabulary<VocabularyInMemory>;
