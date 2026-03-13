// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "index/vocabulary/VocabularyOnDisk.h"

#include <algorithm>
#include <fstream>

#include "util/Generator.h"
#include "util/StringUtils.h"

using OffsetAndSize = VocabularyOnDisk::OffsetAndSize;

// ____________________________________________________________________________
OffsetAndSize VocabularyOnDisk::getOffsetAndSize(uint64_t i) const {
  AD_CORRECTNESS_CHECK(i < size());
  const auto offset = offsets_[i];
  const auto nextOffset = offsets_[i + 1];
  return {offset, nextOffset - offset};
}

// _____________________________________________________________________________
std::string VocabularyOnDisk::operator[](uint64_t idx) const {
  AD_CONTRACT_CHECK(idx < size());
  auto offsetAndSize = getOffsetAndSize(idx);
  std::string result(offsetAndSize.size_, '\0');
  file_.read(result.data(), offsetAndSize.size_, offsetAndSize.offset_);
  return result;
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::lookupBatch(
    ql::span<const size_t> indices) const {
  const size_t n = indices.size();
  if (n == 0) {
    auto data = std::make_shared<VocabBatchLookupData>();
    return VocabBatchLookupData::asResult(std::move(data));
  }

  // Phase 1: Read offset pairs via io_uring from the .offsets file.
  // For each index i, read offsets_[i] and offsets_[i+1] (16 bytes) at file
  // position i * sizeof(uint64_t).
  struct OffsetPair {
    uint64_t offset;
    uint64_t nextOffset;
  };
  std::vector<OffsetPair> offsetPairs(n);
  std::vector<size_t> offsetSizes(n, sizeof(OffsetPair));
  std::vector<uint64_t> offsetFileOffsets(n);
  std::vector<char*> offsetTargets(n);
  for (size_t i = 0; i < n; ++i) {
    AD_CONTRACT_CHECK(indices[i] < size());
    offsetFileOffsets[i] = indices[i] * sizeof(uint64_t);
    offsetTargets[i] = reinterpret_cast<char*>(&offsetPairs[i]);
  }

  auto manager = ioManagers_->pop().value();
  auto offsetHandle = manager->addBatch(offsetsFile_.fd(), offsetSizes,
                                        offsetFileOffsets, offsetTargets);
  manager->wait(offsetHandle);

  // Compute string sizes and total buffer size.
  size_t totalSize = 0;
  for (size_t i = 0; i < n; ++i) {
    totalSize += offsetPairs[i].nextOffset - offsetPairs[i].offset;
  }

  // Phase 2: Read string data via io_uring (reusing the same manager).
  auto data = std::make_shared<VocabBatchLookupData>();
  data->buffer.resize(totalSize);
  data->views.resize(n);

  std::vector<size_t> sizes(n);
  std::vector<uint64_t> fileOffsets(n);
  std::vector<char*> targetPointers(n);
  {
    size_t bufferOffset = 0;
    for (size_t i = 0; i < n; ++i) {
      sizes[i] = offsetPairs[i].nextOffset - offsetPairs[i].offset;
      fileOffsets[i] = offsetPairs[i].offset;
      targetPointers[i] = data->buffer.data() + bufferOffset;
      bufferOffset += sizes[i];
    }
  }

  auto stringHandle =
      manager->addBatch(file_.fd(), sizes, fileOffsets, targetPointers);
  manager->wait(stringHandle);
  ioManagers_->push(std::move(manager));

  // Build string_views pointing into the buffer.
  for (size_t i = 0; i < n; ++i) {
    data->views[i] = std::string_view(targetPointers[i], sizes[i]);
  }

  return VocabBatchLookupData::asResult(std::move(data));
}

// _____________________________________________________________________________
VocabLookupOutput VocabularyOnDisk::lookupBatchesStreamed(
    VocabLookupInput input) const {
  auto gen =
      [](const VocabularyOnDisk* self,
         VocabLookupInput input) -> cppcoro::generator<VocabBatchLookupResult> {
    for (auto& batch : input) {
      co_yield self->lookupBatch(batch);
    }
  }(this, std::move(input));
  return VocabLookupOutput{std::move(gen)};
}

// _____________________________________________________________________________
template <typename Iterable>
void VocabularyOnDisk::buildFromIterable(Iterable&& it,
                                         const std::string& fileName) {
  {
    file_.open(fileName.c_str(), "w");
    ad_utility::MmapVector<Offset> offsets(fileName + offsetSuffix_,
                                           ad_utility::CreateTag{});
    uint64_t currentOffset = 0;
    uint64_t nextId = 0;
    for (const auto& [word, id] : it) {
      AD_CONTRACT_CHECK(nextId == id);
      ++nextId;
      offsets.push_back(currentOffset);
      currentOffset += file_.write(word.data(), word.size());
    }

    // End offset of last vocabulary entry, also consistent with the empty
    // vocabulary.
    offsets.push_back(currentOffset);
    file_.close();
  }  // After this close, the destructor of MmapVector is called, which dumps
     // everything to disk.
  open(fileName);
}

// _____________________________________________________________________________
VocabularyOnDisk::WordWriter::WordWriter(const std::string& outFilename)
    : file_{outFilename.c_str(), "w"},
      offsets_{absl::StrCat(outFilename, VocabularyOnDisk::offsetSuffix_),
               ad_utility::CreateTag{}} {}

// _____________________________________________________________________________
uint64_t VocabularyOnDisk::WordWriter::operator()(
    std::string_view word, [[maybe_unused]] bool isExternalDummy) {
  offsets_.push_back(currentOffset_);
  currentOffset_ += file_.write(word.data(), word.size());
  return offsets_.size() - 1;
}

// _____________________________________________________________________________
void VocabularyOnDisk::WordWriter::finishImpl() {
  // End offset of last vocabulary entry, also consistent with the empty
  // vocabulary.
  offsets_.push_back(currentOffset_);
  file_.close();
  offsets_.close();
}

// _____________________________________________________________________________
VocabularyOnDisk::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows([this]() { this->finish(); },
                                  "Calling `finish` from the destructor of "
                                  "`VocabularyOnDisk::WordWriter`");
  }
}

// _____________________________________________________________________________
void VocabularyOnDisk::buildFromStringsAndIds(
    const std::vector<std::pair<std::string, uint64_t>>& wordsAndIds,
    const std::string& fileName) {
  buildFromIterable(wordsAndIds, fileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::open(const std::string& filename) {
  file_.open(filename.c_str(), "r");
  offsets_.open(filename + offsetSuffix_);
  offsetsFile_.open(std::string(filename) + std::string(offsetSuffix_), "r");
  // Disable readahead for random lookups on both files.
  posix_fadvise(file_.fd(), 0, 0, POSIX_FADV_RANDOM);
  posix_fadvise(offsetsFile_.fd(), 0, 0, POSIX_FADV_RANDOM);
  AD_CORRECTNESS_CHECK(offsets_.size() > 0);
  size_ = offsets_.size() - 1;

  // Initialize pool of persistent BatchIoManagers for lookupBatch.
  static constexpr size_t kNumManagers = 8;
  ioManagers_ = std::make_unique<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchIoManager>>>(kNumManagers);
  for (size_t i = 0; i < kNumManagers; ++i) {
    ioManagers_->push(std::make_unique<ad_utility::BatchIoManager>());
  }
}
