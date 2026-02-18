// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "index/vocabulary/VocabularyOnDisk.h"

#include <algorithm>
#include <fstream>
#include <numeric>

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
#endif

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

  // Step 1: Gather offset and size for each index.
  std::vector<OffsetAndSize> offsetsAndSizes(n);
  size_t totalSize = 0;
  for (size_t i = 0; i < n; ++i) {
    AD_CONTRACT_CHECK(indices[i] < size());
    offsetsAndSizes[i] = getOffsetAndSize(indices[i]);
    totalSize += offsetsAndSizes[i].size_;
  }

  // Step 2: Allocate result data with one contiguous buffer.
  auto data = std::make_shared<VocabBatchLookupData>();
  data->buffer.resize(totalSize);
  data->views.resize(n);

  // Step 3: Compute each view's position in the buffer and build a permutation
  // sorted by file offset for sequential I/O.
  std::vector<size_t> bufferOffsets(n);
  {
    size_t offset = 0;
    for (size_t i = 0; i < n; ++i) {
      bufferOffsets[i] = offset;
      offset += offsetsAndSizes[i].size_;
    }
  }

  std::vector<size_t> perm(n);
  std::iota(perm.begin(), perm.end(), size_t{0});
  std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
    return offsetsAndSizes[a].offset_ < offsetsAndSizes[b].offset_;
  });

  // Step 4: Read data from disk.
  // Helper lambda for synchronous reads in sorted order.
  auto readSynchronously = [&]() {
    for (size_t pi = 0; pi < n; ++pi) {
      size_t i = perm[pi];
      file_.read(data->buffer.data() + bufferOffsets[i],
                 offsetsAndSizes[i].size_, offsetsAndSizes[i].offset_);
    }
  };

#ifdef QLEVER_HAS_IO_URING
  [&]() {
    struct io_uring ring;
    // Initialize ring with enough entries for all reads.
    int ret = io_uring_queue_init(static_cast<unsigned>(n), &ring, 0);
    if (ret < 0) {
      readSynchronously();
      return;
    }

    // Submit read SQEs in sorted order.
    for (size_t pi = 0; pi < n; ++pi) {
      size_t i = perm[pi];
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      if (!sqe) {
        // Not enough SQEs, submit what we have and get more.
        io_uring_submit(&ring);
        sqe = io_uring_get_sqe(&ring);
      }
      if (!sqe) {
        io_uring_queue_exit(&ring);
        readSynchronously();
        return;
      }
      io_uring_prep_read(sqe, file_.fd(),
                         data->buffer.data() + bufferOffsets[i],
                         static_cast<unsigned>(offsetsAndSizes[i].size_),
                         static_cast<__u64>(offsetsAndSizes[i].offset_));
      io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(i));
    }

    ret = io_uring_submit(&ring);
    if (ret < 0) {
      io_uring_queue_exit(&ring);
      readSynchronously();
      return;
    }

    // Reap all CQEs.
    for (size_t completed = 0; completed < n; ++completed) {
      struct io_uring_cqe* cqe;
      ret = io_uring_wait_cqe(&ring, &cqe);
      if (ret < 0) {
        io_uring_queue_exit(&ring);
        throw std::runtime_error(
            "io_uring_wait_cqe failed during vocabulary batch lookup");
      }
      io_uring_cqe_seen(&ring, cqe);
    }

    io_uring_queue_exit(&ring);
  }();
#else
  readSynchronously();
#endif
  // Step 5: Build string_views pointing into the buffer.
  for (size_t i = 0; i < n; ++i) {
    data->views[i] = std::string_view(
        data->buffer.data() + bufferOffsets[i], offsetsAndSizes[i].size_);
  }

  return VocabBatchLookupData::asResult(std::move(data));
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
  AD_CORRECTNESS_CHECK(offsets_.size() > 0);
  size_ = offsets_.size() - 1;
}
