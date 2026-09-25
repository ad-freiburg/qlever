// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/IdColumnSort.h"

#include <algorithm>
#include <cstring>

#include "global/Constants.h"
#include "global/Id.h"
#include "util/Exception.h"
#include "util/jthread.h"

namespace columnBasedIdTable {

SingleKeySorter::SingleKeySorter(::IdTable& table, size_t numThreads)
    : table_{table}, numThreads_{numThreads}, numRows_{table.numRows()} {}

bool SingleKeySorter::isSortable(const ::IdTable& table,
                                 ColumnIndex keyColumn) {
  auto keyTypes = table.getColumn(keyColumn).rawDatatypes();
  return std::memchr(keyTypes.data(),
                     static_cast<int>(Datatype::LocalVocabIndex),
                     keyTypes.size()) == nullptr;
}

void SingleKeySorter::sort(ColumnIndex keyColumn) {
  AD_CONTRACT_CHECK(isSortable(table_, keyColumn));
  if (numRows_ == 0) {
    return;
  }
  auto key = std::as_const(table_).getColumn(keyColumn);
  partitionByDatatype(key.rawDatatypes(), key.rawPayloads());
  sortPartitions();
  applyPermutation();
}

void SingleKeySorter::partitionByDatatype(
    ql::span<const uint8_t> keyTypes, ql::span<const uint64_t> keyPayloads) {
  for (uint8_t type : keyTypes) {
    ++counts_[type];
  }
  permutation_.resize(numRows_);
  // Fast path: single datatype (detected for free from the counts above).
  // Fills in order directly, skipping the bucket-position indirection
  // below, which also defeats auto-vectorization.
  if (counts_[keyTypes[0]] == numRows_) {
    singleDatatype_ = true;
    for (size_t i = 0; i < numRows_; ++i) {
      permutation_[i] = {keyPayloads[i], i};
    }
    return;
  }
  for (size_t t = 0; t < 256; ++t) {
    offsets_[t + 1] = offsets_[t] + counts_[t];
  }
  std::array<size_t, 256> positions{};
  std::copy(offsets_.begin(), offsets_.end() - 1, positions.begin());
  for (size_t i = 0; i < numRows_; ++i) {
    permutation_[positions[keyTypes[i]]++] = {keyPayloads[i], i};
  }
}

void SingleKeySorter::sortPartitions() {
  auto comparator = [](const PayloadAndIndex& a, const PayloadAndIndex& b) {
    return a.payload_ < b.payload_;
  };
  auto sortRange = [this, &comparator](auto begin, auto end) {
    if constexpr (USE_PARALLEL_SORT) {
      ad_utility::parallel_sort(begin, end, comparator,
                                ad_utility::parallel_tag(numThreads_));
    } else {
      std::sort(begin, end, comparator);
    }
  };
  if (singleDatatype_) {
    sortRange(permutation_.begin(), permutation_.end());
    return;
  }
  for (size_t t = 0; t < 256; ++t) {
    if (counts_[t] > 1) {
      sortRange(permutation_.begin() + offsets_[t],
                permutation_.begin() + offsets_[t + 1]);
    }
  }
}

void SingleKeySorter::applyPermutation() {
  const size_t numColumns = table_.numColumns();
  const size_t numWorkers =
      std::min(std::max(numThreads_, size_t{1}), numColumns);
  if (numWorkers <= 1) {
    std::vector<uint64_t> scratchPayloads(numRows_);
    std::vector<uint8_t> scratchTypes(numRows_);
    for (size_t c = 0; c < numColumns; ++c) {
      applyPermutationToColumn(c, scratchPayloads, scratchTypes);
    }
    return;
  }
  std::vector<ad_utility::JThread> workers;
  workers.reserve(numWorkers);
  for (size_t worker = 0; worker < numWorkers; ++worker) {
    workers.emplace_back([this, numColumns, numWorkers, worker]() {
      std::vector<uint64_t> scratchPayloads(numRows_);
      std::vector<uint8_t> scratchTypes(numRows_);
      for (size_t c = worker; c < numColumns; c += numWorkers) {
        applyPermutationToColumn(c, scratchPayloads, scratchTypes);
      }
    });
  }
  // `workers` going out of scope joins every `JThread`.
}

void SingleKeySorter::applyPermutationToColumn(
    size_t c, std::vector<uint64_t>& scratchPayloads,
    std::vector<uint8_t>& scratchTypes) {
  auto column = table_.getColumn(c);
  auto payloads = column.rawPayloads();
  auto types = column.rawDatatypes();
  for (size_t i = 0; i < numRows_; ++i) {
    scratchPayloads[i] = payloads[permutation_[i].index_];
    scratchTypes[i] = types[permutation_[i].index_];
  }
  std::memcpy(payloads.data(), scratchPayloads.data(),
              numRows_ * sizeof(uint64_t));
  std::memcpy(types.data(), scratchTypes.data(), numRows_ * sizeof(uint8_t));
}

}  // namespace columnBasedIdTable
