// Copyright 2024 - 2026, The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/VocabularyInternalExternal.h"

#include <absl/strings/str_cat.h>

#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <optional>
#include <range/v3/view/enumerate.hpp>
#include <string>
#include <string_view>
#include <utility>

#include "util/GlobalExecutor.h"

// _____________________________________________________________________________
std::string VocabularyInternalExternal::operator[](uint64_t i) const {
  auto fromInternal = internalVocab_[i];
  if (fromInternal.has_value()) {
    return std::string{fromInternal.value()};
  }
  return externalVocab_[i];
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyInternalExternal::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  // Make one pass over `indices`: place a word of the internal vocabulary as a
  // view into that vocabulary (no copy), and collect all other indices, with
  // their positions in `indices`, for one batched lookup in the external
  // vocabulary. The internal vocabulary has "holes", so each index needs one
  // membership probe (an allocation-free binary search); indices at or past
  // `internalVocab_.endIndex()` are known misses and skip the search.
  // Create the `assembler` on the first internal hit, so that a batch without
  // any internal word (the common case) does not allocate it.
  std::optional<MultiSourceVocabBatchAssembler> assembler;
  MarkerIndicesAndPositions externalSlots;
  const uint64_t internalEnd = internalVocab_.endIndex();
  for (const auto& [position, index] : ::ranges::views::enumerate(indices)) {
    const auto internalWord = index < internalEnd
                                  ? internalVocab_[index]
                                  : std::optional<std::string_view>{};
    if (internalWord.has_value()) {
      if (!assembler.has_value()) {
        assembler.emplace(indices.size());
      }
      assembler->assignUnownedViewAtPosition(position, internalWord.value());
    } else {
      externalSlots.addPair(index, position);
    }
  }

  if (externalSlots.empty()) {
    // All indices were internal, so the `assembler` exists.
    AD_CORRECTNESS_CHECK(assembler.has_value());
    return std::move(assembler).value().finalizeVocabBatchLookupResult();
  }
  auto external =
      externalVocab_.lookupBatch(externalSlots.getUnderlyingIndices());
  if (externalSlots.size() == indices.size()) {
    // There was no internal hit, so the positions are `0, 1, ...` and the
    // external batch already is the result.
    return external;
  }
  // Some, but not all, indices were internal, so the `assembler` exists.
  AD_CORRECTNESS_CHECK(assembler.has_value());
  assembler->scatterSubBatchResultAtPositions(
      std::move(external), externalSlots.getResultPositions());
  return std::move(assembler).value().finalizeVocabBatchLookupResult();
}

// _____________________________________________________________________________
VocabularyInternalExternal::WordWriter::WordWriter(const std::string& filename,
                                                   size_t milestoneDistance)
    : internalWriter_{absl::StrCat(filename, internalSuffix)},
      externalWriter_{absl::StrCat(filename, externalSuffix)},
      milestoneDistance_{milestoneDistance} {}

// _____________________________________________________________________________
uint64_t VocabularyInternalExternal::WordWriter::operator()(
    std::string_view word, bool isExternal) {
  externalWriter_(word, true);
  return addToInternalVocabIfRequired(word, isExternal);
}

// _____________________________________________________________________________
uint64_t VocabularyInternalExternal::WordWriter::addToInternalVocabIfRequired(
    std::string_view word, bool isExternal) {
  if (!isExternal || sinceMilestone_ >= milestoneDistance_ || idx_ == 0) {
    internalWriter_(word, idx_);
    sinceMilestone_ = 0;
  }
  ++sinceMilestone_;
  return idx_++;
}

// _____________________________________________________________________________
void VocabularyInternalExternal::BlockWriter::appendBlock(WordBlock block) {
  AD_CONTRACT_CHECK(block.firstPosition_ == writer_.idx_);
  uint64_t dataOffset = writer_.externalWriter_.reserveBlock(block);
  // The same rule as in `WordWriter::operator()` for the internal vocabulary;
  // the external vocabulary gets the whole block below.
  for (size_t i = 0; i < block.numWords(); ++i) {
    writer_.addToInternalVocabIfRequired(block.word(i), block.isExternal_[i]);
  }
  // Bound the number of writes in flight (each holds its block in memory).
  // The oldest ones are typically long done, so this rarely waits.
  // TODO<joka921> Contribute the waiting thread to the global executor (run
  // its tasks while the oldest write is not ready) as soon as the executor
  // supports this, which an `asio::thread_pool` currently doesn't.
  const size_t maxNumPendingWrites = 2 * ad_utility::globalExecutorNumThreads();
  while (pendingWrites_.size() >= maxNumPendingWrites) {
    pendingWrites_.front().get();
    pendingWrites_.pop_front();
  }
  pendingWrites_.push_back(boost::asio::post(
      ad_utility::globalExecutor(),
      boost::asio::use_future([this, block = std::move(block), dataOffset]() {
        writer_.externalWriter_.writeBlockAt(block, dataOffset);
      })));
}

// _____________________________________________________________________________
VocabularyInternalExternal::BlockWriter::~BlockWriter() {
  ad_utility::terminateIfThrows([this]() { this->finish(); },
                                "Calling `finish` from the destructor of "
                                "`VocabularyInternalExternal::BlockWriter`");
}

// _____________________________________________________________________________
void VocabularyInternalExternal::BlockWriter::finishImpl() {
  // Wait for the writes on the pool (and rethrow their exceptions) before the
  // files are finished.
  // TODO<joka921> Contribute the waiting thread to the global executor, see
  // the same TODO in `appendBlock`.
  for (auto& write : pendingWrites_) {
    write.get();
  }
  pendingWrites_.clear();
  writer_.finish();
}

// _____________________________________________________________________________
void VocabularyInternalExternal::WordWriter::finishImpl() {
  internalWriter_.finish();
  externalWriter_.finish();
}

// _____________________________________________________________________________
VocabularyInternalExternal::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows([this]() { this->finish(); },
                                  "Calling `finish` from the destructor of "
                                  "`VocabularyInternalExternal::WordWriter`");
  }
}

// _____________________________________________________________________________
void VocabularyInternalExternal::open(const std::string& filename) {
  AD_LOG_INFO << "Reading vocabulary from file " << filename << " ..."
              << std::endl;
  internalVocab_.open(absl::StrCat(filename, internalSuffix));
  externalVocab_.open(absl::StrCat(filename, externalSuffix));
  AD_LOG_INFO << "Done, number of words: " << size() << std::endl;
  AD_LOG_INFO << "Number of words in internal vocabulary (these are also part "
                 "of the external vocabulary): "
              << internalVocab_.size() << std::endl;
}
