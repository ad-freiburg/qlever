// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (kalmbach@cs.uni-freiburg.de)

#include "index/vocabulary/VocabularyInternalExternal.h"

#include <absl/strings/str_cat.h>

#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>

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
void VocabularyInternalExternal::BlockWriter::append(
    std::unique_ptr<PreparedBlockBase> prepared) {
  const WordBlock& block = dynamic_cast<Prepared&>(*prepared).block_;
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
      boost::asio::use_future(
          [this, prepared = std::move(prepared), dataOffset]() {
            writer_.externalWriter_.writeBlockAt(
                dynamic_cast<Prepared&>(*prepared).block_, dataOffset);
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
  // the same TODO in `append`.
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
