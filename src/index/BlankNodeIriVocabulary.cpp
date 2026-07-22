// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/BlankNodeIriVocabulary.h"

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeVector.h"

// ____________________________________________________________________________
BlankNodeIriVocabulary::Writer::Writer(
    std::unique_ptr<WordWriterBase> wordWriter, std::string chunksFilename)
    : wordWriter_{std::move(wordWriter)},
      chunksFilename_{std::move(chunksFilename)} {}

// ____________________________________________________________________________
void BlankNodeIriVocabulary::Writer::operator()(std::string_view iri,
                                                uint64_t blankNodeIndex) {
  AD_CORRECTNESS_CHECK(!finished_);
  // The word writer assigns the positions `0, 1, 2, ...` in the order in which
  // the words are pushed, so the position is always `nextVocabPos_`.
  uint64_t position = (*wordWriter_)(iri, false);
  AD_CORRECTNESS_CHECK(position == nextVocabPos_);
  // Extend the current chunk if the blank node index is contiguous with it;
  // otherwise start a new chunk.
  if (!chunks_.empty() &&
      chunks_.back().blankNodeStartIndex_ + chunks_.back().count_ ==
          blankNodeIndex) {
    ++chunks_.back().count_;
  } else {
    chunks_.push_back(Chunk{position, blankNodeIndex, 1});
  }
  ++nextVocabPos_;
}

// ____________________________________________________________________________
void BlankNodeIriVocabulary::Writer::finish() {
  if (finished_) {
    return;
  }
  finished_ = true;
  wordWriter_->finish();
  ad_utility::serialization::FileWriteSerializer serializer{chunksFilename_};
  serializer << chunks_;
}

// ____________________________________________________________________________
void BlankNodeIriVocabulary::readFromFile(const std::string& vocabBaseName,
                                          ad_utility::VocabularyType type,
                                          const std::string& language,
                                          const std::string& country,
                                          bool ignorePunctuation) {
  vocab_.resetToType(type);
  vocab_.setLocale(language, country, ignorePunctuation);
  vocab_.readFromFile(vocabBaseName);
  ad_utility::serialization::FileReadSerializer serializer{
      chunksFilename(vocabBaseName)};
  serializer >> chunks_;
}

// ____________________________________________________________________________
std::optional<uint64_t> BlankNodeIriVocabulary::getBlankNodeIndex(
    std::string_view iri) const {
  if (chunks_.empty()) {
    return std::nullopt;
  }
  VocabIndex idx;
  if (!vocab_.getId(iri, &idx)) {
    return std::nullopt;
  }
  uint64_t position = idx.get();
  // Find the chunk that contains `position`. The chunks are sorted by
  // `vocabStartPos_` and cover the vocabulary positions contiguously, so the
  // chunk before the first one whose `vocabStartPos_` exceeds `position` is the
  // one we are looking for.
  auto it =
      ql::ranges::upper_bound(chunks_, position, {}, &Chunk::vocabStartPos_);
  AD_CORRECTNESS_CHECK(it != chunks_.begin());
  const Chunk& chunk = *std::prev(it);
  AD_CORRECTNESS_CHECK(position >= chunk.vocabStartPos_ &&
                       position < chunk.vocabStartPos_ + chunk.count_);
  return chunk.blankNodeStartIndex_ + (position - chunk.vocabStartPos_);
}
