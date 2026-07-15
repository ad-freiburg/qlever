// Copyright 2025-2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// 2025-2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/PolymorphicVocabulary.h"

#include "engine/CallFixedSize.h"

// _____________________________________________________________________________
void PolymorphicVocabulary::open(const std::string& filename) {
  std::visit([&filename](auto& vocab) { vocab.open(filename); }, vocab_);
}

// _____________________________________________________________________________
void PolymorphicVocabulary::open(const std::string& filename,
                                 VocabularyType type) {
  resetToType(type);
  open(filename);
}

// _____________________________________________________________________________
void PolymorphicVocabulary::close() {
  std::visit([](auto& vocab) { return vocab.close(); }, vocab_);
}

// _____________________________________________________________________________
size_t PolymorphicVocabulary::size() const {
  return std::visit([](auto& vocab) -> size_t { return vocab.size(); }, vocab_);
}

// _____________________________________________________________________________
std::string PolymorphicVocabulary::operator[](uint64_t i) const {
  return std::visit([i](auto& vocab) { return std::string{vocab[i]}; }, vocab_);
}

// _____________________________________________________________________________
VocabBatchLookupResult PolymorphicVocabulary::lookupBatch(
    ql::span<const size_t> indices) const {
  return std::visit(
      [&indices](const auto& vocab) { return vocab.lookupBatch(indices); },
      vocab_);
}

// _____________________________________________________________________________
VocabLookupOutput PolymorphicVocabulary::lookupBatchesStreamed(
    VocabLookupInput input) const {
  return std::visit(
      [&input](const auto& vocab) {
        return vocab.lookupBatchesStreamed(std::move(input));
      },
      vocab_);
}

// _____________________________________________________________________________
auto PolymorphicVocabulary::makeDiskWriterPtr(const std::string& filename) const
    -> std::unique_ptr<WordWriterBase> {
  return std::visit(
      [&filename](auto& vocab) -> std::unique_ptr<WordWriterBase> {
        return vocab.makeDiskWriterPtr(filename);
      },
      vocab_);
}

// _____________________________________________________________________________
std::unique_ptr<WordWriterBase> PolymorphicVocabulary::makeDiskWriterPtr(
    const std::string& filename, VocabularyType type) {
  PolymorphicVocabulary dummyVocab;
  dummyVocab.resetToType(type);
  return dummyVocab.makeDiskWriterPtr(filename);
}

// _____________________________________________________________________________
void PolymorphicVocabulary::resetToType(VocabularyType type) {
  close();
  // The names of the enum values are the same as the type aliases for the
  // implementations, so we can shorten the following code using a macro.
#undef AD_CASE
#define AD_CASE(vocabType)              \
  case VocabularyType::Enum::vocabType: \
    vocab_.emplace<vocabType>();        \
    break

  switch (type.value()) {
    AD_CASE(InMemoryUncompressed);
    AD_CASE(OnDiskUncompressed);
    AD_CASE(InMemoryCompressed);
    AD_CASE(OnDiskCompressed);
    AD_CASE(OnDiskCompressedGeoSplit);
    default:
      AD_FAIL();
  }
}
