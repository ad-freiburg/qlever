//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

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
