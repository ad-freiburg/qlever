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
  switch (type.value()) {
    case VocabularyType::Enum::InMemoryUncompressed:
      vocab_.emplace<InMemory>();
      break;
    case VocabularyType::Enum::OnDiskUncompressed:
      vocab_.emplace<External>();
      break;
    case VocabularyType::Enum::InMemoryCompressed:
      vocab_.emplace<CompressedInMemory>();
      break;
    case VocabularyType::Enum::OnDiskCompressed:
      vocab_.emplace<CompressedExternal>();
      break;
    default:
      AD_FAIL();
  }
}
