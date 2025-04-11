//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/vocabulary/PolymorphicVocabulary.h"

#include <engine/CallFixedSize.h>

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
PolymorphicVocabulary::WordWriter::WordWriter(WordWriters writer)
    : writer_(std::move(writer)) {}

// _____________________________________________________________________________
void PolymorphicVocabulary::WordWriter::finish() {
  std::visit([](auto& writer) { return writer->finish(); }, writer_);
}

// _____________________________________________________________________________
void PolymorphicVocabulary::WordWriter::operator()(std::string_view word,
                                                   bool isExternal) {
  std::visit(
      [&word, isExternal](auto& writer) { return (*writer)(word, isExternal); },
      writer_);
}

// _____________________________________________________________________________
auto PolymorphicVocabulary::makeDiskWriter(const std::string& filename) const
    -> WordWriter {
  return WordWriter{std::visit(
      [&filename](auto& vocab) -> WordWriters {
        return vocab.makeDiskWriterPtr(filename);
      },
      vocab_)};
}

// _____________________________________________________________________________
PolymorphicVocabulary::WordWriter PolymorphicVocabulary::makeDiskWriter(
    const std::string& filename, VocabularyType type) {
  PolymorphicVocabulary dummyVocab;
  dummyVocab.resetToType(type);
  return dummyVocab.makeDiskWriter(filename);
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
