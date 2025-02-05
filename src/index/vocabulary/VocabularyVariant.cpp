//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/vocabulary/VocabularyVariant.h"

#include <engine/CallFixedSize.h>

void VocabularyVariant::open(const std::string& filename) {
  std::visit([&filename](auto& vocab) { vocab.open(filename); }, vocab_);
}

void VocabularyVariant::open(const std::string& filename, VocabularyType type) {
  resetToType(type);
  open(filename);
}

void VocabularyVariant::close() {
  return std::visit([](auto& vocab) { return vocab.close(); }, vocab_);
}
size_t VocabularyVariant::size() const {
  return std::visit([](auto& vocab) { return vocab.size(); }, vocab_);
}
std::string VocabularyVariant::operator[](uint64_t i) const {
  return std::visit([i](auto& vocab) { return std::string{vocab[i]}; }, vocab_);
}

VocabularyVariant::WordWriter::WordWriter(WordWriters writer)
    : writer_(std::move(writer)) {}

void VocabularyVariant::WordWriter::finish() {
  std::visit([](auto& writer) { return writer->finish(); }, writer_);
}

void VocabularyVariant::WordWriter::operator()(std::string_view word,
                                               bool isExternal) {
  std::visit(
      [&word, isExternal](auto& writer) { return (*writer)(word, isExternal); },
      writer_);
}

auto VocabularyVariant::makeDiskWriter(const std::string& filename) const
    -> WordWriter {
  return WordWriter{std::visit(
      [&filename](auto& vocab) -> WordWriters {
        return vocab.makeDiskWriterPtr(filename);
      },
      vocab_)};
}

VocabularyVariant::WordWriter VocabularyVariant::makeDiskWriter(
    const std::string& filename, VocabularyType type) {
  VocabularyVariant dummyVocab;
  dummyVocab.resetToType(type);
  return dummyVocab.makeDiskWriter(filename);
}

void VocabularyVariant::resetToType(VocabularyType type) {
  close();
  switch (type.value()) {
    case VocabularyType::Enum::InMemory:
      vocab_.emplace<InMemory>();
      break;
    case VocabularyType::Enum::OnDisk:
      vocab_.emplace<External>();
      break;
    case VocabularyType::Enum::CompressedInMemory:
      vocab_.emplace<CompressedInMemory>();
      break;
    case VocabularyType::Enum::CompressedOnDisk:
      vocab_.emplace<CompressedExternal>();
      break;
    default:
      AD_FAIL();
  }
}
