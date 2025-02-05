//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <variant>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/json.h"

namespace polymorphic_vocabulary::detail {

template <typename T>
struct WriterPointers {};

template <typename... Vocabs>
struct WriterPointers<std::variant<Vocabs...>> {
  using type = std::variant<std::unique_ptr<typename Vocabs::WordWriter>...>;
};
}  // namespace polymorphic_vocabulary::detail

class VocabularyVariant {
 public:
  using VocabularyType = ad_utility::VocabularyEnum;

 private:
  using InMemory = VocabularyInMemory;
  using External = VocabularyInternalExternal;
  using CompressedInMemory = CompressedVocabulary<InMemory>;
  using CompressedExternal = CompressedVocabulary<External>;
  using Variant =
      std::variant<InMemory, External, CompressedExternal, CompressedInMemory>;
  using Tuple =
      std::tuple<InMemory, External, CompressedExternal, CompressedInMemory>;

  Variant vocab_;

 public:
  void resetToType(VocabularyType);
  void open(const std::string& filename);
  void open(const std::string& filename, VocabularyType type);
  void close();
  size_t size() const;
  std::string operator[](uint64_t i) const;

  template <typename String, typename Comp>
  WordAndIndex lower_bound(const String& word, Comp comp) const {
    return std::visit(
        [&word, &comp](auto& vocab) {
          return vocab.lower_bound(word, std::move(comp));
        },
        vocab_);
  }

  template <typename String, typename Comp>
  WordAndIndex lower_bound_iterator(const String& word, Comp comp) const {
    return std::visit(
        [&word, &comp](auto& vocab) {
          return vocab.lower_bound_iterator(word, std::move(comp));
        },
        vocab_);
  }

  template <typename String, typename Comp>
  WordAndIndex upper_bound(const String& word, Comp comp) const {
    return std::visit(
        [&word, &comp](auto& vocab) {
          return vocab.upper_bound(word, std::move(comp));
        },
        vocab_);
  }

  template <typename String, typename Comp>
  WordAndIndex upper_bound_iterator(const String& word, Comp comp) const {
    return std::visit(
        [&word, &comp](auto& vocab) {
          return vocab.upper_bound_iterator(word, std::move(comp));
        },
        vocab_);
  }

  using WordWriters =
      polymorphic_vocabulary::detail::WriterPointers<Variant>::type;

  class WordWriter {
    WordWriters writer_;

   public:
    explicit WordWriter(WordWriters);

    void finish();

    void operator()(std::string_view word, bool isExternal);
  };

  WordWriter makeDiskWriter(const std::string& filename) const;
  static WordWriter makeDiskWriter(const std::string& filename,
                                   VocabularyType type);
};
