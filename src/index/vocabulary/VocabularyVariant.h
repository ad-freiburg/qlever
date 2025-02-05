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
#include "util/json.h"

template <typename Variant>
static constexpr auto getWordWriterTypes(const Variant& var) {
  return std::apply(
      []<typename... Vocab>(const Vocab&...) {
        return std::type_identity<
            std::variant<std::unique_ptr<typename Vocab::WordWriter>...>>{};
      },
      var);
}

class VocabularyEnum {
 public:
  enum struct Enum { InMemory, OnDisk, CompressedInMemory, CompressedOnDisk };

 private:
  Enum value_ = Enum::InMemory;

  static constexpr std::array<std::string_view, 4> descriptions{
      "in-memory-uncompressed", "on-disk-uncompressed", "in-memory-compressed",
      "on-disk-compressed"};

 public:
  VocabularyEnum() = default;
  explicit VocabularyEnum(Enum value) : value_{value} {}

  static VocabularyEnum fromString(std::string_view description) {
    auto it = ql::ranges::find(descriptions, description);
    if (it == descriptions.end()) {
      throw std::runtime_error{
          absl::StrCat("\"", description,
                       "\" is not a valid vocabulary type. The currently "
                       "supported vocabulary types are ",
                       absl::StrJoin(descriptions, ", "))};
      ;
    }
    return VocabularyEnum{static_cast<Enum>(it - descriptions.begin())};
  }
  std::string_view toString() const {
    return descriptions.at(static_cast<size_t>(value_));
  }

  Enum value() const { return value_; }

  // Conversion To JSON.
  friend void to_json(nlohmann::json& j, const VocabularyEnum& vocabEnum) {
    j = vocabEnum.toString();
  }

  // Conversion from JSON.
  friend void from_json(const nlohmann::json& j, VocabularyEnum& vocabEnum) {
    vocabEnum = VocabularyEnum::fromString(static_cast<std::string>(j));
  }
};

class VocabularyVariant {
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
  void resetToType(VocabularyEnum);
  void open(const std::string& filename);
  void open(const std::string& filename, VocabularyEnum type);
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

  using WordWriters = decltype(getWordWriterTypes(std::declval<Tuple>()))::type;

  class WordWriter {
    WordWriters writer_;

   public:
    explicit WordWriter(WordWriters);

    void finish();

    void operator()(std::string_view word, bool isExternal);
  };

  WordWriter makeDiskWriter(const std::string& filename) const;
  static WordWriter makeDiskWriter(const std::string& filename,
                                   VocabularyEnum type);
};
