//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//           Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_POLYMORPHICVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_POLYMORPHICVOCABULARY_H

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <variant>

#include "backports/type_traits.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/VocabularyConstraints.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"
#include "util/json.h"

// A vocabulary that can at runtime choose between different vocabulary
// implementations. The only restriction is, that a vocabulary can only be read
// from disk with the same implementation that it was written to.
class PolymorphicVocabulary {
 public:
  using VocabularyType = ad_utility::VocabularyType;

 private:
  // Type aliases for all the currently supported vocabularies. To add another
  // vocabulary,
  // 1. Add an enum value to the `VocabularyTypeEnum`.
  // 2. Add an alias below for the vocabulary that has the same name as the enum
  // value.
  // 3. Add the alias type to the `Variant` below.
  // 4. Add the corresponding line to the `resetToType` function in
  // `PolymorphicVocabulary.cpp`.
  using InMemoryUncompressed = VocabularyInMemory;
  using OnDiskUncompressed = VocabularyInternalExternal;
  using InMemoryCompressed = CompressedVocabulary<InMemoryUncompressed>;
  using OnDiskCompressed = CompressedVocabulary<OnDiskUncompressed>;
  using OnDiskCompressedGeoSplit = SplitGeoVocabulary<OnDiskCompressed>;
  using Variant =
      std::variant<InMemoryUncompressed, OnDiskUncompressed, OnDiskCompressed,
                   InMemoryCompressed, OnDiskCompressedGeoSplit>;

  // In this variant we store the actual vocabulary.
  Variant vocab_;

 public:
  // Read a vocabulary with the given `type` from the file with the `filename`.
  // A vocabulary with the corresponding `type` must have been previously
  // written to that file.
  void open(const std::string& filename, VocabularyType type);

  // Close the vocabulary if it is open, and set the underlying vocabulary
  // implementation according to the `type` without opening the vocabulary.
  void resetToType(VocabularyType type);

  // Same as the overload of `open` above, but expects that the correct
  // `VocabularyType` has already been set via `resetToType` above.
  void open(const std::string& filename);

  // Close the vocabulary s.t. it consumes no more RAM.
  void close();

  // Return the total number of words in the vocabulary.
  size_t size() const;

  // Return the `i`-th word, throw if `i` is out of bounds.
  std::string operator[](uint64_t i) const;

  // Return a reference to currently underlying vocabulary, as a variant of the
  // possible types.
  Variant& getUnderlyingVocabulary() { return vocab_; }
  const Variant& getUnderlyingVocabulary() const { return vocab_; }

  // Same as `std::lower_bound`, return the smallest entry >= `word`.
  template <typename String, typename Comp>
  WordAndIndex lower_bound(const String& word, Comp comp) const {
    return std::visit(
        [&word, &comp](auto& vocab) {
          return vocab.lower_bound(word, std::move(comp));
        },
        vocab_);
  }

  // Analogous to `lower_bound` (see above).
  template <typename String, typename Comp>
  WordAndIndex upper_bound(const String& word, Comp comp) const {
    return std::visit(
        [&word, &comp](auto& vocab) {
          return vocab.upper_bound(word, std::move(comp));
        },
        vocab_);
  }

  // Analogous to `lower_bound`, but since `word` is guaranteed to be a full
  // word, not a prefix, this function can respect the split of an underlying
  // `SplitVocabulary`.
  template <typename String, typename Comp>
  std::pair<uint64_t, uint64_t> getPositionOfWord(const String& word,
                                                  Comp comp) const {
    return std::visit(
        [&word, &comp, this](auto& vocab) {
          using T = std::decay_t<decltype(vocab)>;
          if constexpr (HasSpecialGetPositionOfWord<T>) {
            return vocab.getPositionOfWord(word, std::move(comp));
          } else {
            // If a vocabulary does not require a special implementation for
            // `getPositionOfWord`, we can compute the result using
            // `lower_bound`. When adding new vocabulary types, ensure the
            // correct semantics here by deciding between adding the class to
            // `HasSpecialGetPositionOfWord` or `HasDefaultGetPositionOfWord`.
            static_assert(HasDefaultGetPositionOfWord<T>);
            return vocab.lower_bound(word, comp)
                .positionOfWord(word)
                .value_or(std::pair<uint64_t, uint64_t>{size(), size()});
          }
        },
        vocab_);
  }

  // Retrieve `GeometryInfo` from an underlying vocabulary, if it is a
  // `GeoVocabulary`.
  std::optional<ad_utility::GeometryInfo> getGeoInfo(uint64_t index) const {
    return std::visit(
        [&](const auto& vocab) -> std::optional<ad_utility::GeometryInfo> {
          using T = std::decay_t<decltype(vocab)>;
          // For more details, please see the definition of these concepts
          // in `VocabularyConstraints.h`.
          if constexpr (MaybeProvidesGeometryInfo<T>) {
            return vocab.getGeoInfo(index);
          } else {
            static_assert(NeverProvidesGeometryInfo<T>);
            return std::nullopt;
          }
        },
        vocab_);
  };

  // Checks if any of the underlying vocabularies is a `GeoVocabulary`.
  bool isGeoInfoAvailable() const {
    return std::visit(
        [](const auto& vocab) {
          using T = std::decay_t<decltype(vocab)>;
          // For more details, please see the definition of these concepts
          // in `VocabularyConstraints.h`.
          if constexpr (MaybeProvidesGeometryInfo<T>) {
            return vocab.isGeoInfoAvailable();
          } else {
            static_assert(NeverProvidesGeometryInfo<T>);
            return false;
          }
        },
        vocab_);
  }

  // Create a `WordWriter` that will create a vocabulary with the given `type`
  // at the given `filename`.
  static std::unique_ptr<WordWriterBase> makeDiskWriterPtr(
      const std::string& filename, VocabularyType type);

  // Same as above, but the `VocabularyType` is the currently active type of
  // `this`.
  std::unique_ptr<WordWriterBase> makeDiskWriterPtr(
      const std::string& filename) const;

  // Generic serialization support - delegates to the active variant.
  AD_SERIALIZE_FRIEND_FUNCTION(PolymorphicVocabulary) {
    std::visit([&serializer](auto& vocab) { serializer | vocab; }, arg.vocab_);
  }
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_POLYMORPHICVOCABULARY_H
