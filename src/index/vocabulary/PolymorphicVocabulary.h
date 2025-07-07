//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_VOCABULARY_POLYMORPHICVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_POLYMORPHICVOCABULARY_H

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <type_traits>
#include <variant>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/TypeTraits.h"
#include "util/json.h"

// Forward declaration for concept
class PolymorphicVocabulary;

// Only the `SplitVocabulary` currently needs a special handling for
// `getPositionOfWord` (this includes the `PolymorphicVocabulary` which may
// dynamically hold a `SplitVocabulary`)
template <typename T>
CPP_concept HasSpecialGetPositionOfWord =
    std::is_same_v<T, PolymorphicVocabulary> ||
    ad_utility::isInstantiation<T, SplitVocabulary>;

// As a safeguard for the future: Concept that a vocabulary does NOT require a
// special handling for `getPositionOfWord`. Note that `CompressedVocabulary`
// may not be checked via `isInstantiation` here because we do not know about
// the requirements for `getPositionOfWord` of its underlying vocabulary in
// general.
template <typename T>
CPP_concept HasDefaultGetPositionOfWord =
    ad_utility::SameAsAny<T, VocabularyInMemory, VocabularyInternalExternal,
                          CompressedVocabulary<VocabularyInMemory>,
                          CompressedVocabulary<VocabularyInternalExternal>>;

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

  // Create a `WordWriter` that will create a vocabulary with the given `type`
  // at the given `filename`.
  static std::unique_ptr<WordWriterBase> makeDiskWriterPtr(
      const std::string& filename, VocabularyType type);

  // Same as above, but the `VocabularyType` is the currently active type of
  // `this`.
  std::unique_ptr<WordWriterBase> makeDiskWriterPtr(
      const std::string& filename) const;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_POLYMORPHICVOCABULARY_H
