// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_AUXINDEX_H
#define QLEVER_SRC_INDEX_AUXINDEX_H

#include <array>
#include <memory>
#include <string>
#include <vector>

#include "index/AuxVocabulary.h"
#include "index/Permutation.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/json.h"

// The metadata of an auxiliary index, stored as a JSON file next to its other
// files.
struct AuxIndexMetadata {
  // The generation of this auxiliary index. Generations start at zero and are
  // incremented by one for each rebuild.
  size_t generation_ = 0;
  // The number of triples that this auxiliary index inserts into resp. deletes
  // from the main index.
  size_t numInserted_ = 0;
  size_t numDeleted_ = 0;
  // The number of words in the auxiliary vocabulary.
  size_t numVocabWords_ = 0;
  // The type of the auxiliary vocabulary. It is required to read the vocabulary
  // back from disk, because the type is not encoded in the vocabulary's files.
  ad_utility::VocabularyType vocabularyType_ =
      ad_utility::VocabularyType::OnDiskCompressed;

  friend void to_json(nlohmann::json& j, const AuxIndexMetadata& metadata);
  friend void from_json(const nlohmann::json& j, AuxIndexMetadata& metadata);
};

// An auxiliary index: the on-disk representation of a large set of delta
// triples (triples that were inserted or deleted after the main index was
// built). It consists of
// 1. the six permutations plus the two internal permutations of those triples,
// 2. the auxiliary vocabulary with the words that the triples use and that are
//    not part of the vocabulary of the main index (see `AuxVocabulary`), and
// 3. the metadata above.
//
// Together with the delta triples that are still held in RAM (see
// `DeltaTriples`) and the main index, an auxiliary index forms a three-level
// log-structured merge tree: a scan of the main index merges in the union of
// the RAM delta triples and the triples of the auxiliary index, where the RAM
// delta triples take precedence (they are the more recent ones).
//
// The permutations of an auxiliary index have five columns: the three index
// columns in the order of the permutation, the graph column, and a column that
// stores whether the triple is inserted (`insertedId`) or deleted
// (`deletedId`).
//
// An `AuxIndex` is immutable. A rebuild does not modify it, but creates a new
// generation with a new set of files, and the `Id`s of its vocabulary are only
// valid with respect to that one generation.
class AuxIndex {
 public:
  using Allocator = ad_utility::AllocatorWithLimit<Id>;

  // The number of columns of the permutations, see the class comment.
  static constexpr size_t numColumns = 5;
  // The index of the column that stores whether a triple is inserted or
  // deleted.
  static constexpr ColumnIndex insertOrDeleteColumn = 4;

  // The values that are stored in the `insertOrDeleteColumn`.
  static Id insertedId() { return Id::makeFromBool(true); }
  static Id deletedId() { return Id::makeFromBool(false); }

  // The base name of the files of the auxiliary index with the given
  // `generation` for the main index with the given base name.
  static std::string makeBasename(std::string_view mainIndexBasename,
                                  size_t generation);

  // All files of the auxiliary index with the given base name that currently
  // exist on disk.
  static std::vector<ql::filesystem::path> allFiles(std::string_view basename);

  // The generations of all auxiliary indices that currently exist on disk for
  // the main index with the given base name, in ascending order. Only
  // generations whose metadata file exists are reported, so a generation whose
  // build was interrupted is not included (but its files may still be there,
  // see `deleteFromDisk`).
  static std::vector<size_t> generationsOnDisk(
      std::string_view mainIndexBasename);

  // Delete all files of the auxiliary index with the given base name.
  static void deleteFromDisk(std::string_view basename);

  explicit AuxIndex(Allocator allocator);
  AuxIndex(const AuxIndex&) = delete;
  AuxIndex& operator=(const AuxIndex&) = delete;

  // Read the auxiliary index with the given base name from disk. The locale has
  // to be the locale of the main index, see `AuxVocabulary::open`.
  void loadFromDisk(const std::string& basename, const std::string& language,
                    const std::string& country, bool ignorePunctuation);

  // The auxiliary vocabulary.
  const AuxVocabulary& vocab() const { return vocabulary_; }

  // The metadata.
  const AuxIndexMetadata& metadata() const { return metadata_; }

  // The generation, see `AuxIndexMetadata::generation_`.
  size_t generation() const { return metadata_.generation_; }

  // The base name of the files of this auxiliary index.
  const std::string& basename() const { return basename_; }

  // The permutation of the given kind. `internal` selects the internal
  // permutations (of which only `PSO` and `POS` exist).
  const Permutation& getPermutation(Permutation::Enum permutation,
                                    bool internal) const;

  // The (permanently empty) `LocatedTriplesState` that has to be passed to the
  // scan functions of the permutations of this auxiliary index. The delta
  // triples that are held in RAM are merged into the scan of the *main* index,
  // not into the scan of an auxiliary index.
  //
  // NOTE: Do not use this to obtain the block metadata of a permutation of this
  // auxiliary index via `Permutation::getAugmentedMetadataForPermutation`. That
  // function looks the metadata up by the permutation's *external* slot unless
  // the permutation's type is `INTERNAL`, and the permutations of an auxiliary
  // index are deliberately typed as materialized views (see `loadFromDisk`), so
  // for the internal ones it would return the metadata of the corresponding
  // external permutation. Use `scanFull` below instead.
  const LocatedTriplesState& emptyLocatedTriplesState() const {
    return *emptyLocatedTriplesState_;
  }

  // A full scan of the given permutation of this auxiliary index. Each row
  // holds the three index columns in the order of the permutation, the graph
  // column, and the column that stores whether the triple is inserted or
  // deleted (see `insertOrDeleteColumn`). The returned `LazyScanWithReader`
  // owns the reader that its blocks borrow from, so it has to be kept alive
  // while the blocks are being used.
  Permutation::LazyScanWithReader scanFull(
      Permutation::Enum permutation, bool internal,
      const ad_utility::SharedCancellationHandle& cancellationHandle) const;

  // Return true iff this auxiliary index contains no triples at all.
  bool isEmpty() const {
    return metadata_.numInserted_ + metadata_.numDeleted_ == 0;
  }

 private:
  // Common implementation of the two `getPermutation` cases, also used during
  // loading.
  Permutation& getPermutationImpl(Permutation::Enum permutation, bool internal);

  std::string basename_;
  AuxIndexMetadata metadata_;
  AuxVocabulary vocabulary_;
  Allocator allocator_;
  // The permutations. They are held as `unique_ptr`s because `Permutation` is
  // not movable.
  std::array<std::unique_ptr<Permutation>, 6> permutations_;
  std::array<std::unique_ptr<Permutation>, 2> internalPermutations_;
  std::shared_ptr<const LocatedTriplesState> emptyLocatedTriplesState_;
  bool isLoaded_ = false;
};

#endif  // QLEVER_SRC_INDEX_AUXINDEX_H
