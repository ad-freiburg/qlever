// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/AuxIndex.h"

#include <absl/strings/str_cat.h>

#include <fstream>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "global/Constants.h"
#include "global/FileSuffixConstants.h"
#include "index/DeltaTriples.h"
#include "index/LocalVocab.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"
#include "util/Log.h"

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const AuxIndexMetadata& metadata) {
  j = nlohmann::json{{"generation", metadata.generation_},
                     {"num-inserted", metadata.numInserted_},
                     {"num-deleted", metadata.numDeleted_},
                     {"num-vocab-words", metadata.numVocabWords_},
                     {"vocabulary-type", metadata.vocabularyType_}};
}

// ____________________________________________________________________________
void from_json(const nlohmann::json& j, AuxIndexMetadata& metadata) {
  j.at("generation").get_to(metadata.generation_);
  j.at("num-inserted").get_to(metadata.numInserted_);
  j.at("num-deleted").get_to(metadata.numDeleted_);
  j.at("num-vocab-words").get_to(metadata.numVocabWords_);
  j.at("vocabulary-type").get_to(metadata.vocabularyType_);
}

// ____________________________________________________________________________
std::string AuxIndex::makeBasename(std::string_view mainIndexBasename,
                                   size_t generation) {
  return absl::StrCat(mainIndexBasename, AUX_INDEX_INFIX, generation);
}

// ____________________________________________________________________________
std::vector<ql::filesystem::path> AuxIndex::allFiles(
    std::string_view basename) {
  std::vector<ql::filesystem::path> result;
  auto addIfExists = [&result](ql::filesystem::path file) {
    if (ql::filesystem::exists(file)) {
      result.push_back(std::move(file));
    }
  };
  auto addPermutationFiles = [&addIfExists](auto isInternal,
                                            std::string_view base) {
    for (auto permutation : Permutation::all<isInternal>()) {
      for (auto& file : Permutation::fileNames(permutation, base)) {
        addIfExists(std::move(file));
      }
    }
  };
  addPermutationFiles(std::bool_constant<false>{}, basename);
  addPermutationFiles(std::bool_constant<true>{},
                      absl::StrCat(basename, QLEVER_INTERNAL_INDEX_INFIX));
  addIfExists(absl::StrCat(basename, AUX_VOCAB_POSITIONS_SUFFIX));
  addIfExists(absl::StrCat(basename, AUX_INDEX_CONFIGURATION_FILE));
  // The set of vocabulary files depends on the vocabulary type, so enumerate
  // them via their common prefix (the same mechanism as
  // `IndexImpl::allIndexFiles`).
  ql::ranges::move(qlever::util::filesWithBaseNameAndSuffix(
                       std::string{basename}, VOCAB_SUFFIX),
                   std::back_inserter(result));
  // Remove duplicates: the file with the positions in the main vocabulary is
  // also matched by the prefix of the vocabulary files above.
  ql::ranges::sort(result);
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}

// ____________________________________________________________________________
std::vector<size_t> AuxIndex::generationsOnDisk(
    std::string_view mainIndexBasename) {
  std::vector<size_t> result;
  // The metadata file is written last, so its presence marks a complete
  // auxiliary index.
  for (const auto& file : qlever::util::filesWithBaseNameAndSuffix(
           std::string{mainIndexBasename}, AUX_INDEX_INFIX)) {
    std::string_view name = file.native();
    if (!ql::ends_with(name, AUX_INDEX_CONFIGURATION_FILE)) {
      continue;
    }
    // Extract the generation, which is the part between `AUX_INDEX_INFIX` and
    // `AUX_INDEX_CONFIGURATION_FILE`.
    name.remove_suffix(AUX_INDEX_CONFIGURATION_FILE.size());
    auto pos = name.rfind(AUX_INDEX_INFIX);
    AD_CORRECTNESS_CHECK(pos != std::string_view::npos);
    name.remove_prefix(pos + AUX_INDEX_INFIX.size());
    size_t generation = 0;
    if (name.empty() || !ql::ranges::all_of(name, [](char c) {
          return c >= '0' && c <= '9';
        })) {
      continue;
    }
    generation = std::stoull(std::string{name});
    result.push_back(generation);
  }
  ql::ranges::sort(result);
  return result;
}

// ____________________________________________________________________________
void AuxIndex::deleteFromDisk(std::string_view basename) {
  // Delete the metadata file first, such that an interrupted deletion leaves
  // behind an auxiliary index that is not reported by `generationsOnDisk` and
  // hence never used.
  ql::filesystem::path metadataFile =
      absl::StrCat(basename, AUX_INDEX_CONFIGURATION_FILE);
  if (ql::filesystem::exists(metadataFile)) {
    ad_utility::deleteFile(metadataFile);
  }
  for (const auto& file : allFiles(basename)) {
    ad_utility::deleteFile(file);
  }
}

// ____________________________________________________________________________
AuxIndex::AuxIndex(Allocator allocator) : allocator_{std::move(allocator)} {
  for (auto permutation : Permutation::ALL) {
    permutations_.at(static_cast<size_t>(permutation)) =
        std::make_unique<Permutation>(permutation, allocator_);
  }
  for (auto permutation : Permutation::INTERNAL) {
    internalPermutations_.at(static_cast<size_t>(permutation)) =
        std::make_unique<Permutation>(permutation, allocator_);
  }
}

// ____________________________________________________________________________
Permutation& AuxIndex::getPermutationImpl(Permutation::Enum permutation,
                                          bool internal) {
  auto index = static_cast<size_t>(permutation);
  if (internal) {
    AD_CONTRACT_CHECK(index < internalPermutations_.size(),
                      "An auxiliary index only has the internal permutations "
                      "PSO and POS");
    return *internalPermutations_.at(index);
  }
  return *permutations_.at(index);
}

// ____________________________________________________________________________
const Permutation& AuxIndex::getPermutation(Permutation::Enum permutation,
                                            bool internal) const {
  AD_CONTRACT_CHECK(isLoaded_, "The auxiliary index has not been loaded");
  return const_cast<AuxIndex&>(*this).getPermutationImpl(permutation, internal);
}

// ____________________________________________________________________________
Permutation::LazyScanWithReader AuxIndex::scanFull(
    Permutation::Enum permutationEnum, bool internal,
    const ad_utility::SharedCancellationHandle& cancellationHandle) const {
  const auto& permutation = getPermutation(permutationEnum, internal);
  // An auxiliary index never has located triples of its own, so the block
  // metadata is simply the one of the permutation. Note that it must *not* be
  // taken from `emptyLocatedTriplesState()`, see the comment there.
  BlockMetadataSpan blocks{permutation.metaData().blockData()};
  Permutation::ScanSpecAndBlocks scanSpecAndBlocks{
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
      BlockMetadataRanges{{blocks.begin(), blocks.end()}}};
  std::vector<ColumnIndex> additionalColumns{ADDITIONAL_COLUMN_GRAPH_ID,
                                             insertOrDeleteColumn};
  return permutation.lazyScanWithUnlimitedReader(
      scanSpecAndBlocks, additionalColumns, cancellationHandle,
      emptyLocatedTriplesState());
}

// ____________________________________________________________________________
void AuxIndex::loadFromDisk(const std::string& basename,
                            const std::string& language,
                            const std::string& country,
                            bool ignorePunctuation) {
  AD_CONTRACT_CHECK(!isLoaded_);
  basename_ = basename;

  std::string metadataFilename =
      absl::StrCat(basename, AUX_INDEX_CONFIGURATION_FILE);
  std::ifstream metadataFile{metadataFilename};
  AD_CONTRACT_CHECK(metadataFile.is_open(),
                    absl::StrCat("Could not read the metadata file \"",
                                 metadataFilename, "\" of an auxiliary index"));
  metadata_ = nlohmann::json::parse(metadataFile).get<AuxIndexMetadata>();

  vocabulary_.open(basename, metadata_.vocabularyType_, language, country,
                   ignorePunctuation);
  AD_CORRECTNESS_CHECK(vocabulary_.numWords() == metadata_.numVocabWords_);

  // The auxiliary permutations are read raw: the deduplication and the graph
  // filtering are applied by the merge into the scan of the main index, not
  // here, so the graph post-processing of the `CompressedRelationReader` has to
  // be switched off (which is what `Type::MATERIALIZED_VIEW` does).
  for (auto permutation : Permutation::ALL) {
    getPermutationImpl(permutation, false)
        .loadFromDisk(basename, false, Permutation::Type::MATERIALIZED_VIEW);
  }
  std::string internalBasename =
      absl::StrCat(basename, QLEVER_INTERNAL_INDEX_INFIX);
  for (auto permutation : Permutation::INTERNAL) {
    getPermutationImpl(permutation, true)
        .loadFromDisk(internalBasename, false,
                      Permutation::Type::MATERIALIZED_VIEW);
  }

  // Set up the permanently empty `LocatedTriplesState` that the scans of the
  // permutations of this auxiliary index use.
  LocatedTriplesPerBlockAllPermutations<false> emptyLocatedTriples;
  for (auto permutation : Permutation::ALL) {
    emptyLocatedTriples.at(static_cast<size_t>(permutation))
        .setOriginalMetadata(getPermutationImpl(permutation, false)
                                 .metaData()
                                 .blockDataShared());
  }
  LocatedTriplesPerBlockAllPermutations<true> emptyInternalLocatedTriples;
  for (auto permutation : Permutation::INTERNAL) {
    emptyInternalLocatedTriples.at(static_cast<size_t>(permutation))
        .setOriginalMetadata(
            getPermutationImpl(permutation, true).metaData().blockDataShared());
  }
  LocalVocab emptyLocalVocab;
  emptyLocatedTriplesState_ = std::make_shared<const LocatedTriplesState>(
      LocatedTriplesState{std::move(emptyLocatedTriples),
                          std::move(emptyInternalLocatedTriples),
                          emptyLocalVocab.getLifetimeExtender(), 0});

  isLoaded_ = true;
  AD_LOG_INFO << "Registered auxiliary index (generation "
              << metadata_.generation_ << ") with " << metadata_.numInserted_
              << " inserted and " << metadata_.numDeleted_
              << " deleted triples, and " << metadata_.numVocabWords_
              << " new words in its vocabulary" << std::endl;
}
