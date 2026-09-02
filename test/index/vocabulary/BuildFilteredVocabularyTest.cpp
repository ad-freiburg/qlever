// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "index/vocabulary/BuildFilteredVocabulary.h"
#include "util/File.h"

using ad_utility::VocabularyType;
using ad_utility::vocabulary::placeholderForMissingVocabIndex;
using ::testing::HasSubstr;

namespace {

// The (sorted) words of the source vocabularies that are used below. The regex
// in the tests always excludes those words that contain `drop`. Note that the
// regexes are matched against the complete entry (via `RE2::FullMatch`), hence
// the leading `<.*` and trailing `>` in `dropRegex`.
const std::vector<std::string> sourceWords{"<alpha-keep>", "<beta-drop>",
                                           "<delta-drop>", "<epsilon-keep>",
                                           "<gamma-keep>"};

// A regex that fully matches exactly those of the `sourceWords` that contain
// `drop`.
const std::string dropRegex = "<.*-drop>";

// The names of the files that `buildFilteredVocabulary` creates for a filtered
// vocabulary of the given `type` with the given `basename`, see the
// implementation of `buildFilteredVocabulary`. Used below to check that they
// are all deleted again.
std::vector<std::string> temporaryFilenames(VocabularyType type,
                                            const std::string& basename) {
  if (type == VocabularyType::InMemoryCompressedWithHoles) {
    return {absl::StrCat(basename, ".words"),
            absl::StrCat(basename, ".words.ids"),
            absl::StrCat(basename, ".codebooks")};
  }
  return {basename, absl::StrCat(basename, ".ids")};
}

// Delete all files in the current working directory whose name starts with
// `prefix`. Used to clean up the files of a source vocabulary, the exact
// suffixes of which depend on the vocabulary type.
void deleteFilesWithPrefix(const std::string& prefix) {
  for (const auto& entry : ql::filesystem::directory_iterator{"."}) {
    if (ql::starts_with(entry.path().filename().string(), prefix)) {
      ad_utility::deleteFile(entry.path(), false);
    }
  }
}

// Write `sourceWords` to a vocabulary of the given `type` at `filename` and
// open it into `vocabulary`.
void setupSourceVocabulary(PolymorphicVocabulary& vocabulary,
                           VocabularyType type, const std::string& filename) {
  auto writerPtr = PolymorphicVocabulary::makeDiskWriterPtr(filename, type);
  for (const std::string& word : sourceWords) {
    (*writerPtr)(word, false);
  }
  writerPtr->finish();
  vocabulary.open(filename, type);
}

// Check that `vocabulary` contains exactly those of the `sourceWords` that do
// not contain `drop`, under their original indices, and that the indices of the
// dropped words resolve to the placeholder.
void checkFilteredVocabulary(const PolymorphicVocabulary& vocabulary) {
  EXPECT_EQ(vocabulary.size(), 3);
  for (size_t i = 0; i < sourceWords.size(); ++i) {
    const std::string& word = sourceWords.at(i);
    if (absl::StrContains(word, "drop")) {
      EXPECT_EQ(vocabulary[i], placeholderForMissingVocabIndex(i));
    } else {
      EXPECT_EQ(vocabulary[i], word);
    }
  }
}
}  // namespace

// _____________________________________________________________________________
// Test the filtering for each of the vocabulary types that a regular index can
// have (except the geo-split vocabulary, see below): the surviving words keep
// their original indices, an uncompressed source yields an uncompressed result
// and a compressed source a compressed one, and all temporary files are deleted
// again.
TEST(BuildFilteredVocabulary, filterAllSupportedVocabularyTypes) {
  for (const auto& [sourceType, expectedType] :
       std::vector<std::pair<VocabularyType, VocabularyType>>{
           {VocabularyType::InMemoryUncompressed,
            VocabularyType::InMemoryUncompressedWithHoles},
           {VocabularyType::OnDiskUncompressed,
            VocabularyType::InMemoryUncompressedWithHoles},
           {VocabularyType::InMemoryCompressed,
            VocabularyType::InMemoryCompressedWithHoles},
           {VocabularyType::OnDiskCompressed,
            VocabularyType::InMemoryCompressedWithHoles}}) {
    std::string basename =
        absl::StrCat(gtestCurrentTestName(), ".", sourceType.toString());
    std::string sourceFilename = absl::StrCat(basename, ".source");
    std::string temporaryBasename = absl::StrCat(basename, ".filtered");
    absl::Cleanup cleanup = [&basename]() { deleteFilesWithPrefix(basename); };

    PolymorphicVocabulary source;
    setupSourceVocabulary(source, sourceType, sourceFilename);

    auto filtered =
        buildFilteredVocabulary(source, {dropRegex}, temporaryBasename);
    EXPECT_EQ(filtered.type_, expectedType);
    checkFilteredVocabulary(filtered.vocabulary_);

    // The intermediate on-disk representation is deleted again.
    for (const std::string& filename :
         temporaryFilenames(expectedType, temporaryBasename)) {
      EXPECT_FALSE(ql::filesystem::exists(filename)) << filename;
    }
  }
}

// _____________________________________________________________________________
// Test that only the entries that match none of the regexes survive, i.e. that
// several regexes are all applied, and that each of them is matched against the
// complete entry (and not only against a part of it).
TEST(BuildFilteredVocabulary, severalRegexesAreMatchedAgainstTheCompleteEntry) {
  std::string basename = gtestCurrentTestName();
  std::string sourceFilename = absl::StrCat(basename, ".source");
  absl::Cleanup cleanup = [&basename]() { deleteFilesWithPrefix(basename); };

  PolymorphicVocabulary source;
  setupSourceVocabulary(source, VocabularyType::InMemoryUncompressed,
                        sourceFilename);

  // The first regex fully matches `<alpha-keep>` and `<gamma-keep>`, the second
  // one fully matches `<beta-drop>`.
  auto filtered = buildFilteredVocabulary(source, {"<.*a-keep>", "<beta-drop>"},
                                          absl::StrCat(basename, ".filtered"));
  EXPECT_EQ(filtered.vocabulary_.size(), 2);
  EXPECT_EQ(filtered.vocabulary_[2], "<delta-drop>");
  EXPECT_EQ(filtered.vocabulary_[3], "<epsilon-keep>");
  EXPECT_EQ(filtered.vocabulary_[0], placeholderForMissingVocabIndex(0));
  EXPECT_EQ(filtered.vocabulary_[1], placeholderForMissingVocabIndex(1));
  EXPECT_EQ(filtered.vocabulary_[4], placeholderForMissingVocabIndex(4));
}

// _____________________________________________________________________________
// Test that a regex which matches only a part of an entry excludes nothing,
// because the regexes are matched against the complete entry.
TEST(BuildFilteredVocabulary,
     regexThatMatchesOnlyPartOfAnEntryExcludesNothing) {
  std::string basename = gtestCurrentTestName();
  std::string sourceFilename = absl::StrCat(basename, ".source");
  absl::Cleanup cleanup = [&basename]() { deleteFilesWithPrefix(basename); };

  PolymorphicVocabulary source;
  setupSourceVocabulary(source, VocabularyType::InMemoryUncompressed,
                        sourceFilename);

  // `drop` occurs in two of the `sourceWords`, but matches none of them
  // completely, so all of the entries survive with their original indices.
  auto filtered = buildFilteredVocabulary(source, {"drop"},
                                          absl::StrCat(basename, ".filtered"));
  EXPECT_EQ(filtered.vocabulary_.size(), sourceWords.size());
  for (size_t i = 0; i < sourceWords.size(); ++i) {
    EXPECT_EQ(filtered.vocabulary_[i], sourceWords.at(i));
  }
}

// _____________________________________________________________________________
// Test that filtering a vocabulary of type `on-disk-compressed-geo-split` is
// rejected with a descriptive message. Note that the type of the result is
// determined before anything is written, so an empty vocabulary suffices here.
TEST(BuildFilteredVocabulary, filterGeoSplitVocabularyThrows) {
  PolymorphicVocabulary vocabulary;
  vocabulary.resetToType(VocabularyType::OnDiskCompressedGeoSplit);
  AD_EXPECT_THROW_WITH_MESSAGE(
      buildFilteredVocabulary(vocabulary, {dropRegex}, gtestCurrentTestName()),
      HasSubstr("on-disk-compressed-geo-split"));
}

// _____________________________________________________________________________
// Test that filtering a vocabulary that already has holes is rejected with a
// descriptive message.
TEST(BuildFilteredVocabulary, filterVocabularyWithHolesThrows) {
  for (VocabularyType type : {VocabularyType::InMemoryUncompressedWithHoles,
                              VocabularyType::InMemoryCompressedWithHoles}) {
    PolymorphicVocabulary vocabulary;
    vocabulary.resetToType(type);
    AD_EXPECT_THROW_WITH_MESSAGE(
        buildFilteredVocabulary(vocabulary, {dropRegex},
                                gtestCurrentTestName()),
        HasSubstr("already is a vocabulary with holes"));
  }
}

// _____________________________________________________________________________
// Test that an empty list of regexes (which the caller has to handle itself)
// and a regex that is not a valid regular expression are both rejected.
TEST(BuildFilteredVocabulary, invalidArguments) {
  PolymorphicVocabulary vocabulary;
  vocabulary.resetToType(VocabularyType::InMemoryUncompressed);
  EXPECT_ANY_THROW(
      buildFilteredVocabulary(vocabulary, {}, gtestCurrentTestName()));
  AD_EXPECT_THROW_WITH_MESSAGE(
      buildFilteredVocabulary(vocabulary, {"valid", "invalid("},
                              gtestCurrentTestName()),
      HasSubstr("is not a valid regular expression"));
}
