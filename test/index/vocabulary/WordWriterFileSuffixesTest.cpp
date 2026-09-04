// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <filesystem>
#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyType.h"

namespace {

using ad_utility::VocabularyType;
using CompressedVocabularyWithHoles =
    CompressedVocabulary<VocabularyInMemoryBinSearch>;

// The words that are written by the tests below. They are sorted, and one of
// them is a WKT literal, such that for a `SplitGeoVocabulary` each of the
// underlying vocabularies actually gets at least one word.
const std::vector<std::string>& testWords() {
  static const std::vector<std::string> words{
      "\"POINT(1 2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      "\"alpha\"", "\"beta\"", "<http://example.org/x>"};
  return words;
}

// A fresh empty directory, which is deleted (with everything in it) when the
// object goes out of scope. Each of the tests below writes a single vocabulary
// into such a directory, so that the files that were created by that
// vocabulary can be determined by simply listing the directory.
class EmptyDirectory {
 private:
  std::string directory_;

 public:
  explicit EmptyDirectory(std::string directory)
      : directory_{std::move(directory)} {
    std::filesystem::remove_all(directory_);
    std::filesystem::create_directories(directory_);
  }
  EmptyDirectory(const EmptyDirectory&) = delete;
  EmptyDirectory& operator=(const EmptyDirectory&) = delete;
  ~EmptyDirectory() { std::filesystem::remove_all(directory_); }

  // The base filename for the vocabulary that is written into this directory.
  std::string baseFilename() const {
    return absl::StrCat(directory_, "/vocabulary");
  }

  // The names of all the files in this directory.
  std::vector<std::string> filenames() const {
    std::vector<std::string> filenames;
    for (const auto& entry : std::filesystem::directory_iterator{directory_}) {
      filenames.push_back(entry.path().string());
    }
    return filenames;
  }
};

// Copy the suffixes of the given `wordWriter` into a `vector`. This is
// necessary because the `span` that `fileSuffixes` returns is only valid for as
// long as the `wordWriter` lives.
template <typename WordWriter>
std::vector<std::string> copySuffixes(const WordWriter& wordWriter) {
  std::vector<std::string> suffixes;
  for (std::string_view suffix : wordWriter.fileSuffixes()) {
    suffixes.emplace_back(suffix);
  }
  return suffixes;
}

// Check that the `filenames` are exactly the `baseFilename` plus each of the
// `suffixes`. `UnorderedElementsAreArray` also detects duplicate suffixes,
// because then there would be fewer files than suffixes.
void expectFilesAreBaseFilenamePlusSuffixes(
    const std::vector<std::string>& filenames, const std::string& baseFilename,
    const std::vector<std::string>& suffixes,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  std::vector<std::string> expectedFiles;
  for (const std::string& suffix : suffixes) {
    expectedFiles.push_back(absl::StrCat(baseFilename, suffix));
  }
  EXPECT_THAT(filenames, ::testing::UnorderedElementsAreArray(expectedFiles));
}

// Write the `testWords()` with a `WordWriter` for a vocabulary of the given
// `type` into a fresh empty directory, and check that the suffixes that the
// `WordWriter` announces via `fileSuffixes` are the `expectedSuffixes`, and
// that the files that were actually created are exactly the base filename plus
// those suffixes.
void testFileSuffixes(
    VocabularyType::Enum type, const std::vector<std::string>& expectedSuffixes,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc, VocabularyType{type}.toString());
  EmptyDirectory directory{absl::StrCat(
      gtestCurrentTestName(), ".", VocabularyType{type}.toString(), ".dir")};
  std::string filename = directory.baseFilename();
  std::vector<std::string> suffixes;
  {
    auto writer = PolymorphicVocabulary::makeDiskWriterPtr(
        filename, VocabularyType{type});
    suffixes = copySuffixes(*writer);
    for (const std::string& word : testWords()) {
      (*writer)(word, true);
    }
    writer->finish();
  }
  EXPECT_THAT(suffixes, ::testing::ElementsAreArray(expectedSuffixes));
  expectFilesAreBaseFilenamePlusSuffixes(directory.filenames(), filename,
                                         suffixes);
}

}  // namespace

// _____________________________________________________________________________
// For each of the vocabulary types that can be used for index building, the
// files that its `WordWriter` creates are exactly the ones that it announces
// via `fileSuffixes`. The expected suffixes are also pinned down explicitly,
// such that a change to any of them (which would make previously built indices
// unreadable) is noticed.
TEST(WordWriterFileSuffixes, allVocabularyTypesForIndexBuilding) {
  using Enum = VocabularyType::Enum;
  testFileSuffixes(Enum::InMemoryUncompressed, {""});
  // NOTE: The internal vocabulary of a `VocabularyInternalExternal` is a
  // `VocabularyInMemoryBinSearch`, which needs an additional file for the
  // indices of its words (`.ids`), because it only stores every n-th word.
  testFileSuffixes(
      Enum::OnDiskUncompressed,
      {".internal", ".internal.ids", ".external", ".external.offsets"});
  testFileSuffixes(Enum::InMemoryCompressed, {".words", ".codebooks"});
  testFileSuffixes(Enum::OnDiskCompressed,
                   {".words.internal", ".words.internal.ids", ".words.external",
                    ".words.external.offsets", ".codebooks"});
  // The geo split vocabulary consists of the (on-disk compressed) main
  // vocabulary, plus a `GeoVocabulary` under the `.geometry` prefix (which is
  // another on-disk compressed vocabulary plus the file with the geometry
  // information).
  testFileSuffixes(
      Enum::OnDiskCompressedGeoSplit,
      {".words.internal", ".words.internal.ids", ".words.external",
       ".words.external.offsets", ".codebooks", ".geometry.words.internal",
       ".geometry.words.internal.ids", ".geometry.words.external",
       ".geometry.words.external.offsets", ".geometry.codebooks",
       ".geometry.geoinfo"});

  // All the vocabulary types that can be used for index building are covered
  // by the checks above.
  EXPECT_EQ(VocabularyType::allForIndexBuilding_.size(), 5u);
}

// _____________________________________________________________________________
// The `WordWriter`s of the vocabularies with holes do not inherit from
// `WordWriterBase` (they require explicit indices, which that interface cannot
// express), but they mirror its `fileSuffixes` interface, so test them
// separately.
TEST(WordWriterFileSuffixes, vocabulariesWithHoles) {
  // The indices deliberately have holes.
  const std::vector<uint64_t> indices{3, 7, 8, 20};
  ASSERT_EQ(indices.size(), testWords().size());

  {
    EmptyDirectory directory{
        absl::StrCat(gtestCurrentTestName(), ".binSearch.dir")};
    std::string filename = directory.baseFilename();
    std::vector<std::string> suffixes;
    {
      VocabularyInMemoryBinSearch::WordWriter writer{filename};
      suffixes = copySuffixes(writer);
      for (size_t i = 0; i < testWords().size(); ++i) {
        writer(testWords().at(i), indices.at(i));
      }
      writer.finish();
    }
    EXPECT_THAT(suffixes, ::testing::ElementsAre("", ".ids"));
    expectFilesAreBaseFilenamePlusSuffixes(directory.filenames(), filename,
                                           suffixes);
  }

  {
    EmptyDirectory directory{
        absl::StrCat(gtestCurrentTestName(), ".compressed.dir")};
    std::string filename = directory.baseFilename();
    std::vector<std::string> suffixes;
    {
      CompressedVocabularyWithHoles::WordWriter writer{filename};
      suffixes = copySuffixes(writer);
      for (size_t i = 0; i < testWords().size(); ++i) {
        writer(testWords().at(i), indices.at(i));
      }
      writer.finish();
    }
    EXPECT_THAT(suffixes,
                ::testing::ElementsAre(".words", ".words.ids", ".codebooks"));
    expectFilesAreBaseFilenamePlusSuffixes(directory.filenames(), filename,
                                           suffixes);
  }
}
