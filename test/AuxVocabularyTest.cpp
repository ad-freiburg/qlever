// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "global/Constants.h"
#include "index/AuxVocabulary.h"
#include "util/BitUtils.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"
#include "util/HashSet.h"

namespace {

using ad_utility::VocabularyType;

// A WKT literal, which the `SplitGeoVocabulary` stores in its second
// sub-vocabulary.
std::string wkt(std::string_view content) {
  return absl::StrCat("\"", content, "\"^^<", GEO_WKT_LITERAL, ">");
}

// A main vocabulary of the given `type` that holds `words`, together with the
// files that back it (which are removed when this object dies).
class MainVocabulary {
 private:
  std::string filename_;
  RdfsVocabulary vocabulary_;

 public:
  MainVocabulary(std::string filename,
                 const ad_utility::HashSet<std::string>& words,
                 VocabularyType type)
      : filename_{std::move(filename)} {
    vocabulary_.resetToType(type);
    vocabulary_.createFromSet(words, filename_);
  }
  ~MainVocabulary() {
    for (const auto& file :
         qlever::util::filesWithBaseNameAndSuffix(filename_, "")) {
      ad_utility::deleteFile(file);
    }
  }
  MainVocabulary(const MainVocabulary&) = delete;
  MainVocabulary& operator=(const MainVocabulary&) = delete;

  const RdfsVocabulary& vocab() const { return vocabulary_; }
};

// Write an `AuxVocabulary` with the given `words` (which have to be pushed in
// the order in which they are given, see `AuxVocabulary::WordWriter`) and read
// it back.
void writeAndOpen(AuxVocabulary& auxVocab, const RdfsVocabulary& mainVocab,
                  const std::string& basename,
                  const std::vector<std::string>& words, VocabularyType type) {
  {
    AuxVocabulary::WordWriter writer{mainVocab, basename, type};
    for (const auto& word : words) {
      writer(word);
    }
    writer.finish();
  }
  auxVocab.open(basename, type, "en", "US", false);
}

// Remove all files that belong to the auxiliary vocabulary with the given base
// name.
void deleteAuxVocabFiles(const std::string& basename) {
  for (const auto& file :
       qlever::util::filesWithBaseNameAndSuffix(basename, "")) {
    ad_utility::deleteFile(file);
  }
}

// ____________________________________________________________________________
TEST(AuxVocabulary, positionsAndLookupWithoutSplit) {
  auto type = VocabularyType{VocabularyType::OnDiskCompressed};
  std::string mainFilename = absl::StrCat(gtestCurrentTestName(), ".main");
  MainVocabulary main{mainFilename, {"\"a\"", "\"c\"", "\"e\""}, type};

  std::string basename = absl::StrCat(gtestCurrentTestName(), ".aux");
  absl::Cleanup cleanup = [&basename] { deleteAuxVocabFiles(basename); };
  AuxVocabulary auxVocab;
  // `"b"` is sorted between `"a"` and `"c"`, `"d"` between `"c"` and `"e"`, and
  // `"f"` after all words of the main vocabulary.
  writeAndOpen(auxVocab, main.vocab(), basename, {"\"b\"", "\"d\"", "\"f\""},
               type);

  ASSERT_EQ(auxVocab.numWords(), 3);
  EXPECT_EQ(auxVocab[AuxVocabIndex::make(0)], "\"b\"");
  EXPECT_EQ(auxVocab[AuxVocabIndex::make(1)], "\"d\"");
  EXPECT_EQ(auxVocab[AuxVocabIndex::make(2)], "\"f\"");

  // Without a split, the `Id`s of the auxiliary vocabulary are dense, so they
  // agree with their offsets.
  for (uint64_t i = 0; i < 3; ++i) {
    EXPECT_EQ(auxVocab.offsetOf(AuxVocabIndex::make(i)), i);
  }

  // The positions are the indices of the first greater word of the main
  // vocabulary: `"b"` before `"c"` (index 1), `"d"` before `"e"` (index 2), and
  // `"f"` after the last word (index 3).
  auto position = [&auxVocab](uint64_t i) {
    return Id::fromBits(auxVocab.positionInMainVocab(AuxVocabIndex::make(i)))
        .getVocabIndex()
        .get();
  };
  EXPECT_EQ(position(0), 1);
  EXPECT_EQ(position(1), 2);
  EXPECT_EQ(position(2), 3);

  // The reverse direction: how many words of the auxiliary vocabulary are
  // smaller than a given word of the main vocabulary.
  auto numSmaller = [&auxVocab](uint64_t mainIndex) {
    return auxVocab.numWordsSmallerThanMainVocabPosition(
        Id::makeFromVocabIndex(VocabIndex::make(mainIndex)));
  };
  EXPECT_EQ(numSmaller(0), 0);  // "a"
  EXPECT_EQ(numSmaller(1), 1);  // "c", greater than "b"
  EXPECT_EQ(numSmaller(2), 2);  // "e", greater than "b" and "d"

  // Lookup by word.
  EXPECT_EQ(auxVocab.getId("\"d\""), AuxVocabIndex::make(1));
  EXPECT_EQ(auxVocab.getId("\"a\""), std::nullopt);
  EXPECT_EQ(auxVocab.numWordsSmallerThan("\"a\""), 0);
  EXPECT_EQ(auxVocab.numWordsSmallerThan("\"c\""), 1);
  EXPECT_EQ(auxVocab.numWordsSmallerThan("\"g\""), 3);
}

// ____________________________________________________________________________
TEST(AuxVocabulary, positionsAndLookupWithGeoSplit) {
  auto type = VocabularyType{VocabularyType::OnDiskCompressedGeoSplit};
  std::string mainFilename = absl::StrCat(gtestCurrentTestName(), ".main");
  // The main vocabulary holds two ordinary literals and two WKT literals, the
  // latter in its second sub-vocabulary.
  MainVocabulary main{mainFilename,
                      {"\"a\"", "\"c\"", wkt("POINT(1 1)"), wkt("POINT(3 3)")},
                      type};

  std::string basename = absl::StrCat(gtestCurrentTestName(), ".aux");
  absl::Cleanup cleanup = [&basename] { deleteAuxVocabFiles(basename); };
  AuxVocabulary auxVocab;
  // The words have to be pushed grouped by sub-vocabulary, the ordinary
  // literals (marker 0) first, the WKT literals (marker 1) afterwards.
  writeAndOpen(auxVocab, main.vocab(), basename,
               {"\"b\"", "\"d\"", wkt("POINT(2 2)"), wkt("POINT(4 4)")}, type);

  ASSERT_EQ(auxVocab.numWords(), 4);

  // The `Id`s of the second sub-vocabulary carry the marker bit, so they are
  // *not* dense, but their offsets are.
  auto markerBit = uint64_t{1} << 59;
  EXPECT_EQ(auxVocab[AuxVocabIndex::make(0)], "\"b\"");
  EXPECT_EQ(auxVocab[AuxVocabIndex::make(1)], "\"d\"");
  EXPECT_EQ(auxVocab[AuxVocabIndex::make(markerBit)], wkt("POINT(2 2)"));
  EXPECT_EQ(auxVocab[AuxVocabIndex::make(markerBit | 1)], wkt("POINT(4 4)"));

  EXPECT_EQ(auxVocab.offsetOf(AuxVocabIndex::make(0)), 0);
  EXPECT_EQ(auxVocab.offsetOf(AuxVocabIndex::make(1)), 1);
  EXPECT_EQ(auxVocab.offsetOf(AuxVocabIndex::make(markerBit)), 2);
  EXPECT_EQ(auxVocab.offsetOf(AuxVocabIndex::make(markerBit | 1)), 3);

  // Each word is positioned in the sub-vocabulary of the main vocabulary that
  // it belongs to, so the WKT literals get positions that carry the marker bit,
  // which is what keeps the positions ascending.
  auto position = [&auxVocab](uint64_t i) {
    return auxVocab.positionInMainVocab(AuxVocabIndex::make(i)) &
           ad_utility::bitMaskForLowerBits(ValueId::numDataBits);
  };
  EXPECT_EQ(position(0), 1);  // `"b"` before `"c"`
  EXPECT_EQ(position(1), 2);  // `"d"` after all ordinary literals
  EXPECT_EQ(position(markerBit), markerBit | 1);      // before `POINT(3 3)`
  EXPECT_EQ(position(markerBit | 1), markerBit | 2);  // after all geometries

  // The reverse direction has to respect the sub-vocabularies as well: all
  // words of the first sub-vocabulary are smaller than every word of the second
  // one.
  auto numSmaller = [&auxVocab](uint64_t mainIndex) {
    return auxVocab.numWordsSmallerThanMainVocabPosition(
        Id::makeFromVocabIndex(VocabIndex::make(mainIndex)));
  };
  EXPECT_EQ(numSmaller(0), 0);              // `"a"`
  EXPECT_EQ(numSmaller(1), 1);              // `"c"`, greater than `"b"`
  EXPECT_EQ(numSmaller(markerBit), 2);      // `POINT(1 1)`
  EXPECT_EQ(numSmaller(markerBit | 1), 3);  // `POINT(3 3)`

  // Lookup by word routes to the correct sub-vocabulary.
  EXPECT_EQ(auxVocab.getId(wkt("POINT(4 4)")),
            AuxVocabIndex::make(markerBit | 1));
  EXPECT_EQ(auxVocab.getId(wkt("POINT(1 1)")), std::nullopt);
  EXPECT_EQ(auxVocab.getId("\"b\""), AuxVocabIndex::make(0));
  EXPECT_EQ(auxVocab.numWordsSmallerThan(wkt("POINT(0 0)")), 2);
  EXPECT_EQ(auxVocab.numWordsSmallerThan(wkt("POINT(3 3)")), 3);
  EXPECT_EQ(auxVocab.numWordsSmallerThan("\"c\""), 1);

  // The precomputed geometry information of the WKT literals is available, just
  // as it is for the main vocabulary.
  EXPECT_TRUE(auxVocab.vocab().isGeoInfoAvailable());
  EXPECT_TRUE(
      auxVocab.vocab().getGeoInfo(AuxVocabIndex::make(markerBit)).has_value());
}

// ____________________________________________________________________________
TEST(AuxVocabulary, semanticComparisonAndRangesWithGeoSplit) {
  using namespace valueIdComparators;
  auto type = VocabularyType{VocabularyType::OnDiskCompressedGeoSplit};
  std::string mainFilename = absl::StrCat(gtestCurrentTestName(), ".main");
  MainVocabulary main{mainFilename,
                      {"\"a\"", "\"c\"", wkt("POINT(1 1)"), wkt("POINT(3 3)")},
                      type};

  std::string basename = absl::StrCat(gtestCurrentTestName(), ".aux");
  absl::Cleanup cleanup = [&basename] { deleteAuxVocabFiles(basename); };
  AuxVocabulary auxVocab;
  writeAndOpen(auxVocab, main.vocab(), basename,
               {"\"b\"", "\"d\"", wkt("POINT(2 2)"), wkt("POINT(4 4)")}, type);
  auto ordering = auxVocab.ordering();

  auto markerBit = uint64_t{1} << 59;
  auto mainId = [](uint64_t index) {
    return Id::makeFromVocabIndex(VocabIndex::make(index));
  };
  auto auxId = [](uint64_t index) {
    return Id::makeFromAuxVocabIndex(AuxVocabIndex::make(index));
  };
  // The merged order of the two vocabularies is
  // "a" < "b" < "c" < "d" < POINT(1 1) < POINT(2 2) < POINT(3 3) < POINT(4 4),
  // where the quoted literals come from the first and the WKT literals from the
  // second sub-vocabulary.
  std::vector<Id> semanticOrder{mainId(0),
                                auxId(0),
                                mainId(1),
                                auxId(1),
                                mainId(markerBit),
                                auxId(markerBit),
                                mainId(markerBit | 1),
                                auxId(markerBit | 1)};
  for (size_t i = 0; i < semanticOrder.size(); ++i) {
    for (size_t j = 0; j < semanticOrder.size(); ++j) {
      auto expected = i < j ? ql::strong_ordering::less
                            : (i > j ? ql::strong_ordering::greater
                                     : ql::strong_ordering::equal);
      EXPECT_EQ(compareStringIdsSemantically(semanticOrder[i], semanticOrder[j],
                                             ordering),
                expected)
          << i << " vs " << j;
    }
  }

  // The `Id`s of the auxiliary vocabulary are all greater than those of the
  // main vocabulary when compared bitwise, which is the order in which the
  // index scans emit them and in which the range filters expect their input.
  std::vector<Id> bitwiseOrder{
      mainId(0), mainId(1), mainId(markerBit), mainId(markerBit | 1),
      auxId(0),  auxId(1),  auxId(markerBit),  auxId(markerBit | 1)};
  ASSERT_TRUE(ql::ranges::is_sorted(bitwiseOrder, compareByBits));

  // Turn the ranges of iterators that the range filters return into index
  // pairs.
  auto rangesFor = [&bitwiseOrder, &ordering](Id referenceId,
                                              Comparison comparison) {
    std::vector<std::pair<size_t, size_t>> result;
    for (auto [begin, end] :
         getRangesForId(bitwiseOrder.begin(), bitwiseOrder.end(), referenceId,
                        comparison, ordering)) {
      result.emplace_back(begin - bitwiseOrder.begin(),
                          end - bitwiseOrder.begin());
    }
    return result;
  };
  using Ranges = std::vector<std::pair<size_t, size_t>>;

  // Everything smaller than `"c"`: `"a"` from the main and `"b"` from the
  // auxiliary vocabulary, which are in two disjoint ranges.
  EXPECT_THAT(rangesFor(mainId(1), Comparison::LT),
              ::testing::Eq(Ranges{{0, 1}, {4, 5}}));
  // Everything smaller than `POINT(1 1)`: all words of the first
  // sub-vocabularies, no matter what they are.
  EXPECT_THAT(rangesFor(mainId(markerBit), Comparison::LT),
              ::testing::Eq(Ranges{{0, 2}, {4, 6}}));
  // Everything greater than `POINT(2 2)`, which is a word of the auxiliary
  // vocabulary.
  EXPECT_THAT(rangesFor(auxId(markerBit), Comparison::GT),
              ::testing::Eq(Ranges{{3, 4}, {7, 8}}));
  // A word is only equal to itself, because the two vocabularies are disjoint.
  EXPECT_THAT(rangesFor(auxId(markerBit), Comparison::EQ),
              ::testing::Eq(Ranges{{6, 7}}));
  EXPECT_THAT(rangesFor(mainId(1), Comparison::EQ),
              ::testing::Eq(Ranges{{1, 2}}));
}

// ____________________________________________________________________________
TEST(AuxVocabulary, wordWriterChecksPreconditions) {
  auto type = VocabularyType{VocabularyType::OnDiskCompressed};
  std::string mainFilename = absl::StrCat(gtestCurrentTestName(), ".main");
  MainVocabulary main{mainFilename, {"\"a\"", "\"c\""}, type};

  std::string basename = absl::StrCat(gtestCurrentTestName(), ".aux");
  absl::Cleanup cleanup = [&basename] { deleteAuxVocabFiles(basename); };

  // A word that is contained in the main vocabulary must not be added.
  {
    AuxVocabulary::WordWriter writer{main.vocab(), basename, type};
    AD_EXPECT_THROW_WITH_MESSAGE(
        writer("\"a\""),
        ::testing::HasSubstr("contained in the vocabulary of the main index"));
  }
  // The words have to be strictly ascending.
  {
    AuxVocabulary::WordWriter writer{main.vocab(), basename, type};
    writer("\"d\"");
    AD_EXPECT_THROW_WITH_MESSAGE(
        writer("\"b\""), ::testing::HasSubstr("strictly ascending order"));
  }
}

// ____________________________________________________________________________
TEST(AuxVocabulary, geoSplitWordWriterRequiresGroupedWords) {
  auto type = VocabularyType{VocabularyType::OnDiskCompressedGeoSplit};
  std::string mainFilename = absl::StrCat(gtestCurrentTestName(), ".main");
  MainVocabulary main{mainFilename, {"\"a\""}, type};

  std::string basename = absl::StrCat(gtestCurrentTestName(), ".aux");
  absl::Cleanup cleanup = [&basename] { deleteAuxVocabFiles(basename); };

  // Once a word of the second sub-vocabulary has been pushed, no word of the
  // first one may follow.
  AuxVocabulary::WordWriter writer{main.vocab(), basename, type};
  writer(wkt("POINT(1 1)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      writer("\"b\""), ::testing::HasSubstr("ascending order of the markers"));
}

}  // namespace
