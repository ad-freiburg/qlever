// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "../util/GTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "engine/MaterializedViews.h"
#include "global/Id.h"
#include "global/MaterializedViewConstants.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/IndexFormatConverter.h"
#include "index/IndexFormatVersion.h"
#include "index/IndexImpl.h"
#include "util/BitUtils.h"
#include "util/CancellationHandle.h"
#include "util/json.h"

namespace {

namespace fs = ql::filesystem;
using namespace qlever::indexFormatConverter;
using ::testing::HasSubstr;
using ::testing::UnorderedElementsAreArray;

// The base name of the index in the previous index format that the tests below
// convert, and the directory in which its files are checked in (see the
// `README.md` there for how that index was created).
constexpr std::string_view oldIndexBasename = "oldFormat";
fs::path oldIndexDirectory() {
  return fs::path{QLEVER_TEST_DATA_DIR} / "oldIndexFormat";
}

// Return the `Id` of the previous index format with the given datatype bits and
// value bits.
Id oldFormatId(uint64_t datatypeBits, uint64_t valueBits) {
  return Id::fromBits((datatypeBits << ValueId::numDataBits) | valueBits);
}

// Return the value bits of the given `id`, that is, its bits without the
// datatype bits.
uint64_t valueBits(Id id) {
  return id.getBits() & ad_utility::bitMaskForLowerBits(ValueId::numDataBits);
}

// The 20 triples of the index in the previous format, in the order of the `SPO`
// permutation, in the string representation of `tripleToString` below. Note
// that the subjects are ordered by their `Id` (first the words of the
// vocabulary, then the blank nodes, then the encoded IRIs), not by their string
// value.
const std::vector<std::string>& expectedTriples() {
  static const std::vector<std::string> triples{
      R"triple(<http://example.org/s1> <http://example.org/bool> "true"^^<http://www.w3.org/2001/XMLSchema#boolean>)triple",
      R"triple(<http://example.org/s1> <http://example.org/date> "2020-05-17"^^<http://www.w3.org/2001/XMLSchema#date>)triple",
      R"triple(<http://example.org/s1> <http://example.org/dateTime> "2021-07-01T12:34:56"^^<http://www.w3.org/2001/XMLSchema#dateTime>)triple",
      R"triple(<http://example.org/s1> <http://example.org/double> "3.5"^^<http://www.w3.org/2001/XMLSchema#decimal>)triple",
      R"triple(<http://example.org/s1> <http://example.org/geometry> "POINT(2.294481 48.858370)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s1> <http://example.org/int> "42"^^<http://www.w3.org/2001/XMLSchema#int>)triple",
      R"triple(<http://example.org/s1> <http://example.org/label> "alpha")triple",
      R"triple(<http://example.org/s1> <http://example.org/label> "beta"@en)triple",
      R"triple(<http://example.org/s1> <http://example.org/related> <https://example.org/id/123>)triple",
      R"triple(<http://example.org/s2> <http://example.org/connector> _:bn0)triple",
      R"triple(<http://example.org/s2> <http://example.org/geometry> "POINT(7.835000 47.999000)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s2> <http://example.org/label> "gamma delta epsilon")triple",
      R"triple(<http://example.org/s3> <http://example.org/double> "-0.125"^^<http://www.w3.org/2001/XMLSchema#decimal>)triple",
      R"triple(<http://example.org/s3> <http://example.org/int> "1000000"^^<http://www.w3.org/2001/XMLSchema#int>)triple",
      R"triple(<http://example.org/s3> <http://example.org/label> "zeta")triple",
      R"triple(_:bn0 <http://example.org/label> "blank one")triple",
      R"triple(_:bn0 <http://example.org/related> <http://example.org/s1>)triple",
      R"triple(<https://example.org/id/123> <http://example.org/label> "encoded one")triple",
      R"triple(<https://example.org/id/123> <http://example.org/related> <https://example.org/id/456>)triple",
      R"triple(<https://example.org/id/456> <http://example.org/int> "-17"^^<http://www.w3.org/2001/XMLSchema#int>)triple"};
  return triples;
}

// A fixture that copies the index in the previous format (see
// `oldIndexDirectory` above) into a fresh directory, so that the tests can
// convert it without modifying the checked-in files.
class IndexFormatConverterTest : public ::testing::Test {
 protected:
  // The directory of this test, which contains both the index in the previous
  // format and the converted one.
  fs::path directory_;
  // The base names of the index in the previous format and of the converted
  // index.
  std::string oldBasename_;
  std::string newBasename_;

  void SetUp() override {
    directory_ = fs::path{gtestCurrentTestName()};
    fs::remove_all(directory_);
    fs::create_directories(directory_ / "old");
    oldBasename_ = (directory_ / "old" / oldIndexBasename).string();
    newBasename_ = (directory_ / "converted" / oldIndexBasename).string();
    // Copy the files of the index, but not the input files and the script that
    // it was created from.
    for (const auto& entry : fs::directory_iterator{oldIndexDirectory()}) {
      std::string filename = entry.path().filename().string();
      if (!ql::starts_with(filename, absl::StrCat(oldIndexBasename, "."))) {
        continue;
      }
      fs::copy_file(
          entry.path(),
          absl::StrCat(oldBasename_, std::string_view{filename}.substr(
                                         oldIndexBasename.size())));
    }
  }

  void TearDown() override { fs::remove_all(directory_); }

  // Load the converted index and return it, together with its located triples
  // (which every scan needs, and which are empty).
  std::pair<Index, LocatedTriplesSharedState> loadConvertedIndex() {
    Index index{ad_utility::makeUnlimitedAllocator<Id>()};
    index.usePatterns() = true;
    index.loadAllPermutations() = true;
    index.createFromOnDiskIndex(newBasename_, false);
    index.addTextFromOnDiskIndex();
    auto locatedTriples =
        index.deltaTriplesManager().getCurrentLocatedTriplesSharedState();
    return {std::move(index), std::move(locatedTriples)};
  }

  // Return all triples of the given `permutation` of the `index`, with their
  // columns in the order in which they are stored (so `PSO` yields
  // predicate-subject-object).
  static std::vector<std::array<Id, 3>> scanAllTriples(
      const Index& index, Permutation::Enum permutationEnum,
      const LocatedTriplesSharedState& locatedTriples) {
    const auto& permutation = index.getImpl().getPermutation(permutationEnum);
    IdTable table = permutation.scan(
        permutation.getScanSpecAndBlocks(
            ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
            *locatedTriples),
        {}, std::make_shared<ad_utility::CancellationHandle<>>(),
        *locatedTriples);
    AD_CORRECTNESS_CHECK(table.numColumns() == 3);
    std::vector<std::array<Id, 3>> triples;
    for (const auto& row : table) {
      triples.push_back({row[0], row[1], row[2]});
    }
    return triples;
  }
};

// _____________________________________________________________________________
TEST(IndexFormatConverter, convertIdOfEachDatatype) {
  // The datatype of each `Id` of the previous format, in the order of the
  // datatype bits that it had there. The value bits always stay the same.
  std::vector<Datatype> expectedDatatypes{Datatype::Undefined,
                                          Datatype::Bool,
                                          Datatype::Int,
                                          Datatype::Double,
                                          Datatype::VocabIndex,
                                          Datatype::LocalVocabIndex,
                                          Datatype::TextRecordIndex,
                                          Datatype::Date,
                                          Datatype::GeoPoint,
                                          Datatype::WordVocabIndex,
                                          Datatype::BlankNodeIndex,
                                          Datatype::EncodedVal};
  for (uint64_t datatypeBits = 0; datatypeBits < expectedDatatypes.size();
       ++datatypeBits) {
    SCOPED_TRACE(absl::StrCat("datatype bits ", datatypeBits));
    auto expectedDatatype = expectedDatatypes.at(datatypeBits);
    // An `Id` of type `LocalVocabIndex` is never stored on disk, so it always
    // is an error, see the test below.
    if (expectedDatatype == Datatype::LocalVocabIndex) {
      continue;
    }
    for (uint64_t value : {uint64_t{0}, uint64_t{17}, ValueId::maxIndex}) {
      Id converted = convertId(oldFormatId(datatypeBits, value));
      EXPECT_EQ(converted.getDatatype(), expectedDatatype);
      EXPECT_EQ(valueBits(converted), value);
    }
  }
}

// _____________________________________________________________________________
TEST(IndexFormatConverter, convertIdPreservesTheOrder) {
  // The conversion of a permutation relies on the order of the `Id`s being
  // preserved, so that the converted permutation is still sorted.
  std::vector<Id> convertedIds;
  for (uint64_t datatypeBits = 0; datatypeBits < 12; ++datatypeBits) {
    if (datatypeBits == static_cast<uint64_t>(Datatype::LocalVocabIndex)) {
      continue;
    }
    for (uint64_t value : {uint64_t{0}, uint64_t{17}}) {
      convertedIds.push_back(convertId(oldFormatId(datatypeBits, value)));
    }
  }
  EXPECT_TRUE(ql::ranges::is_sorted(convertedIds));
  EXPECT_TRUE(ql::ranges::adjacent_find(convertedIds) == convertedIds.end());
}

// _____________________________________________________________________________
TEST(IndexFormatConverter, convertIdOfInvalidId) {
  // The datatype bits of an `Id` of type `LocalVocabIndex` are a pointer, which
  // is meaningless in a different process, so such an `Id` must never be stored
  // on disk.
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertId(
          oldFormatId(static_cast<uint64_t>(Datatype::LocalVocabIndex), 17)),
      HasSubstr("must never be stored on disk"));
  // The previous format had 12 datatypes, so the four remaining values of the
  // datatype bits are invalid.
  for (uint64_t datatypeBits = 12; datatypeBits < 16; ++datatypeBits) {
    AD_EXPECT_THROW_WITH_MESSAGE(convertId(oldFormatId(datatypeBits, 17)),
                                 HasSubstr("invalid datatype"));
  }
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, convertedIndexHasTheSameContent) {
  convertIndexToCurrentFormat(oldBasename_, newBasename_);

  // The converted index has the current index format, and everything else in
  // its configuration is unchanged.
  nlohmann::json oldConfiguration;
  ad_utility::makeIfstream(absl::StrCat(oldBasename_, CONFIGURATION_FILE)) >>
      oldConfiguration;
  nlohmann::json newConfiguration;
  ad_utility::makeIfstream(absl::StrCat(newBasename_, CONFIGURATION_FILE)) >>
      newConfiguration;
  EXPECT_EQ(newConfiguration.at("index-format-version")
                .get<qlever::IndexFormatVersion>(),
            qlever::indexFormatVersion);
  oldConfiguration.erase("index-format-version");
  newConfiguration.erase("index-format-version");
  EXPECT_EQ(oldConfiguration, newConfiguration);

  auto [index, locatedTriples] = loadConvertedIndex();

  // The `SPO` permutation contains exactly the expected triples, and none of
  // their `Id`s was mangled by the conversion (all of them can be exported
  // again).
  auto tripleToString = [&index = index](const std::array<Id, 3>& triple) {
    std::vector<std::string> components;
    for (Id id : triple) {
      // A blank node has no representation as a `LiteralOrIri`, so it is the
      // one datatype that has to be handled separately here.
      if (id.getDatatype() == Datatype::BlankNodeIndex) {
        components.push_back(
            absl::StrCat("_:bn", id.getBlankNodeIndex().get()));
        continue;
      }
      LocalVocab emptyLocalVocab{};
      auto word =
          ql::exportIds::idToLiteralOrIri(index.getImpl(), id, emptyLocalVocab);
      components.push_back(word.has_value()
                               ? word.value().toStringRepresentation()
                               : "NOT EXPORTABLE");
    }
    return absl::StrJoin(components, " ");
  };
  auto spoTriples = scanAllTriples(index, Permutation::SPO, locatedTriples);
  std::vector<std::string> spoTriplesAsStrings;
  ql::ranges::transform(spoTriples, std::back_inserter(spoTriplesAsStrings),
                        tripleToString);
  EXPECT_THAT(spoTriplesAsStrings,
              ::testing::ElementsAreArray(expectedTriples()));

  // All datatypes that the index contains are converted, so that the test above
  // is not accidentally weakened by an index that has, say, no `Date`s at all.
  ad_utility::HashSet<Datatype> datatypes;
  for (const auto& triple : spoTriples) {
    for (Id id : triple) {
      datatypes.insert(id.getDatatype());
    }
  }
  EXPECT_THAT(datatypes,
              UnorderedElementsAreArray(std::vector<Datatype>{
                  Datatype::VocabIndex, Datatype::BlankNodeIndex,
                  Datatype::EncodedVal, Datatype::Int, Datatype::Double,
                  Datatype::Bool, Datatype::Date, Datatype::GeoPoint}));

  // All permutations are still sorted (the conversion does not change the order
  // of the `Id`s), and they all contain the same set of triples.
  auto sortedTriplesOfPermutation =
      [&index = index,
       &locatedTriples = locatedTriples](Permutation::Enum permutationEnum) {
        auto triples = scanAllTriples(index, permutationEnum, locatedTriples);
        EXPECT_TRUE(ql::ranges::is_sorted(triples))
            << Permutation::toString(permutationEnum);
        // Undo the permutation of the columns, so that the triples of all
        // permutations can be compared to each other.
        auto keyOrder = Permutation::toKeyOrder(permutationEnum).keys();
        for (auto& triple : triples) {
          std::array<Id, 3> inSpoOrder{};
          for (size_t i = 0; i < 3; ++i) {
            inSpoOrder.at(keyOrder.at(i)) = triple.at(i);
          }
          triple = inSpoOrder;
        }
        ql::ranges::sort(triples);
        return triples;
      };
  auto expectedSortedTriples = sortedTriplesOfPermutation(Permutation::SPO);
  EXPECT_EQ(expectedSortedTriples.size(), expectedTriples().size());
  for (auto permutationEnum : Permutation::ALL) {
    EXPECT_EQ(sortedTriplesOfPermutation(permutationEnum),
              expectedSortedTriples)
        << Permutation::toString(permutationEnum);
  }
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, convertedIndexHasPatternsAndTextIndex) {
  convertIndexToCurrentFormat(oldBasename_, newBasename_);
  auto [index, locatedTriples] = loadConvertedIndex();

  // The patterns (the sets of predicates of the subjects) are converted. Note
  // that the patterns of the two subjects that have only one predicate are the
  // same pattern, which is stored only once.
  std::vector<std::vector<std::string>> patterns;
  for (const auto& pattern : index.getPatterns()) {
    std::vector<std::string> predicates;
    for (Id id : pattern) {
      ASSERT_EQ(id.getDatatype(), Datatype::VocabIndex);
      predicates.push_back(std::string{index.getVocab()[id.getVocabIndex()]});
    }
    patterns.push_back(std::move(predicates));
  }
  EXPECT_THAT(
      patterns,
      UnorderedElementsAreArray(std::vector<std::vector<std::string>>{
          {"<http://example.org/label>", "<http://example.org/related>"},
          {"<http://example.org/bool>", "<http://example.org/date>",
           "<http://example.org/dateTime>", "<http://example.org/double>",
           "<http://example.org/geometry>", "<http://example.org/int>",
           "<http://example.org/label>", "<http://example.org/related>"},
          {"<http://example.org/connector>", "<http://example.org/geometry>",
           "<http://example.org/label>"},
          {"<http://example.org/double>", "<http://example.org/int>",
           "<http://example.org/label>"},
          {"<http://example.org/int>"}}));

  // The text index needs no conversion at all (it stores plain integers and
  // reconstructs its `Id`s when it is read), but it is copied, so the converted
  // index still has it.
  EXPECT_EQ(index.getImpl().getTextExcerpt(TextRecordIndex::make(0)),
            "A text record about alpha and s1.");
  EXPECT_EQ(index.getImpl().getTextExcerpt(TextRecordIndex::make(1)),
            "A text record about gamma delta and s2.");
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, convertedMaterializedView) {
  // The version of the on-disk format of the materialized views was raised
  // together with the index format, so the view of the index in the previous
  // format cannot be loaded.
  AD_EXPECT_THROW_WITH_MESSAGE(MaterializedView(oldBasename_, "testview"),
                               HasSubstr("saved with format version 1"));

  convertIndexToCurrentFormat(oldBasename_, newBasename_);

  // The converted view can be loaded, which also checks its version, its
  // columns, and its query, and it contains all its rows.
  MaterializedView view{newBasename_, "testview"};
  EXPECT_EQ(view.permutation()->metaData().totalElements(), 6);
  EXPECT_THAT(view.originalQuery(),
              ::testing::Optional(HasSubstr("<http://example.org/label>")));
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, refusesToConvertIndexTwice) {
  convertIndexToCurrentFormat(oldBasename_, newBasename_);
  // The converted index already is in the current format.
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(newBasename_,
                                  (directory_ / "again").string()),
      HasSubstr("already is in the current index format"));
  // The files of the converted index must not be overwritten.
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, newBasename_),
      HasSubstr("must not overwrite"));
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, refusesToConvertUnsuitableIndexes) {
  // The base names have to differ, else the conversion would overwrite the
  // index that it reads.
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, oldBasename_),
      HasSubstr("has to differ"));

  // An index that does not exist at all.
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat((directory_ / "doesNotExist").string(),
                                  newBasename_),
      HasSubstr("is not the base name of a QLever index"));

  // An index with an index format that is neither the previous nor the current
  // one.
  std::string configurationFilename =
      absl::StrCat(oldBasename_, CONFIGURATION_FILE);
  nlohmann::json configuration;
  ad_utility::makeIfstream(configurationFilename) >> configuration;
  auto restoreConfiguration = [&configurationFilename, configuration]() {
    ad_utility::makeOfstream(configurationFilename) << configuration.dump();
  };
  {
    auto modifiedConfiguration = configuration;
    modifiedConfiguration["index-format-version"]["pull-request-number"] = 1;
    ad_utility::makeOfstream(configurationFilename)
        << modifiedConfiguration.dump();
    AD_EXPECT_THROW_WITH_MESSAGE(
        convertIndexToCurrentFormat(oldBasename_, newBasename_),
        HasSubstr("Please rebuild the index"));
  }
  {
    auto modifiedConfiguration = configuration;
    modifiedConfiguration.erase("index-format-version");
    ad_utility::makeOfstream(configurationFilename)
        << modifiedConfiguration.dump();
    AD_EXPECT_THROW_WITH_MESSAGE(
        convertIndexToCurrentFormat(oldBasename_, newBasename_),
        HasSubstr("before versioning was introduced"));
  }
  restoreConfiguration();

  // An index with persisted updates, which contain `Id`s that this converter
  // deliberately does not convert.
  std::string updatesFilename =
      absl::StrCat(oldBasename_, UPDATE_TRIPLES_SUFFIX);
  ad_utility::makeOfstream(updatesFilename) << "irrelevant content";
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, newBasename_),
      HasSubstr("has persisted updates"));
  ad_utility::deleteFile(updatesFilename);

  // An index that has only one of the two permutations of a pair.
  ad_utility::deleteFile(absl::StrCat(oldBasename_, ".index.sop"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, newBasename_),
      HasSubstr("has only one of the permutations"));
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, refusesToConvertUnsuitableMaterializedViews) {
  std::string viewBasename =
      materializedViewFilenameBase(oldBasename_, "testview");

  // A view that is not in the format version that belongs to the source format
  // of the converter.
  std::string viewInfoFilename = absl::StrCat(viewBasename, VIEW_INFO_SUFFIX);
  nlohmann::json viewInfo;
  ad_utility::makeIfstream(viewInfoFilename) >> viewInfo;
  auto originalViewInfo = viewInfo;
  viewInfo["version"] = MATERIALIZED_VIEWS_VERSION;
  ad_utility::makeOfstream(viewInfoFilename) << viewInfo.dump();
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, newBasename_),
      HasSubstr("only converts views in the format version 1"));
  ad_utility::makeOfstream(viewInfoFilename) << originalViewInfo.dump();

  // A view of which only some of its files exist. Note that the file that is
  // deleted here must not be the info file, because the views are enumerated by
  // exactly those files.
  ad_utility::deleteFile(absl::StrCat(viewBasename, VIEW_SPO_SUFFIX));
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_,
                                  (directory_ / "incompleteView").string()),
      HasSubstr("files that belong to a materialized view"));
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, emptyBasenamesAreARequirementViolation) {
  AD_EXPECT_THROW_WITH_MESSAGE(convertIndexToCurrentFormat("", newBasename_),
                               HasSubstr("must not be empty"));
  AD_EXPECT_THROW_WITH_MESSAGE(convertIndexToCurrentFormat(oldBasename_, ""),
                               HasSubstr("must not be empty"));
}

// _____________________________________________________________________________
TEST(IndexFormatConverter, supportedFormatsAreUpToDate) {
  // The converter hardcodes the two index formats that it converts between, so
  // that it cannot silently be applied to a different change of the index
  // format. Those two formats have to be the current index format and the one
  // that directly precedes it (see the note at `qlever::indexFormatVersion`).
  EXPECT_EQ(targetVersion, qlever::indexFormatVersion);
  EXPECT_EQ(sourceVersion, qlever::previousIndexFormatVersion);
  EXPECT_NE(sourceVersion, targetVersion);
}

}  // namespace
