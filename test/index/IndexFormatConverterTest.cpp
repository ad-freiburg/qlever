// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/base/casts.h>
#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/MaterializedViewConstants.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExportIds.h"
#include "index/ExternalSortFunctors.h"
#include "index/Index.h"
#include "index/IndexFormatConverter.h"
#include "index/IndexFormatVersion.h"
#include "index/IndexImpl.h"
#include "index/vocabulary/VocabularyType.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/BitUtils.h"
#include "util/CancellationHandle.h"
#include "util/FilesystemHelpers.h"
#include "util/ProgressBar.h"
#include "util/json.h"

namespace {

namespace fs = ql::filesystem;
using namespace qlever::indexFormatConverter;
using namespace ad_utility::memory_literals;
using ::testing::HasSubstr;
using ::testing::UnorderedElementsAreArray;

// The base name of the index in the previous index format that the tests below
// convert, and the directory in which its files are checked in (see the
// `README.md` there for how that index was created).
constexpr std::string_view oldIndexBasename = "oldFormat";
fs::path oldIndexDirectory() {
  return fs::path{QLEVER_TEST_DATA_DIR} / "oldIndexFormat";
}

// The bit representation of the point with the given coordinates in the
// previous index format: the quantized latitude in the upper and the quantized
// longitude in the lower 30 bits.
GeoPoint::T oldFormatBits(double lat, double lng) {
  return (GeoPoint::quantizeCoordinate(lat, COORDINATE_LAT_MAX)
          << GeoPoint::numDataBitsCoordinate) |
         GeoPoint::quantizeCoordinate(lng, COORDINATE_LNG_MAX);
}

// The `Id` of the point with the given coordinates in the previous format.
Id oldFormatPointId(double lat, double lng) {
  return Id::makeFromGeoPointBits(oldFormatBits(lat, lng));
}

// The points of the checked-in index (see `input.ttl` there), as (lat, lng).
const std::vector<std::pair<double, double>>& pointsOfOldIndex() {
  static const std::vector<std::pair<double, double>> points{
      {48.858370, 2.294481}, {47.999, 7.835}, {48.0, 7.85},   {37.8, -122.4},
      {35.7, 139.7},         {-33.9, 151.2},  {-22.9, -43.2}, {52.5, 13.4}};
  return points;
}

// The triples of the checked-in index in the string representation of
// `tripleToString` below, in no particular order.
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
      R"triple(<http://example.org/s4> <http://example.org/geometry> "POINT(7.850000 48.000000)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s4> <http://example.org/geometry> "POINT(-122.400000 37.800000)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s5> <http://example.org/geometry> "POINT(2.294481 48.858370)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s5> <http://example.org/centroid> "POINT(139.700000 35.700000)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s6> <http://example.org/geometry> "POINT(151.200000 -33.900000)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s6> <http://example.org/geometry> "POINT(-43.200000 -22.900000)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s6> <http://example.org/centroid> "POINT(13.400000 52.500000)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s7> <http://example.org/geometry> "LINESTRING(7.8 48.0, 7.9 48.1, 8.0 48.0)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s8> <http://example.org/geometry> "POLYGON((-73.99 40.75, -73.98 40.75, -73.98 40.76, -73.99 40.76, -73.99 40.75))"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(<http://example.org/s9> <http://example.org/geometry> "INVALID(1 2)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>)triple",
      R"triple(_:bn0 <http://example.org/label> "blank one")triple",
      R"triple(_:bn0 <http://example.org/related> <http://example.org/s1>)triple",
      R"triple(<https://example.org/id/123> <http://example.org/label> "encoded one")triple",
      R"triple(<https://example.org/id/123> <http://example.org/related> <https://example.org/id/456>)triple",
      R"triple(<https://example.org/id/456> <http://example.org/int> "-17"^^<http://www.w3.org/2001/XMLSchema#int>)triple"};
  return triples;
}

// The WKT literals of the checked-in index that are stored in its geometry
// vocabulary (with quotes and datatype, as in the vocabulary): a linestring, a
// polygon, and an invalid one.
constexpr std::string_view wktDatatype =
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
const std::string linestringLiteral = absl::StrCat(
    R"lit("LINESTRING(7.8 48.0, 7.9 48.1, 8.0 48.0)")lit", wktDatatype);
const std::string polygonLiteral = absl::StrCat(
    R"lit("POLYGON((-73.99 40.75, -73.98 40.75, -73.98 40.76, -73.99 40.76, -73.99 40.75))")lit",
    wktDatatype);
const std::string invalidWktLiteral =
    absl::StrCat(R"lit("INVALID(1 2)")lit", wktDatatype);

// Check that the precomputed `GeometryInfo` that `index` has for the WKT
// `literal` (with quotes and datatype) is the one that the current code
// computes for that literal, that is, its encoded points are in the current
// format. For a literal that is not a valid geometry, both are empty.
void expectGeoInfoIsInCurrentFormat(const Index& index,
                                    const std::string& literal) {
  Id id = ad_utility::testing::makeGetId(index)(literal);
  ASSERT_EQ(id.getDatatype(), Datatype::VocabIndex);
  auto stored = index.getVocab().getGeoInfo(id.getVocabIndex());
  auto expected = ad_utility::GeometryInfo::fromWktLiteral(literal);
  ASSERT_EQ(stored.has_value(), expected.has_value()) << literal;
  if (!expected.has_value()) {
    return;
  }
  EXPECT_EQ(stored->getBoundingBox().pair(), expected->getBoundingBox().pair())
      << literal;
  EXPECT_EQ(stored->getCentroid().centroid(),
            expected->getCentroid().centroid())
      << literal;
  EXPECT_EQ(stored->getWktType(), expected->getWktType()) << literal;
  EXPECT_EQ(stored->getMetricLength().length(),
            expected->getMetricLength().length())
      << literal;
}

// The comparator by which the rows of a permutation are sorted (its first
// four columns, see `sortRunsOfGeoPoints`).
SortByColumns keyComparator() { return SortByColumns{{0, 1, 2, 3}}; }

// Return all triples of the given `permutation` of the `index`, with their
// columns in the order in which they are stored (so `PSO` yields
// predicate-subject-object).
std::vector<std::array<Id, 3>> scanAllTriples(
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

  // Convert the index with the log redirected, and return the log.
  std::string convertAndReturnLog() {
    auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();
    convertIndexToCurrentFormat(oldBasename_, newBasename_);
    return logStream.str();
  }
};

// _____________________________________________________________________________
TEST(IndexFormatConverter, convertGeoPointBits) {
  // The conversion of the bits of a point yields exactly the bits that the
  // current code computes for that point.
  for (const auto& [lat, lng] : pointsOfOldIndex()) {
    EXPECT_EQ(convertGeoPointBits(oldFormatBits(lat, lng)),
              (GeoPoint{lat, lng}.toBitRepresentation()));
    EXPECT_EQ(convertId(oldFormatPointId(lat, lng)),
              Id::makeFromGeoPoint(GeoPoint{lat, lng}));
  }
  // The extreme values.
  EXPECT_EQ(convertGeoPointBits(0), 0u);
  EXPECT_EQ(convertGeoPointBits(oldFormatBits(90, 180)),
            ad_utility::bitMaskForLowerBits(GeoPoint::numDataBits));
}

// _____________________________________________________________________________
TEST(IndexFormatConverter, convertIdOfOtherDatatypes) {
  // The conversion is the identity for every datatype but `GeoPoint`, and
  // throws for a `LocalVocabIndex`, which must never be stored on disk.
  std::vector<Id> ids{Id::makeUndefined(),
                      Id::makeFromBool(true),
                      Id::makeFromInt(-17),
                      Id::makeFromDouble(3.5),
                      Id::makeFromVocabIndex(VocabIndex::make(42)),
                      Id::makeFromTextRecordIndex(TextRecordIndex::make(1)),
                      Id::makeFromWordVocabIndex(WordVocabIndex::make(7)),
                      Id::makeFromBlankNodeIndex(BlankNodeIndex::make(3))};
  for (Id id : ids) {
    EXPECT_EQ(convertId(id), id) << id;
    EXPECT_FALSE(isGeoPoint(id));
  }
  EXPECT_TRUE(isGeoPoint(oldFormatPointId(1.0, 2.0)));
  Id localVocabId =
      Id::fromBits((static_cast<uint64_t>(Datatype::LocalVocabIndex)
                    << ValueId::numDataBits) |
                   17);
  AD_EXPECT_THROW_WITH_MESSAGE(convertId(localVocabId),
                               HasSubstr("must never be stored on disk"));
}

// _____________________________________________________________________________
TEST(IndexFormatConverter, convertIdChangesOnlyTheOrderOfPoints) {
  // The order of the points of the checked-in index differs between the two
  // formats (this is what requires the re-sorting in the first place), while
  // the order of any `Id`s of different datatypes is preserved.
  std::vector<Id> oldIds;
  for (const auto& [lat, lng] : pointsOfOldIndex()) {
    oldIds.push_back(oldFormatPointId(lat, lng));
  }
  ql::ranges::sort(oldIds);
  std::vector<Id> newIds;
  ql::ranges::transform(oldIds, std::back_inserter(newIds), &convertId);
  EXPECT_FALSE(ql::ranges::is_sorted(newIds));

  std::vector<Id> mixedIds{Id::makeFromInt(5),
                           Id::makeFromDouble(1.0),
                           Id::makeFromVocabIndex(VocabIndex::make(0)),
                           oldFormatPointId(-90, -180),
                           oldFormatPointId(90, 180),
                           Id::makeFromBlankNodeIndex(BlankNodeIndex::make(0))};
  ASSERT_TRUE(ql::ranges::is_sorted(mixedIds));
  std::vector<Id> mixedConverted;
  ql::ranges::transform(mixedIds, std::back_inserter(mixedConverted),
                        &convertId);
  EXPECT_TRUE(ql::ranges::is_sorted(mixedConverted));
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, convertedIndexHasTheSameContent) {
  std::string log = convertAndReturnLog();
  EXPECT_THAT(log, HasSubstr("Converting 8 permutations"));
  EXPECT_THAT(log, ::testing::Not(HasSubstr("needs no conversion")));

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
  // again). In particular, the points have the coordinates of the input.
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
              UnorderedElementsAreArray(expectedTriples()));

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

  // All permutations are sorted (which the re-sorting of the points has to
  // restore), and they all contain the same set of triples.
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
  // that subjects with the same set of predicates share one pattern, which is
  // stored only once.
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
          {"<http://example.org/int>"},
          {"<http://example.org/geometry>"},
          {"<http://example.org/centroid>", "<http://example.org/geometry>"}}));

  // The text index needs no conversion at all (it stores plain integers and
  // reconstructs its `Id`s when it is read), but it is copied, so the converted
  // index still has it.
  EXPECT_EQ(index.getImpl().getTextExcerpt(TextRecordIndex::make(0)),
            "A text record about alpha and s1.");
  EXPECT_EQ(index.getImpl().getTextExcerpt(TextRecordIndex::make(1)),
            "A text record about gamma delta and s2.");
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, materializedViewsAreNotConverted) {
  // The index has a materialized view, which the conversion does not convert:
  // the converted index has no view files, and a warning names the view.
  ASSERT_FALSE(
      qlever::util::filesWithBaseNameAndSuffix(oldBasename_, VIEW_FILE_INFIX)
          .empty());
  std::string log = convertAndReturnLog();
  EXPECT_THAT(log, HasSubstr("materialized view(s), which this converter does "
                             "not convert: testview"));
  EXPECT_TRUE(
      qlever::util::filesWithBaseNameAndSuffix(newBasename_, VIEW_FILE_INFIX)
          .empty());
  // The old index still has its view.
  ASSERT_FALSE(
      qlever::util::filesWithBaseNameAndSuffix(oldBasename_, VIEW_FILE_INFIX)
          .empty());
}

// _____________________________________________________________________________
// The in-place upgrade stages the upgraded index in an
// `index-in-new-format.<datetime>.tmp` subdirectory, checks it, and only then
// retires the index in the old format to `index-in-old-format.<datetime of
// its build>` and moves the upgraded index to the base name of the old one.
TEST_F(IndexFormatConverterTest, upgradeIndexInPlace) {
  // Remember the files of the index in the old format for the retirement
  // check below.
  fs::path oldDirectory = fs::path{oldBasename_}.parent_path();
  std::vector<std::string> filesBefore;
  for (const auto& entry : fs::directory_iterator{oldDirectory}) {
    filesBefore.push_back(entry.path().filename().string());
  }

  std::string log;
  {
    auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();
    upgradeIndexInPlace(oldBasename_);
    log = logStream.str();
  }
  EXPECT_THAT(log, HasSubstr("The upgrade was successful"));
  EXPECT_THAT(log, HasSubstr("The materialized view(s) testview were not "
                             "converted, but moved to that directory"));

  // The upgraded index is at the base name the old index lived at, is in the
  // current format, and can be loaded with all of its triples. Its content is
  // that of `convertIndexToCurrentFormat` (which the upgrade calls, and which
  // the tests above check in detail).
  newBasename_ = oldBasename_;
  nlohmann::json configuration;
  ad_utility::makeIfstream(absl::StrCat(newBasename_, CONFIGURATION_FILE)) >>
      configuration;
  EXPECT_EQ(configuration.at("index-format-version")
                .get<qlever::IndexFormatVersion>(),
            qlever::indexFormatVersion);
  auto [index, locatedTriples] = loadConvertedIndex();
  EXPECT_EQ(scanAllTriples(index, Permutation::SPO, locatedTriples).size(),
            expectedTriples().size());

  // The index in the old format was retired to
  // `index-in-old-format.<datetime>` with all of its files (including its
  // materialized view, which the upgraded index does not have), and the
  // staging directory was removed again.
  auto retiredDirs = qlever::util::directoriesWithPrefix(
      oldDirectory, std::string{retiredDirPrefix});
  ASSERT_EQ(retiredDirs.size(), 1u);
  std::vector<std::string> retiredFiles;
  for (const auto& entry : fs::directory_iterator{retiredDirs.front()}) {
    retiredFiles.push_back(entry.path().filename().string());
  }
  EXPECT_THAT(retiredFiles, ::testing::UnorderedElementsAreArray(filesBefore));
  EXPECT_TRUE(qlever::util::directoriesWithPrefix(oldDirectory,
                                                  std::string{stagingDirPrefix})
                  .empty());
  EXPECT_TRUE(
      qlever::util::filesWithBaseNameAndSuffix(newBasename_, VIEW_FILE_INFIX)
          .empty());

  // A second upgrade refuses, because the index already is in the current
  // format.
  AD_EXPECT_THROW_WITH_MESSAGE(
      upgradeIndexInPlace(oldBasename_),
      HasSubstr("already is in the current index format"));
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
  // deliberately does not convert. Both the conversion and the in-place
  // upgrade refuse.
  std::string updatesFilename =
      absl::StrCat(oldBasename_, UPDATE_TRIPLES_SUFFIX);
  ad_utility::makeOfstream(updatesFilename) << "irrelevant content";
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, newBasename_),
      HasSubstr("has persisted updates"));
  AD_EXPECT_THROW_WITH_MESSAGE(upgradeIndexInPlace(oldBasename_),
                               HasSubstr("has persisted updates"));
  EXPECT_FALSE(indexNeedsNoConversion(oldBasename_));
  ad_utility::deleteFile(updatesFilename);

  // An index that has only one of the two permutations of a pair.
  ad_utility::deleteFile(absl::StrCat(oldBasename_, ".index.sop"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, newBasename_),
      HasSubstr("has only one of the permutations"));
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, emptyBasenamesAreARequirementViolation) {
  AD_EXPECT_THROW_WITH_MESSAGE(convertIndexToCurrentFormat("", newBasename_),
                               HasSubstr("must not be empty"));
  AD_EXPECT_THROW_WITH_MESSAGE(convertIndexToCurrentFormat(oldBasename_, ""),
                               HasSubstr("must not be empty"));
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, equalBasenamesAreAUserFacingError) {
  // The comparison normalizes the paths, so also a spelled-differently base
  // name of the same index is caught.
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, oldBasename_),
      HasSubstr("has to differ from the base name"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_,
                                  absl::StrCat("./", oldBasename_)),
      HasSubstr("has to differ from the base name"));
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, convertedGeometryInfoIsInCurrentFormat) {
  // The geometry information of the linestring and the polygon (their bounding
  // boxes and centroids, which are encoded points) is converted, and the
  // invalid geometry stays invalid. Before the conversion, the stored bounding
  // boxes are not even valid when read with the current code.
  convertIndexToCurrentFormat(oldBasename_, newBasename_);
  auto [index, locatedTriples] = loadConvertedIndex();
  for (const auto& literal :
       {linestringLiteral, polygonLiteral, invalidWktLiteral}) {
    expectGeoInfoIsInCurrentFormat(index, literal);
  }
  // The linestring runs from (48.0, 7.8) to (48.1, 8.0).
  auto id = ad_utility::testing::makeGetId(index)(linestringLiteral);
  auto boundingBox =
      index.getVocab().getGeoInfo(id.getVocabIndex()).value().getBoundingBox();
  EXPECT_NEAR(boundingBox.lowerLeft().getLat(), 48.0, 1e-5);
  EXPECT_NEAR(boundingBox.lowerLeft().getLng(), 7.8, 1e-5);
  EXPECT_NEAR(boundingBox.upperRight().getLat(), 48.1, 1e-5);
  EXPECT_NEAR(boundingBox.upperRight().getLng(), 8.0, 1e-5);
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, refusesToConvertWithoutEnoughFreeSpace) {
  // The conversion writes a second copy of the index, so it refuses to start
  // when the filesystem has less free space than that copy needs, and warns
  // when the space may not suffice for the external sorting on top of it.
  uint64_t bytesOfIndex = 0;
  for (const auto& file : IndexImpl::allIndexFiles(oldBasename_)) {
    bytesOfIndex += fs::file_size(file);
  }
  absl::Cleanup restoreFreeSpace = []() {
    freeSpaceForTesting() = std::nullopt;
  };

  freeSpaceForTesting() = bytesOfIndex - 1;
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, newBasename_),
      ::testing::AllOf(HasSubstr("writes a second copy of the index"),
                       HasSubstr("Please free up space")));

  // With enough space for the copy, but not for the sorting on top of it, the
  // conversion runs and only warns.
  freeSpaceForTesting() = bytesOfIndex;
  std::string log = convertAndReturnLog();
  EXPECT_THAT(log, HasSubstr("may run out of space"));
  EXPECT_THAT(log, HasSubstr("Conversion of the index completed"));
}

// _____________________________________________________________________________
TEST_F(IndexFormatConverterTest, checkedInIndexContainsGeoPoints) {
  // The checked-in index has geometries in its geometry vocabulary, whose
  // information holds encoded points, and it has points in its permutations,
  // so it needs the conversion in any case.
  EXPECT_TRUE(indexContainsGeoPoints(oldBasename_));
  EXPECT_FALSE(indexNeedsNoConversion(oldBasename_));
  // Without the geometry vocabulary, the points in the permutations decide.
  // This is determined from `OSP` if the index has it, and from `POS`
  // otherwise.
  ad_utility::deleteFile(
      absl::StrCat(oldBasename_, ".vocabulary.geometry.geoinfo"));
  EXPECT_TRUE(indexContainsGeoPoints(oldBasename_));
  for (auto suffix : {".index.osp", ".index.ops", ".index.spo", ".index.sop"}) {
    ad_utility::deleteFile(absl::StrCat(oldBasename_, suffix));
    ad_utility::deleteFile(
        absl::StrCat(oldBasename_, suffix, META_FILE_SUFFIX));
  }
  EXPECT_TRUE(indexContainsGeoPoints(oldBasename_));
  // Without any permutations, the question cannot be decided, and the answer
  // is the conservative one.
  ad_utility::deleteFile(absl::StrCat(oldBasename_, ".index.pos"));
  EXPECT_TRUE(indexContainsGeoPoints(oldBasename_));
  EXPECT_TRUE(indexContainsGeoPoints((directory_ / "doesNotExist").string()));
}

// A fixture for the conversion of indexes with properties that the checked-in
// index in the previous format (see `oldIndexDirectory` above) does not have:
// permutations with more than one block (in particular, runs of points that
// span several blocks), empty permutations, an index without points, and a
// relation that is large enough to have a metadata entry of its own. That
// index has exactly one block per permutation and only tiny relations, and it
// cannot be changed, because the current code can no longer create an index in
// that format. The tests below therefore build an index with the *current*
// index builder and pretend that it is in the previous format.
//
// For an index without points, the conversion is the identity, so the
// converted index has to have exactly the same content. For an index with
// points, the conversion rearranges the bits of each point once more (which
// yields points with other coordinates, but that is irrelevant here), so the
// converted index has to contain exactly the rows of the index that was
// converted with `convertId` applied to every `Id`, sorted. That checks the
// whole pipeline of the conversion on permutations with many blocks (the
// scan, the conversion of the `Id`s, the re-sorting of the runs, and the
// writing), see `convertAndExpectTheConvertedContent` below.
class MultiBlockIndexFormatConverterTest : public ::testing::Test {
 protected:
  // The directory of this test, which contains both the index that is converted
  // and the converted index.
  fs::path directory_;
  // The base names of the index that is converted and of the converted index.
  std::string oldBasename_;
  std::string newBasename_;

  void SetUp() override {
    directory_ = fs::path{gtestCurrentTestName()};
    fs::remove_all(directory_);
    fs::create_directories(directory_);
    oldBasename_ = (directory_ / "old").string();
    newBasename_ = (directory_ / "converted").string();
  }

  void TearDown() override { fs::remove_all(directory_); }

  // Return the number of columns that the given `permutation` has on disk (see
  // `getNumColumns` in `IndexFormatConverter.cpp`).
  static size_t numColumnsOnDisk(const Permutation& permutation) {
    const auto& blocks = permutation.metaData().blockData();
    AD_CORRECTNESS_CHECK(!blocks.empty());
    return blocks.front().offsetsAndCompressedSize_.value().size();
  }

  // Return the complete content of the given `permutation`: the three columns
  // of the (permuted) triple, the graph column, and, for the permutations that
  // store the patterns, the two pattern columns.
  static IdTable scanAllColumns(
      const Permutation& permutation,
      const LocatedTriplesSharedState& locatedTriples) {
    std::vector<ColumnIndex> additionalColumns;
    for (size_t column = NumColumnsIndexBuilding - 1;
         column < numColumnsOnDisk(permutation); ++column) {
      additionalColumns.push_back(static_cast<ColumnIndex>(column));
    }
    return permutation.scan(
        permutation.getScanSpecAndBlocks(
            ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
            *locatedTriples),
        additionalColumns, std::make_shared<ad_utility::CancellationHandle<>>(),
        *locatedTriples);
  }

  // Return the content that the conversion of a permutation with the given
  // `content` has to produce: every `Id` converted, and the rows sorted.
  static IdTable expectedConvertedContent(const IdTable& content) {
    IdTable expected = content.clone();
    for (auto column : expected.getColumns()) {
      ql::ranges::for_each(column, [](Id& id) { id = convertId(id); });
    }
    ql::ranges::sort(expected, keyComparator());
    return expected;
  }

  // Set the index format version in the configuration of the index at
  // `oldBasename_` to the source format of the converter, so that the converter
  // accepts that index. Nothing else in the index has to be changed, see the
  // documentation of this fixture.
  void pretendThatTheIndexIsInThePreviousFormat() {
    setFormatVersionOfOldIndex(sourceVersion);
  }

  // Set the index format version in the configuration of the index at
  // `oldBasename_` to `version`.
  void setFormatVersionOfOldIndex(const qlever::IndexFormatVersion& version) {
    std::string filename = absl::StrCat(oldBasename_, CONFIGURATION_FILE);
    nlohmann::json configuration;
    ad_utility::makeIfstream(filename) >> configuration;
    configuration["index-format-version"] = version;
    ad_utility::makeOfstream(filename) << configuration.dump(4);
  }

  // Load the converted index at `newBasename_` and return it. With
  // `allPermutations` set to `false`, the index has only the `PSO` and `POS`
  // permutations and no patterns.
  Index loadConvertedIndex(bool allPermutations = true) const {
    Index index{ad_utility::makeUnlimitedAllocator<Id>()};
    index.usePatterns() = allPermutations;
    index.loadAllPermutations() = allPermutations;
    index.createFromOnDiskIndex(newBasename_, false);
    return index;
  }

  // The permutations of an index that was built with all permutations resp.
  // with only `PSO` and `POS` (`--only-pso-and-pos-permutations`).
  static std::vector<Permutation::Enum> permutationsOfIndex(
      bool allPermutations) {
    if (allPermutations) {
      return std::vector<Permutation::Enum>(Permutation::ALL);
    }
    return {Permutation::PSO, Permutation::POS};
  }

  // Build an index from the given `turtleInput` with the settings for tests
  // (which use a block size of two triples per block, so that even a small
  // index has many blocks), pretend that it is in the previous format, and
  // convert it. Check that the converted index has exactly the content that
  // the conversion has to produce (see the documentation of this fixture), and
  // return the number of blocks that each permutation of the index that was
  // converted had (in the order of `Permutation::ALL`), so that a test can
  // check which case it actually covers. The `needsConversion` says whether
  // the input has encoded points (in its permutations or in its geometry
  // vocabulary), which decides whether the index is actually converted or only
  // copied, and which is checked against the log of the conversion. The
  // `vocabularyType` is that of the index that is built (a random one if not
  // given, like in all other tests).
  std::vector<size_t> convertAndExpectTheConvertedContent(
      std::string turtleInput, bool needsConversion,
      bool allPermutations = true,
      std::optional<ad_utility::VocabularyType> vocabularyType = std::nullopt) {
    auto permutations = permutationsOfIndex(allPermutations);
    std::vector<size_t> numBlocks;
    std::vector<IdTable> expectedContent;
    Index::NumNormalAndInternal numTriples;
    {
      ad_utility::testing::TestIndexConfig config{std::move(turtleInput)};
      config.loadAllPermutations = allPermutations;
      config.usePatterns = allPermutations;
      config.vocabularyType = vocabularyType;
      Index oldIndex =
          ad_utility::testing::makeTestIndex(oldBasename_, std::move(config));
      auto locatedTriples =
          oldIndex.deltaTriplesManager().getCurrentLocatedTriplesSharedState();
      numTriples = oldIndex.numTriples();
      EXPECT_EQ(indexContainsGeoPoints(oldBasename_), needsConversion);
      for (auto permutationEnum : permutations) {
        const auto& permutation =
            oldIndex.getImpl().getPermutation(permutationEnum);
        numBlocks.push_back(permutation.metaData().blockData().size());
        expectedContent.push_back(expectedConvertedContent(
            scanAllColumns(permutation, locatedTriples)));
      }
    }
    pretendThatTheIndexIsInThePreviousFormat();

    // Convert with the log redirected, so that the progress of the conversion
    // can be checked below.
    std::string conversionLog;
    {
      auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();
      convertIndexToCurrentFormat(oldBasename_, newBasename_);
      conversionLog = logStream.str();
    }

    if (needsConversion) {
      // The progress bar of the conversion covers exactly the permutations
      // that the index has (each triple once per permutation, with two
      // internal permutations in addition to the normal ones), and it ends at
      // 100%, which says that its total is the number of triples that the
      // conversion actually wrote. Neither the loading nor the writing nor the
      // sorting of a permutation logs a message of its own, which would
      // interrupt the bar.
      std::string numTriplesTotal = ad_utility::withThousandSeparators(
          permutations.size() * numTriples.normal + 2 * numTriples.internal);
      EXPECT_THAT(conversionLog, HasSubstr(absl::StrCat(
                                     "Triples converted: ", numTriplesTotal,
                                     " of ", numTriplesTotal, " (100.0%)")));
      EXPECT_THAT(conversionLog, ::testing::Not(HasSubstr("Registered ")));
      EXPECT_THAT(conversionLog, ::testing::Not(HasSubstr("Triples sorted")));
    } else {
      EXPECT_THAT(conversionLog, HasSubstr("needs no conversion"));
      EXPECT_THAT(conversionLog,
                  ::testing::Not(HasSubstr("Triples converted")));
    }

    Index newIndex = loadConvertedIndex(allPermutations);
    auto locatedTriples =
        newIndex.deltaTriplesManager().getCurrentLocatedTriplesSharedState();
    size_t i = 0;
    for (auto permutationEnum : permutations) {
      EXPECT_EQ(
          scanAllColumns(newIndex.getImpl().getPermutation(permutationEnum),
                         locatedTriples),
          expectedContent.at(i))
          << Permutation::toString(permutationEnum);
      ++i;
    }
    return numBlocks;
  }
};

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest, indexWithoutPointsIsOnlyCopied) {
  // An index without points needs no conversion, so all its files are copied
  // unchanged (including its materialized view, if it had one), and only the
  // format version is set to the current one.
  std::string turtle;
  for (size_t object = 0; object < 20; ++object) {
    absl::StrAppend(&turtle,
                    "<http://example.org/big> <http://example.org/p0> "
                    "<http://example.org/o",
                    object, "> .\n");
  }
  for (size_t subject = 0; subject < 4; ++subject) {
    for (size_t predicate = 1; predicate < 4; ++predicate) {
      for (size_t object = 0; object < 5; ++object) {
        absl::StrAppend(&turtle, "<http://example.org/s", subject,
                        "> <http://example.org/p", predicate,
                        "> <http://example.org/o", object, "> .\n");
      }
    }
  }
  auto numBlocks = convertAndExpectTheConvertedContent(turtle, false);
  EXPECT_THAT(numBlocks, ::testing::Each(::testing::Gt(size_t{2})));
  EXPECT_TRUE(indexNeedsNoConversion(oldBasename_));
  // The permutation files are byte-identical copies.
  for (auto suffix : {".index.pso", ".index.osp", ".internal.index.pos"}) {
    std::string oldFile = absl::StrCat(oldBasename_, suffix);
    std::string newFile = absl::StrCat(newBasename_, suffix);
    EXPECT_EQ(fs::file_size(oldFile), fs::file_size(newFile)) << suffix;
  }
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest,
       geometriesWithoutPointsNeedConversion) {
  // An index with a geometry vocabulary that has geometries but no points: the
  // permutations have no points, but the geometry information holds encoded
  // points (the bounding boxes and centroids), so the index is converted, and
  // the geometry information of the converted index is that of the index that
  // was converted with the points converted once more.
  std::string turtle;
  for (size_t i = 0; i < 12; ++i) {
    absl::StrAppend(&turtle, "<http://example.org/s", i,
                    "> <http://example.org/geometry> \"LINESTRING(", i, " ", i,
                    ", ", i + 1, " ", i + 2, ")\"", wktDatatype, " .\n");
    absl::StrAppend(&turtle, "<http://example.org/s", i,
                    "> <http://example.org/label> \"label ", i, "\" .\n");
  }
  absl::StrAppend(&turtle, "<http://example.org/s0> <http://example.org/bad> ",
                  "\"INVALID(1 2)\"", wktDatatype, " .\n");
  ad_utility::VocabularyType geoSplit{
      ad_utility::VocabularyType::Enum::OnDiskCompressedGeoSplit};
  auto numBlocks =
      convertAndExpectTheConvertedContent(turtle, true, true, geoSplit);
  EXPECT_THAT(numBlocks, ::testing::Each(::testing::Gt(size_t{2})));
  EXPECT_FALSE(indexNeedsNoConversion(oldBasename_));

  // Compare the geometry information of every word of the two geometry
  // vocabularies, as raw bytes: the points of the index that was converted
  // are already in the current format, so converting them once more yields
  // bits that may not decode to a valid bounding box. The index that was
  // converted is loaded with its true format version, as an index with
  // geometries in the previous format is refused (which a test in
  // `IndexTest.cpp` covers).
  setFormatVersionOfOldIndex(qlever::indexFormatVersion);
  Index oldIndex{ad_utility::makeUnlimitedAllocator<Id>()};
  oldIndex.createFromOnDiskIndex(oldBasename_, false);
  Index newIndex = loadConvertedIndex();
  auto getOldId = ad_utility::testing::makeGetId(oldIndex);
  auto getNewId = ad_utility::testing::makeGetId(newIndex);
  // The four padding bytes after the number of geometries (a `uint32_t` that
  // is followed by a `double`) hold whatever was in memory when the record
  // was written, so they are ignored.
  using Bytes = std::array<uint8_t, sizeof(ad_utility::GeometryInfo)>;
  static_assert(sizeof(ad_utility::GeometryInfo) == 48);
  auto withoutPadding = [](const ad_utility::GeometryInfo& info) {
    Bytes bytes = absl::bit_cast<Bytes>(info);
    ql::ranges::fill(bytes.begin() + 28, bytes.begin() + 32, uint8_t{0});
    return bytes;
  };
  size_t numValid = 0;
  for (size_t i = 0; i < 12; ++i) {
    std::string literal = absl::StrCat("\"LINESTRING(", i, " ", i, ", ", i + 1,
                                       " ", i + 2, ")\"", wktDatatype);
    auto oldInfo =
        oldIndex.getVocab().getGeoInfo(getOldId(literal).getVocabIndex());
    auto newInfo =
        newIndex.getVocab().getGeoInfo(getNewId(literal).getVocabIndex());
    ASSERT_TRUE(oldInfo.has_value() && newInfo.has_value()) << literal;
    EXPECT_EQ(
        withoutPadding(newInfo.value()),
        withoutPadding(oldInfo->withPointBitsMappedBy(&convertGeoPointBits)))
        << literal;
    ++numValid;
  }
  EXPECT_EQ(numValid, 12u);
  std::string invalid = absl::StrCat("\"INVALID(1 2)\"", wktDatatype);
  EXPECT_FALSE(
      newIndex.getVocab().getGeoInfo(getNewId(invalid).getVocabIndex()));
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest,
       geometryVocabularyWithoutGeometriesNeedsNoConversion) {
  // An index with a geometry vocabulary, but without a single WKT literal: its
  // geometry information has no valid record, so it needs no conversion.
  std::string turtle;
  for (size_t i = 0; i < 10; ++i) {
    absl::StrAppend(&turtle, "<http://example.org/s", i,
                    "> <http://example.org/label> \"label ", i, "\" .\n");
  }
  ad_utility::VocabularyType geoSplit{
      ad_utility::VocabularyType::Enum::OnDiskCompressedGeoSplit};
  convertAndExpectTheConvertedContent(turtle, false, true, geoSplit);
  EXPECT_TRUE(indexNeedsNoConversion(oldBasename_));
  EXPECT_TRUE(
      fs::exists(absl::StrCat(newBasename_, ".vocabulary.geometry.geoinfo")));
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest, runsOfPointsInManyBlocks) {
  // Many subjects with one to three points each for one predicate, and one
  // predicate with a point for some of them, plus some triples without points.
  // With two triples per block, the run of all points in `OSP` and `OPS`, the
  // runs per predicate in `POS`, and the runs of the subjects with several
  // points in `SPO` and `PSO` all span several blocks.
  std::string turtle;
  std::mt19937 gen{42};
  std::uniform_real_distribution<double> latDist{-90.0, 90.0};
  std::uniform_real_distribution<double> lngDist{-180.0, 180.0};
  auto point = [&]() {
    return absl::StrCat("\"POINT(", lngDist(gen), " ", latDist(gen),
                        ")\"^^<http://www.opengis.net/ont/geosparql#"
                        "wktLiteral>");
  };
  for (size_t subject = 0; subject < 30; ++subject) {
    for (size_t i = 0; i <= subject % 3; ++i) {
      absl::StrAppend(&turtle, "<http://example.org/s", subject,
                      "> <http://example.org/geometry> ", point(), " .\n");
    }
    if (subject % 2 == 0) {
      absl::StrAppend(&turtle, "<http://example.org/s", subject,
                      "> <http://example.org/centroid> ", point(), " .\n");
    }
    absl::StrAppend(&turtle, "<http://example.org/s", subject,
                    "> <http://example.org/label> \"label ", subject, "\" .\n");
    absl::StrAppend(&turtle, "<http://example.org/s", subject,
                    "> <http://example.org/int> ", subject, " .\n");
  }
  auto numBlocks = convertAndExpectTheConvertedContent(turtle, true);
  EXPECT_THAT(numBlocks, ::testing::Each(::testing::Gt(size_t{10})));
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest, runsOfPointsSortedExternally) {
  // The same as above, but with a memory budget so small that every run of
  // more than one row is sorted externally.
  ad_utility::MemorySize previousMemory = memoryForSorting();
  memoryForSorting() = 1_B;
  ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
  absl::Cleanup restore = [previousMemory]() {
    memoryForSorting() = previousMemory;
    ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING =
        false;
  };
  std::string turtle;
  std::mt19937 gen{4711};
  std::uniform_real_distribution<double> latDist{-90.0, 90.0};
  std::uniform_real_distribution<double> lngDist{-180.0, 180.0};
  for (size_t subject = 0; subject < 20; ++subject) {
    for (size_t i = 0; i <= subject % 4; ++i) {
      absl::StrAppend(&turtle, "<http://example.org/s", subject,
                      "> <http://example.org/geometry> \"POINT(", lngDist(gen),
                      " ", latDist(gen),
                      ")\"^^<http://www.opengis.net/ont/geosparql#"
                      "wktLiteral> .\n");
    }
  }
  auto numBlocks = convertAndExpectTheConvertedContent(turtle, true);
  EXPECT_THAT(numBlocks, ::testing::Each(::testing::Gt(size_t{5})));
  // The temporary files of the external sorters (`<permutation
  // file>.sort-tmp.<n>`) are gone.
  for (const auto& entry : fs::directory_iterator{directory_}) {
    EXPECT_THAT(entry.path().filename().string(),
                ::testing::Not(HasSubstr(".sort-tmp")));
  }
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest, convertIndexWithOnlyPsoAndPos) {
  // An index built with `--only-pso-and-pos-permutations` has only two of the
  // six normal permutations and no patterns. The conversion has to skip the
  // pairs that the index does not have, and must not write them. Whether the
  // index has points is then determined from `POS`.
  std::string turtle;
  for (size_t object = 0; object < 10; ++object) {
    absl::StrAppend(&turtle,
                    "<http://example.org/s> <http://example.org/p> "
                    "<http://example.org/o",
                    object, "> .\n");
  }
  for (size_t i = 0; i < 6; ++i) {
    absl::StrAppend(&turtle,
                    "<http://example.org/s> <http://example.org/p> \"POINT(", i,
                    " ", 10 - i,
                    ")\"^^<http://www.opengis.net/ont/geosparql#"
                    "wktLiteral> .\n");
  }
  auto numBlocks = convertAndExpectTheConvertedContent(turtle, true, false);
  EXPECT_THAT(numBlocks, ::testing::SizeIs(2));
  EXPECT_THAT(numBlocks, ::testing::Each(::testing::Gt(size_t{2})));
  for (auto suffix : {".ops", ".osp", ".spo", ".sop"}) {
    EXPECT_FALSE(
        fs::exists(absl::StrCat(newBasename_, PERMUTATION_FILE_INFIX, suffix)))
        << suffix;
  }
  EXPECT_FALSE(fs::exists(absl::StrCat(newBasename_, PATTERNS_FILE_SUFFIX)));
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest, convertEmptyIndex) {
  // An index without any triples. All its permutations are empty, which means
  // that they have no blocks at all, so it has no points, and it is only
  // copied.
  {
    Index oldIndex = ad_utility::testing::makeTestIndex(
        oldBasename_, ad_utility::testing::TestIndexConfig{""});
    for (auto permutationEnum : Permutation::ALL) {
      EXPECT_TRUE(oldIndex.getImpl()
                      .getPermutation(permutationEnum)
                      .metaData()
                      .blockData()
                      .empty())
          << Permutation::toString(permutationEnum);
    }
  }
  pretendThatTheIndexIsInThePreviousFormat();
  EXPECT_FALSE(indexContainsGeoPoints(oldBasename_));

  convertIndexToCurrentFormat(oldBasename_, newBasename_);

  // The converted index can be loaded, and all its permutations are still
  // empty, both according to their metadata and when they are scanned.
  Index newIndex = loadConvertedIndex();
  auto locatedTriples =
      newIndex.deltaTriplesManager().getCurrentLocatedTriplesSharedState();
  for (auto permutationEnum : Permutation::ALL) {
    SCOPED_TRACE(Permutation::toString(permutationEnum));
    const auto& permutation =
        newIndex.getImpl().getPermutation(permutationEnum);
    EXPECT_TRUE(permutation.metaData().blockData().empty());
    EXPECT_EQ(permutation.metaData().totalElements(), 0u);
    IdTable table = permutation.scan(
        permutation.getScanSpecAndBlocks(
            ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
            *locatedTriples),
        {}, std::make_shared<ad_utility::CancellationHandle<>>(),
        *locatedTriples);
    EXPECT_EQ(table.numRows(), 0u);
  }
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest, relationWithItsOwnMetadata) {
  // Only a relation that fills more than 80% of a block of the converted
  // permutation gets a `CompressedRelationMetadata` entry of its own; the
  // metadata of a smaller relation is derived from the block that it shares
  // with other relations (see
  // `CompressedRelationReader::getMetadataForSmallRelation`). A permutation
  // that consists only of small relations therefore never invokes the metadata
  // callback of `writePermutation`. The conversion uses the block size of the
  // index that is converted, which is two triples per block in the tests (see
  // `convertAndExpectTheConvertedContent` above), so a relation with two rows
  // already is large enough.

  // The subject `<big>` has two points, so it is a large relation in the `SPO`
  // permutation, and the subject `<small>` has one triple, so it stays a small
  // relation there. The points make sure that the index is actually converted
  // and not only copied.
  std::string turtle =
      "<http://example.org/big> <http://example.org/p> \"POINT(1 2)\"^^"
      "<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<http://example.org/big> <http://example.org/p> \"POINT(3 4)\"^^"
      "<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<http://example.org/small> <http://example.org/p> "
      "<http://example.org/o1> .\n";
  convertAndExpectTheConvertedContent(turtle, true);

  // In the `SPO` permutation of the converted index, the large relation has a
  // metadata entry of its own, which only the metadata callback of
  // `writePermutation` can have added, and the small relation has none.
  Index newIndex = loadConvertedIndex();
  auto getId = ad_utility::testing::makeGetId(newIndex);
  const auto& metaData =
      newIndex.getImpl().getPermutation(Permutation::SPO).metaData();
  auto largeRelation =
      metaData.getMetaDataIfPresent(getId("<http://example.org/big>"));
  ASSERT_TRUE(largeRelation.has_value());
  EXPECT_EQ(largeRelation.value().numRows_, 2u);
  EXPECT_FALSE(
      metaData.getMetaDataIfPresent(getId("<http://example.org/small>"))
          .has_value());
}

// A fixture for `sortRunsOfGeoPoints` on blocks that are made by hand: rows
// with four key columns and one payload column, whose `Id`s are drawn from a
// small alphabet, so that there are many rows that agree on a prefix of the
// columns, which is what makes runs. The rows are sorted in the order of the
// previous format, then all `Id`s are converted, then the rows are cut into
// blocks of random sizes and passed through `sortRunsOfGeoPoints`, and the
// result has to be exactly the converted rows, sorted.
class SortRunsOfGeoPointsTest : public ::testing::Test {
 protected:
  using Row = std::array<Id, 5>;
  fs::path directory_;
  std::mt19937 gen_{12345};

  void SetUp() override {
    directory_ = fs::path{gtestCurrentTestName()};
    fs::remove_all(directory_);
    fs::create_directories(directory_);
  }
  void TearDown() override { fs::remove_all(directory_); }

  // A random `Id` for a key column: with `pointProbability`, a point in the
  // previous format, else an `Id` of another datatype.
  Id randomId(double pointProbability) {
    std::uniform_real_distribution<double> coin{0.0, 1.0};
    std::uniform_int_distribution<size_t> small{0, 3};
    if (coin(gen_) < pointProbability) {
      std::uniform_int_distribution<GeoPoint::T> coordinate{
          0, GeoPoint::maxCoordinateEncoded};
      return Id::makeFromGeoPointBits(
          (coordinate(gen_) << GeoPoint::numDataBitsCoordinate) |
          coordinate(gen_));
    }
    switch (small(gen_)) {
      case 0:
        return Id::makeFromInt(static_cast<int64_t>(small(gen_)));
      case 1:
        return Id::makeFromVocabIndex(VocabIndex::make(small(gen_)));
      case 2:
        return Id::makeFromBlankNodeIndex(BlankNodeIndex::make(small(gen_)));
      default:
        return Id::makeFromDouble(static_cast<double>(small(gen_)));
    }
  }

  // Random rows in the order of the previous format: `numRows` rows whose key
  // columns have points with the given probabilities (one per column), and a
  // payload column that identifies the row.
  std::vector<Row> randomSortedRows(size_t numRows,
                                    std::array<double, 4> pointProbabilities) {
    std::vector<Row> rows;
    for (size_t i = 0; i < numRows; ++i) {
      Row row;
      for (size_t column = 0; column < 4; ++column) {
        row[column] = randomId(pointProbabilities[column]);
      }
      row[4] = Id::makeFromInt(static_cast<int64_t>(i));
      rows.push_back(row);
    }
    ql::ranges::sort(rows, keyComparator());
    return rows;
  }

  // Convert the `Id`s of all `rows`.
  static std::vector<Row> converted(std::vector<Row> rows) {
    for (auto& row : rows) {
      for (auto& id : row) {
        id = convertId(id);
      }
    }
    return rows;
  }

  // Cut the `rows` into blocks of random sizes between 1 and `maxBlockSize`,
  // with some empty blocks in between.
  ad_utility::InputRangeTypeErased<IdTableStatic<0>> randomBlocks(
      const std::vector<Row>& rows, size_t maxBlockSize) {
    std::vector<IdTableStatic<0>> blocks;
    std::uniform_int_distribution<size_t> blockSize{1, maxBlockSize};
    std::uniform_int_distribution<size_t> emptyBlock{0, 4};
    size_t next = 0;
    while (next < rows.size()) {
      if (emptyBlock(gen_) == 0) {
        blocks.emplace_back(
            IdTable{5, ad_utility::makeUnlimitedAllocator<Id>()});
      }
      IdTable block{5, ad_utility::makeUnlimitedAllocator<Id>()};
      size_t end = std::min(rows.size(), next + blockSize(gen_));
      for (; next < end; ++next) {
        block.push_back(rows[next]);
      }
      blocks.emplace_back(std::move(block));
    }
    return ad_utility::InputRangeTypeErased<IdTableStatic<0>>{
        std::move(blocks)};
  }

  // Run `sortRunsOfGeoPoints` on the `blocks` with the given `memory` and
  // return all rows of its output, checking that every output block is
  // sorted.
  std::vector<Row> sortRuns(
      ad_utility::InputRangeTypeErased<IdTableStatic<0>> blocks,
      ad_utility::MemorySize memory) {
    std::string tempFilename = (directory_ / "sort-tmp").string();
    auto output = sortRunsOfGeoPoints(std::move(blocks), tempFilename, memory);
    std::vector<Row> rows;
    for (const auto& block : output) {
      EXPECT_EQ(block.numColumns(), 5u);
      EXPECT_TRUE(ql::ranges::is_sorted(block, keyComparator()));
      for (const auto& row : block) {
        rows.push_back({row[0], row[1], row[2], row[3], row[4]});
      }
    }
    // The temporary files of the external sorters (`<tempFilename>.<n>`) are
    // deleted once their runs have been output.
    for (const auto& entry : fs::directory_iterator{directory_}) {
      EXPECT_FALSE(
          ql::starts_with(entry.path().filename().string(), "sort-tmp"))
          << entry.path();
    }
    return rows;
  }

  // Check that `sortRunsOfGeoPoints` restores the sort order for random rows
  // with the given point probabilities, both with enough memory for sorting
  // in memory and with the given small `externalMemory`, with which the runs
  // are sorted externally.
  void testRandomRows(size_t numRows, std::array<double, 4> pointProbabilities,
                      size_t maxBlockSize,
                      ad_utility::MemorySize externalMemory = 1_B) {
    auto rows = randomSortedRows(numRows, pointProbabilities);
    // The input of `sortRunsOfGeoPoints` has the converted `Id`s in the order
    // of the previous format (see `scanAndConvertIds`), and its output has to
    // be that input, sorted.
    auto input = converted(rows);
    auto expected = input;
    ql::ranges::sort(expected, keyComparator());
    // The test is only meaningful if the conversion actually disturbs the
    // order.
    if (numRows > 50) {
      EXPECT_NE(input, expected);
    }
    EXPECT_EQ(sortRuns(randomBlocks(input, maxBlockSize), 1_GB), expected);
    ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING = true;
    absl::Cleanup restore = []() {
      ad_utility::EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING =
          false;
    };
    EXPECT_EQ(sortRuns(randomBlocks(input, maxBlockSize), externalMemory),
              expected);
  }
};

// _____________________________________________________________________________
TEST_F(SortRunsOfGeoPointsTest, noPointsPassThroughUnchanged) {
  // Without points, the output has exactly the input rows in the input order,
  // and the blocks are passed through as they are (the same number of
  // non-empty blocks with the same sizes).
  auto rows = randomSortedRows(200, {0.0, 0.0, 0.0, 0.0});
  auto blocks = randomBlocks(rows, 7);
  std::vector<size_t> inputSizes;
  std::vector<IdTableStatic<0>> blocksCopy;
  for (auto& block : blocks) {
    if (!block.empty()) {
      inputSizes.push_back(block.numRows());
    }
    blocksCopy.push_back(std::move(block));
  }
  auto output = sortRunsOfGeoPoints(
      ad_utility::InputRangeTypeErased<IdTableStatic<0>>{std::move(blocksCopy)},
      (directory_ / "sort-tmp").string(), 1_GB);
  std::vector<size_t> outputSizes;
  std::vector<Row> outputRows;
  for (const auto& block : output) {
    outputSizes.push_back(block.numRows());
    for (const auto& row : block) {
      outputRows.push_back({row[0], row[1], row[2], row[3], row[4]});
    }
  }
  EXPECT_EQ(outputSizes, inputSizes);
  EXPECT_EQ(outputRows, rows);
}

// _____________________________________________________________________________
TEST_F(SortRunsOfGeoPointsTest, emptyInput) {
  auto output =
      sortRunsOfGeoPoints(ad_utility::InputRangeTypeErased<IdTableStatic<0>>{},
                          (directory_ / "sort-tmp").string(), 1_GB);
  EXPECT_TRUE(output.get() == std::nullopt);
  EXPECT_TRUE(sortRuns(randomBlocks({}, 3), 1_GB).empty());
}

// _____________________________________________________________________________
TEST_F(SortRunsOfGeoPointsTest, pointsInEachOfTheKeyColumns) {
  // The points of a permutation sit in one key column: in the first one for
  // `OSP` and `OPS`, where they form a single run without a prefix, in the
  // second for `POS`, where there is one run per predicate, and in the third
  // for `PSO`, `SPO` and `SOP`, where a run is usually a single row.
  for (size_t column = 0; column < 3; ++column) {
    SCOPED_TRACE(absl::StrCat("points in column ", column));
    for (double probability : {0.6, 1.0}) {
      std::array<double, 4> probabilities{};
      probabilities[column] = probability;
      testRandomRows(500, probabilities, 5);
    }
  }
  // Points in several columns at once, so that rows whose first point is in
  // different columns are interleaved, including points in the graph column
  // and in the payload column (where they never matter).
  testRandomRows(600, {0.2, 0.3, 0.5, 0.2}, 4);
}

// _____________________________________________________________________________
TEST_F(SortRunsOfGeoPointsTest, runThatDoesNotFitIntoMemory) {
  // A single run of many rows, which the external sorter with a memory of a
  // few megabytes writes as several blocks and then merges again.
  testRandomRows(120'000, {1.0, 0.0, 0.0, 0.0}, 10'000, 3_MB);
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest, indexWithoutRowsPerBlock) {
  // An index that was built before its block size was stored in its
  // configuration was built with the default block size, so it is converted
  // with the default block size. With that, the relation `<big>`, which is
  // large with the block size of two triples per block of the tests (see
  // `relationWithItsOwnMetadata` above), is small and the whole `SPO`
  // permutation is a single block. The points make sure that the index is
  // actually converted and not only copied.
  std::string turtle =
      "<http://example.org/big> <http://example.org/p> \"POINT(1 2)\"^^"
      "<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<http://example.org/big> <http://example.org/p> \"POINT(3 4)\"^^"
      "<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<http://example.org/small> <http://example.org/p> "
      "<http://example.org/o1> .\n";
  ad_utility::testing::makeTestIndex(
      oldBasename_, ad_utility::testing::TestIndexConfig{turtle});
  pretendThatTheIndexIsInThePreviousFormat();
  {
    std::string filename = absl::StrCat(oldBasename_, CONFIGURATION_FILE);
    nlohmann::json configuration;
    ad_utility::makeIfstream(filename) >> configuration;
    ASSERT_EQ(configuration.at(INDEX_ROWS_PER_BLOCK_KEY), 2);
    configuration.erase(std::string{INDEX_ROWS_PER_BLOCK_KEY});
    ad_utility::makeOfstream(filename) << configuration.dump(4);
  }
  {
    auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();
    convertIndexToCurrentFormat(oldBasename_, newBasename_);
  }

  Index newIndex = loadConvertedIndex();
  EXPECT_EQ(newIndex.rowsPerBlock(), DEFAULT_INDEX_ROWS_PER_BLOCK);
  auto getId = ad_utility::testing::makeGetId(newIndex);
  const auto& metaData =
      newIndex.getImpl().getPermutation(Permutation::SPO).metaData();
  EXPECT_EQ(metaData.blockData().size(), 1u);
  EXPECT_FALSE(metaData.getMetaDataIfPresent(getId("<http://example.org/big>"))
                   .has_value());
}

// _____________________________________________________________________________
TEST(IndexFormatConverter, conversionDescription) {
  std::string description = conversionDescription();
  // The description names both index formats between which the converter
  // converts, each with its pull request number and its date.
  for (const auto& version : {sourceVersion, targetVersion}) {
    EXPECT_THAT(description,
                HasSubstr(absl::StrCat("PR = ", version.prNumber_)));
    EXPECT_THAT(description,
                HasSubstr(absl::StrCat("Date = ",
                                       version.date_.toStringAndType().first)));
  }
  // It also states the difference between the two formats and how the
  // in-place upgrade proceeds (staging directory, retirement directory).
  EXPECT_THAT(description, HasSubstr("geo point"));
  EXPECT_THAT(description, HasSubstr("index-in-new-format."));
  EXPECT_THAT(description, HasSubstr("index-in-old-format."));
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
