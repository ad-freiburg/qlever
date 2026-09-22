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

#include <string>
#include <vector>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "engine/MaterializedViews.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/MaterializedViewConstants.h"
#include "index/CompressedRelationMetadata.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/IndexFormatConverter.h"
#include "index/IndexFormatVersion.h"
#include "index/IndexImpl.h"
#include "index/IndexMetaData.h"
#include "index/PatternCreator.h"
#include "util/BitUtils.h"
#include "util/CancellationHandle.h"
#include "util/CompactStringVector.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"
#include "util/NBitInteger.h"
#include "util/ProgressBar.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/Serializer.h"
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

// The number of bits used for the datatype resp. the payload in the previous
// index format (a single 64-bit word, unlike the current format's full
// datatype byte and full 64-bit payload word).
constexpr uint64_t oldNumDatatypeBits = 4;
constexpr uint64_t oldNumDataBits = 64 - oldNumDatatypeBits;

// Return the raw bits of an `Id` of the previous index format with the given
// datatype bits and payload (which must fit into `oldNumDataBits` bits).
uint64_t oldFormatId(uint64_t datatypeBits, uint64_t payload) {
  AD_CONTRACT_CHECK(payload <= ad_utility::bitMaskForLowerBits(oldNumDataBits));
  return (datatypeBits << oldNumDataBits) | payload;
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
  // Datatype numbering is unchanged between the two formats, so the datatype
  // bits of a previous-format `Id` are directly the current `Datatype` value.
  for (uint8_t datatypeBits = 0;
       datatypeBits <= static_cast<uint8_t>(Datatype::MaxValue);
       ++datatypeBits) {
    SCOPED_TRACE(absl::StrCat("datatype bits ", static_cast<int>(datatypeBits)));
    auto datatype = static_cast<Datatype>(datatypeBits);
    // An `Id` of type `LocalVocabIndex` is never stored on disk, so it always
    // is an error, see the test below.
    if (datatype == Datatype::LocalVocabIndex) {
      continue;
    }
    if (datatype == Datatype::Double) {
      // Values whose lowest `oldNumDatatypeBits` mantissa bits are zero
      // survive the shift-based round trip without loss.
      for (double value : {0.0, 1.0, -1.0, 3.5, 1e10}) {
        uint64_t rawBits = absl::bit_cast<uint64_t>(value);
        ASSERT_EQ(rawBits & ad_utility::bitMaskForLowerBits(oldNumDatatypeBits),
                  0u);
        Id converted =
            convertId(oldFormatId(datatypeBits, rawBits >> oldNumDatatypeBits));
        EXPECT_EQ(converted.getDatatype(), Datatype::Double);
        EXPECT_EQ(converted.getDouble(), value);
      }
      continue;
    }
    if (datatype == Datatype::Int) {
      // The payload is a 60-bit two's complement integer (see
      // `ad_utility::NBitInteger`).
      for (int64_t value :
           {int64_t{0}, int64_t{17}, int64_t{-17},
            ad_utility::NBitInteger<oldNumDataBits>::max(),
            ad_utility::NBitInteger<oldNumDataBits>::min()}) {
        uint64_t payload =
            ad_utility::NBitInteger<oldNumDataBits>::toNBit(value);
        Id converted = convertId(oldFormatId(datatypeBits, payload));
        EXPECT_EQ(converted.getDatatype(), Datatype::Int);
        EXPECT_EQ(converted.getInt(), value);
      }
      continue;
    }
    if (datatype == Datatype::EncodedVal) {
      // Like `Double`, shifted the same way; no separate value to decode,
      // the encoding is internal to `EncodedIriManager`.
      for (uint64_t payload :
           {uint64_t{0}, uint64_t{17},
            ad_utility::bitMaskForLowerBits(oldNumDataBits)}) {
        Id converted = convertId(oldFormatId(datatypeBits, payload));
        EXPECT_EQ(converted.getDatatype(), Datatype::EncodedVal);
        EXPECT_EQ(converted.getBits().payload_, payload << oldNumDatatypeBits);
      }
      continue;
    }
    // Every other datatype stores its payload directly and unshifted.
    for (uint64_t payload :
         {uint64_t{0}, uint64_t{17},
          ad_utility::bitMaskForLowerBits(oldNumDataBits)}) {
      Id converted = convertId(oldFormatId(datatypeBits, payload));
      EXPECT_EQ(converted.getDatatype(), datatype);
      EXPECT_EQ(converted.getBits().payload_, payload);
    }
  }
}

// _____________________________________________________________________________
TEST(IndexFormatConverter, convertIdPreservesTheOrder) {
  // Permutation conversion relies on `Id` order being preserved. `Double`/
  // `Int` are covered separately below in this test: an arbitrary raw
  // payload isn't order-preserving for them until decoded to its value.
  std::vector<Id> convertedIds;
  for (uint8_t datatypeBits = 0;
       datatypeBits <= static_cast<uint8_t>(Datatype::MaxValue);
       ++datatypeBits) {
    auto datatype = static_cast<Datatype>(datatypeBits);
    if (datatype == Datatype::LocalVocabIndex ||
        datatype == Datatype::Double || datatype == Datatype::Int) {
      continue;
    }
    for (uint64_t payload : {uint64_t{0}, uint64_t{17}}) {
      convertedIds.push_back(convertId(oldFormatId(datatypeBits, payload)));
    }
  }
  EXPECT_TRUE(ql::ranges::is_sorted(convertedIds));
  EXPECT_TRUE(ql::ranges::adjacent_find(convertedIds) == convertedIds.end());

  // `ValueId`'s ordering ("positive doubles ascending, then negative doubles
  // reversed" for `Double`, "positive then negative ascending" for `Int`)
  // holds identically in both formats, so listing values in that order and
  // checking the conversion keeps them sorted is a meaningful check.
  std::vector<Id> convertedDoubles;
  for (double value : {0.0, 1.0, 3.5, 1e10, -1.0, -3.5, -1e10}) {
    uint64_t rawBits = absl::bit_cast<uint64_t>(value) >> oldNumDatatypeBits;
    convertedDoubles.push_back(convertId(
        oldFormatId(static_cast<uint64_t>(Datatype::Double), rawBits)));
  }
  EXPECT_TRUE(ql::ranges::is_sorted(convertedDoubles));

  std::vector<Id> convertedInts;
  for (int64_t value : {int64_t{0}, int64_t{1}, int64_t{1000}, int64_t{-1000},
                        int64_t{-1}}) {
    uint64_t payload = ad_utility::NBitInteger<oldNumDataBits>::toNBit(value);
    convertedInts.push_back(
        convertId(oldFormatId(static_cast<uint64_t>(Datatype::Int), payload)));
  }
  EXPECT_TRUE(ql::ranges::is_sorted(convertedInts));
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
  // The datatype bits are 4 bits wide, so values beyond `Datatype::MaxValue`
  // are invalid.
  for (uint64_t datatypeBits = static_cast<uint64_t>(Datatype::MaxValue) + 1;
       datatypeBits < 16; ++datatypeBits) {
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
  // This transition leaves a view's own `version` metadata unchanged (see
  // `materializedViewsVersionOfSourceFormat`), so it cannot detect the still
  // incompatible `Id` byte layout in the view's permutation; loading it
  // directly fails, but without a graceful, user-facing message.
  EXPECT_ANY_THROW(MaterializedView(oldBasename_, "testview"));

  convertIndexToCurrentFormat(oldBasename_, newBasename_);

  // The converted view can be loaded, which also checks its version, its
  // columns, and its query, and it contains all its rows.
  MaterializedView view{newBasename_, "testview"};
  EXPECT_EQ(view.permutation()->metaData().totalElements(), 6);
  EXPECT_THAT(view.originalQuery(),
              ::testing::Optional(HasSubstr("<http://example.org/label>")));
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

  upgradeIndexInPlace(oldBasename_);

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
  // `index-in-old-format.<datetime>` with all of its files, and the staging
  // directory was removed again.
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

  // A view not in the converter's source format version. The source format's
  // views already have the current `MATERIALIZED_VIEWS_VERSION`, so the
  // "wrong" version here must be some other, genuinely unsuitable value.
  std::string viewInfoFilename = absl::StrCat(viewBasename, VIEW_INFO_SUFFIX);
  nlohmann::json viewInfo;
  ad_utility::makeIfstream(viewInfoFilename) >> viewInfo;
  auto originalViewInfo = viewInfo;
  viewInfo["version"] = MATERIALIZED_VIEWS_VERSION + 1;
  ad_utility::makeOfstream(viewInfoFilename) << viewInfo.dump();
  AD_EXPECT_THROW_WITH_MESSAGE(
      convertIndexToCurrentFormat(oldBasename_, newBasename_),
      HasSubstr(absl::StrCat("only converts views in the format version ",
                              MATERIALIZED_VIEWS_VERSION)));
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

// The exact inverse of `LegacyId::convert`: encode a current-format `Id` as
// the raw bits of the previous (4 datatype bits + 60 payload bits) format.
// Used to "downgrade" a current-format index into synthetic fixtures for
// `MultiBlockIndexFormatConverterTest` below.
uint64_t legacyBitsFromId(Id id) {
  auto bits = id.getBits();
  auto datatype = static_cast<Datatype>(bits.datatype_);
  uint64_t payload = bits.payload_;
  if (datatype == Datatype::Double || datatype == Datatype::EncodedVal) {
    payload = payload >> oldNumDatatypeBits;
  } else if (datatype == Datatype::Int) {
    payload = ad_utility::NBitInteger<oldNumDataBits>::toNBit(id.getInt());
  }
  return oldFormatId(bits.datatype_, payload);
}

// A previous-format `Id`/`PermutedTriple`, laid out like `LegacyId`/
// `LegacyPermutedTriple` expect to read them. Write-only: `legacyBitsFromId`
// above already covers the only needed conversion direction.
struct TestLegacyId {
  uint64_t bits_;
  template <typename T>
  friend std::true_type allowTrivialSerialization(TestLegacyId, T);
};
struct TestLegacyPermutedTriple {
  TestLegacyId col0Id_;
  TestLegacyId col1Id_;
  TestLegacyId col2Id_;
  TestLegacyId graphId_;
  template <typename T>
  friend std::true_type allowTrivialSerialization(TestLegacyPermutedTriple, T);
};

// Mirrors `LegacyCompressedBlockMetadata` (see `IndexFormatConverter.cpp`)
// field for field, so that it serializes to exactly the bytes that the
// converter's own `LegacyCompressedBlockMetadata` reads.
struct TestLegacyBlockMetadata {
  std::vector<CompressedBlockMetadata::OffsetAndCompressedSize>
      offsetsAndCompressedSize_;
  size_t numRows_;
  TestLegacyPermutedTriple firstTriple_;
  TestLegacyPermutedTriple lastTriple_;
  std::optional<std::vector<TestLegacyId>> graphInfo_;
  bool containsDuplicatesWithDifferentGraphs_;
  size_t blockIndex_;
};
AD_SERIALIZE_FUNCTION(TestLegacyBlockMetadata) {
  serializer | arg.offsetsAndCompressedSize_;
  serializer | arg.numRows_;
  serializer | arg.firstTriple_;
  serializer | arg.lastTriple_;
  serializer | arg.graphInfo_;
  serializer | arg.containsDuplicatesWithDifferentGraphs_;
  serializer | arg.blockIndex_;
}

// Write `allRows` (from `scanAllColumns`) to `filename` as a *previous*-
// format permutation file, split into blocks per `blockMetadata` (the
// *current* format's block boundaries of the same, unconverted permutation;
// row counts and first/last triples are reused, `Id`s downgraded via
// `legacyBitsFromId`). `graphInfo_`/`containsDuplicatesWithDifferentGraphs_`/
// `blockIndex_` are set to innocuous constants: the converter recomputes
// them fresh and never reads them from the source format.
//
// Exists so `MultiBlockIndexFormatConverterTest` can build varying fixtures
// dynamically instead of needing more checked-in binaries like
// `test/data/oldIndexFormat`.
void writeLegacyPermutationFile(
    const std::string& filename, size_t numColumns,
    const std::vector<CompressedBlockMetadata>& blockMetadata,
    const IdTable& allRows) {
  ad_utility::File file{filename, "w"};
  std::vector<TestLegacyBlockMetadata> blocks;
  blocks.reserve(blockMetadata.size());
  size_t rowOffset = 0;
  for (size_t blockIdx = 0; blockIdx < blockMetadata.size(); ++blockIdx) {
    size_t numRows = blockMetadata[blockIdx].numRows_;
    std::vector<CompressedBlockMetadata::OffsetAndCompressedSize> offsets;
    offsets.reserve(numColumns);
    for (size_t col = 0; col < numColumns; ++col) {
      std::vector<uint64_t> legacyBits;
      legacyBits.reserve(numRows);
      for (size_t row = rowOffset; row < rowOffset + numRows; ++row) {
        legacyBits.push_back(legacyBitsFromId(allRows(row, col)));
      }
      auto compressed = ZstdWrapper::compress(
          legacyBits.data(), legacyBits.size() * sizeof(uint64_t));
      off_t offsetInFile = file.tell();
      file.write(compressed.data(), compressed.size());
      offsets.push_back({offsetInFile, compressed.size()});
    }
    auto tripleAt = [&](size_t row) {
      return TestLegacyPermutedTriple{
          TestLegacyId{legacyBitsFromId(allRows(row, 0))},
          TestLegacyId{legacyBitsFromId(allRows(row, 1))},
          TestLegacyId{legacyBitsFromId(allRows(row, 2))},
          TestLegacyId{legacyBitsFromId(allRows(row, 3))}};
    };
    blocks.push_back(TestLegacyBlockMetadata{
        std::move(offsets), numRows, tripleAt(rowOffset),
        tripleAt(rowOffset + numRows - 1), std::nullopt, false, blockIdx});
    rowOffset += numRows;
  }
  off_t startOfMeta = file.tell();
  ad_utility::serialization::FileWriteSerializer serializer{std::move(file)};
  uint64_t magicNumber = MAGIC_NUMBER_FOR_SERIALIZATION;
  serializer << magicNumber;
  uint64_t version = V_CURRENT;
  serializer << version;
  std::string name;
  serializer << name;
  serializer << blocks;
  off_t offsetAfter = 0;
  serializer << offsetAfter;
  size_t totalElements = rowOffset;
  serializer << totalElements;
  size_t numDistinctCol0 = 0;
  serializer << numDistinctCol0;
  file = std::move(serializer).file();
  file.write(&startOfMeta, sizeof(startOfMeta));
}

// The counterpart of `writeLegacyPermutationFile` for the patterns file (if
// the index at `basename` has one), mirroring `convertPatterns` in
// `IndexFormatConverter.cpp` in the write direction.
void downgradePatternsFile(const std::string& basename) {
  std::string filename = absl::StrCat(basename, PATTERNS_FILE_SUFFIX);
  if (!fs::exists(filename)) {
    return;
  }
  PatternStatistics statistics;
  CompactVectorOfStrings<Id> patterns;
  {
    ad_utility::serialization::FileReadSerializer reader{filename};
    reader >> statistics;
    reader >> patterns;
  }
  std::vector<std::vector<TestLegacyId>> converted;
  converted.reserve(patterns.size());
  for (auto pattern : patterns) {
    std::vector<TestLegacyId> convertedPattern;
    convertedPattern.reserve(pattern.size());
    for (Id id : pattern) {
      convertedPattern.push_back(TestLegacyId{legacyBitsFromId(id)});
    }
    converted.push_back(std::move(convertedPattern));
  }
  CompactVectorOfStrings<TestLegacyId> legacyPatterns{converted};
  ad_utility::serialization::FileWriteSerializer writer{filename};
  writer << statistics;
  writer << legacyPatterns;
}

// A fixture for indexes with properties the checked-in previous-format index
// (`oldIndexDirectory` above) can't have (it has exactly one block per
// permutation and only tiny relations, and can no longer be regenerated):
// multi-block permutations, empty permutations, a relation large enough for
// its own metadata entry. Built with the *current* index builder, then
// downgraded in place (see `writeLegacyPermutationFile`/
// `downgradePatternsFile` above), so fixtures stay dynamic instead of more
// checked-in binaries.
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

  // Downgrade `oldIndex`'s permutation files (plus its patterns file, if
  // any) at `oldBasename_` to the previous format, in place, via
  // `writeLegacyPermutationFile`/`downgradePatternsFile`.
  // `pretendThatTheIndexIsInThePreviousFormat` must still be called
  // afterwards for the converter to accept the downgraded files.
  void downgradeIndexToLegacyFormat(
      const Index& oldIndex, const std::vector<Permutation::Enum>& permutations,
      const LocatedTriplesSharedState& locatedTriples) {
    auto downgrade = [&](const Permutation& permutation, bool isInternal) {
      // An empty permutation needs no downgrading: with zero blocks, the
      // trailing metadata serializes to just its length (0), which already
      // parses correctly as either format's trailer.
      if (permutation.metaData().blockData().empty()) {
        return;
      }
      std::string filename = absl::StrCat(
          oldBasename_, isInternal ? QLEVER_INTERNAL_INDEX_INFIX : "",
          PERMUTATION_FILE_INFIX, permutation.fileSuffix());
      writeLegacyPermutationFile(filename, numColumnsOnDisk(permutation),
                                 permutation.metaData().blockData(),
                                 scanAllColumns(permutation, locatedTriples));
    };
    for (auto permutationEnum : permutations) {
      downgrade(oldIndex.getImpl().getPermutation(permutationEnum), false);
    }
    downgrade(
        oldIndex.getImpl().getPermutation(Permutation::PSO).internalPermutation(),
        true);
    downgrade(
        oldIndex.getImpl().getPermutation(Permutation::POS).internalPermutation(),
        true);
    downgradePatternsFile(oldBasename_);
  }

  // Set the index format version in the configuration of the index at
  // `oldBasename_` to the source format of the converter, so that the converter
  // accepts that index. Nothing else in the index has to be changed, see the
  // documentation of this fixture.
  void pretendThatTheIndexIsInThePreviousFormat() {
    std::string filename = absl::StrCat(oldBasename_, CONFIGURATION_FILE);
    nlohmann::json configuration;
    ad_utility::makeIfstream(filename) >> configuration;
    configuration["index-format-version"] = sourceVersion;
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
  // convert it. Check that the converted index has exactly the same content as
  // the index that it was converted from, and return the number of blocks that
  // each permutation of the latter had (in the order of `Permutation::ALL`), so
  // that a test can check which case it actually covers.
  std::vector<size_t> convertAndExpectTheSameContent(
      std::string turtleInput, bool allPermutations = true) {
    auto permutations = permutationsOfIndex(allPermutations);
    std::vector<size_t> numBlocks;
    std::vector<IdTable> expectedContent;
    Index::NumNormalAndInternal numTriples;
    {
      ad_utility::testing::TestIndexConfig config{std::move(turtleInput)};
      config.loadAllPermutations = allPermutations;
      config.usePatterns = allPermutations;
      Index oldIndex =
          ad_utility::testing::makeTestIndex(oldBasename_, std::move(config));
      auto locatedTriples =
          oldIndex.deltaTriplesManager().getCurrentLocatedTriplesSharedState();
      numTriples = oldIndex.numTriples();
      for (auto permutationEnum : permutations) {
        const auto& permutation =
            oldIndex.getImpl().getPermutation(permutationEnum);
        numBlocks.push_back(permutation.metaData().blockData().size());
        expectedContent.push_back(scanAllColumns(permutation, locatedTriples));
      }
      downgradeIndexToLegacyFormat(oldIndex, permutations, locatedTriples);
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

    // The progress bar of the conversion covers exactly the permutations that
    // the index has (each triple once per permutation, with two internal
    // permutations in addition to the normal ones), and it ends at 100%, which
    // says that its total is the number of triples that the conversion actually
    // wrote. Neither the loading nor the writing of a permutation logs a
    // message of its own, which would interrupt the bar.
    std::string numTriplesTotal = ad_utility::withThousandSeparators(
        permutations.size() * numTriples.normal + 2 * numTriples.internal);
    EXPECT_THAT(conversionLog,
                HasSubstr(absl::StrCat("Triples converted: ", numTriplesTotal,
                                       " of ", numTriplesTotal, " (100.0%)")));
    EXPECT_THAT(conversionLog, ::testing::Not(HasSubstr("Registered ")));
    EXPECT_THAT(conversionLog, ::testing::Not(HasSubstr("Triples sorted")));

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
TEST_F(MultiBlockIndexFormatConverterTest, permutationsWithExactlyTwoBlocks) {
  // A single relation with three triples, which the block size of two triples
  // per block splits into exactly two blocks.
  std::string turtle =
      "<http://example.org/s> <http://example.org/p> <http://example.org/o1> "
      ".\n"
      "<http://example.org/s> <http://example.org/p> <http://example.org/o2> "
      ".\n"
      "<http://example.org/s> <http://example.org/p> <http://example.org/o3> "
      ".\n";
  auto numBlocks = convertAndExpectTheSameContent(turtle);
  // This test deliberately covers only permutations with at most two blocks,
  // which is the case where the scan of the conversion uses the cancellation
  // handle without dereferencing it (see `scanAndConvertIds`). The test below
  // covers the case of more than two blocks.
  EXPECT_THAT(numBlocks, ::testing::Each(::testing::Le(size_t{2})));
  EXPECT_THAT(numBlocks, ::testing::Contains(size_t{2}));
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest, permutationsWithManyBlocks) {
  // One relation that is large enough to span several blocks on its own, plus
  // several small relations, which together give every permutation many blocks.
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
  auto numBlocks = convertAndExpectTheSameContent(turtle);
  EXPECT_THAT(numBlocks, ::testing::Each(::testing::Gt(size_t{2})));
}

// _____________________________________________________________________________
TEST_F(MultiBlockIndexFormatConverterTest, convertIndexWithOnlyPsoAndPos) {
  // An index built with `--only-pso-and-pos-permutations` has only two of the
  // six normal permutations and no patterns. The conversion has to skip the
  // pairs that the index does not have, and must not write them.
  std::string turtle;
  for (size_t object = 0; object < 10; ++object) {
    absl::StrAppend(&turtle,
                    "<http://example.org/s> <http://example.org/p> "
                    "<http://example.org/o",
                    object, "> .\n");
  }
  auto numBlocks = convertAndExpectTheSameContent(turtle, false);
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
  // that they have no blocks at all, a case that the conversion has to handle
  // separately: there is no block from which it could read the number of
  // columns of the permutation (see `getNumColumns` in
  // `IndexFormatConverter.cpp`), and there is no first and last triple that it
  // could compare (see `verifyConvertedPermutation` there).
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
  // callback of `writePermutation`, and with the default block size of the
  // conversion, a large relation has more than 25000 rows, which is far too
  // much for a unit test. The conversion is thus run with a block size of two
  // triples per block, which is the same block size that the index that is
  // converted is built with (see `convertAndExpectTheSameContent` above). A
  // relation with two rows then already is large enough.
  //
  // Two rows per block means two `Id`s (16 bytes each in the current format)
  // per column per block, i.e. 32 bytes, matching the default
  // `blocksizePermutations` of `TestIndexConfig` (see `IndexTestHelpers.h`).
  ad_utility::MemorySize previousBlocksize = blocksizeOfConvertedPermutations();
  blocksizeOfConvertedPermutations() = 32_B;
  absl::Cleanup restoreBlocksize = [previousBlocksize]() {
    blocksizeOfConvertedPermutations() = previousBlocksize;
  };

  // The subject `<big>` has two triples, so it is a large relation in the `SPO`
  // permutation, and the subject `<small>` has one triple, so it stays a small
  // relation there.
  std::string turtle =
      "<http://example.org/big> <http://example.org/p> <http://example.org/o1> "
      ".\n"
      "<http://example.org/big> <http://example.org/p> <http://example.org/o2> "
      ".\n"
      "<http://example.org/small> <http://example.org/p> "
      "<http://example.org/o1> .\n";
  convertAndExpectTheSameContent(turtle);

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
  EXPECT_THAT(description, HasSubstr("secondary vocabulary"));
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
