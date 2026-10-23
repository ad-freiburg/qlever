// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <string>
#include <string_view>

#include "../../util/GTestHelpers.h"
#include "libqlever/blobDiff/BlobDiffConfig.h"
#include "util/File.h"

using namespace qlever;
using namespace testing;

// _____________________________________________________________________________
TEST(BlobDiffConfig, parsing) {
  auto config = BlobDiffConfig::fromFile(
      absl::StrCat(QLEVER_LIBQLEVER_TEST_DATA_DIR, "/snapshot-config.json"));
  EXPECT_EQ(config.vocabularyType_,
            ad_utility::VocabularyType::InMemoryCompressed);
  EXPECT_EQ(config.numThreads_, 4u);
  EXPECT_EQ(config.maxGeoSegments_, 8u);
  EXPECT_DOUBLE_EQ(config.maxDeadShapeRatio_, 0.3);
  EXPECT_EQ(config.diffCompressionLevel_,
            blobDiff::defaultBlobDiffCompressionLevel);
  ASSERT_EQ(config.namedQueries_.size(), 5u);
  EXPECT_EQ(config.namedQueries_[2].name_, "streetGeometries");
  EXPECT_EQ(config.namedQueries_[2].geoIndexVar_, "?wkt");
  EXPECT_EQ(config.namedQueries_[2].simplificationMeters_, std::nullopt);
  EXPECT_EQ(config.namedQueries_[3].simplificationMeters_, 0.5);
  // The JSON representation round-trips.
  auto roundTrip = BlobDiffConfig::fromJson(config.toJson());
  EXPECT_EQ(roundTrip.toJson(), config.toJson());
  EXPECT_EQ(roundTrip.fingerprint(), config.fingerprint());

  // The minimal config has the defaults.
  auto minimal = BlobDiffConfig::fromJson(
      nlohmann::json::parse(R"({"namedQueries": []})"));
  EXPECT_EQ(minimal.vocabularyType_,
            ad_utility::VocabularyType::InMemoryCompressed);
  EXPECT_EQ(minimal.numThreads_, std::nullopt);
  EXPECT_TRUE(minimal.namedQueries_.empty());

  auto parse = [](std::string_view json) {
    return BlobDiffConfig::fromJson(nlohmann::json::parse(json));
  };
  auto query = [](std::string_view extra = "", std::string_view name = "a") {
    return absl::StrCat(R"({"namedQueries": [{"name": ")", name,
                        R"(", "query": "SELECT * { ?s ?p ?o }")", extra, "}]}");
  };
  EXPECT_NO_THROW(parse(query(R"(, "geoIndexVar": "?o")")));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(R"({"namedQueries": [
        {"name": "a", "query": "SELECT * { ?s ?p ?o }"},
        {"name": "a", "query": "SELECT * { ?s ?p ?o }"}]})"),
                               AllOf(HasSubstr("Invalid blob diff config"),
                                     HasSubstr("`a` is used twice")));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(query(R"(, "geoIndexVar": "o")")),
                               HasSubstr("starts with `?`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(query(R"(, "geoIndexVar": "?o", "simplificationMeters": 0)")),
      HasSubstr("has to be positive"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(query(R"(, "simplificationMeters": 1.5)")),
                               HasSubstr("but no `geoIndexVar`"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(query("", "bad name")),
                               HasSubstr("letters, digits"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(query(R"(, "typo": 1)")),
                               HasSubstr("unknown key `typo`"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(R"({"index": {}})"),
                               HasSubstr("`namedQueries` has to be an array"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(R"({"index": {"vocabularyType": "on-disk-compressed"},
                "namedQueries": []})"),
      AllOf(HasSubstr("cannot be serialized into a blob"),
            HasSubstr("in-memory-compressed")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(R"({"index": {"vocabularyType": "nonsense"}, "namedQueries": []})"),
      HasSubstr("index.vocabularyType"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(R"({"index": {"numThreads": "four"}, "namedQueries": []})"),
      HasSubstr("`index.numThreads` has the wrong type"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(R"({"geoCompaction": {"maxDeadShapeRatio": 2},
                "namedQueries": []})"),
      HasSubstr("maxDeadShapeRatio"));
  EXPECT_EQ(
      parse(R"({"blob": {"diffCompressionLevel": 5}, "namedQueries": []})")
          .diffCompressionLevel_,
      5);
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(R"({"blob": {"diffCompressionLevel": 0}, "namedQueries": []})"),
      HasSubstr("diffCompressionLevel"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(R"({"blob": {"diffCompressionLevel": 23}, "namedQueries": []})"),
      HasSubstr("diffCompressionLevel"));

  // A file that is not JSON.
  std::string filename = absl::StrCat(gtestCurrentTestName(), ".json");
  {
    auto stream = ad_utility::makeOfstream(filename);
    stream << "{ not json";
  }
  absl::Cleanup cleanup = [&filename] { ad_utility::deleteFile(filename); };
  AD_EXPECT_THROW_WITH_MESSAGE(BlobDiffConfig::fromFile(filename),
                               HasSubstr("is not valid JSON"));
  EXPECT_ANY_THROW(BlobDiffConfig::fromFile(filename + ".missing"));
}
