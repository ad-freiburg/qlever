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

// Test that a config is read from a file and round-trips through YAML, that
// missing keys get their defaults, and that invalid configs are rejected with a
// descriptive message.
TEST(BlobDiffConfig, parsing) {
  auto config = BlobDiffConfig::fromFile(
      absl::StrCat(QLEVER_LIBQLEVER_TEST_DATA_DIR, "/snapshot-config.yaml"));
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

  // The YAML representation round-trips.
  auto roundTrip = BlobDiffConfig::fromYaml(config.toYaml());
  EXPECT_EQ(roundTrip.toYaml(), config.toYaml());
  EXPECT_EQ(roundTrip.fingerprint(), config.fingerprint());

  // The minimal config has the defaults.
  auto minimal = BlobDiffConfig::fromYaml("namedQueries: []");
  EXPECT_EQ(minimal.vocabularyType_,
            ad_utility::VocabularyType::InMemoryCompressed);
  EXPECT_EQ(minimal.numThreads_, std::nullopt);
  EXPECT_TRUE(minimal.namedQueries_.empty());

  // A key without a value is treated like a missing key.
  auto withNulls = BlobDiffConfig::fromYaml(
      "index:\n  numThreads:\nblob:\nnamedQueries: []");
  EXPECT_EQ(withNulls.numThreads_, std::nullopt);
  EXPECT_EQ(withNulls.diffCompressionLevel_,
            blobDiff::defaultBlobDiffCompressionLevel);

  auto parse = [](std::string_view yaml) {
    return BlobDiffConfig::fromYaml(yaml);
  };
  auto query = [](std::string_view extra = "", std::string_view name = "a") {
    return absl::StrCat("namedQueries:\n  - name: ", name,
                        "\n    query: SELECT * { ?s ?p ?o }\n", extra);
  };

  // Invalid named queries are rejected.
  EXPECT_NO_THROW(parse(query("    geoIndexVar: \"?o\"\n")));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(R"(namedQueries:
  - name: a
    query: SELECT * { ?s ?p ?o }
  - name: a
    query: SELECT * { ?s ?p ?o }
)"),
                               AllOf(HasSubstr("Invalid blob diff config"),
                                     HasSubstr("`a` is used twice")));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(query("    geoIndexVar: o\n")),
                               HasSubstr("starts with `?`"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(query("    geoIndexVar: \"?a b\"\n")),
                               HasSubstr("has to be a valid variable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(query("    geoIndexVar: \"?o\"\n    simplificationMeters: 0\n")),
      HasSubstr("has to be positive and finite"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(query("    geoIndexVar: \"?o\"\n    simplificationMeters: .inf\n")),
      HasSubstr("has to be positive and finite"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(query("    simplificationMeters: 1.5\n")),
                               HasSubstr("but no `geoIndexVar`"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(query("", "bad name")),
                               HasSubstr("letters, digits"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(query("    typo: 1\n")),
                               HasSubstr("unknown key `typo`"));

  // A config with the wrong structure is rejected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("namedQueries:\n  - [a, b]\n"),
      HasSubstr("`namedQueries[]` has to be a YAML map"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("index: {}"),
      HasSubstr("`namedQueries` has to be a YAML sequence"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse("- a\n- b\n"),
                               HasSubstr("`<root>` has to be a YAML map"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse("{[a, b]: 1, namedQueries: []}"),
                               HasSubstr("has a key that is not a string"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse("namedQueries: [unterminated"),
                               HasSubstr("the config is not valid YAML"));

  // Duplicate keys and more than one YAML document are rejected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("namedQueries: []\nnamedQueries: []\n"),
      HasSubstr("the key `namedQueries` occurs twice in `<root>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("index:\n  numThreads: 1\n  numThreads: 2\nnamedQueries: []"),
      HasSubstr("the key `numThreads` occurs twice in `index`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(query("    name: b\n")),
      HasSubstr("the key `name` occurs twice in `namedQueries[]`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("namedQueries: []\n---\nnamedQueries: []\n"),
      HasSubstr("exactly one YAML document, but consists of 2"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(""), HasSubstr("exactly one YAML document, but consists of 0"));

  // Invalid values in the `index` section are rejected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("index:\n  vocabularyType: on-disk-compressed\nnamedQueries: []"),
      AllOf(HasSubstr("cannot be serialized into a blob"),
            HasSubstr("in-memory-compressed")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("index:\n  vocabularyType: nonsense\nnamedQueries: []"),
      HasSubstr("index.vocabularyType"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("index:\n  numThreads: four\nnamedQueries: []"),
      HasSubstr("`index.numThreads` has the wrong type"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("index:\n  numThreads: -1\nnamedQueries: []"),
      HasSubstr("`index.numThreads` has the wrong type"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("index:\n  numThreads: 0\nnamedQueries: []"),
      HasSubstr("`index.numThreads` has to be positive"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("index:\n  blankNodeIriRegexes: abc\nnamedQueries: []"),
      HasSubstr("`index.blankNodeIriRegexes` has the wrong type"));

  // The ranges of the geo compaction and of the diff compression level are
  // checked.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("geoCompaction:\n  maxDeadShapeRatio: 2\nnamedQueries: []"),
      HasSubstr("maxDeadShapeRatio"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("geoCompaction:\n  maxSegments: 0\nnamedQueries: []"),
      HasSubstr("`geoCompaction.maxSegments` has to be positive"));
  EXPECT_EQ(parse("blob:\n  diffCompressionLevel: 5\nnamedQueries: []")
                .diffCompressionLevel_,
            5);
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("blob:\n  diffCompressionLevel: 0\nnamedQueries: []"),
      HasSubstr("diffCompressionLevel"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse("blob:\n  diffCompressionLevel: 23\nnamedQueries: []"),
      HasSubstr("diffCompressionLevel"));

  // A file that is not YAML, and a file that does not exist, are rejected.
  std::string filename = absl::StrCat(gtestCurrentTestName(), ".yaml");
  {
    auto stream = ad_utility::makeOfstream(filename);
    stream << "namedQueries: [unterminated";
  }
  absl::Cleanup cleanup = [&filename] { ad_utility::deleteFile(filename); };
  AD_EXPECT_THROW_WITH_MESSAGE(
      BlobDiffConfig::fromFile(filename),
      HasSubstr(absl::StrCat("the file `", filename, "` is not valid YAML")));
  EXPECT_ANY_THROW(BlobDiffConfig::fromFile(filename + ".missing"));
}

// Test that the fingerprint covers exactly the settings that influence the
// content of the blob, including the content (but not the path) of the
// settings file.
TEST(BlobDiffConfig, fingerprint) {
  auto config = BlobDiffConfig::fromFile(
      absl::StrCat(QLEVER_LIBQLEVER_TEST_DATA_DIR, "/snapshot-config.yaml"));
  const auto original = config.fingerprint();

  // The settings that do not influence the content of the blob are not part of
  // the fingerprint, all others are.
  auto changed = config;
  changed.numThreads_ = 1;
  changed.diffCompressionLevel_ = 3;
  changed.maxGeoSegments_ = 2;
  changed.maxDeadShapeRatio_ = 0.9;
  EXPECT_EQ(changed.fingerprint(), original);
  changed.namedQueries_[0].query_ = "SELECT * { ?s ?p ?o }";
  EXPECT_NE(changed.fingerprint(), original);

  // Without a settings file, the settings are an empty JSON object.
  EXPECT_EQ(config.readSettingsJson(), nlohmann::json::object());

  // The content of the settings file is part of the fingerprint, but neither
  // its path nor its formatting.
  std::string base = gtestCurrentTestName();
  std::string settingsFile = base + ".settings.json";
  std::string otherSettingsFile = base + ".other.settings.json";
  absl::Cleanup cleanup = [&settingsFile, &otherSettingsFile] {
    ad_utility::deleteFile(settingsFile);
    ad_utility::deleteFile(otherSettingsFile);
  };
  ad_utility::makeOfstream(settingsFile) << R"({"a": 1, "b": [2]})";
  ad_utility::makeOfstream(otherSettingsFile) << "{ \"b\" : [ 2 ],\n\"a\":1 }";
  auto withSettings = config;
  withSettings.settingsFile_ = settingsFile;
  EXPECT_EQ(withSettings.readSettingsJson(),
            (nlohmann::json{{"a", 1}, {"b", {2}}}));
  const auto withSettingsFingerprint = withSettings.fingerprint();
  EXPECT_NE(withSettingsFingerprint, original);
  withSettings.settingsFile_ = otherSettingsFile;
  EXPECT_EQ(withSettings.fingerprint(), withSettingsFingerprint);
  ad_utility::makeOfstream(otherSettingsFile) << R"({"a": 2, "b": [2]})";
  EXPECT_NE(withSettings.fingerprint(), withSettingsFingerprint);

  // A settings file that is not valid JSON, and one that does not exist, are
  // rejected.
  ad_utility::makeOfstream(otherSettingsFile) << "{";
  AD_EXPECT_THROW_WITH_MESSAGE(withSettings.fingerprint(),
                               HasSubstr("is not valid JSON"));
  withSettings.settingsFile_ = base + ".missing.json";
  EXPECT_ANY_THROW(withSettings.readSettingsJson());
}
