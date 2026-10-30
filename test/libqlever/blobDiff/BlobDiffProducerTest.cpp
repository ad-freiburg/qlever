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
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <fstream>
#include <iostream>
#include <iterator>
#include <regex>
#include <set>
#include <string>
#include <variant>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/BlobDiff.h"
#include "libqlever/blobDiff/BlobDiffProducer.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"
#include "util/SourceLocation.h"
#include "util/json.h"

using namespace qlever;
using namespace testing;
using qlever::util::readBinaryFile;

namespace {
namespace fs = ql::filesystem;

// The path of a file in the directory with the test data.
std::string dataFile(std::string_view name) {
  return absl::StrCat(QLEVER_LIBQLEVER_TEST_DATA_DIR, "/", name);
}

// The Turtle file of the weekly snapshot `week` (0 to 3) of the synthetic
// street network (see `data/snapshot_v*.ttl`).
std::string snapshotFile(size_t week) {
  return dataFile(absl::StrCat("snapshot_v", week, ".ttl"));
}

// The config that is used by all the tests.
BlobDiffConfig snapshotConfig() {
  return BlobDiffConfig::fromFile(dataFile("snapshot-config.yaml"));
}

// The same config, but without any automatic compaction of the geo indices, so
// that the sizes of the diffs do not depend on the compaction policy.
BlobDiffConfig snapshotConfigWithoutCompaction() {
  auto config = snapshotConfig();
  config.maxGeoSegments_ = 100;
  config.maxDeadShapeRatio_ = 1.0;
  return config;
}

// Return the name of a directory that is named after the running test, and
// make sure that it does not exist. `suffix` allows for several directories.
std::string freshDirectory(std::string_view suffix = "") {
  std::string result = absl::StrCat(gtestCurrentTestName(), suffix);
  fs::remove_all(result);
  return result;
}

// Read the whole file `path` as a string.
std::string readTextFile(const std::string& path) {
  auto bytes = readBinaryFile(path);
  return {bytes.begin(), bytes.end()};
}

// Return the lines of `text` in sorted order.
std::vector<std::string> sortedLines(std::string_view text) {
  std::vector<std::string> lines = absl::StrSplit(text, '\n');
  ql::ranges::sort(lines);
  return lines;
}

// Expect that `actual` and `expected` consist of the same lines (in any order,
// but with the same multiplicities). On a mismatch, report the lines that are
// only in one of them (the first few of them, abbreviated).
void expectSameLines(std::string_view actual, std::string_view expected,
                     std::string_view context) {
  auto actualLines = sortedLines(actual);
  auto expectedLines = sortedLines(expected);
  if (actualLines == expectedLines) {
    return;
  }
  auto difference = [](const std::vector<std::string>& a,
                       const std::vector<std::string>& b) {
    std::vector<std::string> result;
    ql::ranges::set_difference(a, b, std::back_inserter(result));
    if (result.size() > 5) {
      result.resize(5);
    }
    for (auto& line : result) {
      line = line.substr(0, 250);
    }
    return result;
  };
  ADD_FAILURE()
      << context << ": the answers differ (" << actualLines.size() << " vs "
      << expectedLines.size() << " lines). Only in the actual answer: "
      << ::testing::PrintToString(difference(actualLines, expectedLines))
      << ". Only in the expected answer: "
      << ::testing::PrintToString(difference(expectedLines, actualLines));
}

// Load the compressed blob into a fresh `Qlever` instance without index.
std::unique_ptr<Qlever> loadBlob(ql::span<const char> compressedBlob) {
  auto result = std::make_unique<Qlever>(EngineConfig{}, /*skipLoading=*/true);
  EXPECT_NO_THROW(
      result->deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob));
  return result;
}

// The variables that are selected by `query` (separated by spaces), e.g.
// `?street ?wkt`.
std::string selectedVariables(const std::string& query) {
  std::smatch match;
  EXPECT_TRUE(std::regex_search(query, match,
                                std::regex{R"(SELECT\s+((?:\?\w+\s+)+)WHERE)"}))
      << query;
  return match[1];
}

// The query that reads all columns of the pinned result of `query`.
std::string readPinnedQuery(const BlobDiffConfig::NamedQuery& query) {
  return absl::StrCat("SELECT ", selectedVariables(query.query_),
                      "WHERE { SERVICE ql:cached-result-with-name-",
                      query.name_, " {} }");
}

// The variable that is selected first by `query`, e.g. `?street`.
std::string firstSelectedVariable(const std::string& query) {
  auto variables = selectedVariables(query);
  return variables.substr(0, variables.find(' '));
}

// A spatial join of the single `point` (given as `x y`) with the geo index of
// the pinned result `name`, which has the geometries in `geoVar`. Return the
// variable `selected`.
std::string spatialJoinQuery(const BlobDiffConfig::NamedQuery& query,
                             std::string_view point, size_t maxDistance) {
  return absl::StrCat(
      "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/> "
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#> SELECT ",
      firstSelectedVariable(query.query_), " WHERE { VALUES ?geo1 { \"POINT(",
      point, ")\"^^geo:wktLiteral } SERVICE qlss: { _:config qlss:right ",
      query.geoIndexVar_.value(), " ; qlss:left ?geo1 ; qlss:maxDistance ",
      maxDistance,
      " ; qlss:algorithm qlss:experimentalPointPolyline ; "
      "qlss:experimentalRightCacheName \"",
      query.name_, "\" . } }");
}

// The first vertex of the first geometry of each of the weekly snapshots (as
// `x y`), and one vertex from the middle of the first snapshot.
std::vector<std::string> samplePoints() {
  std::vector<std::string> result;
  std::regex vertex{R"(LINESTRING\(([0-9.]+) ([0-9.]+)[,)])"};
  for (size_t week = 0; week < 4; ++week) {
    auto text = readTextFile(snapshotFile(week));
    std::smatch match;
    EXPECT_TRUE(std::regex_search(text, match, vertex));
    result.push_back(absl::StrCat(match[1].str(), " ", match[2].str()));
    if (week == 0) {
      auto it = std::sregex_iterator(text.begin(), text.end(), vertex);
      std::advance(it, 5);
      result.push_back(absl::StrCat((*it)[1].str(), " ", (*it)[2].str()));
    }
  }
  return result;
}

// Expect that the two compressed blobs yield the same answers (as sets of
// lines) for the contents of all named queries and for spatial joins with the
// sample points on all geo indices.
void expectSameAnswers(const BlobDiffConfig& config,
                       ql::span<const char> actualBlob,
                       ql::span<const char> expectedBlob,
                       std::string_view context) {
  auto actual = loadBlob(actualBlob);
  auto expected = loadBlob(expectedBlob);
  size_t numNonEmptySpatialAnswers = 0;
  for (const auto& query : config.namedQueries_) {
    auto read = readPinnedQuery(query);
    expectSameLines(actual->query(read, ad_utility::MediaType::tsv),
                    expected->query(read, ad_utility::MediaType::tsv),
                    absl::StrCat(context, ", named query ", query.name_));
    if (!query.geoIndexVar_.has_value()) {
      continue;
    }
    for (const auto& point : samplePoints()) {
      auto spatial = spatialJoinQuery(query, point, 300);
      auto expectedSpatial =
          expected->query(spatial, ad_utility::MediaType::tsv);
      // The header line and the final empty line are always there.
      numNonEmptySpatialAnswers += sortedLines(expectedSpatial).size() > 2;
      expectSameLines(actual->query(spatial, ad_utility::MediaType::tsv),
                      expectedSpatial,
                      absl::StrCat(context, ", spatial join with ", query.name_,
                                   " at ", point));
    }
  }
  // The comparison would be meaningless if all answers were empty.
  EXPECT_GT(numNonEmptySpatialAnswers, 0u) << context;
}

// The result of `init` and of all weekly steps.
struct Weeks {
  // `blobs_[k]` is the blob of week `k`, and `diffs_[k]` the diff file that
  // turns `blobs_[k - 1]` into it (empty for `k == 0`).
  std::vector<std::vector<char>> blobs_;
  std::vector<std::vector<char>> diffs_;
  std::vector<std::string> statistics_;
  std::vector<size_t> numInserted_;
  std::vector<size_t> numDeleted_;
};

// Apply the `diffFile` to the compressed `baseBlob` and return the compressed
// result (fail the test on error).
std::vector<char> applyOrFail(
    ql::span<const char> baseBlob, ql::span<const char> diffFile,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto result = blobDiff::applyBlobDiffToCompressedBase(baseBlob, diffFile);
  if (const auto* error = std::get_if<blobDiff::BlobDiffError>(&result)) {
    ADD_FAILURE() << error->message_;
    return {};
  }
  return std::get<std::vector<char>>(std::move(result));
}

// Run `init` with the first and `step` with the following weekly snapshots
// (up to and including `lastWeek`), and check the self-consistency of every
// step.
Weeks runWeeks(const BlobDiffConfig& config, const std::string& stateDir,
               size_t lastWeek, std::set<size_t> compactedWeeks = {}) {
  Weeks weeks;
  weeks.blobs_.push_back(
      BlobDiffProducer::init(config, snapshotFile(0), stateDir));
  weeks.diffs_.emplace_back();
  weeks.statistics_.emplace_back();
  weeks.numInserted_.push_back(0);
  weeks.numDeleted_.push_back(0);
  EXPECT_EQ(readBinaryFile(absl::StrCat(stateDir, "/current.blob")),
            weeks.blobs_[0]);
  for (size_t week = 1; week <= lastWeek; ++week) {
    auto result = BlobDiffProducer::step(config, snapshotFile(week), stateDir,
                                         compactedWeeks.contains(week));
    EXPECT_EQ(applyOrFail(weeks.blobs_.back(), result.diffFile_), result.blob_)
        << "week " << week;
    EXPECT_EQ(readBinaryFile(absl::StrCat(stateDir, "/current.blob")),
              result.blob_);
    std::cout << "Week " << week << ": compressed blob " << result.blob_.size()
              << " bytes, diff file " << result.diffFile_.size() << " bytes, "
              << result.numInsertedTriples_ << " triples inserted, "
              << result.numDeletedTriples_ << " triples deleted\n"
              << result.statistics_ << std::endl;
    weeks.blobs_.push_back(std::move(result.blob_));
    weeks.diffs_.push_back(std::move(result.diffFile_));
    weeks.statistics_.push_back(std::move(result.statistics_));
    weeks.numInserted_.push_back(result.numInsertedTriples_);
    weeks.numDeleted_.push_back(result.numDeletedTriples_);
  }
  return weeks;
}

// Decompress the compressed `blob` (fail the test on error).
std::vector<char> decompress(ql::span<const char> blob) {
  auto result = NamedCachedQueryBlobManager::tryToDecompressBlob(blob, {});
  if (const auto* error =
          std::get_if<NamedCachedQueryBlobManager::BlobError>(&result)) {
    ADD_FAILURE() << error->message_;
    return {};
  }
  const auto& bytes = std::get<0>(result);
  return {bytes.begin(), bytes.end()};
}

// The ratio of the size of the diff file to the size of the compressed blob of
// the `week`.
double diffRatio(const Weeks& weeks, size_t week) {
  return static_cast<double>(weeks.diffs_[week].size()) /
         static_cast<double>(weeks.blobs_[week].size());
}

// The number of segments of the geo index of the pinned result `name`.
size_t numGeoSegments(const Qlever& qlever, std::string_view name) {
  auto entry = qlever.namedResultCache().get(std::string{name});
  AD_CORRECTNESS_CHECK(entry != nullptr && entry->cachedGeoIndex_.has_value());
  return entry->cachedGeoIndex_->numSegments();
}
}  // namespace

// _____________________________________________________________________________
TEST(BlobDiffProducer, weeklyStepsAreSelfConsistentAndEquivalentToFreshBuilds) {
  auto config = snapshotConfig();
  std::string stateDir = freshDirectory();
  std::string referenceDir = freshDirectory("_reference");
  absl::Cleanup cleanup = [&] {
    fs::remove_all(stateDir);
    fs::remove_all(referenceDir);
  };
  auto weeks = runWeeks(config, stateDir, 3);

  // The `inspect` function describes the blob and the diff.
  auto description = BlobDiffProducer::inspect(
      weeks.blobs_[3], ql::span<const char>{weeks.diffs_[3]});
  EXPECT_THAT(description, HasSubstr("Blob: "));
  EXPECT_THAT(description, HasSubstr("SHA-256"));
  EXPECT_THAT(description, HasSubstr("The diff yields this blob."));
  EXPECT_THAT(BlobDiffProducer::inspect(weeks.blobs_[2],
                                        ql::span<const char>{weeks.diffs_[3]}),
              HasSubstr("The diff applies to this blob."));
  EXPECT_THAT(BlobDiffProducer::inspect(weeks.blobs_[0],
                                        ql::span<const char>{weeks.diffs_[3]}),
              HasSubstr("The diff is unrelated to this blob."));
  EXPECT_THAT(BlobDiffProducer::inspect(weeks.blobs_[3], std::nullopt),
              Not(HasSubstr("The diff")));
  // A diff file with an invalid header is reported, but does not throw.
  std::vector<char> garbage{'n', 'o', ' ', 'd', 'i', 'f', 'f'};
  auto garbageDescription =
      BlobDiffProducer::inspect(weeks.blobs_[3], ql::span<const char>{garbage});
  EXPECT_THAT(garbageDescription, HasSubstr("Blob: "));
  EXPECT_THAT(garbageDescription, HasSubstr("The diff file is invalid"));

  // The diff of a week only applies to the blob of the week before.
  auto wrongBase =
      blobDiff::applyBlobDiffToCompressedBase(weeks.blobs_[0], weeks.diffs_[3]);
  ASSERT_TRUE(std::holds_alternative<blobDiff::BlobDiffError>(wrongBase));
  EXPECT_EQ(std::get<blobDiff::BlobDiffError>(wrongBase).type_,
            blobDiff::BlobDiffErrorType::wrongBase);

  // Each blob answers the same as the blob of a freshly built index of the
  // same snapshot, which was serialized without a base.
  for (size_t week = 0; week <= 3; ++week) {
    auto reference = BlobDiffProducer::init(
        config, snapshotFile(week), absl::StrCat(referenceDir, "/week", week));
    expectSameAnswers(config, weeks.blobs_[week], reference,
                      absl::StrCat("week ", week));
  }

  // The data changes in every week.
  for (size_t week = 1; week <= 3; ++week) {
    EXPECT_GT(weeks.numInserted_[week], 0u);
    EXPECT_GT(weeks.numDeleted_[week], 0u);
  }
}

// _____________________________________________________________________________
TEST(BlobDiffProducer, diffsAreSmallAndIdsAreStable) {
  auto config = snapshotConfigWithoutCompaction();
  std::string stateDir = freshDirectory();
  std::string referenceDir = freshDirectory("_reference");
  absl::Cleanup cleanup = [&] {
    fs::remove_all(stateDir);
    fs::remove_all(referenceDir);
  };
  auto weeks = runWeeks(config, stateDir, 3);

  for (size_t week = 1; week <= 3; ++week) {
    std::cout << "Week " << week << ": diff file " << weeks.diffs_[week].size()
              << " of " << weeks.blobs_[week].size() << " compressed bytes ("
              << diffRatio(weeks, week) << ")" << std::endl;
  }
  // The observed ratios (week 1 to 3) are about 0.13, 0.28, and 0.04: the
  // blob is small, so the cache keys and the maps of the entries (which change
  // every week) are a noticeable part of the diff, and week 2 replaces about
  // half of the data. The bounds have about twice the observed values.
  EXPECT_LT(diffRatio(weeks, 1), 0.3);
  EXPECT_LT(diffRatio(weeks, 2), 0.6);
  EXPECT_LT(diffRatio(weeks, 3), 0.08);

  // The diffs are only small because the blobs are written incrementally,
  // which keeps the `Id`s of all words and the segments of the secondary
  // vocabulary and of the geo indices stable: the diff between the blob of
  // week 2 and a freshly built blob of week 3 (same data, but the words of
  // all weeks are in the main vocabulary) is many times larger (observed:
  // about six times).
  auto reference =
      BlobDiffProducer::init(config, snapshotFile(3), referenceDir);
  auto freshDiff = blobDiff::computeBlobDiff(decompress(weeks.blobs_[2]),
                                             decompress(reference));
  std::cout << "Diff to a fresh build of week 3: " << freshDiff.size()
            << " bytes, incremental: " << weeks.diffs_[3].size() << " bytes"
            << std::endl;
  EXPECT_GT(freshDiff.size(), 3 * weeks.diffs_[3].size());
}

// _____________________________________________________________________________
TEST(BlobDiffProducer, stepWithoutChangesGivesTinyDiff) {
  auto config = snapshotConfig();
  std::string stateDir = freshDirectory();
  absl::Cleanup cleanup = [&] { fs::remove_all(stateDir); };
  auto weeks = runWeeks(config, stateDir, 1);
  auto unchanged =
      BlobDiffProducer::step(config, snapshotFile(1), stateDir, false);
  EXPECT_EQ(unchanged.numInsertedTriples_, 0u);
  EXPECT_EQ(unchanged.numDeletedTriples_, 0u);
  std::cout << "Diff file for no changes: " << unchanged.diffFile_.size()
            << " bytes for a blob of " << unchanged.blob_.size() << " bytes"
            << std::endl;
  // Observed: about 120 bytes, mostly the header of the diff file.
  EXPECT_LT(unchanged.diffFile_.size(), 1000u);
  EXPECT_EQ(decompress(unchanged.blob_), decompress(weeks.blobs_[1]));
}

// _____________________________________________________________________________
// A literal that only changes its lexical form but not its value is neither
// inserted nor deleted (its `Id` is the same), whereas a change of the value or
// of the type is.
TEST(BlobDiffProducer, lexicalChangeOfLiteralIsNoDelta) {
  auto config = BlobDiffConfig::fromYaml(R"(
index:
  numThreads: 2
namedQueries:
  - name: all
    query: SELECT ?s ?o WHERE { ?s <p> ?o }
)");
  std::string stateDir = freshDirectory();
  std::string base = gtestCurrentTestName();
  std::vector<std::string> files;
  absl::Cleanup cleanup = [&] {
    fs::remove_all(stateDir);
    for (const auto& file : files) {
      ad_utility::deleteFile(file);
    }
  };
  auto writeTurtle = [&](std::string_view suffix, std::string_view content) {
    std::string file = absl::StrCat(base, "-", suffix, ".ttl");
    files.push_back(file);
    auto stream = ad_utility::makeOfstream(file);
    stream << content;
    return file;
  };
  constexpr std::string_view xsd = "^^<http://www.w3.org/2001/XMLSchema#";
  auto turtle = [&](std::string_view intLiteral,
                    std::string_view doubleLiteral) {
    return absl::StrCat("<a> <p> \"", intLiteral, "\"", xsd,
                        "int> .\n<a> <p> \"", doubleLiteral, "\"", xsd,
                        "double> .\n<b> <p> <c> .\n");
  };
  BlobDiffProducer::init(config, writeTurtle("week0", turtle("1", "1.0")),
                         stateDir);

  // Only the lexical forms change.
  auto lexical = BlobDiffProducer::step(
      config, writeTurtle("week1", turtle("01", "1.0e0")), stateDir);
  EXPECT_EQ(lexical.numInsertedTriples_, 0u);
  EXPECT_EQ(lexical.numDeletedTriples_, 0u);

  // A change of the value.
  auto changed = BlobDiffProducer::step(
      config, writeTurtle("week2", turtle("2", "1.0e0")), stateDir);
  EXPECT_EQ(changed.numInsertedTriples_, 1u);
  EXPECT_EQ(changed.numDeletedTriples_, 1u);
  EXPECT_EQ(applyOrFail(lexical.blob_, changed.diffFile_), changed.blob_);

  // A change of the type with the same number (`2` to `2.0`) is a change as
  // well.
  auto retyped = BlobDiffProducer::step(
      config,
      writeTurtle("week3", absl::StrCat("<a> <p> \"2.0\"", xsd,
                                        "double> .\n<a> <p> \"1.0e0\"", xsd,
                                        "double> .\n<b> <p> <c> .\n")),
      stateDir);
  EXPECT_EQ(retyped.numInsertedTriples_, 1u);
  EXPECT_EQ(retyped.numDeletedTriples_, 1u);
  EXPECT_EQ(applyOrFail(changed.blob_, retyped.diffFile_), retyped.blob_);
}

// _____________________________________________________________________________
TEST(BlobDiffProducer, geoIndicesAreSegmentedAndCanBeCompacted) {
  auto config = snapshotConfigWithoutCompaction();
  std::string stateDir = freshDirectory();
  absl::Cleanup cleanup = [&] { fs::remove_all(stateDir); };
  auto weeks = runWeeks(config, stateDir, 2);
  const std::vector<std::string> geoEntries{"streetGeometries", "districts"};

  // Without compaction, each week adds a segment (every snapshot has new
  // geometries in every week), and the old segments stay.
  auto afterWeek0 = loadBlob(weeks.blobs_[0]);
  auto afterWeek1 = loadBlob(weeks.blobs_[1]);
  auto afterWeek2 = loadBlob(weeks.blobs_[2]);
  for (const auto& name : geoEntries) {
    EXPECT_EQ(numGeoSegments(*afterWeek0, name), 1u) << name;
    EXPECT_EQ(numGeoSegments(*afterWeek1, name), 2u) << name;
    EXPECT_EQ(numGeoSegments(*afterWeek2, name), 3u) << name;
  }

  // A forced compaction rebuilds each geo index as a single segment, and the
  // blob still answers like a fresh build of the same snapshot.
  auto compacted =
      BlobDiffProducer::step(config, snapshotFile(3), stateDir, true);
  auto afterCompaction = loadBlob(compacted.blob_);
  for (const auto& name : geoEntries) {
    EXPECT_EQ(numGeoSegments(*afterCompaction, name), 1u) << name;
  }
  EXPECT_EQ(applyOrFail(weeks.blobs_[2], compacted.diffFile_), compacted.blob_);
  std::string referenceDir = freshDirectory("_reference");
  absl::Cleanup cleanupReference = [&] { fs::remove_all(referenceDir); };
  auto reference =
      BlobDiffProducer::init(config, snapshotFile(3), referenceDir + "/week3");
  expectSameAnswers(config, compacted.blob_, reference, "compacted week 3");
  std::cout << "Compacted diff: " << compacted.diffFile_.size()
            << " bytes for a blob of " << compacted.blob_.size() << " bytes\n"
            << compacted.statistics_ << std::endl;

  // Afterwards the next step extends the compact indices again.
  auto next = BlobDiffProducer::step(config, snapshotFile(2), stateDir, false);
  auto afterNext = loadBlob(next.blob_);
  for (const auto& name : geoEntries) {
    EXPECT_EQ(numGeoSegments(*afterNext, name), 2u) << name;
  }
  expectSameAnswers(
      config, next.blob_,
      BlobDiffProducer::init(config, snapshotFile(2), referenceDir + "/week2"),
      "week 2 again");
}

// _____________________________________________________________________________
TEST(BlobDiffProducer, compactionPolicyOfTheConfigIsApplied) {
  std::string stateDir = freshDirectory();
  absl::Cleanup cleanup = [&] { fs::remove_all(stateDir); };
  const std::vector<std::string> geoEntries{"streetGeometries", "districts"};

  // Compaction because of too many segments: after the week 2, the extended
  // indices would have three segments.
  auto config = snapshotConfigWithoutCompaction();
  config.maxGeoSegments_ = 2;
  auto weeks = runWeeks(config, stateDir, 2);
  auto afterWeek1 = loadBlob(weeks.blobs_[1]);
  auto afterWeek2 = loadBlob(weeks.blobs_[2]);
  for (const auto& name : geoEntries) {
    EXPECT_EQ(numGeoSegments(*afterWeek1, name), 2u) << name;
    EXPECT_EQ(numGeoSegments(*afterWeek2, name), 1u) << name;
  }

  // Compaction because of too many dead shapes: week 2 deletes more than 30
  // percent of the shapes that were indexed up to then.
  fs::remove_all(stateDir);
  config = snapshotConfigWithoutCompaction();
  config.maxDeadShapeRatio_ = 0.3;
  weeks = runWeeks(config, stateDir, 2);
  afterWeek2 = loadBlob(weeks.blobs_[2]);
  for (const auto& name : geoEntries) {
    EXPECT_EQ(numGeoSegments(*afterWeek2, name), 1u) << name;
  }
}

// _____________________________________________________________________________
TEST(BlobDiffProducer, stateDirectoryBookkeeping) {
  auto config = snapshotConfig();
  std::string stateDir = freshDirectory();
  absl::Cleanup cleanup = [&] { fs::remove_all(stateDir); };
  auto blob0 = BlobDiffProducer::init(config, snapshotFile(0), stateDir);

  auto readState = [&stateDir] {
    auto stream = ad_utility::makeIfstream(stateDir + "/state.json");
    return nlohmann::json::parse(stream);
  };
  auto state = readState();
  EXPECT_EQ(state.at("week"), 0);
  EXPECT_EQ(state.at("configFingerprint"), config.fingerprint());
  EXPECT_EQ(state.at("blobSizeCompressed"), blob0.size());
  EXPECT_EQ(state.at("turtleSize"), fs::file_size(snapshotFile(0)));
  EXPECT_FALSE(state.at("createdAt").get<std::string>().empty());
  EXPECT_TRUE(fs::exists(stateDir + "/current.blob"));
  EXPECT_EQ(readTextFile(stateDir + "/current.ttl"),
            readTextFile(snapshotFile(0)));
  EXPECT_TRUE(fs::exists(stateDir + "/index/index.meta-data.json"));
  EXPECT_FALSE(fs::exists(stateDir + "/index/index.update-triples"));

  // A second `init` into the non-empty directory is rejected and changes
  // nothing.
  AD_EXPECT_THROW_WITH_MESSAGE(
      BlobDiffProducer::init(config, snapshotFile(1), stateDir),
      HasSubstr("not empty"));
  EXPECT_EQ(readBinaryFile(stateDir + "/current.blob"), blob0);
  EXPECT_EQ(readState(), state);

  // After a step, the week counter is increased, the delta triples are
  // persisted, and the files are rotated.
  auto result = BlobDiffProducer::step(config, snapshotFile(1), stateDir);
  auto state1 = readState();
  EXPECT_EQ(state1.at("week"), 1);
  EXPECT_EQ(state1.at("createdAt"), state.at("createdAt"));
  EXPECT_EQ(state1.at("blobSizeCompressed"), result.blob_.size());
  EXPECT_EQ(state1.at("lastDiffSizeCompressed"), result.diffFile_.size());
  EXPECT_EQ(state1.at("turtleSize"), fs::file_size(snapshotFile(1)));
  EXPECT_TRUE(fs::exists(stateDir + "/index/index.update-triples"));
  EXPECT_FALSE(fs::exists(stateDir + "/index/index.update-triples.backup"));
  EXPECT_EQ(readBinaryFile(stateDir + "/current.blob"), result.blob_);
  EXPECT_EQ(readTextFile(stateDir + "/current.ttl"),
            readTextFile(snapshotFile(1)));

  // A step that fails does not change the state.
  std::string garbage = stateDir + "/garbage.ttl";
  {
    auto stream = ad_utility::makeOfstream(garbage);
    stream << "this is not <valid> turtle";
  }
  auto updatesBefore = readBinaryFile(stateDir + "/index/index.update-triples");
  EXPECT_ANY_THROW(BlobDiffProducer::step(config, garbage, stateDir));
  EXPECT_EQ(readState().at("week"), 1);
  EXPECT_EQ(readBinaryFile(stateDir + "/current.blob"), result.blob_);
  EXPECT_EQ(readBinaryFile(stateDir + "/index/index.update-triples"),
            updatesBefore);
  EXPECT_EQ(readTextFile(stateDir + "/current.ttl"),
            readTextFile(snapshotFile(1)));
  // The state is still usable.
  auto next = BlobDiffProducer::step(config, snapshotFile(2), stateDir);
  EXPECT_EQ(applyOrFail(result.blob_, next.diffFile_), next.blob_);
  EXPECT_EQ(readState().at("week"), 2);

  // A different config is rejected, as is a missing state directory.
  auto otherConfig = snapshotConfig();
  otherConfig.namedQueries_.pop_back();
  AD_EXPECT_THROW_WITH_MESSAGE(
      BlobDiffProducer::step(otherConfig, snapshotFile(3), stateDir),
      HasSubstr("config differs"));
  // The compaction thresholds, the number of threads, and the compression
  // level of the diffs do not matter.
  auto sameConfig = snapshotConfig();
  sameConfig.maxGeoSegments_ = 3;
  sameConfig.numThreads_ = 1;
  sameConfig.diffCompressionLevel_ = 3;
  EXPECT_EQ(sameConfig.fingerprint(), config.fingerprint());
  AD_EXPECT_THROW_WITH_MESSAGE(
      BlobDiffProducer::step(config, snapshotFile(3), stateDir + "_missing"),
      HasSubstr("Run `init` first"));
}
