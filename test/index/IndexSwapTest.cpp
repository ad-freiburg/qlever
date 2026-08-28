// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <string>

#include "../util/FileTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/filesystem.h"
#include "global/FileSuffixConstants.h"
#include "index/IndexImpl.h"
#include "index/IndexSwap.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"

using namespace qlever;
using namespace testing;

namespace {
// The pieces produced by `setUpSwap` below: the base name of the "old" index
// and the base name of the staged new index (in a temporary directory).
struct SwapSetup {
  std::string oldBase_;
  std::string stagedBase_;
};

// Build an "old" index at `<baseFolder>/index` and a staged new index at
// `<baseFolder>/rebuild.tmp/index` (a distinct temporary directory), and return
// their base names for use with `moveIndexIntoPlace`.
SwapSetup setUpSwap(const std::string& baseFolder) {
  ql::filesystem::create_directory(baseFolder);
  std::string oldBase = baseFolder + "/index";
  std::string tmpDir = baseFolder + "/rebuild.tmp";
  ql::filesystem::create_directory(tmpDir);
  std::string stagedBase = tmpDir + "/index";

  ad_utility::testing::makeTestIndex(oldBase, "<a> <b> <c> .");
  ad_utility::testing::makeTestIndex(stagedBase, "<a> <b> <c> . <d> <e> <f> .");
  // Remove the input files of the index build (which are not part of the index
  // itself), so that the temporary directory holds nothing but the staged
  // index and can hence be removed once that index has been moved out of it
  // (see `moveIndexIntoPlace`, and
  // `moveIndexIntoPlaceWithNonRemovableBuildDirectory` for the opposite case).
  for (auto suffix : {".ttl", ".ttl.settings.json"}) {
    ql::filesystem::remove(stagedBase + suffix);
  }
  return {std::move(oldBase), std::move(stagedBase)};
}

// The naming scheme that `qlever-server`'s index rebuild uses, with the
// datetime of the build of the "old" index passed in explicitly (the
// production code takes it from `IndexImpl::dateOfIndexBuild`).
IndexSwapNaming testNaming(std::string retiredDirDatetime = "2026-01-02") {
  return IndexSwapNaming{"rebuild.", "previous.", std::move(retiredDirDatetime),
                         " or specify a directory explicitly"};
}
}  // namespace

// _____________________________________________________________________________
// Build an "old" index and a staged new index (in a temporary directory), then
// move the staged index into the place of the old one and check the resulting
// on-disk layout.
TEST(IndexSwap, moveIndexIntoPlace) {
  std::string baseFolder = gtestCurrentTestName();
  absl::Cleanup removeFiles{
      [&baseFolder] { ql::filesystem::remove_all(baseFolder); }};
  auto setup = setUpSwap(baseFolder);

  // Use a base name for the old index that lives in a not-yet-existing
  // directory AND uses a different file-name prefix than the original index.
  // This exercises that the individual files are re-prefixed, not just moved.
  std::string oldIndexBackup = baseFolder + "/previous/old-index";
  std::string newBase = baseFolder + "/index";
  IndexSwapConfig config{setup.oldBase_, setup.stagedBase_, oldIndexBackup,
                         newBase};

  // The old index carries a build log, and the staged index a rebuild log;
  // both must travel with their respective index (exercising the log-moving
  // branches).
  auto touch = [](const std::string& path) {
    ad_utility::makeOfstream(path) << "log";
  };
  touch(setup.oldBase_ + INDEX_LOG_SUFFIX);
  touch(setup.stagedBase_ + REBUILD_INDEX_LOG_SUFFIX);

  moveIndexIntoPlace(config);

  // The old index's files were moved to the base name for the old index, with
  // their file-name prefix changed to match that base name. This includes the
  // build log.
  EXPECT_TRUE(ql::filesystem::exists(oldIndexBackup + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(oldIndexBackup + ".index.pso"));
  EXPECT_TRUE(ql::filesystem::exists(oldIndexBackup + INDEX_LOG_SUFFIX));

  // The staged index now lives at the final base name (the place of the old
  // index) and no longer in the temporary directory. Its rebuild log traveled
  // with it to the final base name.
  EXPECT_TRUE(ql::filesystem::exists(newBase + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(newBase + ".index.pso"));
  EXPECT_TRUE(ql::filesystem::exists(newBase + REBUILD_INDEX_LOG_SUFFIX));
  // The temporary directory in which the new index was staged became empty by
  // the move and was therefore removed (which also implies that no file of the
  // new index was left behind in it).
  EXPECT_FALSE(ql::filesystem::exists(baseFolder + "/rebuild.tmp"));
}

// _____________________________________________________________________________
// The base name for the retired old index may also be a plain directory (given
// with a trailing separator). The directory does not exist yet and has to be
// created; the old index's files then live directly inside it, with an empty
// file-name prefix (e.g. `<dir>/.index.pso`).
TEST(IndexSwap, moveIndexIntoPlaceWithDirectoryBasename) {
  std::string baseFolder = gtestCurrentTestName();
  absl::Cleanup removeFiles{
      [&baseFolder] { ql::filesystem::remove_all(baseFolder); }};
  auto setup = setUpSwap(baseFolder);

  std::string oldDir = baseFolder + "/previous/";
  std::string newBase = baseFolder + "/index";
  IndexSwapConfig config{setup.oldBase_, setup.stagedBase_, oldDir, newBase};

  // Complementary to `moveIndexIntoPlace` above: here neither index has a log
  // file, so this test covers the "no log file to move" branches (whereas the
  // other test covers their counterparts). Do not add log files here, or that
  // negative coverage is lost.
  moveIndexIntoPlace(config);

  // The (previously non-existent) directory was created and the old index's
  // files now live inside it (with an empty base-name prefix).
  EXPECT_TRUE(ql::filesystem::is_directory(oldDir));
  EXPECT_TRUE(ql::filesystem::exists(oldDir + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(oldDir + ".index.pso"));

  // The new index is installed at its final base name as usual, and no rebuild
  // log was created for it.
  EXPECT_TRUE(ql::filesystem::exists(newBase + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(newBase + ".index.pso"));
  EXPECT_FALSE(ql::filesystem::exists(newBase + REBUILD_INDEX_LOG_SUFFIX));
}

// _____________________________________________________________________________
// The standard production layout: the index is served with a BARE base name
// (no directory component) from the current working directory, which is how
// `qlever-control` starts the server. The file enumeration must then return
// bare file names as well, otherwise the base-name prefix substitution of the
// move fails on the globbed files (vocabulary, views).
TEST(IndexSwap, moveIndexIntoPlaceWithBareBasename) {
  auto cleanup = ad_utility::testing::useFreshWorkingDirectory();
  ad_utility::testing::makeTestIndex("index", "<a> <b> <c> .");
  ql::filesystem::create_directory("rebuild.tmp");
  ad_utility::testing::makeTestIndex("rebuild.tmp/index",
                                     "<a> <b> <c> . <d> <e> <f> .");

  IndexSwapConfig config{"index", "rebuild.tmp/index", "previous/index",
                         "index"};
  moveIndexIntoPlace(config);

  // The old index (including its vocabulary, which is enumerated via the glob)
  // was moved away completely, and the new index is installed in its place.
  EXPECT_TRUE(ql::filesystem::exists(std::string{"previous/index"} +
                                     std::string{CONFIGURATION_FILE}));
  EXPECT_FALSE(
      qlever::util::filesWithBaseNameAndSuffix("previous/index", VOCAB_SUFFIX)
          .empty());
  EXPECT_TRUE(ql::filesystem::exists(std::string{"index"} +
                                     std::string{CONFIGURATION_FILE}));
  EXPECT_FALSE(
      qlever::util::filesWithBaseNameAndSuffix("index", VOCAB_SUFFIX).empty());
  EXPECT_TRUE(IndexImpl::allIndexFiles("rebuild.tmp/index").empty());
  // A bare base name has no directory component, so nothing has to be created
  // for it. In particular, the base name itself must not be mistaken for a
  // directory to create.
  EXPECT_FALSE(ql::filesystem::exists("index"));
}

// _____________________________________________________________________________
// The staged new index may also live at a BARE base name (no directory
// component), i.e. directly in the working directory. There is then no
// temporary directory that the new index has to be taken out of, so the removal
// of that directory has to be skipped (in particular, the working directory
// itself must not be removed).
TEST(IndexSwap, moveIndexIntoPlaceWithBareNewIndexSource) {
  auto cleanup = ad_utility::testing::useFreshWorkingDirectory();
  ad_utility::testing::makeTestIndex("index", "<a> <b> <c> .");
  ad_utility::testing::makeTestIndex("rebuilt", "<a> <b> <c> . <d> <e> <f> .");

  IndexSwapConfig config{"index", "rebuilt", "previous/index", "index"};
  auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
  moveIndexIntoPlace(config);

  // The swap happened as usual: the old index was moved away and the new index
  // took its place, with nothing left at the base name it was built under.
  EXPECT_TRUE(ql::filesystem::exists(std::string{"previous/index"} +
                                     std::string{CONFIGURATION_FILE}));
  EXPECT_TRUE(ql::filesystem::exists(std::string{"index"} +
                                     std::string{CONFIGURATION_FILE}));
  EXPECT_TRUE(IndexImpl::allIndexFiles("rebuilt").empty());
  // No directory removal was attempted, hence also no warning about a failed
  // one, and the working directory is of course still there.
  EXPECT_THAT(logStream.str(),
              Not(HasSubstr("Could not remove the directory")));
  EXPECT_TRUE(ql::filesystem::is_directory(ql::filesystem::current_path()));
}

// _____________________________________________________________________________
// If the directory in which the new index was staged cannot be removed after
// the new index has been moved out of it (here because it still holds a file
// that is not part of the index), the swap is still complete and only a warning
// is logged.
TEST(IndexSwap, moveIndexIntoPlaceWithNonRemovableBuildDirectory) {
  SKIP_IF_LOGLEVEL_IS_LOWER(WARN);
  std::string baseFolder = gtestCurrentTestName();
  absl::Cleanup removeFiles{
      [&baseFolder] { ql::filesystem::remove_all(baseFolder); }};
  auto setup = setUpSwap(baseFolder);

  // Put a file that is not part of the index into the temporary directory of
  // the rebuild: it is not moved along and hence keeps the directory from being
  // removed (unlike in `moveIndexIntoPlace`, where the directory becomes empty
  // and is removed).
  std::string unrelatedFile = baseFolder + "/rebuild.tmp/unrelated.txt";
  ad_utility::makeOfstream(unrelatedFile) << "keep me";

  std::string oldIndexBackup = baseFolder + "/previous/index";
  std::string newBase = baseFolder + "/index";
  IndexSwapConfig config{setup.oldBase_, setup.stagedBase_, oldIndexBackup,
                         newBase};

  auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
  moveIndexIntoPlace(config);
  EXPECT_THAT(logStream.str(), HasSubstr("Could not remove the directory"));

  // Both indexes still ended up where they belong, and only the unrelated file
  // was left behind.
  EXPECT_TRUE(ql::filesystem::exists(oldIndexBackup + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(newBase + CONFIGURATION_FILE));
  EXPECT_TRUE(IndexImpl::allIndexFiles(setup.stagedBase_).empty());
  EXPECT_TRUE(ql::filesystem::exists(unrelatedFile));
}

// _____________________________________________________________________________
// The `IndexSwapConfig` constructor rejects base-name combinations that
// would collide destructively. Because the validation lives in the constructor,
// this needs no index on disk at all.
TEST(IndexSwap, configRejectsCollidingBaseNames) {
  // The four positional arguments are: current index, staged new index,
  // retired old index, new index. The common (valid) case has the new index
  // served from the place of the current index.
  EXPECT_NO_THROW(
      IndexSwapConfig("index", "tmp/index", "previous/old", "index"));

  // The old index and the staged new index must differ.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexSwapConfig("index", "index", "previous/old", "index"),
      ::testing::HasSubstr("old index and the staged new index"));

  // The retired-old-index base name must differ from the old index, ...
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexSwapConfig("index", "tmp/index", "index", "index"),
      ::testing::HasSubstr("differ from the old index"));

  // ... from the staged new index, ...
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexSwapConfig("index", "tmp/index", "tmp/index", "index"),
      ::testing::HasSubstr("differ from the staged new index"));

  // ... and from the new index.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexSwapConfig("index", "tmp/index", "shared", "shared"),
      ::testing::HasSubstr("retired old index and the new index must differ"));

  // Collisions are detected up to lexical path normalization, so `abc/../index`
  // (which denotes `index`) collides with the currently served index `index`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexSwapConfig("index", "tmp/index", "abc/../index", "index"),
      ::testing::HasSubstr("differ from the old index"));

  // Base names that merely share a string prefix (e.g. `index` and `indexdata`)
  // do NOT collide: the `.`-delimited file-name suffixes keep the two indexes'
  // files apart, so such a configuration is valid.
  EXPECT_NO_THROW(IndexSwapConfig("index", "tmp/index", "indexdata", "index"));

  // But a base name that is another base name followed by a '.' DOES collide:
  // `index.view` sits inside the `index.view.*` glob that enumerates `index`'s
  // materialized views, so retiring the old index to `index.view` would sweep
  // up the current index's view files.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexSwapConfig("index", "tmp/index", "index.view", "index"),
      ::testing::HasSubstr("differ from the old index"));
  // The same holds regardless of which of the two base names is the longer one.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexSwapConfig("index", "index.view", "previous/old", "newidx"),
      ::testing::HasSubstr("old index and the staged new index"));
}

// _____________________________________________________________________________
// `makeIndexSwapConfig` turns the two directories that a swap can be
// configured with into base names (by appending the file name of the current
// index) and validates them. The test runs in a fresh working directory,
// because the directories are resolved against the working directory and have
// to lie inside the directory of the current index (which is the working
// directory here as the index is served from the bare base name `index`).
TEST(IndexSwap, makeIndexSwapConfig) {
  auto cleanup = ad_utility::testing::useFreshWorkingDirectory();
  // No index has to be built here: `makeIndexSwapConfig` only looks at the
  // given base name and at the directories on disk. The file makes the working
  // directory non-empty, which the check for the empty directory string below
  // relies on.
  ad_utility::makeOfstream(absl::StrCat("index", CONFIGURATION_FILE)) << "{}";
  auto makeConfig = [](std::optional<std::string> stagingDir,
                       std::optional<std::string> retiredDir) {
    return makeIndexSwapConfig("index", testNaming(), std::move(stagingDir),
                               std::move(retiredDir));
  };

  // Both directories default to a directory that does not exist yet, derived
  // from the current time resp. `retiredDirDatetime_`. The new index ends up at
  // the base name the current index is served from.
  {
    auto config = makeConfig(std::nullopt, std::nullopt);
    EXPECT_EQ(config.oldIndexSource(), "index");
    EXPECT_EQ(config.newIndexTarget(), "index");
    EXPECT_THAT(config.newIndexSource(),
                AllOf(StartsWith("rebuild."), EndsWith(".tmp/index")));
    EXPECT_EQ(config.oldIndexTarget(), "previous.2026-01-02/index");
  }

  // Explicitly given directories that do not exist yet are accepted as is (the
  // swap creates them), and the file name of the current index is used inside
  // them.
  {
    auto config = makeConfig("tmpForRebuild", "oldIndex");
    EXPECT_EQ(config.newIndexSource(), "tmpForRebuild/index");
    EXPECT_EQ(config.oldIndexTarget(), "oldIndex/index");
  }

  // Directories that already exist are fine as long as they are empty.
  ql::filesystem::create_directory("emptyTmpForRebuild");
  ql::filesystem::create_directory("emptyOldIndex");
  EXPECT_NO_THROW(makeConfig("emptyTmpForRebuild", "emptyOldIndex"));

  // But a directory that exists and is not empty is rejected, no matter which
  // of the two directories it is.
  ql::filesystem::create_directory("notEmpty");
  ad_utility::makeOfstream("notEmpty/someFile") << "not empty";
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeConfig("notEmpty", std::nullopt),
      ::testing::HasSubstr("already exists and is not empty"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeConfig(std::nullopt, "notEmpty"),
      ::testing::HasSubstr("already exists and is not empty"));

  // An empty directory string yields a base name without a directory component
  // (here simply `index`), which refers to the working directory itself. That
  // directory exists and is not empty (it holds the current index), so it is
  // rejected with the same message as above, and the message names the working
  // directory.
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeConfig("", std::nullopt),
      AllOf(HasSubstr(ql::filesystem::current_path().string()),
            HasSubstr("already exists and is not empty")));

  // The directories must be relative paths, ...
  AD_EXPECT_THROW_WITH_MESSAGE(makeConfig("/absolute", std::nullopt),
                               HasSubstr("must be a relative path"));
  AD_EXPECT_THROW_WITH_MESSAGE(makeConfig(std::nullopt, "/absolute"),
                               HasSubstr("must be a relative path"));

  // ... and must lie inside the directory of the current index.
  AD_EXPECT_THROW_WITH_MESSAGE(makeConfig("../outside", std::nullopt),
                               HasSubstr("not a subdirectory"));

  // The default directory for the old index is
  // `<retiredDirPrefix_><retiredDirDatetime_>`, where the datetime typically
  // has a granularity of one second. When that directory is already taken by a
  // previous swap (e.g. rebuilds in quick succession with
  // `--rebuild-index-strategy`), `.1`, `.2`, ... is appended; an occupied
  // directory must not fail the swap, because the datetime of the served index
  // only changes on a successful swap, so all subsequent swaps would fail with
  // the same name.
  {
    std::string defaultPreviousDir = "previous.2026-01-02";
    ql::filesystem::create_directory(defaultPreviousDir);
    ad_utility::makeOfstream(defaultPreviousDir + "/index.meta-data.json")
        << "occupied";
    EXPECT_EQ(makeConfig(std::nullopt, std::nullopt).oldIndexTarget(),
              absl::StrCat(defaultPreviousDir, ".1/index"));
    ql::filesystem::create_directory(defaultPreviousDir + ".1");
    ad_utility::makeOfstream(defaultPreviousDir + ".1/index.meta-data.json")
        << "occupied";
    EXPECT_EQ(makeConfig(std::nullopt, std::nullopt).oldIndexTarget(),
              absl::StrCat(defaultPreviousDir, ".2/index"));

    // After `.99`, the swap fails with a readable message that includes the
    // hint from the `IndexSwapNaming`.
    for (size_t i = 2; i <= 99; ++i) {
      ql::filesystem::create_directory(
          absl::StrCat(defaultPreviousDir, ".", i));
    }
    AD_EXPECT_THROW_WITH_MESSAGE(
        makeConfig(std::nullopt, std::nullopt),
        AllOf(HasSubstr("all already exist"),
              HasSubstr("specify a directory explicitly")));
  }
}

// _____________________________________________________________________________
// The two directories that a swap defaults to are resolved relative to the
// directory of the current index, not relative to the working directory. This
// matters as soon as the index is not served from the working directory (e.g.
// `qlever-server -i /data/wikidata/wikidata` started somewhere else): the
// defaulted `rebuild.<datetime>.tmp` and `previous.<datetime>` have to end up
// next to the current index, or they would be created in the working directory
// and then immediately fail the check that they must lie inside the directory
// of the current index. The test `makeIndexSwapConfig` above cannot catch
// this, because there the index is served from the bare base name `index`, for
// which the two directories coincide.
TEST(IndexSwap, makeIndexSwapConfigWithIndexInSubdirectory) {
  auto cleanup = ad_utility::testing::useFreshWorkingDirectory();
  ql::filesystem::create_directory("indexDir");
  auto makeConfig = [](std::optional<std::string> stagingDir,
                       std::optional<std::string> retiredDir) {
    return makeIndexSwapConfig("indexDir/index", testNaming(),
                               std::move(stagingDir), std::move(retiredDir));
  };

  // Both defaulted directories lie inside `indexDir` (and inside them, the
  // file name of the current index is used).
  auto config = makeConfig(std::nullopt, std::nullopt);
  EXPECT_EQ(config.oldIndexSource(), "indexDir/index");
  EXPECT_EQ(config.newIndexTarget(), "indexDir/index");
  EXPECT_THAT(config.newIndexSource(),
              AllOf(StartsWith("indexDir/rebuild."), EndsWith(".tmp/index")));
  EXPECT_EQ(config.oldIndexTarget(), "indexDir/previous.2026-01-02/index");

  // An explicitly given directory is still resolved against the working
  // directory, so it has to name the directory of the index explicitly, ...
  EXPECT_EQ(makeConfig("indexDir/tmp", "indexDir/previous").newIndexSource(),
            "indexDir/tmp/index");

  // ... and is rejected if it lies outside of the directory of the current
  // index (which is exactly what used to happen to the defaults above).
  AD_EXPECT_THROW_WITH_MESSAGE(makeConfig("tmp", std::nullopt),
                               HasSubstr("not a subdirectory"));
  AD_EXPECT_THROW_WITH_MESSAGE(makeConfig(std::nullopt, "previous"),
                               HasSubstr("not a subdirectory"));
}
