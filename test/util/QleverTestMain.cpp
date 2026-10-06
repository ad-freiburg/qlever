// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <signal.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>

#include "backports/filesystem.h"
#include "util/Random.h"

namespace fs = ql::filesystem;

// Each scratch directory name starts with this prefix, followed by the id of
// the process that created it. This way, stale scratch directories (of
// processes that no longer exist) can be identified and removed; see
// `removeStaleScratchDirectories` below.
constexpr std::string_view scratchDirectoryPrefix = "qlever-test-pid";

// Remove the scratch directories left behind by processes that no longer
// exist. A process cannot remove its scratch directory itself when it crashes
// or is killed, so instead each test run cleans up after such earlier runs.
static void removeStaleScratchDirectories(const fs::path& tempDirectory) {
  for (const auto& entry : ql::directoryRange(tempDirectory)) {
    std::string name = entry.path().filename().string();
    if (!absl::StartsWith(name, scratchDirectoryPrefix)) {
      continue;
    }
    size_t pidBegin = scratchDirectoryPrefix.size();
    size_t pidEnd = name.find('-', pidBegin);
    int64_t pid;
    if (pidEnd == std::string::npos ||
        !absl::SimpleAtoi(name.substr(pidBegin, pidEnd - pidBegin), &pid)) {
      continue;
    }
    // Sending the signal `0` performs only the error checking, in particular,
    // `ESRCH` means that no process with the given id exists.
    if (kill(static_cast<pid_t>(pid), 0) == -1 && errno == ESRCH) {
      ql::error_code errorCode;
      fs::remove_all(entry.path(), errorCode);
    }
  }
}

// The `main` function for all QLever test binaries (linked instead of
// `gmock_main`). Before running the tests, change the working directory to a
// scratch directory that is unique to this process. Many tests create index
// files and other artifacts via paths that are relative to the working
// directory. Running such a test binary manually (for example, from the root
// of a checkout) would otherwise litter the current directory. The scratch
// directory is deleted after the tests have run, whether they passed or not.
// To inspect the artifacts of failed tests, set the environment variable
// `QLEVER_KEEP_TEST_ARTIFACTS`, then the scratch directory is kept and its
// path is printed. When the process crashes or is interrupted, it cannot
// delete its scratch directory, so each test run additionally removes the
// scratch directories left behind by processes that no longer exist.
int main(int argc, char** argv) {
  // In the "threadsafe" death test style, GTest runs each death test in a
  // child process that executes this `main` function again. Do not create a
  // scratch directory in that case: GTest changes the working directory of
  // the child to the directory in which the parent ran its tests, which is
  // the parent's scratch directory.
  //
  // NOTE: This check must happen before `InitGoogleMock`, which removes the
  // flag from `argv`.
  for (int i = 1; i < argc; ++i) {
    if (absl::StartsWith(argv[i], "--gtest_internal_run_death_test")) {
      ::testing::InitGoogleMock(&argc, argv);
      return RUN_ALL_TESTS();
    }
  }
  ::testing::InitGoogleMock(&argc, argv);
  std::string binaryName =
      argc > 0 ? fs::path(argv[0]).filename().string() : "unknown";
  fs::path tempDirectory = fs::temp_directory_path();
  try {
    removeStaleScratchDirectories(tempDirectory);
  } catch (...) {
    // A failed cleanup of stale scratch directories should never prevent the
    // tests from running.
  }
  std::string uuid = ad_utility::UuidGenerator{}();
  fs::path scratchDirectory =
      tempDirectory / absl::StrCat(scratchDirectoryPrefix, getpid(), "-",
                                   binaryName, "-", uuid);
  fs::create_directories(scratchDirectory);
  fs::current_path(scratchDirectory);
  int result = RUN_ALL_TESTS();
  // Leave the scratch directory before deleting or renaming it (deleting the
  // working directory fails on some platforms).
  fs::current_path(tempDirectory);
  ql::error_code errorCode;
  if (std::getenv("QLEVER_KEEP_TEST_ARTIFACTS") != nullptr) {
    // Rename the kept directory such that it no longer matches the pattern
    // checked by `removeStaleScratchDirectories` (the id of this process
    // would be reported as no longer existing once the process has exited).
    fs::path keptDirectory =
        tempDirectory /
        absl::StrCat("qlever-test-kept-", binaryName, "-", uuid);
    fs::rename(scratchDirectory, keptDirectory, errorCode);
    if (errorCode) {
      keptDirectory = scratchDirectory;
    }
    std::cerr << "Note: test artifacts remain in " << keptDirectory
              << std::endl;
  } else {
    fs::remove_all(scratchDirectory, errorCode);
    if (result != 0) {
      std::cerr << "Note: the artifacts of the failed tests were deleted; set "
                   "QLEVER_KEEP_TEST_ARTIFACTS=1 to keep them"
                << std::endl;
    }
  }
  return result;
}
