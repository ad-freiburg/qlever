// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <iostream>
#include <string>

#include "backports/filesystem.h"
#include "util/Random.h"

// The `main` function for all QLever test binaries (linked instead of
// `gmock_main`). Before running the tests, change the working directory to a
// scratch directory that is unique to this process. Many tests create index
// files and other artifacts via paths that are relative to the working
// directory. Running such a test binary manually (for example, from the root
// of a checkout) would otherwise litter the current directory, in particular
// when the cleanup at the end of a test is skipped because of a crash or an
// interrupt. On success, the scratch directory is deleted; on failure, it is
// kept and its path is printed, so that the artifacts of the failed tests can
// be inspected.
int main(int argc, char** argv) {
  ::testing::InitGoogleMock(&argc, argv);
  namespace fs = ql::filesystem;
  std::string binaryName =
      argc > 0 ? fs::path(argv[0]).filename().string() : "unknown";
  fs::path scratchDirectory =
      fs::temp_directory_path() / absl::StrCat("qlever-test-", binaryName, "-",
                                               ad_utility::UuidGenerator{}());
  fs::create_directories(scratchDirectory);
  fs::current_path(scratchDirectory);
  int result = RUN_ALL_TESTS();
  if (result == 0) {
    // Leave the scratch directory before deleting it (deleting the working
    // directory fails on some platforms).
    fs::current_path(fs::temp_directory_path());
    ql::error_code errorCode;
    fs::remove_all(scratchDirectory, errorCode);
  } else {
    std::cerr << "Note: test artifacts remain in " << scratchDirectory
              << std::endl;
  }
  return result;
}
