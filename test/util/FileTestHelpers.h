// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_FILETESTHELPERS_H
#define QLEVER_TEST_UTIL_FILETESTHELPERS_H

#include <absl/cleanup/cleanup.h>

#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/filesystem.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Random.h"

namespace ad_utility::testing {
inline auto filenameForTesting() {
  auto tmpFile = ql::filesystem::temp_directory_path() / UuidGenerator()();
  // Make sure no file like this exists
  ql::filesystem::remove(tmpFile);
  absl::Cleanup cleanup{[tmpFile]() { ql::filesystem::remove(tmpFile); }};
  return std::make_pair(std::move(tmpFile), std::move(cleanup));
}

// Read every line of a text file into a vector. Intended for tests
// that need to inspect the contents of a file written by another
// component (e.g. a background writer thread that has already drained
// and closed the stream).
inline std::vector<std::string> readLines(const ql::filesystem::path& path) {
  std::ifstream in{path.string()};
  std::vector<std::string> lines;
  std::string line;
  while (std::getline(in, line)) {
    lines.push_back(std::move(line));
  }
  return lines;
}

// Create a temporary directory inside the Google Test temporary directory
// with the given `name`. The directory and all its contents are deleted when
// the returned `absl::Cleanup` is destroyed.
inline auto makeTemporaryDirectory(std::string_view name) {
  std::string directory = ::testing::TempDir();
  if (!ql::ends_with(directory, "/")) {
    directory.push_back('/');
  }
  AD_CORRECTNESS_CHECK(!ql::starts_with(name, '/'));
  directory += name;
  // Create directory.
  ql::filesystem::create_directory(directory);

  // Remove all files in directory when done.
  absl::Cleanup cleanup{[directory]() {
    ql::error_code ec;
    ql::filesystem::remove_all(directory, ec);
    if (ec) {
      AD_LOG(ERROR) << "Could not remove temporary directory " << directory
                    << ": " << ec.message();
    }
  }};
  return std::make_pair(std::move(directory), std::move(cleanup));
}
}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_FILETESTHELPERS_H
