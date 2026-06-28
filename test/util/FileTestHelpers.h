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

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "util/Random.h"

namespace ad_utility::testing {
inline auto filenameForTesting() {
  auto tmpFile = std::filesystem::temp_directory_path() / UuidGenerator()();
  // Make sure no file like this exists
  std::filesystem::remove(tmpFile);
  absl::Cleanup cleanup{[tmpFile]() { std::filesystem::remove(tmpFile); }};
  return std::make_pair(std::move(tmpFile), std::move(cleanup));
}

// Read every line of a text file into a vector. Intended for tests
// that need to inspect the contents of a file written by another
// component (e.g. a background writer thread that has already drained
// and closed the stream).
inline std::vector<std::string> readLines(const std::filesystem::path& path) {
  std::ifstream in{path};
  std::vector<std::string> lines;
  std::string line;
  while (std::getline(in, line)) {
    lines.push_back(std::move(line));
  }
  return lines;
}
}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_FILETESTHELPERS_H
