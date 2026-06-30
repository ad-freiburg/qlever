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
#include <string>

#include "backports/StartsWithAndEndsWith.h"
#include "util/File.h"
#include "util/Random.h"

namespace ad_utility::testing {
inline auto filenameForTesting() {
  auto tmpFile = std::filesystem::temp_directory_path() / UuidGenerator()();
  // Make sure no file like this exists
  std::filesystem::remove(tmpFile);
  absl::Cleanup cleanup{[tmpFile]() { std::filesystem::remove(tmpFile); }};
  return std::make_pair(std::move(tmpFile), std::move(cleanup));
}

// Delete every file in the current working directory whose name starts with
// `prefix`. Used to clean up index/vocabulary files with a fixed basename
// (including all sidecar files) regardless of how many or which files are
// produced. Never logs and never throws on a failed deletion, so it is safe to
// call from a destructor (where the logging might already be destroyed).
inline void deleteFilesWithPrefix(std::string_view prefix) {
  namespace fs = std::filesystem;
  std::error_code ec;
  for (const auto& entry : fs::directory_iterator(fs::current_path(), ec)) {
    if (entry.is_regular_file(ec) &&
        ql::starts_with(entry.path().filename().string(), prefix)) {
      ad_utility::deleteFile(entry.path(), false);
    }
  }
}

// Return an RAII guard that, on destruction, calls `deleteFilesWithPrefix`.
// Useful for tests that write files with a fixed basename and want them cleaned
// up even if the test fails via an `EXPECT`/`ASSERT`.
[[nodiscard]] inline auto deleteFilesWithPrefixOnDestruction(
    std::string prefix) {
  return absl::Cleanup{
      [prefix = std::move(prefix)]() { deleteFilesWithPrefix(prefix); }};
}
}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_FILETESTHELPERS_H
