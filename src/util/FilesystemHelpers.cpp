//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H
#define QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H

#include "util/FilesystemHelpers.h"

#include <filesystem>
#include <string>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"

namespace qlever::util {
bool filesWithPrefixExist(const std::string& baseName) {
  namespace fs = std::filesystem;
  fs::path base = fs::absolute(baseName);
  fs::path dir = base.parent_path();
  if (!fs::exists(dir)) {
    return false;
  }

  if (!fs::is_directory(dir)) {
    return true;
  }

  std::string prefix = base.filename().string();
  return ql::ranges::any_of(
      fs::directory_iterator(dir), [&prefix](const auto& entry) {
        std::string name = entry.path().filename().string();
        return ql::starts_with(name, prefix);
      });
}
}  // namespace qlever::util

#endif  // QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H
