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
#include <stdexcept>
#include <string>

#include "backports/StartsWithAndEndsWith.h"

namespace qlever::util {
void ensureNoConflictingFiles(const std::string& baseName) {
  if (baseName.empty()) {
    throw std::runtime_error{"Please provide a valid `baseName`"};
  }
  namespace fs = std::filesystem;
  fs::path base = fs::absolute(baseName);
  fs::path dir = base.parent_path();
  std::string prefix = base.filename().string();

  if (prefix.empty()) {
    throw std::runtime_error{"The `baseName` has no filename component: " +
                             baseName};
  }

  if (!fs::exists(dir)) {
    return;
  }

  if (!fs::is_directory(dir)) {
    throw std::runtime_error{"Parent path is not a directory: " + dir.string()};
  }

  for (const auto& entry : fs::directory_iterator(dir)) {
    std::string name = entry.path().filename().string();
    if (ql::starts_with(name, prefix)) {
      throw std::runtime_error{"Conflicting file would be overwritten: " +
                               entry.path().string()};
    }
  }
}
}  // namespace qlever::util

#endif  // QLEVER_SRC_UTIL_FILESYSTEMHELPERS_H
