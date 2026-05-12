//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/FilesystemHelpers.h"

#include <filesystem>
#include <string>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"

namespace qlever::util {
namespace fs = std::filesystem;

// _____________________________________________________________________________
bool doesDirectoryContainFileWithBasename(const std::string& baseName) {
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

// _____________________________________________________________________________
bool isSubdirectoryOf(const std::string& path,
                      const std::string& containerPath) {
  auto normalize = [](const auto& p) {
    return fs::weakly_canonical(fs::absolute(p)).parent_path();
  };
  return ::ranges::starts_with(normalize(path), normalize(containerPath));
}

}  // namespace qlever::util
