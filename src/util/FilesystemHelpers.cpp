//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/FilesystemHelpers.h"

#include <absl/strings/str_cat.h>

#include <string>
#include <string_view>
#include <vector>

#include "Views.h"
#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "util/File.h"

namespace qlever::util {
namespace fs = ql::filesystem;

// _____________________________________________________________________________
std::vector<fs::path> filesWithBaseNameAndSuffix(const fs::path& onDiskBase,
                                                 std::string_view suffix) {
  fs::path parent = onDiskBase.parent_path();
  // For a base name without a directory component, iterate the current
  // directory, but do NOT prepend it to the returned paths (see below).
  fs::path directory = parent.empty() ? fs::current_path() : parent;
  std::string prefix =
      absl::StrCat(ql::pathFilename(onDiskBase).string(), suffix);
  namespace v = ql::views;
  // With an InputRangeTypeErased (instead of `to_vector`), `ql::directoryRange`
  // backed by the boost filesystem library doesn't work.
  return ad_utility::OwningView{
             ::ranges::to_vector(ql::directoryRange(directory))} |
         v::filter([](const auto& entry) { return entry.is_regular_file(); }) |
         // Return the paths in the same form as `onDiskBase` (directory part of
         // `onDiskBase` plus the file name; an empty `parent` yields the bare
         // file name), so that they textually start with `onDiskBase`. Callers
         // like `Qlever::moveRebuiltIndexIntoPlace` rely on this to replace the
         // base-name prefix of each file.
         v::transform([&parent](const auto& entry) {
           return parent / entry.path().filename();
         }) |
         v::filter([&prefix](const auto& path) {
           return ql::starts_with(path.filename().string(), prefix);
         }) |
         ::ranges::to_vector;
}

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

  // Use `ql::pathFilename` (not `base.filename()`) so that a trailing separator
  // yields an empty prefix (matching any file in `dir`) for both `std` and
  // `boost` filesystem; see the comment on `ql::pathFilename`.
  std::string prefix = ql::pathFilename(base).string();
  return ql::ranges::any_of(
      ql::directoryRange(dir), [&prefix](const auto& entry) {
        std::string name = entry.path().filename().string();
        return ql::starts_with(name, prefix);
      });
}

// _____________________________________________________________________________
size_t deleteFilesInDirectory(
    const fs::path& directory,
    absl::FunctionRef<bool(const fs::path&)> shouldDelete) {
  if (!fs::exists(directory) || !fs::is_directory(directory)) {
    return 0;
  }
  // Collect the matching regular files first and delete them only afterwards.
  // Deleting entries while iterating a directory is unspecified behavior and
  // can cause entries to be skipped on some platforms (observed on macOS),
  // leaving leftover files behind.
  std::vector<fs::path> toDelete;
  for (const auto& entry : ql::directoryRange(directory)) {
    if (entry.is_regular_file() && shouldDelete(entry.path())) {
      toDelete.push_back(entry.path());
    }
  }
  for (const auto& path : toDelete) {
    ad_utility::deleteFile(path);
  }
  return toDelete.size();
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
