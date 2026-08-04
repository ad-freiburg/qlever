//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_KEEPPREVIOUSINDEXDIRS_H
#define QLEVER_SRC_ENGINE_KEEPPREVIOUSINDEXDIRS_H

#include <absl/strings/str_cat.h>

#include <stdexcept>
#include <string_view>

#include "util/Exception.h"

namespace qlever {

// Policy for which `previous.*` index directories to keep after a successful
// index rebuild, configured via the `--rebuild-keep-previous-index-dirs`
// option of `qlever-server`. Each rebuild moves the index that was served so
// far into such a directory (see `Qlever::makeIndexRebuildConfig`), so with
// frequent rebuilds (in particular, automatic ones, see
// `--rebuild-index-strategy`), these directories accumulate and can take up a
// lot of disk space. The choices are the same as for the
// `--keep-previous-index-dirs` option of the `qlever rebuild-index` command
// (which applies the same policy on the client side): keep all of the
// directories, none of them, only the original one (the oldest, which
// contains the index the first rebuild started from), only the most recently
// created one, or the original and the most recent.
enum class KeepPreviousIndexDirs {
  All,
  None,
  OriginalOnly,
  MostRecentOnly,
  OriginalAndMostRecent
};

// Parse the value of the `--rebuild-keep-previous-index-dirs` option. Throws
// `std::runtime_error` for any value that is not one of the five choices.
inline KeepPreviousIndexDirs parseKeepPreviousIndexDirs(
    std::string_view value) {
  if (value == "all") {
    return KeepPreviousIndexDirs::All;
  }
  if (value == "none") {
    return KeepPreviousIndexDirs::None;
  }
  if (value == "original-only") {
    return KeepPreviousIndexDirs::OriginalOnly;
  }
  if (value == "most-recent-only") {
    return KeepPreviousIndexDirs::MostRecentOnly;
  }
  if (value == "original-and-most-recent") {
    return KeepPreviousIndexDirs::OriginalAndMostRecent;
  }
  throw std::runtime_error{
      absl::StrCat("The value \"", value,
                   "\" must be one of \"all\", \"none\", \"original-only\", "
                   "\"most-recent-only\", or \"original-and-most-recent\"")};
}

// The inverse of `parseKeepPreviousIndexDirs`, for log messages.
inline std::string_view toString(KeepPreviousIndexDirs policy) {
  switch (policy) {
    case KeepPreviousIndexDirs::All:
      return "all";
    case KeepPreviousIndexDirs::None:
      return "none";
    case KeepPreviousIndexDirs::OriginalOnly:
      return "original-only";
    case KeepPreviousIndexDirs::MostRecentOnly:
      return "most-recent-only";
    case KeepPreviousIndexDirs::OriginalAndMostRecent:
      return "original-and-most-recent";
  }
  AD_FAIL();
}

// Decide whether the previous index directory at position `index` should be
// kept under the given `policy`, where the `numDirs` directories are numbered
// from the oldest (position 0, the original) to the newest (position
// `numDirs - 1`, the most recent).
inline bool keepPreviousIndexDir(KeepPreviousIndexDirs policy, size_t index,
                                 size_t numDirs) {
  AD_CONTRACT_CHECK(index < numDirs);
  bool isOriginal = index == 0;
  bool isMostRecent = index + 1 == numDirs;
  switch (policy) {
    case KeepPreviousIndexDirs::All:
      return true;
    case KeepPreviousIndexDirs::None:
      return false;
    case KeepPreviousIndexDirs::OriginalOnly:
      return isOriginal;
    case KeepPreviousIndexDirs::MostRecentOnly:
      return isMostRecent;
    case KeepPreviousIndexDirs::OriginalAndMostRecent:
      return isOriginal || isMostRecent;
  }
  AD_FAIL();
}

}  // namespace qlever

#endif  // QLEVER_SRC_ENGINE_KEEPPREVIOUSINDEXDIRS_H
