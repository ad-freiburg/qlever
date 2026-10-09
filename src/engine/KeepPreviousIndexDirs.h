//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_KEEPPREVIOUSINDEXDIRS_H
#define QLEVER_SRC_ENGINE_KEEPPREVIOUSINDEXDIRS_H

#include <array>
#include <string_view>
#include <utility>

#include "util/EnumWithStrings.h"
#include "util/Exception.h"

namespace qlever {

namespace detail {
enum class KeepPreviousIndexDirsEnum {
  All,
  None,
  OriginalOnly,
  MostRecentOnly,
  OriginalAndMostRecent
};
}

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
// created one, or the original and the most recent. The `EnumWithStrings`
// base class provides the conversion from and to these strings (in
// particular, for the command-line option).
class KeepPreviousIndexDirs
    : public ad_utility::EnumWithStrings<KeepPreviousIndexDirs,
                                         detail::KeepPreviousIndexDirsEnum> {
 public:
  using Enum = detail::KeepPreviousIndexDirsEnum;

  static constexpr std::array<std::pair<Enum, std::string_view>, 5>
      descriptions_{
          {{Enum::All, "all"},
           {Enum::None, "none"},
           {Enum::OriginalOnly, "original-only"},
           {Enum::MostRecentOnly, "most-recent-only"},
           {Enum::OriginalAndMostRecent, "original-and-most-recent"}}};
  static const KeepPreviousIndexDirs All;
  static const KeepPreviousIndexDirs None;
  static const KeepPreviousIndexDirs OriginalOnly;
  static const KeepPreviousIndexDirs MostRecentOnly;
  static const KeepPreviousIndexDirs OriginalAndMostRecent;

  static constexpr std::string_view typeName() {
    return "policy for keeping previous index directories";
  }

  using EnumWithStrings::EnumWithStrings;
};

const inline KeepPreviousIndexDirs KeepPreviousIndexDirs::All{Enum::All};
const inline KeepPreviousIndexDirs KeepPreviousIndexDirs::None{Enum::None};
const inline KeepPreviousIndexDirs KeepPreviousIndexDirs::OriginalOnly{
    Enum::OriginalOnly};
const inline KeepPreviousIndexDirs KeepPreviousIndexDirs::MostRecentOnly{
    Enum::MostRecentOnly};
const inline KeepPreviousIndexDirs KeepPreviousIndexDirs::OriginalAndMostRecent{
    Enum::OriginalAndMostRecent};

// Decide whether the previous index directory at position `index` should be
// kept under the given `policy`, where the `numDirs` directories are numbered
// from the oldest (position 0, the original) to the newest (position
// `numDirs - 1`, the most recent).
inline bool keepPreviousIndexDir(KeepPreviousIndexDirs policy, size_t index,
                                 size_t numDirs) {
  AD_CONTRACT_CHECK(index < numDirs);
  bool isOriginal = index == 0;
  bool isMostRecent = index + 1 == numDirs;
  using enum KeepPreviousIndexDirs::Enum;
  switch (policy.value()) {
    case All:
      return true;
    case None:
      return false;
    case OriginalOnly:
      return isOriginal;
    case MostRecentOnly:
      return isMostRecent;
    case OriginalAndMostRecent:
      return isOriginal || isMostRecent;
  }
  AD_FAIL();
}

}  // namespace qlever

#endif  // QLEVER_SRC_ENGINE_KEEPPREVIOUSINDEXDIRS_H
