// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2026        Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_GLOBAL_MATERIALIZEDVIEWCONSTANTS_H
#define QLEVER_SRC_GLOBAL_MATERIALIZEDVIEWCONSTANTS_H

#include <absl/strings/str_cat.h>

#include <array>
#include <string>
#include <string_view>

#include "global/FileSuffixConstants.h"
#include "util/StringUtils.h"

// The constants that describe the on-disk representation of a materialized view
// (see `engine/MaterializedViews.h`). They live in this separate header because
// they are also needed by code that must not depend on the query engine, in
// particular by the index-swap machinery (see `index/IndexSwap.h`).

// Materialized views save their version. If we change something about the way
// materialized views are stored, we can break the existing ones cleanly without
// breaking the entire index format.
constexpr inline size_t MATERIALIZED_VIEWS_VERSION = 1;

// The file-name infix of the materialized views of an index, e.g. the `.view.`
// in `<base>.view.<name>.viewinfo.json`.
constexpr inline std::string_view VIEW_FILE_INFIX = ".view.";

// Filename suffixes for the on-disk representation of a materialized view.
constexpr inline std::string_view VIEW_INFO_SUFFIX = ".viewinfo.json";
constexpr inline std::string_view VIEW_SPO_SUFFIX = ".index.spo";
constexpr inline std::string_view VIEW_SPO_META_SUFFIX =
    ad_utility::constexprStrCat<VIEW_SPO_SUFFIX, META_FILE_SUFFIX>();

// All suffixes of the files that make up a materialized view's on-disk
// representation. Used to delete a view's files.
constexpr inline std::array VIEW_ALL_SUFFIXES = {
    VIEW_INFO_SUFFIX, VIEW_SPO_SUFFIX, VIEW_SPO_META_SUFFIX};

// Return the base name that all files of the materialized view `name` of the
// index with the base name `onDiskBase` share. Note that this function does not
// check for validity or existence.
inline std::string materializedViewFilenameBase(std::string_view onDiskBase,
                                                std::string_view name) {
  return absl::StrCat(onDiskBase, VIEW_FILE_INFIX, name);
}

#endif  // QLEVER_SRC_GLOBAL_MATERIALIZEDVIEWCONSTANTS_H
