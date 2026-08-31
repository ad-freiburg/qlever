// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/IndexSwap.h"

#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>

#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "global/FileSuffixConstants.h"
#include "global/MaterializedViewConstants.h"
#include "index/IndexImpl.h"
#include "util/Exception.h"
#include "util/FilesystemHelpers.h"
#include "util/Log.h"

namespace qlever {

namespace fs = ql::filesystem;

namespace {
// Two base names "collide" if the prefix-based file enumerators
// (`allIndexFiles`, `viewFilesOnDisk`, `filesWithBaseNameAndSuffix`) could
// confuse the files belonging to one with the files belonging to the other.
// Everything QLever appends to a base name starts with a '.' (`.index.pso`,
// `.meta`, `.vocabulary`, `.internal`, `.view.<name>`, the log suffixes, ...),
// so a merely shared textual prefix is harmless: base names `foo` and `foobar`
// never clash, because `foobar.index...` does not fall inside the `foo.` glob.
// The two dangerous cases are that the (lexically normalized) base names are
// equal, or that one is the other followed by a '.', e.g. `foo` and `foo.view`:
// there `foo.view`'s own index files sit inside the `foo.view.*` glob that
// enumerates `foo`'s materialized views, so moving/replacing one base name
// would sweep up the other's files.
//
// Both cases collapse into a single check once we append the separating '.' to
// each normalized name: they collide iff one dotted form is a prefix of the
// other. Equal names give identical dotted forms; `foo` vs `foo.view` is caught
// because `foo.` is a prefix of `foo.view.`; and `foo` vs `foobar` is not,
// because `foo.` is not a prefix of `foobar.`.
bool baseNamesCollide(const std::string& a, const std::string& b) {
  auto normalizedWithSeparator = [](const std::string& s) {
    return absl::StrCat(fs::path{s}.lexically_normal().string(), ".");
  };
  std::string na = normalizedWithSeparator(a);
  std::string nb = normalizedWithSeparator(b);
  return ql::starts_with(na, nb) || ql::starts_with(nb, na);
}
}  // namespace

// ___________________________________________________________________________
IndexSwapConfig::IndexSwapConfig(std::string oldIndexSource,
                                 std::string newIndexSource,
                                 std::string oldIndexTarget,
                                 std::string newIndexTarget)
    : oldIndexSource_{std::move(oldIndexSource)},
      newIndexSource_{std::move(newIndexSource)},
      oldIndexTarget_{std::move(oldIndexTarget)},
      newIndexTarget_{std::move(newIndexTarget)} {
  // Both the relocation of the old index and the installation of the new index
  // are implemented (in `moveIndexIntoPlace`) as "replace the base-name prefix
  // of each file". For this to be well-defined and non-destructive, the
  // involved base names must not collide in ways that would overwrite files
  // that are still needed, or that would turn a move into a (potentially
  // partial) self-overwrite. Note that `newIndexTarget_ == oldIndexSource_` is
  // the common (and intended) case: the old index is moved away first, so its
  // place is free for the new index.
  AD_CONTRACT_CHECK(
      !baseNamesCollide(oldIndexSource_, newIndexSource_),
      "The old index and the staged new index must not share a base name.");
  AD_CONTRACT_CHECK(
      !baseNamesCollide(oldIndexTarget_, oldIndexSource_),
      "The base name for the retired old index must differ from the old "
      "index.");
  AD_CONTRACT_CHECK(
      !baseNamesCollide(oldIndexTarget_, newIndexSource_),
      "The base name for the retired old index must differ from the staged "
      "new index.");
  AD_CONTRACT_CHECK(
      !baseNamesCollide(oldIndexTarget_, newIndexTarget_),
      "The base names for the retired old index and the new index must "
      "differ.");
}

// ___________________________________________________________________________
IndexSwapConfig makeIndexSwapConfig(const std::string& currentBaseName,
                                    const IndexSwapNaming& naming,
                                    std::optional<std::string> stagingDir,
                                    std::optional<std::string> retiredDir) {
  // Resolve one of the two directories (falling back to `defaultDirectory` if
  // it was not specified) and turn it into a base name.
  // NOTE: Use `ql::pathFilename` and not `path::filename()`, so that a base
  // name with a trailing directory separator yields an empty file name
  // component (and hence a directory base name, see the test
  // `moveRebuiltIndexIntoPlaceWithDirectoryBasename`) in both the
  // `std::filesystem` and the `boost::filesystem` backend.
  auto resolveBaseName =
      [indexFileName = ql::pathFilename(fs::path{currentBaseName})](
          std::optional<std::string> directory, std::string defaultDirectory) {
        return (fs::path{std::move(directory).value_or(
                    std::move(defaultDirectory))} /
                indexFileName)
            .string();
      };

  // The default directories lie inside the directory of the current index (an
  // explicitly given directory is resolved against the working directory
  // instead, but has to lie inside the directory of the current index as well,
  // see the check below).
  auto defaultDirectory =
      [indexDirectory =
           fs::path{currentBaseName}.parent_path()](std::string name) {
        return (indexDirectory / std::move(name)).string();
      };

  // Uniquify the default name of the retired directory with `.1`, `.2`, ... if
  // it is already taken, see the comment on `makeIndexSwapConfig` in the header
  // for why.
  //
  // NOTE: The check-then-use is not atomic; this is fine because swaps of the
  // same index are serialized (rebuilds via `Server::rebuildInProgress_`).
  auto uniquify = [&naming](const std::string& directory) -> std::string {
    if (!fs::exists(directory)) {
      return directory;
    }
    for (size_t i = 1; i <= 99; ++i) {
      std::string candidate = absl::StrCat(directory, ".", i);
      if (!fs::exists(candidate)) {
        return candidate;
      }
    }
    throw std::runtime_error{
        absl::StrCat("The directories \"", directory, "\" and \"", directory,
                     ".1\" through \"", directory,
                     ".99\" all already exist; remove some of them",
                     naming.retiredDirConflictHint_)};
  };
  bool stagingDirWasExplicit = stagingDir.has_value();
  bool retiredDirWasExplicit = retiredDir.has_value();
  std::string baseNameForStaging = resolveBaseName(
      std::move(stagingDir),
      defaultDirectory(
          absl::StrCat(naming.stagingDirPrefix_,
                       IndexImpl::formatIndexBuildTime(absl::Now()), ".tmp")));
  std::string baseNameForOldIndex = resolveBaseName(
      std::move(retiredDir),
      uniquify(defaultDirectory(
          absl::StrCat(naming.retiredDirPrefix_, naming.retiredDirDatetime_))));

  // Check the two base names that were derived from the arguments: an
  // explicitly given directory must be relative (it is resolved against the
  // working directory; a default directory instead inherits the directory of
  // `currentBaseName` and hence needs no such check), and both directories must
  // be empty or not exist yet and be a subdirectory of the directory of the
  // current index. Base names that would collide with each other or with the
  // current index are rejected by the `IndexSwapConfig` constructor below.
  for (const auto& [baseName, wasExplicit] :
       {std::pair{baseNameForStaging, stagingDirWasExplicit},
        std::pair{baseNameForOldIndex, retiredDirWasExplicit}}) {
    fs::path path{baseName};
    if (wasExplicit && !path.is_relative()) {
      throw std::runtime_error{absl::StrCat("The directory \"",
                                            path.parent_path().string(),
                                            "\" must be a relative path")};
    }
    // The parent path is empty if the base name lies in the working directory
    // itself, which the checks below then refer to.
    fs::path dir =
        path.has_parent_path() ? path.parent_path() : fs::current_path();
    if (fs::exists(dir) && !fs::is_empty(dir)) {
      throw std::runtime_error{
          absl::StrCat("The directory \"", dir.string(),
                       "\" already exists and is not empty")};
    }
    if (!qlever::util::isSubdirectoryOf(baseName, currentBaseName)) {
      throw std::runtime_error{absl::StrCat(
          "The directory \"", dir.string(),
          "\" is not a subdirectory of the directory of the current index")};
    }
  }

  // The new index ends up at the base name the current index lives at, so that
  // a later start of the server loads it.
  return IndexSwapConfig{currentBaseName, baseNameForStaging,
                         baseNameForOldIndex, currentBaseName};
}

// ___________________________________________________________________________
void moveIndexIntoPlace(const IndexSwapConfig& config) {
  // Move a `file` whose name starts with `fromBasename` so that its base-name
  // prefix becomes `toBasename` while the file-specific suffix is preserved
  // (e.g. `<from>.index.pso` -> `<to>.index.pso`).
  auto moveByBasename = [](const fs::path& file, std::string_view fromBasename,
                           std::string_view toBasename) {
    std::string fileString = file.string();
    AD_CORRECTNESS_CHECK(ql::starts_with(fileString, fromBasename));
    fs::rename(file,
               absl::StrCat(toBasename, std::string_view{fileString}.substr(
                                            fromBasename.size())));
  };

  // Move all files that make up an index from the `source` base name to the
  // `target` base name: its permutation and vocabulary files, its materialized
  // views, and its build log. Both file enumerators and the existence check
  // below only touch files that actually exist, so this is a no-op for anything
  // the index does not have (e.g. a freshly rebuilt new index has no
  // materialized views yet, and only one of the two build-log variants ever
  // exists for a given index).
  auto moveIndex = [&moveByBasename](std::string_view source,
                                     const std::string& target) {
    // Move the index to `target`. Create the containing directory first (the
    // base name may point into a directory that does not exist yet).
    fs::path targetDir = fs::path{target}.parent_path();
    if (!targetDir.empty()) {
      fs::create_directories(targetDir);
    }
    auto move = [&](const fs::path& file) {
      moveByBasename(file, source, target);
    };
    ql::ranges::for_each(IndexImpl::allIndexFiles(source), move);
    // The files of the materialized views (this is what
    // `MaterializedViewsManager::viewFilesOnDisk` enumerates, which is not used
    // here directly so that this file does not depend on the engine).
    ql::ranges::for_each(
        qlever::util::filesWithBaseNameAndSuffix(source, VIEW_FILE_INFIX),
        move);
    // Move the log files along with all the actual index files.
    for (auto suffix : {INDEX_LOG_SUFFIX, REBUILD_INDEX_LOG_SUFFIX}) {
      fs::path logFile = absl::StrCat(source, suffix);
      if (fs::exists(logFile)) {
        move(logFile);
      }
    }
  };

  moveIndex(config.oldIndexSource(), config.oldIndexTarget());
  moveIndex(config.newIndexSource(), config.newIndexTarget());

  // The move took the new index (and its rebuild log, if any) out of the
  // directory in which it was staged, so that directory is now empty and can be
  // removed. Everything that matters has already happened at this point, so a
  // failure here is only worth a warning.
  // NOTE: The `error_code` is only there to select the non-throwing overload of
  // `fs::remove`; it does not have to be inspected, because that overload
  // returns `false` whenever it sets an error code (and also if the directory
  // did not exist in the first place, which is just as unexpected here).
  fs::path directoryOfNewIndexSource =
      fs::path{config.newIndexSource()}.parent_path();
  ql::error_code errorCode;
  if (!directoryOfNewIndexSource.empty() &&
      !fs::remove(directoryOfNewIndexSource, errorCode)) {
    AD_LOG_WARN << "Could not remove the directory \""
                << directoryOfNewIndexSource.string()
                << "\" in which the new index was staged" << std::endl;
  }
}

}  // namespace qlever
