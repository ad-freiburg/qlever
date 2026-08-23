// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INDEXSWAP_H
#define QLEVER_SRC_INDEX_INDEXSWAP_H

#include <optional>
#include <string>

// Replacing the index at a given base name by a new index that was first
// staged in a temporary subdirectory: the new index is built (or otherwise
// produced) next to the currently served index, and only when it is complete,
// the old index is retired to a subdirectory and the new index is moved to the
// base name of the old one. This is deliberately the only supported layout:
// two indexes side by side in the same directory (under different base names)
// invite confusion about which one is in use. The runtime index rebuild
// (`Qlever::rebuildIndexToDisk` + `Qlever::swapInRebuiltIndex`) and the
// `qlever-upgrade-index` binary (see `index/IndexFormatConverter.h`) both use
// this machinery, with their own directory names.

namespace qlever {

// The four base names that are involved in swapping a staged index into
// place. All paths are relative to the working directory of the engine. The
// base names are validated and fixed at construction time and afterwards only
// readable via the accessors. The constructor enforces that the base names do
// not collide in a way that would overwrite files that are still needed.
class IndexSwapConfig {
 private:
  // These are documented at their accessors below.
  std::string oldIndexSource_;
  std::string newIndexSource_;
  std::string oldIndexTarget_;
  std::string newIndexTarget_;

 public:
  // Construct from the four base names (see the accessors below for their
  // meaning). Throws if the base names collide.
  IndexSwapConfig(std::string oldIndexSource, std::string newIndexSource,
                  std::string oldIndexTarget, std::string newIndexTarget);

  // The base name of the index that is about to be replaced by the staged new
  // one. This is where the old index is moved *from*.
  const std::string& oldIndexSource() const { return oldIndexSource_; }

  // The base name under which the new index was staged in a temporary
  // location. This is where the new index is moved *from*. After the new
  // index has been moved to its final place, the containing directory is
  // typically removed again.
  const std::string& newIndexSource() const { return newIndexSource_; }

  // The base name to which the files of the old index are moved when the new
  // index is swapped in. This is where the old index is moved *to*. The
  // resulting files form a complete index that a server can be started on in
  // case something is wrong with the new index.
  const std::string& oldIndexTarget() const { return oldIndexTarget_; }

  // The base name under which the new index lives after the swap (and from
  // which a later start of the server loads it). This is where the new index
  // is moved *to*. Typically the same location as the old index, so that the
  // "current" index has a stable location.
  const std::string& newIndexTarget() const { return newIndexTarget_; }
};

// The naming scheme with which `makeIndexSwapConfig` below derives its two
// directories when they are not given explicitly.
struct IndexSwapNaming {
  // The default directory in which the new index is staged is
  // `<stagingDirPrefix_><current datetime>.tmp`.
  std::string stagingDirPrefix_;

  // The default directory to which the old index is retired is
  // `<retiredDirPrefix_><retiredDirDatetime_>`, where `retiredDirDatetime_`
  // is typically the build date of the old index.
  std::string retiredDirPrefix_;
  std::string retiredDirDatetime_;

  // Appended verbatim to the error message for the (unlikely) case that the
  // default name for the retired directory and all of its uniquified variants
  // (see `makeIndexSwapConfig`) are already taken. Callers that have an
  // option for choosing the directory explicitly can mention it here.
  std::string retiredDirConflictHint_;
};

// Assemble the `IndexSwapConfig` for replacing the index with the base name
// `currentBaseName` by a staged new index: the new index is staged in the
// directory `stagingDir` and the old index is retired to the directory
// `retiredDir`. Both default (if `std::nullopt`) to a directory that is
// derived from `naming` (see there) and lies inside the directory of
// `currentBaseName`. Inside these directories, and for the new index after
// the swap, the file name of `currentBaseName` is used: the new index has to
// end up at the base name the old index lived at, so that a later start of
// the server loads it.
//
// The datetime in the default name of the retired directory has a granularity
// of one second, so when swaps happen in quick succession (e.g. automatic
// rebuilds on a small index, see `--rebuild-index-strategy`), two index
// generations can carry the same datetime, and the default directory for the
// second of them is then already taken. Append `.1`, `.2`, ... in that case
// (like the numbered backups of `logrotate`). Without this, the swap would
// fail, and since a failed swap does not re-stamp the datetime of the current
// index, all subsequent swaps would fail the same way. Only the default name
// is uniquified; an explicitly given directory that is taken remains an
// error.
//
// The two directories, when given explicitly, must be relative paths (they
// are resolved against the working directory, just like `currentBaseName`).
// Explicit or defaulted, they must be empty or not exist yet, and must lie
// inside the directory of `currentBaseName`, so that the index directories
// are not nested ever deeper. Throws `std::runtime_error` if one of these
// conditions is violated, and (via the `IndexSwapConfig` constructor) if the
// resulting base names collide.
IndexSwapConfig makeIndexSwapConfig(const std::string& currentBaseName,
                                    const IndexSwapNaming& naming,
                                    std::optional<std::string> stagingDir,
                                    std::optional<std::string> retiredDir);

// Perform the on-disk part of the swap described by `config`: move all files
// of the old index (permutations, vocabularies, materialized views, build
// logs) from `oldIndexSource` to `oldIndexTarget`, move the staged new index
// from `newIndexSource` to `newIndexTarget` the same way, and remove the then
// empty directory in which the new index was staged.
//
// NOTE: If this throws halfway through, the on-disk layout has to be repaired
// manually; a running server that has the old index open keeps running
// consistently on it (open file handles survive the renames).
void moveIndexIntoPlace(const IndexSwapConfig& config);

}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_INDEXSWAP_H
