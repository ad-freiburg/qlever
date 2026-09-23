// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INDEXFORMATCONVERTER_H
#define QLEVER_SRC_INDEX_INDEXFORMATCONVERTER_H

#include <optional>
#include <string>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/IndexFormatVersion.h"
#include "rdfTypes/GeoPoint.h"
#include "util/Iterators.h"
#include "util/MemorySize/MemorySize.h"

// The conversion of an index from the previous on-disk format to the current
// one, see `convertIndexToCurrentFormat` below. This is what the standalone
// `qlever-upgrade-index` executable (see `src/IndexUpgraderMain.cpp`) does. The
// server also uses `indexNeedsNoConversion` below, to accept an index in the
// previous format that the conversion would not change.
namespace qlever::indexFormatConverter {

// The index format that this converter converts from, and the index format that
// it converts to. They are deliberately hardcoded here, because the conversion
// is specific to exactly this pair of formats: the *only* difference between
// them is the bit representation of a `GeoPoint` inside an `Id`. The source
// format stores the quantized latitude in the upper 30 and the quantized
// longitude in the lower 30 of the 60 value bits, the target format interleaves
// the bits of the two coordinates (Z-order, see `GeoPoint`). The datatype bits
// and the quantization are the same in both formats, so an `Id` of any other
// datatype is the same in both formats, and a `GeoPoint` `Id` is converted by
// rearranging its value bits (see `convertId`). As this changes the order of
// the `GeoPoint` `Id`s, the permutations have to be partially re-sorted (see
// `sortRunsOfGeoPoints`).
//
// `convertIndexToCurrentFormat` checks that these two formats still are the
// previous and the current index format (`qlever::previousIndexFormatVersion`
// resp. `qlever::indexFormatVersion`), so that this converter cannot silently
// be applied to a different change of the index format.
inline const IndexFormatVersion sourceVersion{
    3159, DateYearOrDuration{Date{2026, 9, 1}}};
inline const IndexFormatVersion targetVersion{
    3412, DateYearOrDuration{Date{2026, 9, 19}}};

// Return a human-readable description of the two index formats above and of
// their difference. This is the overview message of `qlever-upgrade-index`.
std::string conversionDescription();

// Convert the value bits of a `GeoPoint` from the source format (latitude in
// the upper 30 bits, longitude in the lower 30 bits) to the target format
// (bit-interleaved, see `GeoPoint::interleaveCoordinates`).
GeoPoint::T convertGeoPointBits(GeoPoint::T bits);

// Convert a single `Id` from the source format to the target format. This is
// the identity for all datatypes except `Datatype::GeoPoint`, whose value bits
// are rearranged (see `convertGeoPointBits`). Throw if `id` is of type
// `LocalVocabIndex`, which must never be stored on disk (such an `Id` holds a
// pointer into the memory of the process that created it).
//
// NOTE: The conversion preserves the order of two `Id`s unless both are
// `GeoPoint`s. That is what limits the re-sorting of a permutation to runs of
// rows that agree on all columns before the first column that holds a
// `GeoPoint`, see `sortRunsOfGeoPoints` below.
Id convertId(Id id);

// Return true iff the `id` is a `GeoPoint`.
inline bool isGeoPoint(Id id) { return id.getDatatype() == Datatype::GeoPoint; }

// The block size (per column) with which the permutations of the converted
// index are written. It is set from the configuration of the index that is
// converted (`BLOCKSIZE_PERMUTATIONS_PER_COLUMN_KEY`), so that the converted
// permutations have exactly the blocks that a fresh build of that index with
// the same block size would have. Indexes that were built before that key
// existed use the default block size of the index builder.
//
// NOTE: It is a mutable global because the conversion writes the permutations
// deep inside a call chain that it is not worth threading a parameter through
// (the converter converts one index per process run). A unit test may also set
// it directly; with the default, a relation only gets a
// `CompressedRelationMetadata` of its own if it has more than 25000 rows,
// which no unit test can afford to build (see `writePermutation` in the
// implementation).
inline ad_utility::MemorySize& blocksizeOfConvertedPermutations() {
  static ad_utility::MemorySize blocksize =
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN;
  return blocksize;
}

// The amount of memory that the re-sorting of the permutations may use (see
// `sortRunsOfGeoPoints`). The two permutations of a pair are converted
// concurrently, each of them with half of this amount; a run that does not fit
// into that half is sorted externally. This is the `--memory-for-sorting`
// option of `qlever-upgrade-index`.
inline ad_utility::MemorySize& memoryForSorting() {
  static ad_utility::MemorySize memory = DEFAULT_MEMORY_LIMIT_INDEX_BUILDING;
  return memory;
}

// The number of bytes that the conversion considers free on the filesystem of
// the converted index. It is read from the filesystem unless it is set here,
// which only a unit test does (there is no way to control the free space of a
// filesystem from a test).
inline std::optional<uint64_t>& freeSpaceForTesting() {
  static std::optional<uint64_t> freeSpace = std::nullopt;
  return freeSpace;
}

// Restore the sort order of a permutation whose `Id`s have been converted to
// the target format (see `convertId`). The input `blocks` are the rows of the
// permutation in the order of the source format, with all `Id`s already
// converted, and the output are the same rows in the order of the target
// format, in blocks of arbitrary size. The rows are sorted by their first
// `NumColumnsIndexBuilding` columns (the permuted triple and its graph, which
// is how the index builder sorts them); the remaining columns are payload.
//
// The conversion of an `Id` only changes the relative order of two `GeoPoint`s
// (see `convertId`), so the input is sorted except inside maximal runs of
// consecutive rows that have their first `GeoPoint` in the same column and
// agree on all columns before it. Only those runs are sorted, each one
// individually, and everything else streams through as is. This does not rely
// on any property of the data, but is cheap when the runs are short (a subject
// or a subject-predicate pair rarely has more than one point), and it needs
// exactly one external sort per run that does not fit into memory (for
// example, all points of an `OSP` permutation, or all points of a predicate in
// a `POS` permutation). A run is sorted in memory up to half of `memory`, and
// with the external sorter beyond that, which uses the file `sortTempFilename`.
//
// The returned range has to be consumed before `blocks` would be destroyed,
// and it consumes `blocks` lazily.
ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortRunsOfGeoPoints(
    ad_utility::InputRangeTypeErased<IdTableStatic<0>> blocks,
    std::string sortTempFilename, ad_utility::MemorySize memory);

// Return true iff the index with the base name `basename` contains an encoded
// `GeoPoint` anywhere, that is, either an `Id` of type `GeoPoint` in a
// permutation, or a geometry in its geometry vocabulary (the precomputed
// information of every geometry, see `GeometryInfo`, holds its bounding box
// and its centroid as encoded points, whether the geometry is a point or not).
// The latter is determined by scanning the `.geoinfo` files for a valid
// record, which are small unless the index has many geometries. The former is
// determined from the metadata of the blocks of one permutation with the
// object as its first column (`OSP`, or `OPS`), where all points form one
// contiguous range, so that at most one block has to be read; for an index
// built with `--only-pso-and-pos-permutations`, the blocks of `POS` whose
// metadata does not settle the question are read. The internal permutations
// are checked in the same way. Return true if the index has no such
// permutations at all (which is not a valid index), so that no caller mistakes
// a broken index for an index without points.
bool indexContainsGeoPoints(const std::string& basename);

// Return true iff the index with the base name `basename`, which is in the
// source format, can be used by a QLever binary of the target format without
// any conversion: the two formats differ only in the encoding of `GeoPoint`s,
// so this is the case iff the index contains no `GeoPoint` (see
// `indexContainsGeoPoints`) and has no persisted updates (which contain `Id`s
// that are not checked, see `UPDATE_TRIPLES_SUFFIX`).
bool indexNeedsNoConversion(const std::string& basename);

// Convert the index with the base name `oldBasename` from the source format to
// the target format and write the result to the base name `newBasename`. The
// index at `oldBasename` is left unchanged, and the two base names must be
// different.
//
// If the index needs no conversion (see `indexNeedsNoConversion`), all its
// files are copied unchanged, including its materialized views, and only the
// index format version in its configuration is set to the target format.
// Otherwise, the permutations and the patterns are converted (the `Id`s of the
// permutations are rewritten and their runs of points are re-sorted, see
// `sortRunsOfGeoPoints`), the encoded points in the geometry information of
// the geometry vocabulary (its `.geoinfo` file, see `GeoVocabulary`) are
// rewritten, and all other files are copied unchanged (the rest of the
// vocabulary, the text index, and the settings). Note that the text index
// needs no conversion, because it stores plain integers and reconstructs the
// `Id`s when it is read (see `index/TextIndexReadWrite.cpp`), and that the
// same holds for the words of the vocabulary, which stores no `Id`s at all.
//
// The materialized views of an index with points are deliberately NOT
// converted: they are not written to the converted index, and a warning lists
// them, because they have to be created again for the converted index. This
// keeps the converter simple (a view is a permutation with an arbitrary number
// of columns, sorted by all of them) and costs no more than creating the views
// again, which is what a rebuild of the index would require as well.
//
// Throw if the index at `oldBasename` does not exist, if it is not in the
// source format (in particular if it already is in the target format), if it
// has persisted updates (see `UPDATE_TRIPLES_SUFFIX`; those have to be
// materialized into the index or deleted before the conversion), or if any of
// the files at `newBasename` already exist.
void convertIndexToCurrentFormat(const std::string& oldBasename,
                                 const std::string& newBasename);

// The directory (inside the directory of the index) in which
// `upgradeIndexInPlace` below stages the upgraded index, and the directory to
// which it retires the index in the old format. The names are deliberately
// different from the `rebuild.<datetime>.tmp` and `previous.<datetime>`
// directories of the runtime index rebuild (see
// `Qlever::makeIndexRebuildConfig`), which have nothing to do with an upgrade;
// in particular, the `--rebuild-keep-previous-index-dirs` policy only
// considers `previous.*` directories and hence never deletes a retired index
// in the old format.
inline constexpr std::string_view stagingDirPrefix = "index-in-new-format.";
inline constexpr std::string_view retiredDirPrefix = "index-in-old-format.";

// Upgrade the index with the given base name from the source format to the
// target format in place.
//
// If the index needs no conversion (see `indexNeedsNoConversion`), only the
// index format version in its configuration file is set to the target format,
// and nothing else is touched. Otherwise, the index is converted via
// `convertIndexToCurrentFormat` above (see there for what is converted and for
// the errors that abort the upgrade before it writes anything), as follows:
//
// 1. The upgraded index is written to the staging directory
//    `<stagingDirPrefix><current datetime>.tmp` inside the directory of the
//    index.
// 2. Check that the upgraded index can be loaded and that the number of
//    triples of each of its permutations matches the configuration of the
//    index that was upgraded.
// 3. Only then, the index in the old format is moved to the directory
//    `<retiredDirPrefix><datetime of the build of that index>`, together with
//    its materialized views (which are not converted, see above), the upgraded
//    index is moved to the base name the old index lived at (so that a server
//    start with the same base name now loads it), and the then empty staging
//    directory is removed.
//
// If the upgrade fails in step 1 or 2, the original index is untouched and
// the staging directory is left behind for inspection; it can simply be
// deleted (a later retry stages into a fresh directory).
void upgradeIndexInPlace(const std::string& basename);

}  // namespace qlever::indexFormatConverter

#endif  // QLEVER_SRC_INDEX_INDEXFORMATCONVERTER_H
