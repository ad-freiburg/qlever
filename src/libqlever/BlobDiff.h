// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBDIFF_H
#define QLEVER_SRC_LIBQLEVER_BLOBDIFF_H

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "libqlever/BlobLayout.h"
#include "libqlever/CanonicalRowOrder.h"
#include "util/BinaryDiffApplier.h"

namespace qlever {

// Compute a diff that turns the decompressed blob `base` into the decompressed
// blob `target`, where `baseLayout` and `targetLayout` are the layouts of the
// respective blobs (see `BlobLayout::parse`). The diff is small if the two
// blobs have identical metadata and main vocabulary, if the secondary
// vocabulary of `target` extends the one of `base`, and if the named cache
// entries of both blobs only differ by inserted and deleted rows (both tables
// being in canonical order, see `isInCanonicalOrder`). Everything
// else is inserted literally. In any case
// `computeBlobDiff(...).apply(base) == target` holds.
ad_utility::BinaryDiffApplier computeBlobDiff(ql::span<const char> base,
                                              const BlobLayout& baseLayout,
                                              ql::span<const char> target,
                                              const BlobLayout& targetLayout);

// Serialize `diff` into the content of a diff file: a ZSTD frame that contains
// the magic bytes "QLVRBDIF", a `uint16_t` format version (currently 1), and
// the serialized `diff`, all written with an aligned serializer.
std::vector<char> serializeBlobDiffToFile(
    const ad_utility::BinaryDiffApplier& diff);

// Inverse of `serializeBlobDiffToFile`. Throw if `compressedDiff` is not a
// valid diff file.
ad_utility::BinaryDiffApplier readBlobDiffFromFile(
    ql::span<const char> compressedDiff);

// Apply the diff file `compressedDiff` to the compressed blob
// `compressedBaseBlob` and return the compressed target blob. Throw if the base
// cannot be decompressed, or if the diff does not belong to it.
std::vector<char> applyBlobDiff(ql::span<const char> compressedBaseBlob,
                                ql::span<const char> compressedDiff);

// Statistics of a diff, in total and per section of the target blob.
struct BlobDiffStatistics {
  // The statistics of the instructions of the diff.
  ad_utility::BinaryDiffApplier::Statistics instructions_;

  // The bytes of one kind of section of the target blob.
  struct Section {
    std::string name_;
    uint64_t size_ = 0;
    uint64_t insertedBytes_ = 0;
    uint64_t copiedBytes_ = 0;
    // The bytes that are produced by an `Align` instruction.
    uint64_t paddedBytes_ = 0;
  };
  std::vector<Section> sections_;

  // Return the section with the given name (which has to exist).
  const Section& section(std::string_view name) const;

  // Return a human-readable table.
  std::string toString() const;
};

// Replay the instructions of `diff` against the `targetLayout` and attribute
// every byte of the target to one section (metadata, main vocabulary,
// secondary vocabulary, and per entry its columns, its geo index, and the rest
// of the entry; the rest of the blob is the section "framing"). The `diff`
// must have been computed for a target with that layout.
BlobDiffStatistics computeBlobDiffStatistics(
    const ad_utility::BinaryDiffApplier& diff, const BlobLayout& targetLayout);

// Shorthand for `computeBlobDiffStatistics(...).toString()`.
std::string describeBlobDiff(const ad_utility::BinaryDiffApplier& diff,
                             const BlobLayout& targetLayout);

namespace detail {
// The marker for a target element that has no counterpart in the base.
constexpr size_t noBaseRow = noMatchingRow;

// For each element of `target` return the index of the element of `base` that
// is equal to it, or `noBaseRow`. The matched elements are increasing and each
// element of `base` is used at most once; the matching is greedy and therefore
// optimal if the elements of `base` that occur in `target` are in the same
// order and the elements that are new are pairwise distinct from all elements
// of `base`.
std::vector<size_t> greedyMatch(ql::span<const uint64_t> base,
                                ql::span<const uint64_t> target);
}  // namespace detail

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_H
