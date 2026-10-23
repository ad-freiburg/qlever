// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFPRODUCER_H
#define QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFPRODUCER_H

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "backports/span.h"
#include "libqlever/blobDiff/BlobDiffConfig.h"

namespace qlever {

// The producer side of the weekly workflow: it holds the state that is needed
// to turn the data of week `k` into a blob `B_k` and a small diff file `D_k`,
// such that the consumer, who only holds the blob `B_{k-1}`, can compute `B_k`
// by `blobDiff::applyBlobDiffToCompressedBase(B_{k-1}, D_k)` (byte for byte).
// All functions are static, the whole state lives in a state directory.
//
// WHAT THE PRODUCER KEEPS between weeks (all in the state directory):
//  1. The index of week 0 (`index/index.*`, never modified after `init`).
//  2. The persisted delta triples (`index/index.update-triples`), which
//     contain the net difference of all following weeks to the index of week 0
//     (inserted and deleted triples, and their words that are not in the
//     vocabulary of week 0).
//  3. `current.ttl`, the Turtle file of the current week. The next week's
//     Turtle file is compared against it to obtain the triples to insert and
//     to delete.
//  4. `current.blob`, the current compressed blob. It is the base for the
//     incremental serialization (which keeps the `Id`s of the words of the
//     secondary vocabulary stable) and for the diff.
//  5. `state.json`: the week counter, timestamps, sizes, and the fingerprint of
//     the config. The config itself is passed on every call and has to be the
//     same in every week (its fingerprint is checked).
//
// WHAT THE CONSUMER KEEPS: only its current compressed blob (`B_{k-1}`), and
// afterwards the one it has computed. No index files and no Turtle files.
//
// IMPORTANT: The blob is only a small diff to the previous one if the main
// vocabulary (and hence all `Id`s) of week 0 stay valid. That is why the
// producer never rebuilds the index but applies the weekly changes as delta
// triples to the index of week 0 (with new words going to the append-only
// secondary vocabulary of the blob).
//
// NOTE: The query plan of a pinned query (and thereby the order of the columns
// and the sort order of its result) may differ between two runs, even for the
// same data. The columns of every entry are written to a blob in the order of
// their variable names, and the producer pins every query with an explicit
// sort order on all of its variables (via QLever's `INTERNAL SORT BY`
// modifier, see `pinAllQueries` in the implementation), which makes the blobs,
// and hence the diffs, independent of the plan. The sort order of the query
// itself (if any) is not preserved in the blob.
class BlobDiffProducer {
 public:
  // The result of one weekly step.
  struct WeeklyResult {
    // The new compressed blob and the compressed diff file that turns the
    // previous blob into it.
    std::vector<char> blob_;
    std::vector<char> diffFile_;
    // A human-readable description of the diff (see
    // `blobDiff::describeBlobDiff`).
    std::string statistics_;
    // The number of triples that were inserted/deleted in this step.
    size_t numInsertedTriples_ = 0;
    size_t numDeletedTriples_ = 0;
  };

  // Week 0: Build the index from `turtleFile` into the (not existing or empty)
  // `stateDir`, pin the queries of `config`, write the first blob (without a
  // base) to `current.blob`, and create the remaining state files. Return the
  // compressed blob. Throw if `stateDir` is not empty.
  static std::vector<char> init(const BlobDiffConfig& config,
                                const std::string& turtleFile,
                                const std::string& stateDir);

  // Week `k > 0`: Apply the difference between the previous Turtle file and
  // `newTurtleFile` as delta triples to the index, pin the queries again, write
  // the new blob incrementally against the previous one, compute the diff, and
  // verify that applying it to the previous blob yields the new blob exactly
  // (otherwise throw, and leave the state unchanged). On success, rotate the
  // state (see above). If `forceCompaction` is true, the geo indices are
  // rebuilt as a single segment each, so this week's diff is larger, but the
  // following weeks start from compact indices.
  static WeeklyResult step(const BlobDiffConfig& config,
                           const std::string& newTurtleFile,
                           const std::string& stateDir,
                           bool forceCompaction = false);

  // Return a description of `blob` (its sizes and checksum), and, if a
  // `diffFile` is given, of the diff and whether it yields or applies to the
  // `blob`.
  static std::string inspect(ql::span<const char> blob,
                             std::optional<ql::span<const char>> diffFile);
};

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFPRODUCER_H
