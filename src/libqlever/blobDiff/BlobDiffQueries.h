// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFQUERIES_H
#define QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFQUERIES_H

#include <cstddef>
#include <string>
#include <string_view>

#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/BlobDiffConfig.h"

// The pinning of the named queries of a `BlobDiffConfig` by the
// `BlobDiffProducer`, such that the pinned results do not depend on the query
// plan.
namespace qlever::blobDiff {

// Return the length of the prologue of the `query`, that is, of its leading
// `PREFIX` and `BASE` declarations (each of which ends with an IRI in angle
// brackets). The prologue has to be repeated in front of a query that contains
// the `query` as a subquery (see `withCanonicalSortOrder`).
size_t prologueLength(std::string_view query);

// Return the `query`, wrapped in a query that sorts its result by all of its
// variables in the lexicographic order of their names (via QLever's
// `INTERNAL SORT BY` modifier, which is part of the query and is therefore
// kept when the result is written to a blob, see
// `canonicalizeForSerialization` in `NamedCacheSecondaryVocabRewriter.h`).
// The sort order that the plan of the `query` itself produces may differ
// between two runs (even for the same data), which would make the blobs, and
// hence the diffs, depend on the plan. The variables are obtained by parsing
// the `query` with `qlever`.
std::string withCanonicalSortOrder(const Qlever& qlever,
                                   const std::string& query);

// Drop all pinned results of `qlever` and pin the queries of `config`, each
// with the canonical sort order (see `withCanonicalSortOrder`).
void pinAllQueries(Qlever& qlever, const BlobDiffConfig& config);

}  // namespace qlever::blobDiff

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFQUERIES_H
