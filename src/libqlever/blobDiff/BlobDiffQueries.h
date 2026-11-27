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

// The pinning of the named queries of a `BlobDiffConfig`, such that the pinned
// results do not depend on the query plan.
namespace qlever::blobDiff {

// Return the length of the prologue of the `query`, that is, of its leading
// `PREFIX` and `BASE` declarations, including the whitespace and comments that
// follow them. Throw if the prologue is malformed.
//
// NOTE: The prologue is determined by QLever's SPARQL parser. It has to be
// repeated in front of a query that contains the `query` as a subquery (see
// `withCanonicalSortOrder`).
size_t prologueLength(std::string_view query);

// Return the `query`, wrapped in a query that sorts its result by all of its
// variables via QLever's `INTERNAL SORT BY` modifier. If the `query` has an
// explicit `INTERNAL SORT BY` itself, then its keys come first (in their
// order). The remaining variables follow in the lexicographic order of their
// names. A query without variables is returned unchanged. Throw if the `query`
// is not a `SELECT` query, if it has a `FROM` or `FROM NAMED` clause, or if a
// key of its `INTERNAL SORT BY` is not selected.
//
// NOTE 1: The sort order that the plan of the `query` itself produces may
// differ between two runs (even for the same data), which would make the
// blobs, and hence the diffs, depend on the plan.
//
// NOTE 2: The `INTERNAL SORT BY` is part of the query and is therefore kept
// when the result is written to a blob (see `canonicalizeForSerialization` in
// `NamedCacheSecondaryVocabRewriter.h`). A semantic `ORDER BY` is not kept
// there anyway, so it is not treated specially here.
//
// NOTE 3: A subquery cannot have a `FROM` or `FROM NAMED` clause, so a query
// with such a clause cannot be wrapped.
std::string withCanonicalSortOrder(const Qlever& qlever,
                                   const std::string& query);

// Drop all pinned results of `qlever` and pin the queries of `config`, each
// with the canonical sort order (see `withCanonicalSortOrder`). Throw if one of
// the queries cannot be wrapped (see `withCanonicalSortOrder`), or if its
// `geoIndexVar_` is not one of its selected variables.
//
// NOTE: All queries are checked before the first pinned result is dropped, so
// an invalid config leaves the pinned results unchanged. If running one of
// the queries fails, then only the queries before it are pinned.
void pinAllQueries(Qlever& qlever, const BlobDiffConfig& config);

}  // namespace qlever::blobDiff

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFFQUERIES_H
