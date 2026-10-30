// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BINARY_EXPORT_BINARYIMPORT_H
#define QLEVER_SRC_ENGINE_BINARY_EXPORT_BINARYIMPORT_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <string>
#include <vector>

#include "engine/QueryExecutionContext.h"
#include "engine/Result.h"
#include "util/CancellationHandle.h"
#include "util/MemorySize/MemorySize.h"
#include "util/http/HttpClient.h"

namespace qlever::binary_export {

// Read the result that the exporting instance sent in the binary format (see
// `exportAsQLeverBinary`) from the `response`. The columns of the result are
// the `expectedVariables` (names with a leading `?`) in this order, and the
// response has to contain exactly these variables (in any order). The
// `resultSortedOn` are the columns by which the result is sorted (as in the
// constructor of `Result`). They are not checked, the caller has to make sure
// that the exporting instance sends the result in this order. A single string,
// as well as the strings of a single batch, may take up at most
// `maxStringBatchSize`, which protects against allocating huge amounts of
// memory for an invalid response.
//
// The strings (literals and IRIs) of the result, as well as the IRIs that are
// encoded with a pattern that doesn't exist in the local index, are converted
// like any other input (see `IdRewriter`). That is, they become encoded IRIs
// or IDs of the vocabularies of the local index if possible, and are only
// stored in the `LocalVocab` of the result otherwise.
Result importBinaryHttpResponse(
    bool requestLaziness, HttpOrHttpsResponse response,
    const QueryExecutionContext& qec,
    std::vector<std::string> expectedVariables,
    std::vector<ColumnIndex> resultSortedOn,
    ad_utility::MemorySize maxStringBatchSize,
    ad_utility::SharedCancellationHandle cancellationHandle);

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_BINARY_EXPORT_BINARYIMPORT_H
