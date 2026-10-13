// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_ID_TABLE_VALUE_EXTRACTION_H
#define QLEVER_ID_TABLE_VALUE_EXTRACTION_H

#include <optional>
#include <string>

#include "engine/VariableToColumnMap.h"
#include "global/Id.h"
#include "index/Index.h"
#include "index/LocalVocab.h"

namespace qlever {

// Convert an Id to an integer if it stores a numeric type
std::optional<int64_t> getInt(Id id);

// Convert an Id to a double if it stores a numeric type
std::optional<double> getDouble(Id id);

// Convert an Id to a boolean if it stores a boolean type
std::optional<bool> getBool(Id id);

// Convert an Id to a string representation
std::optional<std::string> getString(Id id, const Index& index,
                                     const LocalVocab& localVocab);

std::optional<int64_t> getFeatureIdFromIri(Id id, const Index& index,
                                           const LocalVocab& localVocab);

// Helper function to convert an Id to URI string
std::string idToUri(Id id, const Index& index, const LocalVocab& localVocab);

// Helper function to convert a URI string back to Id for a specific instance
Id uriToId(const std::string& uri, const Index& index);

// Helper function to extract column index for a variable, with assertion
ColumnIndex getColumnIndex(const VariableToColumnMap& variableColumns,
                           const Variable& var);

std::optional<ColumnIndex> getOptionalColumnIndex(
    const VariableToColumnMap& variableColumns, const Variable& var);

}  // namespace qlever

#endif  // QLEVER_ID_TABLE_VALUE_EXTRACTION_H
