// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_RDFTYPES_EMBEDDINGVECTOR_H
#define QLEVER_SRC_RDFTYPES_EMBEDDINGVECTOR_H

#include <optional>
#include <string_view>
#include <vector>

namespace ad_utility {

// Parse the body of an embedding-vector literal, i.e. a string of the form
// `[0.1, -0.2, 1.0]`, into a vector of `float`s. Returns `std::nullopt` if the
// string is not a well-formed, non-empty array of finite decimal numbers.
// Used by the query-time value getter to decode both stored and inline query
// vectors.
std::optional<std::vector<float>> parseFloatVectorArrayBody(
    std::string_view body);

// Parse a full serialized `fp32Vector` literal of the form
// `"[...]"^^<...fp32Vector>` into a vector of `float`s: strip the surrounding
// quote and datatype suffix, then delegate to `parseFloatVectorArrayBody`.
// Returns `std::nullopt` if the suffix does not match or the body is malformed.
std::optional<std::vector<float>> parseFp32VectorLiteral(std::string_view word);

}  // namespace ad_utility

#endif  // QLEVER_SRC_RDFTYPES_EMBEDDINGVECTOR_H
