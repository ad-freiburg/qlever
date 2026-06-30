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

#include <cstdint>
#include <optional>
#include <string_view>
#include <variant>
#include <vector>

#include "backports/span.h"

namespace ad_utility {

// A vector of `float`s that is either *borrowed* — a `span` into storage that
// outlives the access (the memory map held by the `EmbeddingVocabulary`, valid
// for the index's lifetime) — or *owned* (a `std::vector`, e.g. a freshly
// parsed inline query vector, or a literal parsed on demand when there is no
// embedding sidecar). This lets `getEmbedding` be zero-copy when an embedding
// split is present while still supporting the owning parse path behind one
// type.
//
// The view is always computed on demand by `span()` from whichever alternative
// is active, so the two alternatives stay mutually exclusive and there is never
// a `span` member pointing into a sibling owned `vector` (which a move would
// dangle).
class MaybeOwnedVector {
 private:
  std::variant<ql::span<const float>, std::vector<float>> data_;

 public:
  // Read-only contiguous range of `float` (the typedefs let it be used directly
  // as a container, e.g. with gtest's `ElementsAre`).
  using value_type = float;
  using const_iterator = typename ql::span<const float>::iterator;
  using iterator = const_iterator;

  MaybeOwnedVector() = default;
  explicit MaybeOwnedVector(ql::span<const float> borrowed) : data_{borrowed} {}
  explicit MaybeOwnedVector(std::vector<float> owned)
      : data_{std::move(owned)} {}

  // The vector contents, regardless of ownership.
  ql::span<const float> span() const {
    return std::visit(
        [](const auto& d) -> ql::span<const float> {
          return ql::span<const float>{d};
        },
        data_);
  }

  size_t size() const { return span().size(); }

  // Range access (iterates the underlying storage; the `span` temporary's
  // iterators are plain pointers into that storage, so they stay valid).
  auto begin() const { return span().begin(); }
  auto end() const { return span().end(); }
};

// On-disk format version of the `EmbeddingVocabulary` sidecar. Bump this
// whenever the sidecar layout changes; an index with a mismatching version is
// rejected at load and must be rebuilt.
constexpr inline uint32_t EMBEDDING_VECTOR_VERSION = 1;

// Parse the body of an embedding-vector literal, i.e. a string of the form
// `[0.1, -0.2, 1.0]`, into a vector of `float`s. Returns `std::nullopt` if the
// string is not a well-formed, non-empty array of finite decimal numbers.
// Used by the query-time value getter to decode both stored and inline query
// vectors.
std::optional<std::vector<float>> parseFloatVectorArrayBody(
    std::string_view body);

// Parse a full serialized `emb:fp32Vector` literal of the form
// `"[...]"^^<...fp32Vector>` into a vector of `float`s: strip the surrounding
// quote and datatype suffix, then delegate to `parseFloatVectorArrayBody`.
// Returns `std::nullopt` if the suffix does not match or the body is malformed.
// Shared by the `EmbeddingVocabulary` builder (decoding the stored literal
// word) and the query-time value getter.
std::optional<std::vector<float>> parseFp32VectorLiteral(std::string_view word);

}  // namespace ad_utility

#endif  // QLEVER_SRC_RDFTYPES_EMBEDDINGVECTOR_H
