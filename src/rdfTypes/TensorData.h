// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_TENSOR_DATA_H
#define QLEVER_TENSOR_DATA_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "backports/three_way_comparison.h"
#include "global/Constants.h"
#include "parser/LiteralOrIri.h"
#include "rapidjson/document.h"
#include "util/Date.h"

namespace ad_utility {
using LiteralOrIri = triple_component::LiteralOrIri;

// A dense tensor (as used by the `https://w3id.org/rdf-tensor/` datatypes).
// The values are always stored as `float`s in row-major order; the `dtype`
// is only metadata (it determines how the values are printed, and is
// preserved by `toString`/`parseFromString` and `serialize`/`deserialize`).
// Note: `INT` values are thus only exact up to 2^24.
class TensorData {
 public:
  // NOTE: The numeric values are part of the on-disk format (`serialize`), do
  // not change them.
  enum struct DType : uint8_t { FLOAT = 0, DOUBLE = 1, BOOL = 2, INT = 3 };

 private:
  std::vector<float> tensorData_;
  std::vector<size_t> shape_;
  DType dtype_;

 public:
  TensorData() : tensorData_{}, shape_{0}, dtype_(DType::FLOAT) {}
  // Construct from the (row-major, flattened) data, the shape and the dtype.
  // The product of the `shape` should be equal to `tensorData.size()`; this
  // is not checked.
  explicit TensorData(std::vector<float> tensorData, std::vector<size_t> shape,
                      DType dtype)
      : tensorData_{std::move(tensorData)},
        shape_{std::move(shape)},
        dtype_{dtype} {}
  // Construct a one-dimensional float32 tensor.
  explicit TensorData(std::vector<float> tensorData)
      : tensorData_{std::move(tensorData)},
        // Note: `tensorData` has already been moved from at this point.
        shape_{tensorData_.size()},
        dtype_{DType::FLOAT} {}

  // Convert to a string (without quotes) that represents the stored data (a
  // JSON object of the form `{"data":[...],"shape":[...],"type":"float32"}`),
  // and the IRI of the corresponding datatype (currently always
  // `TENSOR_LITERAL`).
  std::pair<std::string, std::string> toString() const;
  // Only the JSON part of `toString()`.
  std::string toJsonString() const;
  // The literal `"<toJsonString()>"^^<TENSOR_LITERAL>`.
  LiteralOrIri toLiteral() const;

  const std::vector<float>& tensorData() const { return tensorData_; }
  ql::span<const float> span() const { return tensorData_; }
  ql::span<float> mutableSpan() { return tensorData_; }
  // Move the data out of an rvalue tensor.
  std::vector<float> releaseData() && { return std::move(tensorData_); }
  float operator[](size_t idx) const { return tensorData_[idx]; }
  size_t size() const { return tensorData_.size(); }
  bool empty() const { return tensorData_.empty(); }
  const std::vector<size_t>& shape() const { return shape_; }
  size_t numDimensions() const { return shape_.size(); }
  DType dtype() const { return dtype_; }
  void setDtype(DType dtype) { dtype_ = dtype; }

  // The names of the dtypes in the JSON representation (`float32`, `float64`,
  // `bool`, `int64`) and the inverse.
  static std::string_view dtypeToString(DType dtype);
  static std::optional<DType> dtypeFromString(std::string_view name);
  // The dtype of the result of an elementwise arithmetic operation (similar to
  // numpy's type promotion). `bool` is promoted to `int64` for arithmetic, and
  // the result of a division (`isDivision`) or of a scaling with a float is at
  // least `float32`.
  static DType promoteDtypes(DType a, DType b, bool isDivision = false);

  // Parse from the internal (normalized) string representation of a literal,
  // e.g. `"{\"data\":[1,2],\"shape\":[2],\"type\":\"float32\"}"^^<...>`. Also
  // accepts literals whose content still contains RDF escape sequences
  // (`\"`). Returns `std::nullopt` if the input is not a literal with datatype
  // `TENSOR_LITERAL` or `TENSOR_NUMERIC_LITERAL`, or if its content is not a
  // valid tensor. Never throws.
  static std::optional<TensorData> fromLiteral(
      const std::string_view& literalString);
  // Parse the JSON representation (see `toString`). The `data` may be nested
  // (it is flattened in row-major order) and may contain `true`/`false`,
  // `NaN`, `Infinity` and `-Infinity`. Throws `std::runtime_error` with a
  // descriptive message if the input is malformed, the `type` is unknown, or
  // the product of the `shape` doesn't match the number of elements.
  static TensorData parseFromString(const std::string_view& dataString);
  // Like `parseFromString`, but returns `std::nullopt` instead of throwing.
  static std::optional<TensorData> tryParseFromString(
      std::string_view dataString);
  // Extract a tensor from an already parsed JSON document (see
  // `parseFromString`). `dataString` is only used for error messages.
  static TensorData parseFromJSON(rapidjson::Document& json,
                                  const std::string_view& dataString);
  // Parse the `(content, datatype)` pair (e.g. from `toString()`). Returns
  // `std::nullopt` if the pair is `std::nullopt` or the datatype is set, but
  // not a tensor datatype. A `nullptr` datatype is treated as a tensor
  // datatype. Throws if the content is not a valid tensor.
  static std::optional<TensorData> parseFromPair(
      const std::optional<std::pair<std::string, const char*>>& pair);

  // Distances and reductions. The binary functions throw `std::runtime_error`
  // if the shapes differ. The reductions are accumulated in double precision
  // (or computed by BLAS with `QLEVER_USE_TENSOR_BLAS`). The cosine similarity
  // is 0 if one of the inputs has norm zero (including empty tensors); NaN
  // inputs propagate.
  static float cosineSimilarity(const TensorData& tensor1,
                                const TensorData& tensor2);
  static float euclideanDistance(const TensorData& tensor1,
                                 const TensorData& tensor2);
  static float norm(const TensorData& tensor);
  static float dot(const TensorData& tensor1, const TensorData& tensor2);

  // Elementwise arithmetic. Throw `std::runtime_error` if the shapes differ.
  // The dtype of the result is given by `promoteDtypes`.
  static TensorData add(const TensorData& tensor1, const TensorData& tensor2);
  static TensorData multiply(const TensorData& tensor1,
                             const TensorData& tensor2);
  static TensorData divide(const TensorData& tensor1,
                           const TensorData& tensor2);
  static TensorData subtract(const TensorData& tensor1,
                             const TensorData& tensor2);
  static TensorData scale(const TensorData& tensor1, const float& scale);
  // A tensor of zeros with the same shape and dtype.
  static TensorData zeros_like(const TensorData& tensor);

  // In-place variants (no allocation). Throw `std::runtime_error` if the
  // shapes differ. The dtype is updated as for the out-of-place variants.
  TensorData& addInPlace(const TensorData& other);
  TensorData& subtractInPlace(const TensorData& other);
  TensorData& multiplyInPlace(const TensorData& other);
  TensorData& divideInPlace(const TensorData& other);
  TensorData& scaleInPlace(float factor);

  TensorData& operator+=(const TensorData& other) { return addInPlace(other); }
  TensorData& operator-=(const TensorData& other) {
    return subtractInPlace(other);
  }
  TensorData& operator*=(const TensorData& other) {
    return multiplyInPlace(other);
  }
  TensorData& operator/=(const TensorData& other) {
    return divideInPlace(other);
  }

  // Binary format: [dtype (1 byte)][number of dimensions (uint64)]
  // [shape (uint64 per dimension)][data (float32 per element)], all in host
  // byte order, without padding.
  size_t serializedSize() const;
  std::vector<uint8_t> serialize() const;
  // Write exactly `serializedSize()` bytes to `out`.
  void serializeInto(ql::span<uint8_t> out) const;
  // Throw `std::runtime_error` if the buffer is not a valid serialization.
  static TensorData deserialize(const std::vector<uint8_t>& buffer);
  static TensorData deserialize(ql::span<const uint8_t> buffer);

  TensorData operator+(const TensorData& other) const& {
    return add(*this, other);
  }
  TensorData operator+(const TensorData& other) && {
    addInPlace(other);
    return std::move(*this);
  }
  TensorData operator-(const TensorData& other) const& {
    return subtract(*this, other);
  }
  TensorData operator-(const TensorData& other) && {
    subtractInPlace(other);
    return std::move(*this);
  }
  TensorData operator*(const TensorData& other) const& {
    return multiply(*this, other);
  }
  TensorData operator*(const TensorData& other) && {
    multiplyInPlace(other);
    return std::move(*this);
  }
  TensorData operator/(const TensorData& other) const& {
    return divide(*this, other);
  }
  TensorData operator/(const TensorData& other) && {
    divideInPlace(other);
    return std::move(*this);
  }

  // Exact equality of data (NaN != NaN), shape and dtype.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(TensorData, tensorData_, shape_,
                                              dtype_)

  // For GTest.
  friend void PrintTo(const TensorData& t, std::ostream* os);
};
void PrintTo(const TensorData& t, std::ostream* os);

inline std::optional<TensorData> operator+(const std::optional<TensorData>& lhs,
                                           const TensorData& rhs) {
  if (lhs) {
    return TensorData::add(lhs.value(), rhs);
  }
  return std::nullopt;
}
inline std::optional<TensorData> operator-(const std::optional<TensorData>& lhs,
                                           const TensorData& rhs) {
  if (lhs) {
    return TensorData::subtract(lhs.value(), rhs);
  }
  return std::nullopt;
}
inline std::optional<TensorData> operator*(const std::optional<TensorData>& lhs,
                                           const TensorData& rhs) {
  if (lhs) {
    return TensorData::multiply(lhs.value(), rhs);
  }
  return std::nullopt;
}
inline std::optional<TensorData> operator/(const std::optional<TensorData>& lhs,
                                           const TensorData& rhs) {
  if (lhs) {
    return TensorData::divide(lhs.value(), rhs);
  }
  return std::nullopt;
}

// Return true iff the two tensors have the same shape (no real broadcasting
// is supported yet).
inline bool isBroadCastable(const TensorData& tensor1,
                            const TensorData& tensor2) {
  return tensor1.shape() == tensor2.shape();
}

// Apply `func` elementwise to two tensors with the same shape. The result has
// the shape and dtype of `tensor1`. Throws if the shapes differ.
template <typename Operator>
inline TensorData binaryTensorMap(const TensorData& tensor1,
                                  const TensorData& tensor2, Operator func) {
  if (!isBroadCastable(tensor1, tensor2)) {
    throw std::runtime_error{"Tensors have different shapes"};
  }
  std::vector<float> result(tensor1.size());
  std::transform(tensor1.tensorData().begin(), tensor1.tensorData().end(),
                 tensor2.tensorData().begin(), result.begin(), func);
  return TensorData(std::move(result), tensor1.shape(), tensor1.dtype());
}

// Apply `func` elementwise. The result has the shape and dtype of `tensor1`.
template <typename Operator>
inline TensorData unaryTensorMap(const TensorData& tensor1, Operator func) {
  std::vector<float> result(tensor1.size());
  std::transform(tensor1.tensorData().begin(), tensor1.tensorData().end(),
                 result.begin(), func);
  return TensorData(std::move(result), tensor1.shape(), tensor1.dtype());
}

// Overload for rvalues that reuses the storage.
template <typename Operator>
inline TensorData unaryTensorMap(TensorData&& tensor1, Operator func) {
  auto data = tensor1.mutableSpan();
  std::transform(data.begin(), data.end(), data.begin(), func);
  return std::move(tensor1);
}

}  // namespace ad_utility

#endif  //  QLEVER_TENSOR_DATA_H
