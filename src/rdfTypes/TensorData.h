#ifndef QLEVER_TENSOR_DATA_H
#define QLEVER_TENSOR_DATA_H

#include <cstdint>

#include "backports/three_way_comparison.h"
#include "global/Constants.h"
#include "parser/LiteralOrIri.h"
#include "rapidjson/document.h"
#include "util/Date.h"

namespace ad_utility {
using LiteralOrIri = triple_component::LiteralOrIri;
class TensorData {
 public:
  enum struct DType { FLOAT = 0, DOUBLE, BOOL, INT };

 private:
  // only support float tensors with one dimension for now
  std::vector<float> tensorData_;
  std::vector<size_t> shape_;
  DType dtype_;

 public:
  TensorData() : tensorData_({}), shape_({0}), dtype_(DType::FLOAT) {}
  // Construct a `TensorData` given a `TensorData` object.
  explicit TensorData(std::vector<float> tensorData, std::vector<size_t> shape,
                      DType dtype)
      : tensorData_{std::move(tensorData)},
        shape_{std::move(shape)},
        dtype_{dtype} {}
  explicit TensorData(std::vector<float> tensorData)
      : tensorData_{std::move(tensorData)},
        // Note: `tensorData` has already been moved from at this point.
        shape_{std::vector({tensorData_.size()})},
        dtype_{DType::FLOAT} {}

  // Convert to a string (without quotes) that represents the stored data, and a
  // pointer to the IRI of the corresponding datatype (currently always
  // `tensor:DataTensor`).
  std::pair<std::string, std::string> toString() const;
  LiteralOrIri toLiteral() const;
  const std::vector<float> tensorData() const { return tensorData_; }
  float operator[](size_t idx) const { return tensorData_[idx]; }
  size_t size() const { return tensorData_.size(); }
  const auto& shape() const { return shape_; }
  DType dtype() const { return dtype_; }
  static std::optional<TensorData> fromLiteral(
      const std::string_view& literalString);
  static TensorData parseFromString(const std::string_view& dataString);
  static TensorData parseFromJSON(rapidjson::Document& json,
                                  const std::string_view& dataString);
  static std::optional<TensorData> parseFromPair(
      const std::optional<std::pair<std::string, const char*>>& pair);

  static float cosineSimilarity(const TensorData& tensor1,
                                const TensorData& tensor2);
  static float euclideanDistance(const TensorData& tensor1,
                                 const TensorData& tensor2);
  static float norm(const TensorData& tensor);

  static float dot(const TensorData& tensor1, const TensorData& tensor2);
  static TensorData add(const TensorData& tensor1, const TensorData& tensor2);

  static TensorData multiply(const TensorData& tensor1,
                             const TensorData& tensor2);
  static TensorData divide(const TensorData& tensor1,
                           const TensorData& tensor2);
  static TensorData subtract(const TensorData& tensor1,
                             const TensorData& tensor2);

  static TensorData scale(const TensorData& tensor1, const float& scale);
  static TensorData zeros_like(const TensorData& tensor);

  std::vector<uint8_t> serialize() const;
  static TensorData deserialize(const std::vector<uint8_t>& buffer);

  TensorData operator+(const TensorData& other) const {
    return add(*this, other);
  }
  TensorData operator-(const TensorData& other) const {
    return subtract(*this, other);
  }
  TensorData operator*(const TensorData& other) const {
    return multiply(*this, other);
  }
  TensorData operator/(const TensorData& other) const {
    return divide(*this, other);
  }
};

inline std::optional<TensorData> operator+(const std::optional<TensorData>& lhs,
                                           const TensorData& rhs) {
  if (lhs) {
    const auto sum = TensorData::add(lhs.value(), rhs);
    return std::make_optional<TensorData>(sum);
  }
  return std::nullopt;
}
inline std::optional<TensorData> operator-(const std::optional<TensorData>& lhs,
                                           const TensorData& rhs) {
  if (lhs) {
    const auto sum = TensorData::subtract(lhs.value(), rhs);
    return std::make_optional<TensorData>(sum);
  }
  return std::nullopt;
}
inline std::optional<TensorData> operator*(const std::optional<TensorData>& lhs,
                                           const TensorData& rhs) {
  if (lhs) {
    const auto sum = TensorData::multiply(lhs.value(), rhs);
    return std::make_optional<TensorData>(sum);
  }
  return std::nullopt;
}
inline std::optional<TensorData> operator/(const std::optional<TensorData>& lhs,
                                           const TensorData& rhs) {
  if (lhs) {
    const auto sum = TensorData::divide(lhs.value(), rhs);
    return std::make_optional<TensorData>(sum);
  }
  return std::nullopt;
}

static bool isBroadCastable(const TensorData& tensor1,
                            const TensorData& tensor2) {
  // For simplicity, we only support broadcasting for tensors with the same
  // shape
  if (tensor1.shape().size() != tensor2.shape().size()) {
    return false;
  }
  for (size_t i = 0; i < tensor1.shape().size(); i++) {
    if (tensor1.shape()[i] != tensor2.shape()[i]) {
      return false;
    }
  }
  return true;
}
template <typename Operator>
static TensorData binaryTensorMap(const TensorData& tensor1,
                                  const TensorData& tensor2, Operator func) {
  if (!isBroadCastable(tensor1, tensor2)) {
    throw std::runtime_error{"Tensors are not broadcastable for addition"};
  }
  std::vector<float> result(tensor1.tensorData().size());
  std::transform(tensor1.tensorData().begin(), tensor1.tensorData().end(),
                 tensor2.tensorData().begin(), result.begin(), func);
  return TensorData(std::move(result), tensor1.shape(), tensor1.dtype());
}
template <typename Operator>
static TensorData unaryTensorMap(const TensorData& tensor1, Operator func) {
  std::vector<float> result(tensor1.tensorData().size());
  std::transform(tensor1.tensorData().begin(), tensor1.tensorData().end(),
                 result.begin(), func);
  return TensorData(std::move(result), tensor1.shape(), tensor1.dtype());
}

}  // namespace ad_utility

#endif  //  QLEVER_TENSOR_DATA_H
