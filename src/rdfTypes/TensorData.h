#ifndef QLEVER_TENSOR_DATA_H
#define QLEVER_TENSOR_DATA_H

#include <cstdint>

#include "backports/three_way_comparison.h"
#include "global/Constants.h"
#include "parser/LiteralOrIri.h"
#include "rapidjson/document.h"
#include "util/Date.h"

namespace ad_utility {
using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
class TensorData {
 public:
  enum struct DType { FLOAT = 0, DOUBLE, BOOL, INT };

 private:
  // only support float tensors with one dimension for now
  std::vector<float> tensorData_;
  std::vector<size_t> shape_;
  DType dtype_;

 public:
  // Construct a `TensorData` given a `TensorData` object.
  explicit TensorData(std::vector<float> tensorData, std::vector<size_t> shape,
                      DType dtype)
      : tensorData_{std::move(tensorData)},
        shape_{std::move(shape)},
        dtype_{dtype} {}

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
  static bool isBroadCastable(const TensorData& tensor1,
                              const TensorData& tensor2);
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
  static TensorData subtract(const TensorData& tensor1,
                             const TensorData& tensor2);

  std::vector<uint8_t> serialize() const;
  static TensorData deserialize(const std::vector<uint8_t>& buffer);
};
}  // namespace ad_utility
#ifdef QLEVER_CPP_17
static_assert(std::is_default_constructible_v<DateYearOrDuration>);
#endif

#endif  //  QLEVER_TENSOR_DATA_H
