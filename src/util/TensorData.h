#ifndef QLEVER_TENSOR_DATA_H
#define QLEVER_TENSOR_DATA_H

#include <cstdint>

#include "backports/three_way_comparison.h"
#include "global/Constants.h"
#include "nlohmann/json.hpp"
#include "util/Date.h"
#include "util/Duration.h"
#include "util/NBitInteger.h"
#include "util/Serializer/Serializer.h"

class TensorData {
 public:
  enum struct DType { FLOAT = 0, BOOL, INT };

 private:
  // only support float tensors with one dimension for now
  std::vector<float> tensorData_;
  std::vector<int64_t> shape_;
  DType dtype_;

 public:
#ifdef QLEVER_CPP_17
  // We need the default-constructibility for the C++17 version of `bit_cast`.
  DateYearOrDuration() = default;
#endif

  // Construct a `TensorData` given a `TensorData` object.
  explicit TensorData(std::vector<float> tensorData, std::vector<int64_t> shape,
                      DType dtype)
      : tensorData_{std::move(tensorData)},
        shape_{std::move(shape)},
        dtype_{dtype} {}

  // Convert to a string (without quotes) that represents the stored data, and a
  // pointer to the IRI of the corresponding datatype (currently always
  // `tensor:DataTensor`).
  std::pair<std::string, std::string> toString() const;

  static TensorData parseFromString(std::string_view dataString);
  static TensorData parseFromJSON(nlohmann::json json);

  static float cosineSimilarity(const TensorData& tensor1,
                                const TensorData& tensor2);
  static float norm(const TensorData& tensor);
  static float dotProduct(const TensorData& tensor1, const TensorData& tensor2);

  static TensorData add(const TensorData& tensor1, const TensorData& tensor2);
  static TensorData subtract(const TensorData& tensor1,
                             const TensorData& tensor2);
};
#ifdef QLEVER_CPP_17
static_assert(std::is_default_constructible_v<DateYearOrDuration>);
#endif

#endif  //  QLEVER_TENSOR_DATA_H
