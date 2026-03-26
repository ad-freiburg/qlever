#include "rdfTypes/TensorData.h"

#include <cmath>
#include <numeric>
#include <sstream>
using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using namespace ad_utility;

#ifdef QLEVER_USE_TENSOR_BLAS
#ifndef FINTEGER
#define FINTEGER int
#endif
// define the BLAS functions for the dot product and norm calculations
extern "C" {

/* declare BLAS functions, see http://www.netlib.org/clapack/cblas/ */
int sgemm_(const char* transa, const char* transb, FINTEGER* m, FINTEGER* n,
           FINTEGER* k, const float* alpha, const float* a, FINTEGER* lda,
           const float* b, FINTEGER* ldb, float* beta, float* c, FINTEGER* ldc);
float sdot_(FINTEGER* n, const float* x, FINTEGER* incx, const float* y,
            FINTEGER* incy);
float snrm2_(FINTEGER* n, const float* x, FINTEGER* incx);
}
#endif

const std::map<TensorData::DType, std::string> dtypeToString = {
    {TensorData::DType::DOUBLE, "float64"},
    {TensorData::DType::FLOAT, "float32"},
    {TensorData::DType::BOOL, "bool"},
    {TensorData::DType::INT, "int64"},
};
std::pair<std::string, std::string> TensorData::toString() const {
  // to the json string representation of the tensor data, we can use
  // nlohmann::json's dump function, which will add the necessary brackets and
  // commas.

  nlohmann::json json;
  json["data"] = tensorData_;
  json["shape"] = shape_;
  json["type"] = dtypeToString.at(dtype_);
  return {json.dump(), TENSOR_LITERAL};
}

LiteralOrIri TensorData::toLiteral() const {
  auto str_repr = toString();
  return LiteralOrIri::literalWithoutQuotes(str_repr.first, str_repr.second);
}

TensorData TensorData::parseFromString(std::string_view dataString) {
  // we can use nlohmann::json's parse function to parse the string
  // representation of the tensor data.
  try {
    auto json = nlohmann::json::parse(dataString);
    return parseFromJSON(json);
  } catch (const nlohmann::json::parse_error& e) {
    throw std::runtime_error{
        absl::StrCat("Failed to parse tensor data from string: ", dataString,
                     " - ", std::string(e.what()))};
  }
  auto json = nlohmann::json::parse(dataString);
  return parseFromJSON(json);
}

TensorData TensorData::parseFromJSON(nlohmann::json json) {
  if (json.find("data") == json.end() || json.find("shape") == json.end() ||
      json.find("type") == json.end()) {
    throw std::runtime_error{"Missing required fields in JSON"};
  }
  std::vector<float> data;
  std::vector<size_t> shape;
  TensorData::DType dtype;
  try {
    data = json["data"].get<std::vector<float>>();
    shape = json["shape"].get<std::vector<size_t>>();

    dtype =
        std::find_if(dtypeToString.begin(), dtypeToString.end(),
                     [&json](const auto& pair) {
                       return pair.second == json["type"].get<std::string>();
                     })
            ->first;
  } catch (const std::exception& e) {
    throw std::runtime_error{"Unknown type in JSON" + std::string(e.what())};
  }

  if (dtype == dtypeToString.end()->first) {
    throw std::runtime_error{"Unknown dtype in JSON: " +
                             json["type"].get<std::string>()};
  }
  if (shape.empty()) {
    throw std::runtime_error{"Shape cannot be empty"};
  }
  if (std::accumulate(shape.begin(), shape.end(), (size_t)1,
                      std::multiplies<>()) != data.size()) {
    throw std::runtime_error{
        "The number of elements in data does not match the shape"};
  }

  return TensorData(std::move(data), std::move(shape), dtype);
}

std::optional<TensorData> TensorData::parseFromPair(
    std::optional<std::pair<std::string, const char*>> pair) {
  if (pair.has_value()) {
    auto type = pair.value().second;
    if (type == nullptr) {
      return ad_utility::TensorData::parseFromString(pair.value().first);
    }
    auto type_str = std::string(type);
    // if(type.end() == '>') {
    //   // The datatype still has the ending `>` from the IRI form
    //   `^^<datatypeIri>`, so we need to remove it. type.remove_suffix(1);
    // }
    if (type_str == TENSOR_LITERAL || type_str == TENSOR_NUMERIC_LITERAL) {
      auto tensor = ad_utility::TensorData::parseFromString(pair.value().first);
    }
    return parseFromString(pair.value().first);
  }
  return std::nullopt;
}

float TensorData::cosineSimilarity(const TensorData& tensor1,
                                   const TensorData& tensor2) {
  if (!isBroadCastable(tensor1, tensor2)) {
    throw std::runtime_error{"Tensors are not broadcastable for subtraction"};
  }
  float inner = dot(tensor1, tensor2);
  float norm1 = norm(tensor1);
  float norm2 = norm(tensor2);

  if (norm1 == 0.0f || norm2 == 0.0f) return 0.0f;
  return inner / (norm1 * norm2);
}

float TensorData::norm(const TensorData& tensor) {
#ifdef QLEVER_USE_TENSOR_BLAS
  FINTEGER n = (FINTEGER)tensor.tensorData_.size();
  FINTEGER inc = 1;
  return snrm2_(&n, tensor.tensorData().data(), &inc);
#else
  float norm = 0;
#pragma omp parallel for reduction(+ : norm)
  for (size_t i = 0; i < tensor.tensorData_.size(); i++) {
    norm += tensor.tensorData_[i] * tensor.tensorData_[i];
  }
  return std::sqrt(norm);
#endif
}

float TensorData::dot(const TensorData& tensor1, const TensorData& tensor2) {
  if (!isBroadCastable(tensor1, tensor2)) {
    throw std::runtime_error{"Tensors are not broadcastable for subtraction"};
  }
#ifdef QLEVER_USE_TENSOR_BLAS
  FINTEGER n = (FINTEGER)tensor1.tensorData_.size();
  FINTEGER inc = 1;
  return sdot_(&n, tensor1.tensorData_.data(), &inc, tensor2.tensorData_.data(),
               &inc);
#else
  float sum = 0;
#pragma omp parallel for reduction(+ : sum)
  for (size_t i = 0; i < tensor1.tensorData_.size(); i++) {
    sum += tensor1.tensorData_[i] * tensor2.tensorData_[i];
  }
  return sum;
#endif
}

TensorData TensorData::add(const TensorData& tensor1,
                           const TensorData& tensor2) {
  if (!isBroadCastable(tensor1, tensor2)) {
    throw std::runtime_error{"Tensors are not broadcastable for subtraction"};
  }
  std::vector<float> result(tensor1.tensorData_.size());
  std::transform(tensor1.tensorData_.begin(), tensor1.tensorData_.end(),
                 tensor2.tensorData_.begin(), result.begin(),
                 std::plus<float>());
  return TensorData(std::move(result), tensor1.shape_, tensor1.dtype_);
}

TensorData TensorData::subtract(const TensorData& tensor1,
                                const TensorData& tensor2) {
  if (!isBroadCastable(tensor1, tensor2)) {
    throw std::runtime_error{"Tensors are not broadcastable for subtraction"};
  }
  std::vector<float> result(tensor1.tensorData_.size());
  std::transform(tensor1.tensorData_.begin(), tensor1.tensorData_.end(),
                 tensor2.tensorData_.begin(), result.begin(),
                 std::minus<float>());
  return TensorData(std::move(result), tensor1.shape_, tensor1.dtype_);
}

bool TensorData::isBroadCastable(const TensorData& tensor1,
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
