#include "rdfTypes/TensorData.h"

#include <cmath>
#include <nlohmann/detail/string_escape.hpp>
#include <numeric>
#include <sstream>

#include "RdfEscaping.h"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using namespace ad_utility;
using namespace rapidjson;

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
  // Convert to rapidjson format
  Document document;
  document.SetObject();
  auto& allocator = document.GetAllocator();

  Value dataArray(kArrayType);
  for (float value : tensorData_) {
    dataArray.PushBack(value, allocator);
  }
  document.AddMember("data", dataArray, allocator);

  Value shapeArray(kArrayType);
  for (size_t dim : shape_) {
    shapeArray.PushBack(static_cast<uint64_t>(dim), allocator);
  }
  document.AddMember("shape", shapeArray, allocator);

  document.AddMember("type", Value(dtypeToString.at(dtype_).c_str(), allocator),
                     allocator);

  StringBuffer buffer;
  Writer<StringBuffer> writer(buffer);
  document.Accept(writer);

  return {std::string(buffer.GetString()), TENSOR_LITERAL};
}

LiteralOrIri TensorData::toLiteral() const {
  auto str_repr = toString();
  return LiteralOrIri::literalWithoutQuotes(str_repr.first, str_repr.second);
}

TensorData TensorData::parseFromString(const std::string_view& dataString) {
  // add the kParseStopWhenDoneFlag be a bit more robust in parsing
  Document document;
  document.Parse<kParseStopWhenDoneFlag>(dataString.data(), dataString.size());
  if (document.HasParseError()) {
    ssize_t offset = document.GetErrorOffset();
    size_t contextLowerBound = std::max(static_cast<ssize_t>(0), offset - 10);
    size_t contextUpperBound =
        std::min(static_cast<ssize_t>(dataString.size()),
                 offset + 10);  // Get a snippet around the error
    std::string data = std::string(
        dataString);  // Convert to std::string for easier manipulation
    std::string errorContext = data.substr(
        contextLowerBound,
        contextUpperBound -
            contextLowerBound);  // Get a snippet of the error context
    throw std::runtime_error{
        absl::StrCat("Failed to parse tensor data from string: ", data, " - ",
                     GetParseError_En(document.GetParseError()), " at offset ",
                     offset, " (context: '", errorContext, "')")};
  }
  return parseFromJSON(document, dataString);
}
TensorData TensorData::parseFromJSON(Document& json,
                                     const std::string_view& dataString) {
  if (!json.HasMember("data") || !json.HasMember("shape") ||
      !json.HasMember("type")) {
    throw std::runtime_error{"Missing required fields in JSON"};
  }

  std::vector<float> data;
  std::vector<size_t> shape;
  TensorData::DType dtype;

  try {
    // Parse data array
    const auto& dataArray = json["data"];
    if (!dataArray.IsArray()) {
      throw std::runtime_error{
          absl::StrCat("data field is not an array in: ", dataString)};
    }
    for (const auto& val : dataArray.GetArray()) {
      data.push_back(val.GetFloat());
    }

    // Parse shape array
    const auto& shapeArray = json["shape"];
    if (!shapeArray.IsArray()) {
      throw std::runtime_error{
          absl::StrCat("shape field is not an array in: ", dataString)};
    }
    for (const auto& val : shapeArray.GetArray()) {
      shape.push_back(val.GetUint64());
    }

    // Parse type
    if (!json["type"].IsString()) {
      throw std::runtime_error{
          absl::StrCat("Type field is not a string in: ", dataString)};
    }
    std::string typeStr = json["type"].GetString();

    auto it = std::find_if(
        dtypeToString.begin(), dtypeToString.end(),
        [&typeStr](const auto& pair) { return pair.second == typeStr; });
    if (it == dtypeToString.end()) {
      throw std::runtime_error{absl::StrCat("Unknown dtype in JSON: ", typeStr,
                                            " in: ", dataString)};
    }
    dtype = it->first;

  } catch (const std::exception& e) {
    throw std::runtime_error{
        absl::StrCat("Failed to parse tensor JSON: ", std::string(e.what()),
                     " in: ", dataString)};
  }

  if (shape.empty()) {
    throw std::runtime_error{
        absl::StrCat("Shape cannot be empty in: ", dataString)};
  }
  if (std::accumulate(shape.begin(), shape.end(), (size_t)1,
                      std::multiplies<>()) != data.size()) {
    throw std::runtime_error{absl::StrCat(
        "The number of elements in data does not match the shape in: ",
        dataString)};
  }

  return TensorData(std::move(data), std::move(shape), dtype);
}

std::optional<TensorData> TensorData::parseFromPair(
    const std::optional<std::pair<std::string, const char*>>& pair) {
  if (pair.has_value()) {
    auto type = pair.value().second;
    if (type == nullptr) {
      return parseFromString(pair.value().first);
    }
    auto type_str = std::string(type);
    // if(type.end() == '>') {
    //   // The datatype still has the ending `>` from the IRI form
    //   `^^<datatypeIri>`, so we need to remove it. type.remove_suffix(1);
    // }
    if (type_str == TENSOR_LITERAL || type_str == TENSOR_NUMERIC_LITERAL) {
      auto tensor = parseFromString(pair.value().first);
    }
    return parseFromString(pair.value().first);
  }
  return std::nullopt;
}
std::optional<TensorData> TensorData::fromLiteral(
    const std::string_view& literalString) {
  auto literal =
      ad_utility::triple_component::Literal::fromStringRepresentation(
          std::string{literalString});
  if (literal.hasDatatype()) {
    auto dtype = asStringViewUnsafe(literal.getDatatype());
    if (dtype == TENSOR_LITERAL || dtype == TENSOR_NUMERIC_LITERAL) {
      try {
        // Remove & unescape Pairs
        auto unescaped = RdfEscaping::normalizeLiteralWithoutQuotes(
            asStringViewUnsafe(literal.getContent()));
        return parseFromString(asStringViewUnsafe(unescaped));
      } catch (std::exception& e) {
        return std::nullopt;
      }
    }
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
  // if (!isBroadCastable(tensor1, tensor2)) {
  //   throw std::runtime_error{"Tensors are not broadcastable for
  //   subtraction"};
  // }
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

// ____________________________________________________________________________
std::vector<uint8_t> TensorData::serialize() const {
  // Serialize the tensor data into a byte vector. The format is as follows:
  // [dtype (1 byte)][number of dimensions (8 bytes)][shape (8 bytes per
  // dimension)][tensor data (4 bytes per float)]
  std::vector<uint8_t> buffer;
  buffer.push_back(static_cast<uint8_t>(dtype_));

  uint64_t numDimensions = shape_.size();
  buffer.insert(
      buffer.end(), reinterpret_cast<uint8_t*>(&numDimensions),
      reinterpret_cast<uint8_t*>(&numDimensions) + sizeof(numDimensions));

  for (size_t dim : shape_) {
    buffer.insert(buffer.end(), reinterpret_cast<uint8_t*>(&dim),
                  reinterpret_cast<uint8_t*>(&dim) + sizeof(dim));
  }

  for (float value : tensorData_) {
    buffer.insert(buffer.end(), reinterpret_cast<const uint8_t*>(&value),
                  reinterpret_cast<const uint8_t*>(&value) + sizeof(value));
  }

  return buffer;
}

TensorData TensorData::deserialize(const std::vector<uint8_t>& buffer) {
  if (buffer.size() < 1 + sizeof(uint64_t)) {
    throw std::runtime_error{"Buffer too small to deserialize TensorData"};
  }

  DType dtype = static_cast<DType>(buffer[0]);

  uint64_t numDimensions;
  std::memcpy(&numDimensions, buffer.data() + 1, sizeof(numDimensions));

  if (buffer.size() < 1 + sizeof(uint64_t) + numDimensions * sizeof(uint64_t)) {
    throw std::runtime_error{
        "Buffer too small to deserialize TensorData shape"};
  }

  std::vector<size_t> shape(numDimensions);
  for (size_t i = 0; i < numDimensions; i++) {
    uint64_t dim;
    std::memcpy(&dim,
                buffer.data() + 1 + sizeof(uint64_t) + i * sizeof(uint64_t),
                sizeof(dim));
    shape[i] = dim;
  }

  size_t expectedDataSize =
      buffer.size() - (1 + sizeof(uint64_t) + numDimensions * sizeof(uint64_t));
  if (expectedDataSize % sizeof(float) != 0) {
    throw std::runtime_error{"Buffer size is not a multiple of float size"};
  }
  size_t numElements = expectedDataSize / sizeof(float);

  std::vector<float> tensorData(numElements);
  std::memcpy(
      tensorData.data(),
      buffer.data() + 1 + sizeof(uint64_t) + numDimensions * sizeof(uint64_t),
      numElements * sizeof(float));

  return TensorData(std::move(tensorData), std::move(shape), dtype);
}
