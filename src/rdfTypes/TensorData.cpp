// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include "rdfTypes/TensorData.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <charconv>
#include <climits>
#include <cmath>
#include <cstring>
#include <limits>
#include <ostream>

#include "rapidjson/error/en.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Log.h"

using namespace ad_utility;

#ifdef QLEVER_USE_TENSOR_BLAS
#ifndef FINTEGER
#define FINTEGER int
#endif
// Declaration of the (Fortran) BLAS level 1 functions that we use, see
// http://www.netlib.org/blas/. They are provided by OpenBLAS (which is linked
// via `LINK_BLAS_LIBRARIES`).
extern "C" {
float sdot_(const FINTEGER* n, const float* x, const FINTEGER* incx,
            const float* y, const FINTEGER* incy);
float snrm2_(const FINTEGER* n, const float* x, const FINTEGER* incx);
}
#endif

// With `QLEVER_TENSOR_PARALLEL` (OpenMP enabled), the loops below are
// vectorized and, for large inputs, parallelized via `#pragma omp parallel for
// simd`. The `if` clause keeps small vectors (the common case, e.g. embeddings
// with a few hundred dimensions) on the calling thread, because starting a
// parallel region costs far more than the loop itself.
#ifdef QLEVER_TENSOR_PARALLEL
#define QL_TENSOR_PRAGMA(x) _Pragma(#x)
#else
#define QL_TENSOR_PRAGMA(x)
#endif

namespace {
// Minimal number of elements for which a loop is run in parallel.
constexpr size_t PARALLEL_THRESHOLD = size_t{1} << 16;

#ifdef QLEVER_USE_TENSOR_BLAS
// BLAS takes the length as `FINTEGER`, larger inputs use the plain loops.
bool fitsBlas(size_t n) {
  return n <= static_cast<size_t>(std::numeric_limits<FINTEGER>::max());
}
#endif

// `sum_i a[i] * b[i]`, accumulated in double precision.
double dotImpl(const float* a, const float* b, size_t n) {
  double sum = 0.0;
  QL_TENSOR_PRAGMA(
      omp parallel for simd reduction(+ : sum) if (n >= PARALLEL_THRESHOLD))
  for (size_t i = 0; i < n; ++i) {
    sum += static_cast<double>(a[i]) * static_cast<double>(b[i]);
  }
  return sum;
}

// `sum_i (a[i] - b[i])^2`, accumulated in double precision.
double squaredDistanceImpl(const float* a, const float* b, size_t n) {
  double sum = 0.0;
  QL_TENSOR_PRAGMA(
      omp parallel for simd reduction(+ : sum) if (n >= PARALLEL_THRESHOLD))
  for (size_t i = 0; i < n; ++i) {
    double d = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    sum += d * d;
  }
  return sum;
}

// Elementwise `a[i] = a[i] op b[i]`. `a` and `b` may be identical (`t += t`),
// which is fine because iteration `i` only touches index `i`.
template <typename Op>
void elementwiseImpl(float* a, const float* b, size_t n, Op op) {
  QL_TENSOR_PRAGMA(omp parallel for simd if (n >= PARALLEL_THRESHOLD))
  for (size_t i = 0; i < n; ++i) {
    a[i] = op(a[i], b[i]);
  }
}

void scaleImpl(float* a, float factor, size_t n) {
  QL_TENSOR_PRAGMA(omp parallel for simd if (n >= PARALLEL_THRESHOLD))
  for (size_t i = 0; i < n; ++i) {
    a[i] *= factor;
  }
}
}  // namespace

// ____________________________________________________________________________
//                       DTypes
// ____________________________________________________________________________
std::string_view TensorData::dtypeToString(DType dtype) {
  switch (dtype) {
    case DType::FLOAT:
      return "float32";
    case DType::DOUBLE:
      return "float64";
    case DType::BOOL:
      return "bool";
    case DType::INT:
      return "int64";
  }
  throw std::runtime_error{"Invalid tensor dtype"};
}

// ____________________________________________________________________________
std::optional<TensorData::DType> TensorData::dtypeFromString(
    std::string_view name) {
  for (auto dtype : {DType::FLOAT, DType::DOUBLE, DType::BOOL, DType::INT}) {
    if (dtypeToString(dtype) == name) {
      return dtype;
    }
  }
  return std::nullopt;
}

// ____________________________________________________________________________
TensorData::DType TensorData::promoteDtypes(DType a, DType b,
                                            bool isDivision) {
  // Rank of the types in the promotion lattice bool < int64 < float32 <
  // float64.
  auto rank = [](DType d) {
    switch (d) {
      case DType::BOOL:
        return 0;
      case DType::INT:
        return 1;
      case DType::FLOAT:
        return 2;
      case DType::DOUBLE:
        return 3;
    }
    return 2;
  };
  static constexpr DType byRank[] = {DType::BOOL, DType::INT, DType::FLOAT,
                                     DType::DOUBLE};
  int r = std::max(rank(a), rank(b));
  // Arithmetic on booleans yields integers, division yields floats.
  r = std::max(r, isDivision ? 2 : 1);
  return byRank[r];
}

// ____________________________________________________________________________
//                       Printing
// ____________________________________________________________________________
namespace {
// Append the shortest representation of `value` that round-trips to the same
// float. Integral values get a trailing `.0` (e.g. `1.0`), non-finite values
// are written as `NaN`, `Infinity`, `-Infinity` (which `parseFromString`
// accepts, but which are not strict JSON).
void appendFloat(std::string& out, float value) {
  if (std::isnan(value)) {
    out.append("NaN");
    return;
  }
  if (std::isinf(value)) {
    out.append(value > 0 ? "Infinity" : "-Infinity");
    return;
  }
  char buf[32];
  auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
  AD_CORRECTNESS_CHECK(ec == std::errc{});
  std::string_view repr{buf, static_cast<size_t>(ptr - buf)};
  out.append(repr);
  if (repr.find_first_of(".e") == std::string_view::npos) {
    out.append(".0");
  }
}

void appendValue(std::string& out, float value, TensorData::DType dtype) {
  if (dtype == TensorData::DType::BOOL) {
    out.append(value != 0.0f ? "true" : "false");
    return;
  }
  if (dtype == TensorData::DType::INT && std::isfinite(value) && std::trunc(value) == value &&
      std::abs(value) < 9.2e18f) {
    char buf[24];
    auto [ptr, ec] =
        std::to_chars(buf, buf + sizeof(buf), static_cast<int64_t>(value));
    AD_CORRECTNESS_CHECK(ec == std::errc{});
    out.append(buf, ptr);
    return;
  }
  appendFloat(out, value);
}

template <typename T>
void appendInteger(std::string& out, T value) {
  char buf[24];
  auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
  AD_CORRECTNESS_CHECK(ec == std::errc{});
  out.append(buf, ptr);
}
}  // namespace

// ____________________________________________________________________________
std::string TensorData::toJsonString() const {
  std::string result;
  result.reserve(48 + tensorData_.size() * 14 + shape_.size() * 8);
  result.append(R"({"data":[)");
  for (size_t i = 0; i < tensorData_.size(); ++i) {
    if (i != 0) {
      result.push_back(',');
    }
    appendValue(result, tensorData_[i], dtype_);
  }
  result.append(R"(],"shape":[)");
  for (size_t i = 0; i < shape_.size(); ++i) {
    if (i != 0) {
      result.push_back(',');
    }
    appendInteger(result, static_cast<uint64_t>(shape_[i]));
  }
  result.append(R"(],"type":")");
  result.append(dtypeToString(dtype_));
  result.append(R"("})");
  return result;
}

// ____________________________________________________________________________
std::pair<std::string, std::string> TensorData::toString() const {
  return {toJsonString(), std::string(TENSOR_LITERAL)};
}

// ____________________________________________________________________________
LiteralOrIri TensorData::toLiteral() const {
  // The JSON contains no characters that would have to be escaped, so we can
  // directly use it as the normalized content.
  return LiteralOrIri{triple_component::Literal::literalWithNormalizedContent(
      toJsonString(),
      triple_component::Iri::fromIrirefWithoutBrackets(TENSOR_LITERAL))};
}

// ____________________________________________________________________________
void ad_utility::PrintTo(const TensorData& t, std::ostream* os) {
  *os << t.toJsonString();
}

// ____________________________________________________________________________
//                       Parsing
// ____________________________________________________________________________
namespace {
// Shorten `s` for error messages.
std::string_view truncateForError(std::string_view s) {
  constexpr size_t maxLen = 256;
  return s.size() <= maxLen ? s : s.substr(0, maxLen);
}
std::string_view ellipsis(std::string_view s) {
  return s.size() > 256 ? "..." : "";
}

// Product of the `shape`, or `std::nullopt` on overflow.
std::optional<size_t> numElementsOfShape(const std::vector<size_t>& shape) {
  size_t product = 1;
  for (size_t dim : shape) {
    if (dim != 0 && product > std::numeric_limits<size_t>::max() / dim) {
      return std::nullopt;
    }
    product *= dim;
  }
  return product;
}

// A fast, allocation-free (except for the result vectors) parser for the
// common form of tensor JSON objects: an object with exactly the keys `data`,
// `shape` and `type` (in any order), keys without escape sequences. Returns
// `std::nullopt` on any deviation, in which case the caller falls back to the
// general (rapidjson-based) parser, which also generates proper error
// messages.
class FastTensorParser {
  const char* p_;
  const char* end_;

 public:
  explicit FastTensorParser(std::string_view s)
      : p_{s.data()}, end_{s.data() + s.size()} {}

  std::optional<TensorData> parse() {
    if (!consume('{')) return std::nullopt;
    std::vector<float> data;
    std::vector<size_t> shape;
    std::optional<TensorData::DType> dtype;
    bool hasData = false;
    bool hasShape = false;
    if (!consume('}')) {
      while (true) {
        auto key = parseSimpleString();
        if (!key.has_value() || !consume(':')) return std::nullopt;
        if (key.value() == "data") {
          if (hasData) return std::nullopt;
          // Upper bound for the number of elements.
          data.reserve(static_cast<size_t>(std::count(p_, end_, ',')) + 1);
          if (!parseDataArray(data, 0)) return std::nullopt;
          hasData = true;
        } else if (key.value() == "shape") {
          if (hasShape || !parseShapeArray(shape)) return std::nullopt;
          hasShape = true;
        } else if (key.value() == "type") {
          auto typeStr = parseSimpleString();
          if (dtype.has_value() || !typeStr.has_value()) return std::nullopt;
          dtype = TensorData::dtypeFromString(typeStr.value());
          if (!dtype.has_value()) return std::nullopt;
        } else {
          return std::nullopt;
        }
        if (consume(',')) continue;
        if (consume('}')) break;
        return std::nullopt;
      }
    }
    if (!hasData || !hasShape || !dtype.has_value() || shape.empty() ||
        numElementsOfShape(shape) != data.size()) {
      return std::nullopt;
    }
    return TensorData{std::move(data), std::move(shape), dtype.value()};
  }

 private:
  void skipWs() {
    while (p_ < end_ &&
           (*p_ == ' ' || *p_ == '\n' || *p_ == '\t' || *p_ == '\r')) {
      ++p_;
    }
  }
  bool consume(char c) {
    skipWs();
    if (p_ < end_ && *p_ == c) {
      ++p_;
      return true;
    }
    return false;
  }
  bool consumeWord(std::string_view word) {
    if (static_cast<size_t>(end_ - p_) >= word.size() &&
        std::string_view{p_, word.size()} == word) {
      p_ += word.size();
      return true;
    }
    return false;
  }
  // A string without escape sequences.
  std::optional<std::string_view> parseSimpleString() {
    if (!consume('"')) return std::nullopt;
    const char* begin = p_;
    while (p_ < end_ && *p_ != '"') {
      if (*p_ == '\\') return std::nullopt;
      ++p_;
    }
    if (p_ == end_) return std::nullopt;
    std::string_view result{begin, static_cast<size_t>(p_ - begin)};
    ++p_;
    return result;
  }
  bool parseNumber(float& out) {
    skipWs();
    if (p_ == end_) return false;
    switch (*p_) {
      case 't':
        out = 1.0f;
        return consumeWord("true");
      case 'f':
        out = 0.0f;
        return consumeWord("false");
      case 'N':
        out = std::numeric_limits<float>::quiet_NaN();
        return consumeWord("NaN");
      case 'I':
        out = std::numeric_limits<float>::infinity();
        return consumeWord("Infinity");
      case '-':
        if (end_ - p_ > 1 && p_[1] == 'I') {
          out = -std::numeric_limits<float>::infinity();
          return consumeWord("-Infinity");
        }
        break;
      default:
        break;
    }
    auto [ptr, ec] = std::from_chars(p_, end_, out);
    if (ec == std::errc::result_out_of_range) {
      // Overflow or underflow of `float`, parse as double and round (to
      // +-infinity or zero).
      double d;
      auto [ptr2, ec2] = std::from_chars(p_, end_, d);
      if (ec2 != std::errc{}) return false;
      out = static_cast<float>(d);
      ptr = ptr2;
    } else if (ec != std::errc{}) {
      return false;
    }
    p_ = ptr;
    return true;
  }
  // Parse a (possibly nested) array of numbers and append them to `out`.
  bool parseDataArray(std::vector<float>& out, size_t depth) {
    if (depth > 64 || !consume('[')) return false;
    if (consume(']')) return true;
    while (true) {
      skipWs();
      if (p_ < end_ && *p_ == '[') {
        if (!parseDataArray(out, depth + 1)) return false;
      } else {
        float value;
        if (!parseNumber(value)) return false;
        out.push_back(value);
      }
      if (consume(',')) continue;
      return consume(']');
    }
  }
  bool parseShapeArray(std::vector<size_t>& out) {
    if (!consume('[')) return false;
    if (consume(']')) return true;
    while (true) {
      skipWs();
      uint64_t dim;
      auto [ptr, ec] = std::from_chars(p_, end_, dim);
      if (ec != std::errc{}) return false;
      p_ = ptr;
      out.push_back(static_cast<size_t>(dim));
      if (consume(',')) continue;
      return consume(']');
    }
  }
};

// Recursively flatten the (possibly nested) rapidjson array `value`.
void flattenJsonData(const rapidjson::Value& value, std::vector<float>& out,
                     size_t depth) {
  if (depth > 64) {
    throw std::runtime_error{"data array is nested too deeply"};
  }
  for (const auto& element : value.GetArray()) {
    if (element.IsArray()) {
      flattenJsonData(element, out, depth + 1);
    } else if (element.IsNumber()) {
      out.push_back(static_cast<float>(element.GetDouble()));
    } else if (element.IsBool()) {
      out.push_back(element.GetBool() ? 1.0f : 0.0f);
    } else {
      throw std::runtime_error{"data array contains a non-numeric value"};
    }
  }
}
}  // namespace

// ____________________________________________________________________________
TensorData TensorData::parseFromString(const std::string_view& dataString) {
  AD_LOG_TIME_START(tensorFromString);
  if (auto fast = FastTensorParser{dataString}.parse(); fast.has_value()) {
    AD_LOG_TIME_END(tensorFromString);
    return std::move(fast.value());
  }
  // Slow path, also generates the error messages.
  rapidjson::Document document;
  document.Parse<rapidjson::kParseStopWhenDoneFlag |
                 rapidjson::kParseNanAndInfFlag |
                 rapidjson::kParseFullPrecisionFlag>(dataString.data(),
                                                     dataString.size());
  if (document.HasParseError()) {
    size_t offset = document.GetErrorOffset();
    size_t lower = offset >= 10 ? offset - 10 : 0;
    size_t upper = std::min(dataString.size(), offset + 10);
    std::string_view context =
        lower < upper ? dataString.substr(lower, upper - lower) : "";
    throw std::runtime_error{absl::StrCat(
        "Failed to parse tensor data from string: ",
        truncateForError(dataString), ellipsis(dataString), " - ",
        rapidjson::GetParseError_En(document.GetParseError()), " at offset ",
        offset, " (context: '", context, "')")};
  }
  return parseFromJSON(document, dataString);
}

// ____________________________________________________________________________
std::optional<TensorData> TensorData::tryParseFromString(
    std::string_view dataString) {
  if (auto fast = FastTensorParser{dataString}.parse(); fast.has_value()) {
    return fast;
  }
  try {
    return parseFromString(dataString);
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

// ____________________________________________________________________________
TensorData TensorData::parseFromJSON(rapidjson::Document& json,
                                     const std::string_view& dataString) {
  auto fail = [&dataString](std::string_view msg) {
    return std::runtime_error{absl::StrCat("Failed to parse tensor JSON: ", msg,
                                           " in: ", truncateForError(dataString),
                                           ellipsis(dataString))};
  };
  if (!json.IsObject()) {
    throw fail("not a JSON object");
  }
  if (!json.HasMember("data") || !json.HasMember("shape") ||
      !json.HasMember("type")) {
    throw fail("Missing required fields (data, shape, type)");
  }

  const auto& dataArray = json["data"];
  if (!dataArray.IsArray()) {
    throw fail("data field is not an array");
  }
  std::vector<float> data;
  data.reserve(dataArray.Size());
  try {
    flattenJsonData(dataArray, data, 0);
  } catch (const std::runtime_error& e) {
    throw fail(e.what());
  }

  const auto& shapeArray = json["shape"];
  if (!shapeArray.IsArray()) {
    throw fail("shape field is not an array");
  }
  std::vector<size_t> shape;
  shape.reserve(shapeArray.Size());
  for (const auto& val : shapeArray.GetArray()) {
    if (!val.IsUint64()) {
      throw fail("shape contains a value that is not a non-negative integer");
    }
    shape.push_back(static_cast<size_t>(val.GetUint64()));
  }

  const auto& typeValue = json["type"];
  if (!typeValue.IsString()) {
    throw fail("type field is not a string");
  }
  std::string_view typeStr{typeValue.GetString(), typeValue.GetStringLength()};
  auto dtype = dtypeFromString(typeStr);
  if (!dtype.has_value()) {
    throw fail(absl::StrCat("Unknown dtype \"", typeStr, "\""));
  }

  if (shape.empty()) {
    throw fail("Shape cannot be empty");
  }
  auto numElements = numElementsOfShape(shape);
  if (numElements != data.size()) {
    throw fail(absl::StrCat("The number of elements in data (", data.size(),
                            ") does not match the shape"));
  }
  return TensorData(std::move(data), std::move(shape), dtype.value());
}

// ____________________________________________________________________________
std::optional<TensorData> TensorData::parseFromPair(
    const std::optional<std::pair<std::string, const char*>>& pair) {
  if (!pair.has_value()) {
    return std::nullopt;
  }
  const char* type = pair.value().second;
  if (type != nullptr) {
    std::string_view typeStr{type};
    if (typeStr != TENSOR_LITERAL && typeStr != TENSOR_NUMERIC_LITERAL) {
      return std::nullopt;
    }
  }
  return parseFromString(pair.value().first);
}

// ____________________________________________________________________________
std::optional<TensorData> TensorData::fromLiteral(
    const std::string_view& literalString) {
  // `LiteralView::fromStringRepresentation` requires a leading and a
  // trailing quote.
  if (literalString.size() < 2 || literalString.front() != '"' ||
      literalString.rfind('"') == 0) {
    return std::nullopt;
  }
  try {
    auto literal =
        triple_component::LiteralView::fromStringRepresentation(literalString);
    if (!literal.hasDatatype()) {
      return std::nullopt;
    }
    auto datatype = literal.getDatatype();
    if (datatype != TENSOR_LITERAL && datatype != TENSOR_NUMERIC_LITERAL) {
      return std::nullopt;
    }
    std::string_view content = literal.getContent();
    // The internal representation is normalized (no escape sequences), but
    // we also accept content that is still escaped (e.g. `{\"data\": ...}`).
    if (content.find('\\') == std::string_view::npos) {
      return tryParseFromString(content);
    }
    std::string unescaped;
    unescaped.reserve(content.size());
    RdfEscaping::unescapeLiteral(content, unescaped);
    return tryParseFromString(unescaped);
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

// ____________________________________________________________________________
//                       Arithmetic
// ____________________________________________________________________________
namespace {
void checkSameShape(const TensorData& a, const TensorData& b,
                    std::string_view what) {
  if (!isBroadCastable(a, b)) [[unlikely]] {
    throw std::runtime_error{
        absl::StrCat("Tensors have different shapes for ", what, ": [",
                     absl::StrJoin(a.shape(), ","), "] vs. [",
                     absl::StrJoin(b.shape(), ","), "]")};
  }
}
}  // namespace

namespace {
// Dot product and norm, via BLAS if available.
double dotProduct(const std::vector<float>& a, const std::vector<float>& b) {
#ifdef QLEVER_USE_TENSOR_BLAS
  if (fitsBlas(a.size())) {
    FINTEGER n = static_cast<FINTEGER>(a.size());
    FINTEGER inc = 1;
    return sdot_(&n, a.data(), &inc, b.data(), &inc);
  }
#endif
  return dotImpl(a.data(), b.data(), a.size());
}
double euclideanNorm(const std::vector<float>& a) {
#ifdef QLEVER_USE_TENSOR_BLAS
  if (fitsBlas(a.size())) {
    FINTEGER n = static_cast<FINTEGER>(a.size());
    FINTEGER inc = 1;
    return snrm2_(&n, a.data(), &inc);
  }
#endif
  return std::sqrt(dotImpl(a.data(), a.data(), a.size()));
}
}  // namespace

// ____________________________________________________________________________
float TensorData::cosineSimilarity(const TensorData& tensor1,
                                   const TensorData& tensor2) {
  checkSameShape(tensor1, tensor2, "cosine similarity");
  double inner = dotProduct(tensor1.tensorData_, tensor2.tensorData_);
  double norm1 = euclideanNorm(tensor1.tensorData_);
  double norm2 = euclideanNorm(tensor2.tensorData_);
  // The similarity is undefined for zero vectors (this includes empty
  // tensors), we define it as 0. NaN inputs propagate to a NaN result.
  if (norm1 == 0.0 || norm2 == 0.0) {
    return 0.0f;
  }
  // Computed in double precision, so the product of the norms can't overflow.
  return static_cast<float>(inner / (norm1 * norm2));
}

// ____________________________________________________________________________
float TensorData::euclideanDistance(const TensorData& tensor1,
                                    const TensorData& tensor2) {
  checkSameShape(tensor1, tensor2, "euclidean distance");
  return static_cast<float>(std::sqrt(squaredDistanceImpl(
      tensor1.tensorData_.data(), tensor2.tensorData_.data(), tensor1.size())));
}

// ____________________________________________________________________________
float TensorData::norm(const TensorData& tensor) {
  return static_cast<float>(euclideanNorm(tensor.tensorData_));
}

// ____________________________________________________________________________
float TensorData::dot(const TensorData& tensor1, const TensorData& tensor2) {
  checkSameShape(tensor1, tensor2, "dot product");
  return static_cast<float>(
      dotProduct(tensor1.tensorData_, tensor2.tensorData_));
}

// ____________________________________________________________________________
TensorData& TensorData::addInPlace(const TensorData& other) {
  checkSameShape(*this, other, "addition");
  elementwiseImpl(tensorData_.data(), other.tensorData_.data(), size(),
                  [](float x, float y) { return x + y; });
  dtype_ = promoteDtypes(dtype_, other.dtype_);
  return *this;
}
TensorData& TensorData::subtractInPlace(const TensorData& other) {
  checkSameShape(*this, other, "subtraction");
  elementwiseImpl(tensorData_.data(), other.tensorData_.data(), size(),
                  [](float x, float y) { return x - y; });
  dtype_ = promoteDtypes(dtype_, other.dtype_);
  return *this;
}
TensorData& TensorData::multiplyInPlace(const TensorData& other) {
  checkSameShape(*this, other, "multiplication");
  elementwiseImpl(tensorData_.data(), other.tensorData_.data(), size(),
                  [](float x, float y) { return x * y; });
  dtype_ = promoteDtypes(dtype_, other.dtype_);
  return *this;
}
TensorData& TensorData::divideInPlace(const TensorData& other) {
  checkSameShape(*this, other, "division");
  // IEEE semantics: `x / 0` is +-infinity or NaN.
  elementwiseImpl(tensorData_.data(), other.tensorData_.data(), size(),
                  [](float x, float y) { return x / y; });
  dtype_ = promoteDtypes(dtype_, other.dtype_, true);
  return *this;
}
TensorData& TensorData::scaleInPlace(float factor) {
  scaleImpl(tensorData_.data(), factor, size());
  dtype_ = promoteDtypes(dtype_, DType::FLOAT);
  return *this;
}

// ____________________________________________________________________________
TensorData TensorData::add(const TensorData& tensor1,
                           const TensorData& tensor2) {
  checkSameShape(tensor1, tensor2, "addition");
  TensorData result{tensor1};
  result.addInPlace(tensor2);
  return result;
}
TensorData TensorData::subtract(const TensorData& tensor1,
                                const TensorData& tensor2) {
  checkSameShape(tensor1, tensor2, "subtraction");
  TensorData result{tensor1};
  result.subtractInPlace(tensor2);
  return result;
}
TensorData TensorData::multiply(const TensorData& tensor1,
                                const TensorData& tensor2) {
  checkSameShape(tensor1, tensor2, "multiplication");
  TensorData result{tensor1};
  result.multiplyInPlace(tensor2);
  return result;
}
TensorData TensorData::divide(const TensorData& tensor1,
                              const TensorData& tensor2) {
  checkSameShape(tensor1, tensor2, "division");
  TensorData result{tensor1};
  result.divideInPlace(tensor2);
  return result;
}
TensorData TensorData::scale(const TensorData& tensor1, const float& scale) {
  TensorData result{tensor1};
  result.scaleInPlace(scale);
  return result;
}
TensorData TensorData::zeros_like(const TensorData& tensor) {
  return TensorData{std::vector<float>(tensor.size(), 0.0f), tensor.shape_,
                    tensor.dtype_};
}

// ____________________________________________________________________________
//                       Binary serialization
// ____________________________________________________________________________
namespace {
constexpr size_t HEADER_SIZE = 1 + sizeof(uint64_t);
}

// ____________________________________________________________________________
size_t TensorData::serializedSize() const {
  return HEADER_SIZE + shape_.size() * sizeof(uint64_t) +
         tensorData_.size() * sizeof(float);
}

// ____________________________________________________________________________
void TensorData::serializeInto(ql::span<uint8_t> out) const {
  AD_CONTRACT_CHECK(out.size() == serializedSize());
  uint8_t* ptr = out.data();
  *ptr++ = static_cast<uint8_t>(dtype_);
  uint64_t numDimensions = shape_.size();
  std::memcpy(ptr, &numDimensions, sizeof(numDimensions));
  ptr += sizeof(numDimensions);
  for (size_t dim : shape_) {
    uint64_t d = dim;
    std::memcpy(ptr, &d, sizeof(d));
    ptr += sizeof(d);
  }
  if (!tensorData_.empty()) {
    std::memcpy(ptr, tensorData_.data(), tensorData_.size() * sizeof(float));
  }
}

// ____________________________________________________________________________
std::vector<uint8_t> TensorData::serialize() const {
  std::vector<uint8_t> buffer(serializedSize());
  serializeInto(buffer);
  return buffer;
}

// ____________________________________________________________________________
TensorData TensorData::deserialize(const std::vector<uint8_t>& buffer) {
  return deserialize(ql::span<const uint8_t>{buffer});
}

// ____________________________________________________________________________
TensorData TensorData::deserialize(ql::span<const uint8_t> buffer) {
  AD_LOG_TIME_START(tensorFromBuffer);
  if (buffer.size() < HEADER_SIZE) {
    throw std::runtime_error{"Buffer too small to deserialize TensorData"};
  }
  uint8_t dtypeByte = buffer[0];
  if (dtypeByte > static_cast<uint8_t>(DType::INT)) {
    throw std::runtime_error{
        absl::StrCat("Invalid dtype ", dtypeByte, " in serialized TensorData")};
  }
  auto dtype = static_cast<DType>(dtypeByte);

  uint64_t numDimensions;
  std::memcpy(&numDimensions, buffer.data() + 1, sizeof(numDimensions));
  if (numDimensions > (buffer.size() - HEADER_SIZE) / sizeof(uint64_t)) {
    throw std::runtime_error{
        "Buffer too small to deserialize TensorData shape"};
  }
  const uint8_t* ptr = buffer.data() + HEADER_SIZE;
  std::vector<size_t> shape(numDimensions);
  for (size_t i = 0; i < numDimensions; ++i) {
    uint64_t dim;
    std::memcpy(&dim, ptr, sizeof(dim));
    ptr += sizeof(dim);
    shape[i] = static_cast<size_t>(dim);
  }

  size_t dataBytes = static_cast<size_t>(buffer.data() + buffer.size() - ptr);
  if (dataBytes % sizeof(float) != 0) {
    throw std::runtime_error{"Buffer size is not a multiple of float size"};
  }
  size_t numElements = dataBytes / sizeof(float);
  if (numElementsOfShape(shape) != numElements) {
    throw std::runtime_error{
        "The number of elements in the serialized TensorData does not match "
        "its shape"};
  }
  // Use `resize_and_overwrite`-like semantics: allocate once, single memcpy.
  std::vector<float> tensorData(numElements);
  if (numElements > 0) {
    std::memcpy(tensorData.data(), ptr, dataBytes);
  }
  TensorData result{std::move(tensorData), std::move(shape), dtype};
  AD_LOG_TIME_END(tensorFromBuffer);
  return result;
}
