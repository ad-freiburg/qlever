#include "util/TensorData.h"

#include <cmath>
#include <numeric>
#include <sstream>

std::pair<std::string, std::string> TensorData::toString() const {
    // to the json string representation of the tensor data, we can use nlohmann::json's
    // dump function, which will add the necessary brackets and commas.

    nlohmann::json json;
    json["data"] = tensorData_;
    json["shape"] = shape_;
    json["type"] = static_cast<int>(dtype_);
    return {json.dump(), "tensor:DataTensor"};
}

TensorData TensorData::parseFromString(std::string_view dataString) {
    // we can use nlohmann::json's parse function to parse the string representation of the tensor data.
    auto json = nlohmann::json::parse(dataString);
    return parseFromJSON(json);
}

TensorData TensorData::parseFromJSON(nlohmann::json json) {
    auto data = json["data"].get<std::vector<float>>();
    auto shape = json["shape"].get<std::vector<int64_t>>();
    auto dtype = static_cast<DType>(json["type"].get<int>());
    
    return TensorData(std::move(data), std::move(shape), dtype);
}

float TensorData::cosineSimilarity(const TensorData& tensor1,
                                                                     const TensorData& tensor2) {
    float dot = dotProduct(tensor1, tensor2);
    float norm1 = norm(tensor1);
    float norm2 = norm(tensor2);
    
    if (norm1 == 0.0f || norm2 == 0.0f) return 0.0f;
    return dot / (norm1 * norm2);
}

float TensorData::norm(const TensorData& tensor) {
    float sum = std::inner_product(tensor.tensorData_.begin(),
                                                                    tensor.tensorData_.end(),
                                                                    tensor.tensorData_.begin(), 0.0f);
    return std::sqrt(sum);
}

float TensorData::dotProduct(const TensorData& tensor1,
                                                         const TensorData& tensor2) {
    return std::inner_product(tensor1.tensorData_.begin(),
                                                        tensor1.tensorData_.end(),
                                                        tensor2.tensorData_.begin(), 0.0f);
}

TensorData TensorData::add(const TensorData& tensor1,
                                                     const TensorData& tensor2) {
    std::vector<float> result(tensor1.tensorData_.size());
    std::transform(tensor1.tensorData_.begin(), tensor1.tensorData_.end(),
                                 tensor2.tensorData_.begin(), result.begin(),
                                 std::plus<float>());
    return TensorData(std::move(result), tensor1.shape_, tensor1.dtype_);
}

TensorData TensorData::subtract(const TensorData& tensor1,
                                                                const TensorData& tensor2) {
    std::vector<float> result(tensor1.tensorData_.size());
    std::transform(tensor1.tensorData_.begin(), tensor1.tensorData_.end(),
                                 tensor2.tensorData_.begin(), result.begin(),
                                 std::minus<float>());
    return TensorData(std::move(result), tensor1.shape_, tensor1.dtype_);
}