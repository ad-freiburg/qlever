
#ifndef QLEVER_SRC_UTIL_TENSOR_H
#define QLEVER_SRC_UTIL_TENSOR_H

#include <cstdint>
#include <cstdio>
#include <string>

#include "backports/three_way_comparison.h"
#include "concepts/concepts.hpp"
#include "global/ValueId.h"

namespace ad_utility {




// Forward declaration for concept
// class Tensor;

// Concept for the `RequestedInfo` template parameter: any of these types is
// allowed to be requested.
// template <typename T>
// CPP_concept RequestedInfoT =
//     SameAsAny<T, Tensor>;

// The version of the `Tensor`: to ensure correctness when reading disk
// serialized objects of this class.
constexpr uint64_t TENSOR_VERSION = 1;

// A tensor info object holds precomputed details on WKT literals.
// IMPORTANT: Every modification of the attributes of this class will be an
// index-breaking change regarding the `GeoVocabulary`. Please update the
// `TENSOR_INFO_VERSION` constant above accordingly, which will invalidate all
// indices using such a vocabulary.
// class Tensor {
//  private:
//   // `Tensor` must ensure that its attributes' binary representation
//   // cannot be all-zero. (?)
//   uint32_t numGeometries_;

//   static constexpr uint64_t bitMaskTensorType =
//       bitMaskForHigherBits(ValueId::numDatatypeBits);

//  public:
//   Tensor(uint8_t wktType, const BoundingBox& boundingBox,
//                Centroid centroid, NumGeometries numGeometries,
//                MetricLength metricLength, MetricArea metricArea);
// #ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
//   // Required for `bit_cast`.
//   Tensor() = default;
// #endif

//   Tensor(const Tensor& other) = default;

//   // Parse an arbitrary JSON literal and compute all attributes. Return
//   // `std:nullopt` if `json` cannot be parsed.
//   static std::optional<Tensor> fromJSONLiteral(std::string_view json);


//   // Extract the requested information from this object.
//   CPP_template(typename RequestedInfo = Tensor)(
//       requires RequestedInfoT<RequestedInfo>) RequestedInfo
//       getRequestedInfo() const;

//   // Parse the given JSON literal and compute only the requested information.
//   CPP_template(typename RequestedInfo = Tensor)(
//       requires RequestedInfoT<RequestedInfo>) static std::
//       optional<RequestedInfo> getRequestedInfo(std::string_view json);
// };

// For the disk serialization we require that a `Tensor` is trivially
// copyable.
// static_assert(std::is_trivially_copyable_v<Tensor>);

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_TENSOR_H