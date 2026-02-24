// Copyright 2021 - 2025
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "backports/type_traits.h"
#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/Constants.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/GeoSparqlHelpers.h"

namespace sparqlExpression {
namespace detail {

template <typename Arg>
using TypeErasedGetter = std::function<ad_utility::InputRangeTypeErased<Arg>(
    ExpressionResult, EvaluationContext*, size_t)>;

template <typename T>
class NaryExpression2;

template <typename Ret, typename... Args>
class NaryExpression2<Ret(Args...)> : public SparqlExpression {
 public:
  static constexpr size_t N = sizeof...(Args);
  using Children = std::array<SparqlExpression::Ptr, N>;
  using Function = std::function<Ret(Args...)>;

 private:
  Children children_;
  Function function_;

  template <typename Arg>
  using Getter = std::function<ad_utility::InputRangeTypeErased<Arg>(
      ExpressionResult, EvaluationContext*, size_t)>;

  using Getters = std::tuple<Getter<Args>...>;
  Getters getters_;

 public:
  // Construct from an array of `N` child expressions.
  explicit NaryExpression2(Function function, Getters getters,
                           Children&& children)
      : children_{std::move(children)},
        function_{std::move(function)},
        getters_{std::move(getters)} {}

  /*
  // Construct from `N` child expressions. Each of the children must have a type
  // `std::unique_ptr<SubclassOfSparqlExpression>`.
  CPP_template(typename... C)(
      requires(concepts::convertible_to<C, SparqlExpression::Ptr>&&...)
          CPP_and(sizeof...(C) == N)) explicit NaryExpression2(C... children)
      : NaryExpression2{Children{std::move(children)...}} {}
      */

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    return std::apply(
        [&](auto&&... child) {
          return evaluateOnChildrenOperands(context,
                                            child->evaluate(context)...);
        },
        children_);
  }

  // _________________________________________________________________________
  [[nodiscard]] std::string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return std::to_string(ad_utility::FastRandomIntGenerator<uint64_t>{}());
  }

 private:
  // _________________________________________________________________________
  ql::span<SparqlExpression::Ptr> childrenImpl() override { return children_; }

  // Evaluate the `naryOperation` on the `operands` using the `context`.
  template <typename... Operands>
  ExpressionResult evaluateOnChildrenOperands(EvaluationContext* context,
                                              Operands... operands) const {
    // We have to first determine the number of results we will produce.
    // TODO<joka921> This could also be a constant result...
    auto targetSize = context->size();

    auto zipper = std::apply(
        [&](const auto&... getters) {
          return ::ranges::views::zip(ad_utility::OwningView{
              getters(std::move(operands), context, targetSize)}...);
        },
        getters_);
    auto onTuple = [&](auto&& tuple) {
      return std::apply(
          [&](auto&&... args) { return function_(AD_FWD(args)...); },
          AD_FWD(tuple));
    };
    auto resultGenerator = ::ranges::views::transform(
        ad_utility::OwningView{std::move(zipper)}, onTuple);
    // Compute the result.
    using ResultType = Ret;
    VectorWithMemoryLimit<ResultType> result{context->_allocator};
    result.reserve(targetSize);
    ql::ranges::move(resultGenerator, std::back_inserter(result));

    if (result.size() == 1) {
      return std::move(result.at(0));
    } else {
      return result;
    }
  }
};

template <typename ValueGetter>
struct TypeErasedValueGetter {
  ad_utility::InputRangeTypeErased<typename ValueGetter::Value> operator()(
      ExpressionResult res, EvaluationContext* context, size_t size) const {
    /// Generate `numItems` many values from the `input` and apply the
    /// `valueGetter` to each of the values.
    return std::visit(
        [&](auto&& input) {
          return ad_utility::InputRangeTypeErased{valueGetterGenerator(
              size, context, std::move(input), ValueGetter{})};
        },
        std::move(res));
  }
};
/*
struct TypeErasedGeoPointValueGetter {
  ad_utility::InputRangeTypeErased<std::optional<GeoPoint>> operator()(
      ExpressionResult res, EvaluationContext* context, size_t size) const {
    /// Generate `numItems` many values from the `input` and apply the
    /// `valueGetter` to each of the values.
    return std::visit(
        [&](auto&& input) {
          return ad_utility::InputRangeTypeErased{valueGetterGenerator(
              size, context, std::move(input), GeoPointValueGetter{})};
        },
        std::move(res));
  }
};
*/

template <typename Operation, typename... ValueGetters>
static constexpr auto expressionFactory() {
  using Res = std::invoke_result_t<Operation, typename ValueGetters::Value...>;
  using Sig2 = Res(typename ValueGetters::Value...);
  return [](auto... childPtrs) {
    return std::make_unique<NaryExpression2<Sig2>>(
        Operation{}, std::tuple<TypeErasedValueGetter<ValueGetters>...>{},
        std::array{std::move(childPtrs)...});
  };
}

/* originally:
NARY_EXPRESSION(
    LongitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLongitude, true>, GeoPointValueGetter>);
    */
static constexpr auto longitudeExpression =
    expressionFactory<NumericIdWrapper<ad_utility::WktLongitude, true>,
                      GeoPointValueGetter>();
/* originally
NARY_EXPRESSION(
    LatitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLatitude, true>, GeoPointValueGetter>);
    */
static constexpr auto latitudeExpression =
    expressionFactory<NumericIdWrapper<ad_utility::WktLatitude, true>,
                      GeoPointValueGetter>();

static constexpr auto centroidExpression =
    expressionFactory<ad_utility::WktCentroid,
                      GeometryInfoValueGetter<ad_utility::Centroid>>();

static constexpr auto distExpression =
    expressionFactory<NumericIdWrapper<ad_utility::WktDist, true>,
                      GeoPointOrWktValueGetter, GeoPointOrWktValueGetter>();

static constexpr auto metricDistExpression =
    expressionFactory<NumericIdWrapper<ad_utility::WktMetricDist, true>,
                      GeoPointOrWktValueGetter, GeoPointOrWktValueGetter>();

static constexpr auto distWithUnitExpression =
    expressionFactory<NumericIdWrapper<ad_utility::WktDist, true>,
                      GeoPointOrWktValueGetter, GeoPointOrWktValueGetter,
                      UnitOfMeasurementValueGetter>();

static constexpr auto areaExpression =
    expressionFactory<ad_utility::WktArea,
                      GeometryInfoValueGetter<ad_utility::MetricArea>,
                      UnitOfMeasurementValueGetter>();

static constexpr auto metricAreaExpression =
    expressionFactory<ad_utility::WktMetricArea,
                      GeometryInfoValueGetter<ad_utility::MetricArea>>();

static constexpr auto envelopeExpression =
    expressionFactory<ad_utility::WktEnvelope,
                      GeometryInfoValueGetter<ad_utility::BoundingBox>>();

static constexpr auto geometryTypeExpression =
    expressionFactory<ad_utility::WktGeometryType,
                      GeometryInfoValueGetter<ad_utility::GeometryType>>();

static constexpr auto lengthExpression =
    expressionFactory<ad_utility::WktLength,
                      GeometryInfoValueGetter<ad_utility::MetricLength>,
                      UnitOfMeasurementValueGetter>();

static constexpr auto metricLengthExpression =
    expressionFactory<ad_utility::WktMetricLength,
                      GeometryInfoValueGetter<ad_utility::MetricLength>>();

static constexpr auto geometryNExpression =
    expressionFactory<ad_utility::WktGeometryN, GeoPointOrWktValueGetter,
                      IntValueGetter>();

static constexpr auto numGeometriesExpression =
    expressionFactory<ad_utility::WktNumGeometries,
                      GeometryInfoValueGetter<ad_utility::NumGeometries>>();

template <SpatialJoinType Relation>
static constexpr auto geoRelationExpressionFactory() {
  return expressionFactory<ad_utility::WktGeometricRelation<Relation>,
                           GeoPointValueGetter, GeoPointValueGetter>();
}

template <ad_utility::BoundingCoordinate RequestedCoordinate>
static constexpr auto boundingCoordinateExpressionFactory() {
  return expressionFactory<
      ad_utility::WktBoundingCoordinate<RequestedCoordinate>,
      GeometryInfoValueGetter<ad_utility::BoundingBox>>();
}

}  // namespace detail

using namespace detail;

SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child) {
  return latitudeExpression(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child) {
  return longitudeExpression(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeDistExpression(SparqlExpression::Ptr child1,
                                         SparqlExpression::Ptr child2) {
  return distExpression(std::move(child1), std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeMetricDistExpression(SparqlExpression::Ptr child1,
                                               SparqlExpression::Ptr child2) {
  return metricDistExpression(std::move(child1), std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeDistWithUnitExpression(
    SparqlExpression::Ptr child1, SparqlExpression::Ptr child2,
    std::optional<SparqlExpression::Ptr> child3) {
  // Unit is optional.
  if (child3.has_value()) {
    return distWithUnitExpression(std::move(child1), std::move(child2),
                                  std::move(child3.value()));
  } else {
    return distExpression(std::move(child1), std::move(child2));
  }
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeAreaExpression(SparqlExpression::Ptr child1,
                                         SparqlExpression::Ptr child2) {
  return areaExpression(std::move(child1), std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeMetricAreaExpression(SparqlExpression::Ptr child1) {
  return metricAreaExpression(std::move(child1));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeCentroidExpression(SparqlExpression::Ptr child) {
  return centroidExpression(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeEnvelopeExpression(SparqlExpression::Ptr child) {
  return envelopeExpression(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeGeometryTypeExpression(SparqlExpression::Ptr child) {
  return geometryTypeExpression(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeLengthExpression(SparqlExpression::Ptr child1,
                                           SparqlExpression::Ptr child2) {
  return lengthExpression(std::move(child1), std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeMetricLengthExpression(SparqlExpression::Ptr child1) {
  return metricLengthExpression(std::move(child1));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeGeometryNExpression(SparqlExpression::Ptr child1,
                                              SparqlExpression::Ptr child2) {
  return geometryNExpression(std::move(child1), std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeNumGeometriesExpression(SparqlExpression::Ptr child) {
  return numGeometriesExpression(std::move(child));
}

// _____________________________________________________________________________
template <SpatialJoinType Relation>
SparqlExpression::Ptr makeGeoRelationExpression(SparqlExpression::Ptr child1,
                                                SparqlExpression::Ptr child2) {
  return geoRelationExpressionFactory<Relation>()(std::move(child1),
                                                  std::move(child2));
}

// _____________________________________________________________________________
template <ad_utility::BoundingCoordinate RequestedCoordinate>
SparqlExpression::Ptr makeBoundingCoordinateExpression(
    SparqlExpression::Ptr child) {
  return boundingCoordinateExpressionFactory<RequestedCoordinate>()(
      std::move(child));
}

}  // namespace sparqlExpression

// Explicit instantiations for the different geometric relations to avoid linker
// problems.
using Ptr = sparqlExpression::SparqlExpression::Ptr;

#ifdef QL_INSTANTIATE_GEO_RELATION_EXPR
#error "Macro QL_INSTANTIATE_GEO_RELATION_EXPR already defined"
#endif
#define QL_INSTANTIATE_GEO_RELATION_EXPR(joinType)                            \
  template Ptr                                                                \
      sparqlExpression::makeGeoRelationExpression<SpatialJoinType::joinType>( \
          Ptr, Ptr);

QL_INSTANTIATE_GEO_RELATION_EXPR(INTERSECTS);
QL_INSTANTIATE_GEO_RELATION_EXPR(CONTAINS);
QL_INSTANTIATE_GEO_RELATION_EXPR(COVERS);
QL_INSTANTIATE_GEO_RELATION_EXPR(CROSSES);
QL_INSTANTIATE_GEO_RELATION_EXPR(TOUCHES);
QL_INSTANTIATE_GEO_RELATION_EXPR(EQUALS);
QL_INSTANTIATE_GEO_RELATION_EXPR(OVERLAPS);
QL_INSTANTIATE_GEO_RELATION_EXPR(WITHIN);

// Explicit instantiations for the bounding coordinate expressions.
#ifdef QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR
#error "Macro QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR already defined"
#endif
#define QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(RequestedCoordinate) \
  template Ptr sparqlExpression::makeBoundingCoordinateExpression<   \
      ad_utility::BoundingCoordinate::RequestedCoordinate>(Ptr);

QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(MIN_X);
QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(MIN_Y);
QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(MAX_X);
QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(MAX_Y);
