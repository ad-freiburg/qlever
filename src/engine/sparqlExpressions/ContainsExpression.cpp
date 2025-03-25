//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#include "./ContainsExpression.h"

#include <util/RtreeBasicGeometry.h>

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "global/ValueIdComparators.h"
#include "re2/re2.h"

using namespace std::literals;

class BoundingBoxType {
 public:
  BasicGeometry::BoundingBox value{};

  explicit BoundingBoxType(BasicGeometry::BoundingBox boundingBox) {
    this->value = boundingBox;
  }
};

std::string removeQuotes(std::string_view input) {
  AD_CORRECTNESS_CHECK(input.size() >= 2 && input.starts_with('"') &&
                       input.ends_with('"'));
  input.remove_prefix(1);
  input.remove_suffix(1);
  return std::string{input};
}

namespace sparqlExpression {
// ___________________________________________________________________________
ContainsExpression::ContainsExpression(SparqlExpression::Ptr child,
                                       SparqlExpression::Ptr boundingBox)
    : child_{std::move(child)} {
  if (!dynamic_cast<const VariableExpression*>(child_.get())) {
    throw std::runtime_error(
        "Contain expressions are currently supported only on variables.");
  }
  std::string boundingBoxString;
  std::string originalBoundingBoxString;
  if (auto boundingBoxPtr =
          dynamic_cast<const StringLiteralExpression*>(boundingBox.get())) {
    originalBoundingBoxString =
        asStringViewUnsafe(boundingBoxPtr->value().getContent());
    /*
    if (!boundingBoxPtr->value().datatypeOrLangtag().empty()) {
      throw std::runtime_error(
          "The second argument to the Contain function (which contains the "
          "bounding box) must not contain a language tag or a datatype");
    }
    */
    boundingBoxString = removeQuotes(originalBoundingBoxString);
  } else {
    throw std::runtime_error(
        "The second argument to the Contains function must be a "
        "string literal (which contains the bounding box of format "
        "\"minX,minY,maxX,maxY\")");
  }

  boundingBoxAsString_ = boundingBoxString;

  std::vector<std::string> boundingBoxEntriesAsString(4);
  std::string errorMessage;
  std::string::size_type searchFrom = 0;
  for (int i = 0; i < 4; i++) {
    if (i == 3) {
      if (searchFrom >= boundingBoxAsString_.size()) {
        errorMessage = "The fourth argument was not provided";
        break;
      }
      boundingBoxEntriesAsString[i] = boundingBoxAsString_.substr(
          searchFrom, boundingBoxAsString_.size() - searchFrom);
      break;
    }
    std::string::size_type end = boundingBoxAsString_.find(',', searchFrom);
    if (end >= boundingBoxAsString_.size() - 1 || end == std::string::npos) {
      errorMessage = "There are not enough arguments";
      break;
    }
    boundingBoxEntriesAsString[i] =
        boundingBoxAsString_.substr(searchFrom, end - searchFrom);
    searchFrom = end + 1;
  }

  if (errorMessage.empty()) {
    double minX;
    double minY;
    double maxX;
    double maxY;

    try {
      minX = std::stod(boundingBoxEntriesAsString[0]);
      minY = std::stod(boundingBoxEntriesAsString[1]);
      maxX = std::stod(boundingBoxEntriesAsString[2]);
      maxY = std::stod(boundingBoxEntriesAsString[3]);

      boundingBox_ = new BoundingBoxType(
          BasicGeometry::CreateBoundingBox(minX, minY, maxX, maxY));
    } catch (const std::invalid_argument& e) {
      errorMessage = e.what();
    } catch (const std::out_of_range& e) {
      errorMessage = e.what();
    }
  }

  if (!errorMessage.empty()) {
    throw std::runtime_error{
        absl::StrCat("The bounding box ", originalBoundingBoxString,
                     " is not supported by QLever (must be of format "
                     "\"minX,minY,maxX,maxY\"). "
                     "Error message is: ",
                     errorMessage)};
  }
}

// ___________________________________________________________________________
string ContainsExpression::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return absl::StrCat("Bounding Box CONTAINS expression ",
                      child_->getCacheKey(varColMap), " with ",
                      boundingBoxAsString_);
}

// ___________________________________________________________________________
std::span<SparqlExpression::Ptr> ContainsExpression::childrenImpl() {
  return {&child_, 1};
}

// ___________________________________________________________________________
ExpressionResult ContainsExpression::evaluate(
    sparqlExpression::EvaluationContext* context) const {
  auto resultAsVariant = child_->evaluate(context);
  auto variablePtr = std::get_if<Variable>(&resultAsVariant);
  AD_CONTRACT_CHECK(variablePtr);

  // search in the rtree
  ad_utility::HashSet<long long> resultIds;
  if (boundingBox_ != nullptr) {
    Rtree rtree = context->_qec.getIndex().getRtree();
    multiBoxGeo treeResults =
        rtree.SearchTree(boundingBox_->value, "./rtree_build");

    for (RTreeValue item : treeResults) {
      long long id = item.id;
      resultIds.insert(id);
    }
  }

  auto resultSize = context->size();
  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(resultSize);

  for (auto id : detail::makeGenerator(*variablePtr, resultSize, context)) {
    result.push_back(Id::makeFromBool(resultIds.contains(id.getInt())));
  }

  return result;
}

}  // namespace sparqlExpression
