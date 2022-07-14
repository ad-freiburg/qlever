// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "SparqlQleverVisitor.h"

#include <string>
#include <vector>

// ___________________________________________________________________________
antlrcpp::Any SparqlQleverVisitor::processIriFunctionCall(
    const std::string& iri,
    std::vector<SparqlQleverVisitor::ExpressionPtr> argList) {
  // Lambda that checks the number of arguments and throws an error if it's
  // not right.
  auto checkNumArgs = [&argList](const std::string_view prefix,
                                 const std::string_view functionName,
                                 size_t numArgs) {
    static std::array<std::string, 6> wordForNumArgs = {
        "no", "one", "two", "three", "four", "five"};
    if (argList.size() != numArgs) {
      throw ParseException{absl::StrCat(
          "Function ", prefix, functionName, " takes ",
          numArgs < 5 ? wordForNumArgs[numArgs] : std::to_string(numArgs),
          numArgs == 1 ? " argument" : " arguments")};
    }
  };

  constexpr static std::string_view geofPrefix =
      "<http://www.opengis.net/def/function/geosparql/";
  std::string_view iriView = iri;
  if (iriView.starts_with(geofPrefix)) {
    iriView.remove_prefix(geofPrefix.size());
    AD_CHECK(iriView.ends_with('>'));
    iriView.remove_suffix(1);
    if (iriView == "distance") {
      checkNumArgs("geof:", iriView, 2);
      return createExpression<sparqlExpression::DistExpression>(
          std::move(argList[0]), std::move(argList[1]));
    } else if (iriView == "longitude") {
      checkNumArgs("geof:", iriView, 1);
      return createExpression<sparqlExpression::LongitudeExpression>(
          std::move(argList[0]));
    } else if (iriView == "latitude") {
      checkNumArgs("geof:", iriView, 1);
      return createExpression<sparqlExpression::LatitudeExpression>(
          std::move(argList[0]));
    }
  }
  throw ParseException{"Function \"" + iri + "\" not supported"};
}

// ___________________________________________________________________________
namespace {
string stripAndLowercaseKeywordLiteral(std::string_view lit) {
  if (lit.size() > 2 && lit[0] == '"' && lit.back() == '"') {
    auto stripped = lit.substr(1, lit.size() - 2);
    return ad_utility::getLowercaseUtf8(stripped);
  }
  return std::string{lit};
}

// ___________________________________________________________________________
template <typename Current, typename... Others>
constexpr const ad_utility::Last<Current, Others...>* unwrapVariant(
    const auto& arg) {
  if constexpr (sizeof...(Others) > 0) {
    if constexpr (ad_utility::isSimilar<decltype(arg), Current>) {
      if (const auto ptr = std::get_if<ad_utility::First<Others...>>(&arg)) {
        return unwrapVariant<Others...>(*ptr);
      }
      return nullptr;
    } else {
      return unwrapVariant<Others...>(arg);
    }
  } else {
    return &arg;
  }
}
}  // namespace

// ___________________________________________________________________________
using PathTuples = ad_utility::sparql_types::PathTuples;
PathTuples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PropertyListPathNotEmptyContext* ctx) {
  PathTuples tuples;
  vector<PropertyPath> verbPathOrSimples =
      visitVector<PropertyPath>(ctx->verbPathOrSimple());
  vector<ObjectList> objectLists = visitVector<ObjectList>(ctx->objectList());
  // The predicates are one vector. The objects are split up into a single
  // object and a vector. Join them into a single vector.
  objectLists.insert(objectLists.begin(), visitTypesafe(ctx->objectListPath()));

  AD_CHECK_EQ(verbPathOrSimples.size(),
              objectLists.size());  // number of verbPathOrSimple
  // must be equal to length of objectList + objectListPath
  // TODO use zip-style approach once C++ supports ranges
  for (size_t i = 0; i < verbPathOrSimples.size(); i++) {
    PropertyPath current = std::move(verbPathOrSimples.at(i));
    for (auto& object : objectLists.at(i).first) {
      if (current.asString() == CONTAINS_WORD_PREDICATE ||
          // TODO _NS no longer needed?
          current.asString() == CONTAINS_WORD_PREDICATE_NS) {
        if (const Literal* literal =
                unwrapVariant<VarOrTerm, GraphTerm, Literal>(object)) {
          object = Literal{stripAndLowercaseKeywordLiteral(literal->literal())};
        }
      }
      tuples.push_back({current, std::move(object)});
    }
  }
  return tuples;
}
