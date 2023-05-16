// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkConfigurationShorthandVisitor.h"

#include <absl/strings/str_cat.h>

#include <utility>

#include "util/Exception.h"

// __________________________________________________________________________
nlohmann::json::object_t
ToJsonBenchmarkConfigurationShorthandVisitor::visitShortHandString(
    Parser::ShortHandStringContext* context) const {
  return visitAssignments(context->assignments());
}

// __________________________________________________________________________
nlohmann::json::object_t
ToJsonBenchmarkConfigurationShorthandVisitor::visitAssignments(
    const Parser::AssignmentsContext* context) const {
  nlohmann::json::object_t contextAsJson;

  for (auto assignment : context->listOfAssignments) {
    std::pair<std::string, nlohmann::json> interpretedAssignment =
        visitAssignment(assignment);

    // The same key twice isn't allowed.
    if (contextAsJson.contains(interpretedAssignment.first)) {
      throw ad_utility::Exception(absl::StrCat(
          "Key error in the short hand: There are at least two key value "
          "pairs, at the same level of depth, with the key '",
          interpretedAssignment.first,
          "' given. This is not allowed, keys must be unique in their level of "
          "depth"));
    }

    // Add the json representation of the assignment.
    contextAsJson.insert(std::move(interpretedAssignment));
  }

  return contextAsJson;
}

// __________________________________________________________________________
std::pair<std::string, nlohmann::json>
ToJsonBenchmarkConfigurationShorthandVisitor::visitAssignment(
    Parser::AssignmentContext* context) const {
  return {context->NAME()->getText(), visitContent(context->content())};
}

// __________________________________________________________________________
nlohmann::json::object_t
ToJsonBenchmarkConfigurationShorthandVisitor::visitObject(
    Parser::ObjectContext* context) const {
  return visitAssignments(context->assignments());
}

// __________________________________________________________________________
nlohmann::json::array_t ToJsonBenchmarkConfigurationShorthandVisitor::visitList(
    const Parser::ListContext* context) const {
  nlohmann::json::array_t contextAsJson;

  // Convert the content of the list.
  for (auto element : context->listElement) {
    // Add the json representation of the element.
    contextAsJson.push_back(visitContent(element));
  }

  return contextAsJson;
}

// __________________________________________________________________________
nlohmann::json ToJsonBenchmarkConfigurationShorthandVisitor::visitContent(
    Parser::ContentContext* context) const {
  if (context->LITERAL()) {
    return nlohmann::json::parse(context->LITERAL()->getText());
  } else if (context->list()) {
    return visitList(context->list());
  } else {
    // Must be an object.
    return visitObject(context->object());
  }
}
