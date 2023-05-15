// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <any>
#include <utility>

#include "../benchmark/infrastructure/BenchmarkConfigurationShorthandVisitor.h"

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
    const std::pair<std::string, nlohmann::json>& interpretedAssignment =
        visitAssignment(assignment);

    // Add the json representation of the assignment.
    contextAsJson.insert_or_assign(interpretedAssignment.first,
                                   interpretedAssignment.second);
  }

  return contextAsJson;
}

// __________________________________________________________________________
std::pair<std::string, nlohmann::json>
ToJsonBenchmarkConfigurationShorthandVisitor::visitAssignment(
    Parser::AssignmentContext* context) const {
  return std::make_pair(context->NAME()->getText(),
                        visitContent(context->content()));
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
