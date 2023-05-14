// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkConfigurationShorthandVisitor.h"

#include <any>

// __________________________________________________________________________
nlohmann::json
ToJsonBenchmarkConfigurationShorthandVisitor::visitShortHandString(
    Parser::ShortHandStringContext* context) const {
  return visitAssignments(context->assignments());
}

// __________________________________________________________________________
nlohmann::json ToJsonBenchmarkConfigurationShorthandVisitor::visitAssignments(
    const Parser::AssignmentsContext* context) const {
  nlohmann::json contextAsJson(nlohmann::json::value_t::object);

  for (auto assignment : context->listOfAssignments) {
    // Add the json representation of the assignment.
    contextAsJson.update(visitAssignment(assignment));
  }

  return contextAsJson;
}

// __________________________________________________________________________
nlohmann::json ToJsonBenchmarkConfigurationShorthandVisitor::visitAssignment(
    Parser::AssignmentContext* context) const {
  return nlohmann::json{
      {context->NAME()->getText(), visitContent(context->content())}};
}

// __________________________________________________________________________
nlohmann::json ToJsonBenchmarkConfigurationShorthandVisitor::visitObject(
    Parser::ObjectContext* context) const {
  return visitAssignments(context->assignments());
}

// __________________________________________________________________________
nlohmann::json ToJsonBenchmarkConfigurationShorthandVisitor::visitList(
    const Parser::ListContext* context) const {
  nlohmann::json contextAsJson(nlohmann::json::value_t::array);

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
