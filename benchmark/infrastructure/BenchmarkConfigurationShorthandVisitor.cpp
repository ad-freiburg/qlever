// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkConfigurationShorthandVisitor.h"

#include <any>

// __________________________________________________________________________
nlohmann::json
ToJsonBenchmarkConfigurationShorthandVisitor::visitShortHandString(
    Parser::ShortHandStringContext* context) {
  return visitAssignments(context->assignments());
}

// __________________________________________________________________________
nlohmann::json ToJsonBenchmarkConfigurationShorthandVisitor::visitAssignments(
    Parser::AssignmentsContext* context) {
  nlohmann::json contextAsJson(nlohmann::json::value_t::object);

  for (auto assignment : context->listOfAssignments) {
    // Add the json representation of the assignment.
    contextAsJson.update(visitAssignment(assignment));
  }

  return contextAsJson;
}

// __________________________________________________________________________
nlohmann::json ToJsonBenchmarkConfigurationShorthandVisitor::visitAssignment(
    Parser::AssignmentContext* context) {
  return nlohmann::json{
      {context->NAME()->getText(), visitContent(context->content())}};
}

// __________________________________________________________________________
nlohmann::json ToJsonBenchmarkConfigurationShorthandVisitor::visitObject(
    Parser::ObjectContext* context) {
  return visitAssignments(context->assignments());
}

// __________________________________________________________________________
nlohmann::json ToJsonBenchmarkConfigurationShorthandVisitor::visitList(
    Parser::ListContext* context) {
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
    Parser::ContentContext* context) {
  if (context->LITERAL()) {
    return nlohmann::json::parse(context->LITERAL()->getText());
  } else if (context->list()) {
    return visitList(context->list());
  } else {
    // Must be an object.
    return visitObject(context->object());
  }
}
