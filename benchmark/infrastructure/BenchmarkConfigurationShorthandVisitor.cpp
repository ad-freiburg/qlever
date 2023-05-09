// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkConfigurationShorthandVisitor.h"

#include <any>

#include "util/json.h"

std::any ToJsonBenchmarkConfigurationShorthandVisitor::visitShortHandString(
    BenchmarkConfigurationShorthandParser::ShortHandStringContext* context) {
  return visitAssignments(context->assignments());
}

std::any ToJsonBenchmarkConfigurationShorthandVisitor::visitAssignments(
    BenchmarkConfigurationShorthandParser::AssignmentsContext* context) {
  nlohmann::json contextAsJson(nlohmann::json::value_t::object);

  for (auto assignment : context->listOfAssignments) {
    // Add the json representation of the assignment.
    contextAsJson.update(
        std::any_cast<nlohmann::json>(visitAssignment(assignment)));
  }

  return contextAsJson;
}

std::any ToJsonBenchmarkConfigurationShorthandVisitor::visitAssignment(
    BenchmarkConfigurationShorthandParser::AssignmentContext* context) {
  return nlohmann::json{
      {context->NAME()->getText(),
       std::any_cast<nlohmann::json>(visitContent(context->content()))}};
}

std::any ToJsonBenchmarkConfigurationShorthandVisitor::visitObject(
    BenchmarkConfigurationShorthandParser::ObjectContext* context) {
  return visitAssignments(context->assignments());
}

std::any ToJsonBenchmarkConfigurationShorthandVisitor::visitList(
    BenchmarkConfigurationShorthandParser::ListContext* context) {
  nlohmann::json contextAsJson(nlohmann::json::value_t::array);

  // Convert the content of the list.
  for (auto element : context->listElement) {
    // Add the json representation of the element.
    contextAsJson.push_back(
        std::any_cast<nlohmann::json>(visitContent(element)));
  }

  return contextAsJson;
}

std::any ToJsonBenchmarkConfigurationShorthandVisitor::visitContent(
    BenchmarkConfigurationShorthandParser::ContentContext* context) {
  if (context->LITERAL()) {
    return nlohmann::json::parse(context->LITERAL()->getText());
  } else if (context->list()) {
    return visitList(context->list());
  } else {
    // Must be an object.
    return visitObject(context->object());
  }
}
