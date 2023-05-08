// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkConfigurationShorthandVisitor.h"

#include <any>

#include "util/json.h"

std::any BenchmarkConfigurationShorthandVisitor::visitAssignments(
    BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext*
        context) {
  nlohmann::json contextAsJson(nlohmann::json::value_t::object);

  for (auto assignment : context->listOfAssignments) {
    // Add the json representation of the assignment.
    contextAsJson.update(
        std::any_cast<nlohmann::json>(visitAssignment(assignment)));
  }

  return contextAsJson;
}

std::any BenchmarkConfigurationShorthandVisitor::visitAssignment(
    BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext*
        context) {
  nlohmann::json contextAsJson(nlohmann::json::value_t::object);

  // Do we just have the interpret the literal, or do we have to work through an
  // array?
  if (context->LITERAL()) {
    contextAsJson[context->NAME()->getText()] =
        nlohmann::json::parse(context->LITERAL()->getText());
  } else {
    contextAsJson[context->NAME()->getText()] =
        std::any_cast<nlohmann::json>(visitList(context->list()));
  }

  return contextAsJson;
}

std::any BenchmarkConfigurationShorthandVisitor::visitList(
    BenchmarkConfigurationShorthandAutomaticParser::ListContext* context) {
  nlohmann::json contextAsJson(nlohmann::json::value_t::array);

  // Convert the content of the list.
  for (auto element : context->listElement) {
    // Add the json representation of the element.
    contextAsJson.push_back(nlohmann::json::parse(element->getText()));
  }

  return contextAsJson;
}
