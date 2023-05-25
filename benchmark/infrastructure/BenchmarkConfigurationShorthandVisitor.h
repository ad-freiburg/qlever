// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)
#pragma once

#include <antlr4-runtime.h>

#include "../benchmark/infrastructure/generated/BenchmarkConfigurationShorthandParser.h"
#include "util/json.h"

/*
This visitor will translate the parsed short hand into a `nlohmann::json`
object.
*/
class ToJsonBenchmarkConfigurationShorthandVisitor final {
 public:
  using Parser = BenchmarkConfigurationShorthandParser;

  // __________________________________________________________________________
  nlohmann::json::object_t visitShortHandString(
      Parser::ShortHandStringContext* context) const;

  // __________________________________________________________________________
  nlohmann::json::object_t visitAssignments(
      const Parser::AssignmentsContext* context) const;

  // __________________________________________________________________________
  std::pair<std::string, nlohmann::json> visitAssignment(
      Parser::AssignmentContext* context) const;

  // __________________________________________________________________________
  nlohmann::json::object_t visitObject(Parser::ObjectContext* context) const;

  // __________________________________________________________________________
  nlohmann::json::array_t visitList(const Parser::ListContext* context) const;

  // __________________________________________________________________________
  nlohmann::json visitContent(Parser::ContentContext* context) const;
};
