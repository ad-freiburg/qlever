// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)
#pragma once

#include <antlr4-runtime.h>

#include "../benchmark/infrastructure/generated/BenchmarkConfigurationShorthandAutomaticVisitor.h"

/*
This visitor will translate the parsed short hand into a `nlohmann::json`
object.
*/
class BenchmarkConfigurationShorthandVisitor final
    : public BenchmarkConfigurationShorthandAutomaticVisitor {
 public:
  std::any visitAssignments(
      BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext*
          context);

  std::any visitAssignment(
      BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext*
          context);

  std::any visitList(
      BenchmarkConfigurationShorthandAutomaticParser::ListContext* context);
};
