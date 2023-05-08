
// Generated from BenchmarkConfigurationShorthandAutomatic.g4 by ANTLR 4.12.0

#pragma once

#include "BenchmarkConfigurationShorthandAutomaticParser.h"
#include "antlr4-runtime.h"

/**
 * This class defines an abstract visitor for a parse tree
 * produced by BenchmarkConfigurationShorthandAutomaticParser.
 */
class BenchmarkConfigurationShorthandAutomaticVisitor
    : public antlr4::tree::AbstractParseTreeVisitor {
 public:
  /**
   * Visit parse trees produced by
   * BenchmarkConfigurationShorthandAutomaticParser.
   */
  virtual std::any visitAssignments(
      BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext*
          context) = 0;

  virtual std::any visitAssignment(
      BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext*
          context) = 0;

  virtual std::any visitList(
      BenchmarkConfigurationShorthandAutomaticParser::ListContext* context) = 0;
};
