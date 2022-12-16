
// Generated from AcceptHeader.g4 by ANTLR 4.11.1

#pragma once

#include "AcceptHeaderParser.h"
#include "antlr4-runtime.h"

/**
 * This class defines an abstract visitor for a parse tree
 * produced by AcceptHeaderParser.
 */
class AcceptHeaderVisitor : public antlr4::tree::AbstractParseTreeVisitor {
 public:
  /**
   * Visit parse trees produced by AcceptHeaderParser.
   */
  virtual std::any visitAccept(AcceptHeaderParser::AcceptContext* context) = 0;

  virtual std::any visitAcceptWithEof(
      AcceptHeaderParser::AcceptWithEofContext* context) = 0;

  virtual std::any visitRangeAndParams(
      AcceptHeaderParser::RangeAndParamsContext* context) = 0;

  virtual std::any visitMediaRange(
      AcceptHeaderParser::MediaRangeContext* context) = 0;

  virtual std::any visitType(AcceptHeaderParser::TypeContext* context) = 0;

  virtual std::any visitSubtype(
      AcceptHeaderParser::SubtypeContext* context) = 0;

  virtual std::any visitAcceptParams(
      AcceptHeaderParser::AcceptParamsContext* context) = 0;

  virtual std::any visitWeight(AcceptHeaderParser::WeightContext* context) = 0;

  virtual std::any visitQvalue(AcceptHeaderParser::QvalueContext* context) = 0;

  virtual std::any visitAcceptExt(
      AcceptHeaderParser::AcceptExtContext* context) = 0;

  virtual std::any visitParameter(
      AcceptHeaderParser::ParameterContext* context) = 0;

  virtual std::any visitToken(AcceptHeaderParser::TokenContext* context) = 0;

  virtual std::any visitTchar(AcceptHeaderParser::TcharContext* context) = 0;

  virtual std::any visitQuotedString(
      AcceptHeaderParser::QuotedStringContext* context) = 0;

  virtual std::any visitQuoted_pair(
      AcceptHeaderParser::Quoted_pairContext* context) = 0;
};
