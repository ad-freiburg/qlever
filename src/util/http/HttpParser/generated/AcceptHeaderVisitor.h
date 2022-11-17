
// Generated from AcceptHeader.g4 by ANTLR 4.9.2

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
  virtual antlrcpp::Any visitAccept(
      AcceptHeaderParser::AcceptContext* context) = 0;

  virtual antlrcpp::Any visitAcceptWithEof(
      AcceptHeaderParser::AcceptWithEofContext* context) = 0;

  virtual antlrcpp::Any visitRangeAndParams(
      AcceptHeaderParser::RangeAndParamsContext* context) = 0;

  virtual antlrcpp::Any visitMediaRange(
      AcceptHeaderParser::MediaRangeContext* context) = 0;

  virtual antlrcpp::Any visitType(AcceptHeaderParser::TypeContext* context) = 0;

  virtual antlrcpp::Any visitSubtype(
      AcceptHeaderParser::SubtypeContext* context) = 0;

  virtual antlrcpp::Any visitAcceptParams(
      AcceptHeaderParser::AcceptParamsContext* context) = 0;

  virtual antlrcpp::Any visitWeight(
      AcceptHeaderParser::WeightContext* context) = 0;

  virtual antlrcpp::Any visitQvalue(
      AcceptHeaderParser::QvalueContext* context) = 0;

  virtual antlrcpp::Any visitAcceptExt(
      AcceptHeaderParser::AcceptExtContext* context) = 0;

  virtual antlrcpp::Any visitParameter(
      AcceptHeaderParser::ParameterContext* context) = 0;

  virtual antlrcpp::Any visitToken(
      AcceptHeaderParser::TokenContext* context) = 0;

  virtual antlrcpp::Any visitTchar(
      AcceptHeaderParser::TcharContext* context) = 0;

  virtual antlrcpp::Any visitQuotedString(
      AcceptHeaderParser::QuotedStringContext* context) = 0;

  virtual antlrcpp::Any visitQuoted_pair(
      AcceptHeaderParser::Quoted_pairContext* context) = 0;
};
