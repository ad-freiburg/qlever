
// Generated from AcceptHeader.g4 by ANTLR 4.9.2

#pragma once

#include "AcceptHeaderVisitor.h"
#include "antlr4-runtime.h"

/**
 * This class provides an empty implementation of AcceptHeaderVisitor, which can
 * be extended to create a visitor which only needs to handle a subset of the
 * available methods.
 */
class AcceptHeaderBaseVisitor : public AcceptHeaderVisitor {
 public:
  virtual antlrcpp::Any visitAccept(
      AcceptHeaderParser::AcceptContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAcceptWithEof(
      AcceptHeaderParser::AcceptWithEofContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRangeAndParams(
      AcceptHeaderParser::RangeAndParamsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitMediaRange(
      AcceptHeaderParser::MediaRangeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitType(
      AcceptHeaderParser::TypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubtype(
      AcceptHeaderParser::SubtypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAcceptParams(
      AcceptHeaderParser::AcceptParamsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitWeight(
      AcceptHeaderParser::WeightContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQvalue(
      AcceptHeaderParser::QvalueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAcceptExt(
      AcceptHeaderParser::AcceptExtContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitParameter(
      AcceptHeaderParser::ParameterContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitToken(
      AcceptHeaderParser::TokenContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTchar(
      AcceptHeaderParser::TcharContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQuotedString(
      AcceptHeaderParser::QuotedStringContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQuoted_pair(
      AcceptHeaderParser::Quoted_pairContext* ctx) override {
    return visitChildren(ctx);
  }
};
