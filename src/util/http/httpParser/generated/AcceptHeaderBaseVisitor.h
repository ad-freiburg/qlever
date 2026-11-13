
// Generated from AcceptHeader.g4 by ANTLR 4.11.1

#ifndef QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERBASEVISITOR_H
#define QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERBASEVISITOR_H

#include "AcceptHeaderVisitor.h"
#include "antlr4-runtime.h"

/**
 * This class provides an empty implementation of AcceptHeaderVisitor, which can
 * be extended to create a visitor which only needs to handle a subset of the
 * available methods.
 */
class AcceptHeaderBaseVisitor : public AcceptHeaderVisitor {
 public:
  virtual std::any visitAccept(
      AcceptHeaderParser::AcceptContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAcceptWithEof(
      AcceptHeaderParser::AcceptWithEofContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRangeAndParams(
      AcceptHeaderParser::RangeAndParamsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMediaRange(
      AcceptHeaderParser::MediaRangeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitType(AcceptHeaderParser::TypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubtype(
      AcceptHeaderParser::SubtypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAcceptParams(
      AcceptHeaderParser::AcceptParamsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWeight(
      AcceptHeaderParser::WeightContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQvalue(
      AcceptHeaderParser::QvalueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAcceptExt(
      AcceptHeaderParser::AcceptExtContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParameter(
      AcceptHeaderParser::ParameterContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitToken(AcceptHeaderParser::TokenContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTchar(AcceptHeaderParser::TcharContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuotedString(
      AcceptHeaderParser::QuotedStringContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuoted_pair(
      AcceptHeaderParser::Quoted_pairContext* ctx) override {
    return visitChildren(ctx);
  }
};

#endif  // QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERBASEVISITOR_H
