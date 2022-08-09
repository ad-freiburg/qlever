//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ACCEPTHEADERQLEVERVISITOR_H
#define QLEVER_ACCEPTHEADERQLEVERVISITOR_H

// Generated from AcceptHeader.g4 by ANTLR 4.9.2

#include "../../Exception.h"
#include "../MediaTypes.h"
#include "./generated/AcceptHeaderVisitor.h"
#include "antlr4-runtime.h"

/**
 * /brief Visitor class for the ANTLR-based Accept header parser. Main
 * entrypoint is the `visitAccept` or `visitAcceptWithEof` function, which
 * yields an `antlrcpp::any` that holds a
 * `std::vector<ad_utility::MediaTypeWithQuality>`.
 */
class AcceptHeaderQleverVisitor : public AcceptHeaderVisitor {
 public:
  using MediaType = ad_utility::MediaType;

  class Exception : public std::runtime_error {
    using std::runtime_error::runtime_error;
  };
  class ParseException : public std::runtime_error {
    using std::runtime_error::runtime_error;
  };
  class NotSupportedException : public std::exception {
   public:
    explicit NotSupportedException(std::string_view featureName)
        : _message{"The feature \"" + std::string{featureName} +
                   "\" is currently not supported inside the `Accept:` header "
                   "field of an HTTP request"} {}
    [[nodiscard]] const char* what() const noexcept override {
      return _message.c_str();
    }

   private:
    std::string _message;
  };

  explicit AcceptHeaderQleverVisitor(std::vector<MediaType> supportedMediaTypes)
      : _supportedMediaTypes{std::move(supportedMediaTypes)} {}

 private:
  std::vector<MediaType> _supportedMediaTypes;

 public:
  // ________________________________________________________________________
  antlrcpp::Any visitAcceptWithEof(
      AcceptHeaderParser::AcceptWithEofContext* ctx) override {
    return ctx->accept()->accept(this);
  }

  // ________________________________________________________________________
  antlrcpp::Any visitAccept(AcceptHeaderParser::AcceptContext* ctx) override {
    std::vector<ad_utility::MediaTypeWithQuality> acceptedMediaTypes;
    for (const auto& child : ctx->rangeAndParams()) {
      auto mediaType =
          child->accept(this)
              .as<std::optional<ad_utility::MediaTypeWithQuality>>();
      if (mediaType.has_value()) {
        acceptedMediaTypes.push_back(mediaType.value());
      }
    }
    if (acceptedMediaTypes.empty()) {
      throw Exception{
          "Not a single media type known to this parser was detected in \"" +
          ctx->getText() + "\". " +
          ad_utility::getErrorMessageForSupportedMediaTypes(
              _supportedMediaTypes)};
    }
    return {std::move(acceptedMediaTypes)};
  }

  antlrcpp::Any visitRangeAndParams(
      AcceptHeaderParser::RangeAndParamsContext* ctx) override {
    std::optional<ad_utility::MediaTypeWithQuality> result = std::nullopt;
    float quality = 1.0;
    if (ctx->acceptParams()) {
      quality = ctx->acceptParams()->accept(this).as<float>();
    }
    using V = std::optional<ad_utility::MediaTypeWithQuality::Variant>;
    auto mediaRange = ctx->mediaRange()->accept(this).as<V>();
    if (mediaRange.has_value()) {
      result.emplace(ad_utility::MediaTypeWithQuality{
          quality, std::move(mediaRange.value())});
    }
    return result;
  }

  antlrcpp::Any visitMediaRange(
      AcceptHeaderParser::MediaRangeContext* ctx) override {
    using V = std::optional<ad_utility::MediaTypeWithQuality::Variant>;
    if (!ctx->subtype()) {
      if (!ctx->type()) {
        return V{ad_utility::MediaTypeWithQuality::Wildcard{}};
      } else {
        return V{ad_utility::MediaTypeWithQuality::TypeWithWildcard{
            ctx->type()->getText()}};
      }
    }
    if (!ctx->parameter().empty()) {
      throw NotSupportedException{
          "Media type parameters, e.g.  \"charset=...\""};
    }
    return V{ad_utility::toMediaType(ctx->getText())};
  }

  antlrcpp::Any visitType(AcceptHeaderParser::TypeContext* ctx) override {
    AD_FAIL();  // Should be unreachable.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSubtype(AcceptHeaderParser::SubtypeContext* ctx) override {
    AD_FAIL();  // Should be unreachable.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAcceptParams(
      AcceptHeaderParser::AcceptParamsContext* ctx) override {
    if (!ctx->acceptExt().empty()) {
      throw NotSupportedException{"Media type parameters like \"charset=...\""};
    }
    return ctx->weight()->accept(this);
  }

  antlrcpp::Any visitWeight(AcceptHeaderParser::WeightContext* ctx) override {
    auto throwException = [ctx] {
      throw ParseException{
          "Decimal values for quality parameters in accept header must be "
          "between 0 and 1, and must have at most 3 decimal digits. Found "
          "illegal quality value " +
          ctx->qvalue()->getText()};
    };
    if (ctx->qvalue()->getText().size() > 5) {
      throwException();
    }
    auto quality = std::stof(ctx->qvalue()->getText());
    if (quality > 1.0f) {
      throwException();
    }
    return quality;
  }

  antlrcpp::Any visitQvalue(AcceptHeaderParser::QvalueContext* ctx) override {
    AD_FAIL();  // Should be unreachable.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAcceptExt(
      AcceptHeaderParser::AcceptExtContext* ctx) override {
    AD_FAIL();  // Should be unreachable.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitParameter(
      AcceptHeaderParser::ParameterContext* ctx) override {
    AD_FAIL();  // Should be unreachable.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitToken(AcceptHeaderParser::TokenContext* ctx) override {
    return ctx->getText();
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTchar(AcceptHeaderParser::TcharContext* ctx) override {
    AD_FAIL();  // Should be unreachable.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitQuotedString(
      AcceptHeaderParser::QuotedStringContext* ctx) override {
    AD_FAIL();  // Should be unreachable.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitQuoted_pair(
      AcceptHeaderParser::Quoted_pairContext* ctx) override {
    AD_FAIL();  // Should be unreachable.
    return visitChildren(ctx);
  }
};

#endif  // QLEVER_ACCEPTHEADERQLEVERVISITOR_H
