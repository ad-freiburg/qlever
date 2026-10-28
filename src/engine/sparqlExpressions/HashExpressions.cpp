// Copyright 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// The `MD5` and `SHA...` expressions. They are split from
// `StringExpressions.cpp` to reduce the memory needed for compiling and
// statically analyzing a single translation unit.

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"
#include "util/CryptographicHashUtils.h"
#include "util/StringUtils.h"

namespace sparqlExpression {
namespace detail::string_expressions {

// HASH
template <typename HashFunc>
struct Hash {
  IdOrLiteralOrIri operator()(std::optional<std::string> input) const {
    if (!input.has_value()) {
      return Id::makeUndefined();
    } else {
      std::vector<unsigned char> hashed = HashFunc{}(input.value());
      auto hexStr = absl::StrJoin(hashed, "", ad_utility::hexFormatter);
      return toLiteral(std::move(hexStr));
    }
  }
};

using MD5Expression = StringExpressionImpl<1, Hash<ad_utility::HashMd5>>;
using SHA1Expression = StringExpressionImpl<1, Hash<ad_utility::HashSha1>>;
using SHA256Expression = StringExpressionImpl<1, Hash<ad_utility::HashSha256>>;
using SHA384Expression = StringExpressionImpl<1, Hash<ad_utility::HashSha384>>;
using SHA512Expression = StringExpressionImpl<1, Hash<ad_utility::HashSha512>>;

}  // namespace detail::string_expressions
using namespace detail::string_expressions;
using Expr = SparqlExpression::Ptr;

Expr makeMD5Expression(Expr child) { return make<MD5Expression>(child); }

Expr makeSHA1Expression(Expr child) { return make<SHA1Expression>(child); }

Expr makeSHA256Expression(Expr child) { return make<SHA256Expression>(child); }

Expr makeSHA384Expression(Expr child) { return make<SHA384Expression>(child); }

Expr makeSHA512Expression(Expr child) { return make<SHA512Expression>(child); }

}  // namespace sparqlExpression
