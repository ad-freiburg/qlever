// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_PROXYQUERY_H
#define QLEVER_SRC_PARSER_PROXYQUERY_H

#include <string>
#include <vector>

#include "parser/MagicServiceQuery.h"
#include "rdfTypes/Variable.h"

namespace parsedQuery {

class ProxyException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

// Configuration for the magic `SERVICE qlproxy:`. This is the validated
// configuration ready for the engine.
struct ProxyConfiguration {
  // The remote endpoint URL to send requests to.
  std::string endpoint_;

  // Variables whose bindings are sent to the remote endpoint. The
  // `std::string` is the variable name in the sent bindings, the
  // `Variable` is the local variable to get the bindings from.
  std::vector<std::pair<std::string, Variable>> inputVariables_;

  // Variables that receive bindings from the remote. The `std::string` is the
  // variable name in the received bindings, the `Variable` is the local
  // variable to bind the received values to.
  std::vector<std::pair<std::string, Variable>> outputVariables_;

  // The row variable used for joining the proxy response with the child result.
  // The `std::string` is the variable name used in JSON (without "?"), the
  // `Variable` is the local variable.
  std::pair<std::string, Variable> rowVariable_;

  // Parameters of the proxy request, which are sent as URL query parameters.
  std::vector<std::pair<std::string, std::string>> parameters_;
};

// Class for a parsed magic `SERVICE qlproxy:` request. Sends input bindings
// to a remote endpoint and receives output bindings back. For example,
//
//   SERVICE qlproxy: {
//     _:config qlproxy:endpoint <https://example.org/api> ;
//              qlproxy:input-first ?num1 ;
//              qlproxy:input-second ?num2 ;
//              qlproxy:output-result ?result ;
//              qlproxy:output-row ?row ;
//              qlproxy:param-operation "add" .
//   }
//
// This sends bindings for `?num1` as "first" and `?num2` as "second" to the
// given endpoint. The `qlproxy:param-...` values are sent as URL parameters,
// e.g., here `operation=add`. The service expects bindings for "result" in the
// response, which are mapped to `?result`. The `output-row` variable is used
// to join the response back with the input rows.
struct ProxyQuery : MagicServiceQuery {
  // The remote endpoint URL (required).
  std::optional<std::string> endpoint_;

  // Input variables to send to the remote endpoint.
  // Extracted from predicates like qlproxy:input-<name>.
  std::vector<std::pair<std::string, Variable>> inputVariables_;

  // Output variables to receive from the remote endpoint.
  // Extracted from predicates like qlproxy:output-<name>.
  std::vector<std::pair<std::string, Variable>> outputVariables_;

  // The row variable for joining (from qlproxy:output-row).
  std::optional<std::pair<std::string, Variable>> rowVariable_;

  // Static parameters sent as URL query parameters.
  // Extracted from predicates like qlproxy:param-<name>.
  std::vector<std::pair<std::string, std::string>> parameters_;

  ProxyQuery() = default;
  ProxyQuery(ProxyQuery&& other) noexcept = default;
  ProxyQuery(const ProxyQuery& other) = default;
  ProxyQuery& operator=(const ProxyQuery& other) = default;
  ProxyQuery& operator=(ProxyQuery&& other) noexcept = default;
  ~ProxyQuery() noexcept override = default;

  // See `MagicServiceQuery`.
  void addParameter(const SparqlTriple& triple) override;

  // A proxy query does neither support nor need child graph patterns.
  void addGraph(const GraphPatternOperation& childGraphPattern) override;

  // Convert this `ProxyQuery` to a validated `ProxyConfiguration`
  // Throws `ProxyException` if required parameters are missing.
  ProxyConfiguration toConfiguration() const;

  // See `MagicServiceQuery`.
  void validate() const override { toConfiguration(); }

  // See `MagicServiceQuery`.
  constexpr std::string_view name() const override { return "qlproxy"; }

 private:
  void throwIf(bool condition, std::string_view message) const;
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_PROXYQUERY_H
