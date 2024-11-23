// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//          Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once

class SparqlTriple;

namespace parsedQuery {

struct BasicGraphPattern;

class MagicServiceException : public std::runtime_error {
  // Constructors have to be explicitly inherited
  using std::runtime_error::runtime_error;
};

// Abstract structure for parsing a magic SERVICE statement (used to invoke
// QLever-specific features)
struct MagicServiceQuery {
  // The graph pattern inside the SERVICE
  GraphPattern childGraphPattern_;

  MagicServiceQuery() = default;
  MagicServiceQuery(MagicServiceQuery&& other) noexcept = default;
  MagicServiceQuery(const MagicServiceQuery& other) noexcept = default;
  MagicServiceQuery& operator=(const MagicServiceQuery& other) = default;
  MagicServiceQuery& operator=(MagicServiceQuery&& a) noexcept = default;
  virtual ~MagicServiceQuery() = default;

  /**
   * @brief Add a parameter to the query from the given triple.
   * The predicate of the triple determines the parameter name and the object
   * of the triple determines the parameter value. The subject is ignored.
   * Throws an exception if an unsupported algorithm is given or if the
   * predicate contains an unknown parameter name.
   *
   * @param triple A SparqlTriple that contains the parameter info
   */
  virtual void addParameter(const SparqlTriple& triple) = 0;

  /**
   * @brief Add the parameters from a BasicGraphPattern to the query
   *
   * @param pattern
   */
  void addBasicPattern(const BasicGraphPattern& pattern);

  /**
   * @brief Add a GraphPatternOperation to the query.
   *
   * @param childGraphPattern
   */
  void addGraph(const GraphPatternOperation& childGraphPattern);

  // Utility functions
  Variable getVariable(std::string_view parameter,
                       const TripleComponent& object) const;

  void setVariable(std::string_view parameter, const TripleComponent& object,
                   std::optional<Variable>& existingValue);
};

}  // namespace parsedQuery
