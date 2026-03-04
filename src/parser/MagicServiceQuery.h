// Copyright 2024 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//          Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_MAGICSERVICEQUERY_H
#define QLEVER_SRC_PARSER_MAGICSERVICEQUERY_H

#include "parser/GraphPattern.h"
#include "parser/TripleComponent.h"
class SparqlTriple;

namespace parsedQuery {

struct BasicGraphPattern;

class MagicServiceException : public std::runtime_error {
  // Constructors have to be explicitly inherited
  using std::runtime_error::runtime_error;
};

// Abstract structure for parsing a magic SERVICE statement (used to invoke
// QLever-specific features like path search or spatial search). We currently
// expect a magic service query to consist of an outer SERVICE operation that
// contains configuration triples directly and none or one group graph pattern.
// Each subclass shall implement the virtual addParameter function to process
// the configuration triples accordingly to the logic of each feature.
struct MagicServiceQuery {
  MagicServiceQuery() = default;
  MagicServiceQuery(MagicServiceQuery&& other) noexcept = default;
  MagicServiceQuery(const MagicServiceQuery& other) = default;
  MagicServiceQuery& operator=(const MagicServiceQuery& other) = default;
  MagicServiceQuery& operator=(MagicServiceQuery&& a) noexcept = default;
  virtual ~MagicServiceQuery() = default;

 public:
  // The optional graph pattern inside the SERVICE
  std::optional<GraphPattern> childGraphPattern_;

  /**
   * @brief Add a parameter to the query from the given triple.
   * The predicate of the triple determines the parameter name and the object
   * of the triple determines the parameter value. The subject is ignored.
   * Throw an exception if an unsupported algorithm is given or if the
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
   * @brief Add a GraphPatternOperation to the query. Can be overridden for
   * example if the concrete service query doesn't support nested group graph
   * patterns.
   *
   * @param childGraphPattern
   */
  virtual void addGraph(const GraphPatternOperation& childGraphPattern);

  // Helper that throws if the current configuration values of this
  // `MagicServiceQuery` is invalid. We need this because `MagicServiceQuery`
  // objects are incrementally constructed by adding configuration triples.
  // Using this function the final state of the object can be checked.
  virtual void validate() const {
      // Currently most `MagicServiceQuery` implementations do not make use of
      // this method. Thus it is empty by default.
  };

  // Helper that returns a readable name for the type of `MagicServiceQuery`.
  virtual constexpr std::string_view name() const = 0;

 protected:
  // Utility functions for variables in the magic service configuration triples
  Variable getVariable(std::string_view parameter,
                       const TripleComponent& object) const;

  void setVariable(std::string_view parameter, const TripleComponent& object,
                   std::optional<Variable>& existingValue) const;

  // Utility function to extract the name of a parameter in a magic service
  // configuration. That is, remove the magic IRI predicate if applicable.
  // Always remove the brackets around the IRI.
  std::string_view extractParameterName(const TripleComponent& tripleComponent,
                                        const std::string_view& magicIRI) const;
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_MAGICSERVICEQUERY_H
