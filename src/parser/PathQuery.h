// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//          Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_PATHQUERY_H
#define QLEVER_SRC_PARSER_PATHQUERY_H

#include "index/Index.h"
#include "parser/MagicServiceQuery.h"
// TODO<joka921> is this the right header where the pathSearchConfiguration
// should live, or do we need a forward declaration here?
#include "engine/PathSearch.h"

class SparqlTriple;

namespace parsedQuery {

class PathSearchException : public std::runtime_error {
  // Constructors have to be explicitly inherited
  using std::runtime_error::runtime_error;
};

// The PathQuery object holds intermediate information for the PathSearch.
// The PathSearchConfiguration requires concrete Ids. The vocabulary from the
// QueryPlanner is needed to translate the TripleComponents to ValueIds.
// Also, the members of the PathQuery have defaults and can be set after
// the object creation, simplifying the parsing process. If a required
// value has not been set during parsing, the method 'toPathSearchConfiguration'
// will throw an exception.
// All the error handling for the PathSearch happens in the PathQuery object.
// Thus, if a PathSearchConfiguration can be constructed, it is valid.
struct PathQuery : MagicServiceQuery {
  std::vector<TripleComponent> sources_;
  std::vector<TripleComponent> targets_;
  std::optional<Variable> start_;
  std::optional<Variable> end_;
  std::optional<Variable> pathColumn_;
  std::optional<Variable> edgeColumn_;
  std::vector<Variable> edgeProperties_;
  PathSearchAlgorithm algorithm_;

  bool cartesian_ = true;
  std::optional<uint64_t> numPathsPerTarget_ = std::nullopt;

  PathQuery() = default;
  PathQuery(PathQuery&& other) noexcept = default;
  PathQuery(const PathQuery& other) = default;
  PathQuery& operator=(const PathQuery& other) = default;
  PathQuery& operator=(PathQuery&& a) noexcept = default;
  ~PathQuery() noexcept override = default;

  // See MagicServiceQuery
  void addParameter(const SparqlTriple& triple) override;

  /**
   * @brief Convert the vector of triple components into a SearchSide
   * The SeachSide can either be a variable or a list of Ids.
   * A PathSearchException is thrown if more than one variable is given.
   *
   * @param side A vector of TripleComponents, containing either exactly one
   *             Variable or zero or more ValueIds
   * @param vocab A Vocabulary containing the Ids of the TripleComponents.
   *              The Vocab is only used if the given vector contains IRIs.
   */
  std::variant<Variable, std::vector<Id>> toSearchSide(
      std::vector<TripleComponent> side, const Index::Vocab& vocab,
      const EncodedIriManager& encodedIriManager) const;

  /**
   * @brief Convert this PathQuery into a PathSearchConfiguration object.
   * This method checks if all required parameters are set and converts
   * the PathSearch sources and targets into SearchSides.
   * A PathSearchException is thrown if required parameters are missing.
   * The required parameters are start, end, pathColumn and edgeColumn.
   *
   * @param vocab A vocab containing the Ids of the IRIs in
   *              sources_ and targets_
   * @return A valid PathSearchConfiguration
   */
  PathSearchConfiguration toPathSearchConfiguration(
      const Index::Vocab& vocab,
      const EncodedIriManager& encodedIriManager) const;
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_PATHQUERY_H
