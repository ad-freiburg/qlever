// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "engine/QueryPlanner.h"

#include <absl/strings/str_split.h>

#include <memory>
#include <optional>
#include <type_traits>
#include <variant>

#include "backports/algorithm.h"
#include "engine/Bind.h"
#include "engine/CartesianProductJoin.h"
#include "engine/CheckUsePatternTrick.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/CountConnectedSubgraphs.h"
#include "engine/Describe.h"
#include "engine/Distinct.h"
#include "engine/Filter.h"
#include "engine/GroupBy.h"
#include "engine/HasPredicateScan.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Minus.h"
#include "engine/MultiColumnJoin.h"
#include "engine/NeutralElementOperation.h"
#include "engine/OptionalJoin.h"
#include "engine/OrderBy.h"
#include "engine/PathSearch.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Service.h"
#include "engine/Sort.h"
#include "engine/SpatialJoin.h"
#include "engine/TextIndexScanForEntity.h"
#include "engine/TextIndexScanForWord.h"
#include "engine/TextLimit.h"
#include "engine/TransitivePathBase.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "global/Id.h"
#include "global/RuntimeParameters.h"
#include "parser/Alias.h"
#include "parser/GraphPatternOperation.h"
#include "parser/MagicServiceIriConstants.h"
#include "parser/SparqlParserHelpers.h"
#include "util/Exception.h"

namespace p = parsedQuery;
namespace {

using ad_utility::makeExecutionTree;
using SubtreePlan = QueryPlanner::SubtreePlan;

template <typename Operation>
SubtreePlan makeSubtreePlan(QueryExecutionContext* qec, auto&&... args) {
  return {qec, std::make_shared<Operation>(qec, AD_FWD(args)...)};
}

// Create a `SubtreePlan` that holds the given `operation`. `Op` must be a class
// inheriting from `Operation`.
template <typename Op>
SubtreePlan makeSubtreePlan(std::shared_ptr<Op> operation) {
  auto* qec = operation->getExecutionContext();
  return {qec, std::move(operation)};
}

// Update the `target` query plan such that it knows that it includes all the
// nodes and filters from `a` and `b`. NOTE: This does not actually merge
// the plans from `a` and `b`.
void mergeSubtreePlanIds(SubtreePlan& target, const SubtreePlan& a,
                         const SubtreePlan& b) {
  target._idsOfIncludedNodes = a._idsOfIncludedNodes | b._idsOfIncludedNodes;
  target._idsOfIncludedFilters =
      a._idsOfIncludedFilters | b._idsOfIncludedFilters;
  target.idsOfIncludedTextLimits_ =
      a.idsOfIncludedTextLimits_ | b.idsOfIncludedTextLimits_;
}
}  // namespace

// _____________________________________________________________________________
QueryPlanner::QueryPlanner(QueryExecutionContext* qec,
                           CancellationHandle cancellationHandle)
    : _qec{qec}, cancellationHandle_{std::move(cancellationHandle)} {
  AD_CONTRACT_CHECK(cancellationHandle_);
}

// _____________________________________________________________________________
std::vector<SubtreePlan> QueryPlanner::createExecutionTrees(ParsedQuery& pq,
                                                            bool isSubquery) {
  // Store the dataset clause (FROM and FROM NAMED clauses), s.t. we have access
  // to them down the callstack. Subqueries can't have their own dataset clause,
  // but inherit it from the parent query.
  auto datasetClauseIsEmpty = [](const auto& datasetClause) {
    return !datasetClause.defaultGraphs_.has_value() &&
           !datasetClause.namedGraphs_.has_value();
  };
  if (!isSubquery) {
    AD_CORRECTNESS_CHECK(datasetClauseIsEmpty(activeDatasetClauses_));
    activeDatasetClauses_ = pq.datasetClauses_;
  } else {
    AD_CORRECTNESS_CHECK(datasetClauseIsEmpty(pq.datasetClauses_));
  }

  // Look for ql:has-predicate to determine if the pattern trick should be used.
  // If the pattern trick is used, the ql:has-predicate triple will be removed
  // from the list of where clause triples. Otherwise, the ql:has-predicate
  // triple will be handled using a `HasPredicateScan`.

  using checkUsePatternTrick::PatternTrickTuple;
  const auto patternTrickTuple =
      _enablePatternTrick ? checkUsePatternTrick::checkUsePatternTrick(&pq)
                          : std::nullopt;

  // Do GROUP BY if one of the following applies:
  // 1. There is an explicit group by
  // 2. The pattern trick is applied
  // 3. There is an alias with an aggregate expression
  // TODO<joka921> Non-aggretating aliases (for example (?x AS ?y)) are
  // currently not handled properly. When fixing this you have to distinguish
  // the following two cases:
  // 1. Mix of aggregating and non-aggregating aliases without GROUP BY.
  // 2. Only non-aggretating aliases without GROUP BY.
  // Note: When a GROUP BY is present, then all aliases have to be aggregating,
  // this is handled correctly in all cases.
  bool doGroupBy = !pq._groupByVariables.empty() ||
                   patternTrickTuple.has_value() ||
                   ql::ranges::any_of(pq.getAliases(), [](const Alias& alias) {
                     return alias._expression.containsAggregate();
                   });

  // Set TEXTLIMIT
  textLimit_ = pq._limitOffset.textLimit_;

  // Optimize the graph pattern tree
  std::vector<std::vector<SubtreePlan>> plans;
  plans.push_back(optimize(&pq._rootGraphPattern));
  checkCancellation();

  // Add the query level modifications

  // GROUP BY (Either the pattern trick or a "normal" GROUP BY)
  if (patternTrickTuple.has_value()) {
    plans.emplace_back(getPatternTrickRow(pq.selectClause(), plans,
                                          patternTrickTuple.value()));
  } else if (doGroupBy) {
    plans.emplace_back(getGroupByRow(pq, plans));
  }
  checkCancellation();

  // HAVING
  if (!pq._havingClauses.empty()) {
    plans.emplace_back(getHavingRow(pq, plans));
    checkCancellation();
  }

  // DISTINCT
  if (pq.hasSelectClause()) {
    const auto& selectClause = pq.selectClause();
    if (selectClause.distinct_) {
      plans.emplace_back(getDistinctRow(selectClause, plans));
      checkCancellation();
    }
  }

  // ORDER BY
  if (!pq._orderBy.empty()) {
    // If there is an order by clause, add another row to the table and
    // just add an order by / sort to every previous result if needed.
    // If the ordering is perfect already, just copy the plan.
    plans.emplace_back(getOrderByRow(pq, plans));
    checkCancellation();
  }

  // Now find the cheapest execution plan and store that as the optimal
  // plan for this graph pattern.
  vector<SubtreePlan>& lastRow = plans.back();

  for (auto& plan : lastRow) {
    if (plan._qet->getRootOperation()->supportsLimit()) {
      plan._qet->setLimit(pq._limitOffset);
    }
  }

  AD_CONTRACT_CHECK(!lastRow.empty());
  if (pq._rootGraphPattern._optional) {
    for (auto& plan : lastRow) {
      plan.type = SubtreePlan::OPTIONAL;
    }
  }

  checkCancellation();

  for (const auto& warning : pq.warnings()) {
    warnings_.push_back(warning);
  }
  return lastRow;
}

// _____________________________________________________________________________
QueryExecutionTree QueryPlanner::createExecutionTree(ParsedQuery& pq,
                                                     bool isSubquery) {
  try {
    auto lastRow = createExecutionTrees(pq, isSubquery);
    auto minInd = findCheapestExecutionTree(lastRow);
    LOG(DEBUG) << "Done creating execution plan" << std::endl;
    auto result = std::move(*lastRow[minInd]._qet);
    auto& rootOperation = *result.getRootOperation();
    // Collect all the warnings and pass them to the created tree such that
    // they become visible to the user once the query is executed.
    for (const auto& warning : warnings_) {
      rootOperation.addWarning(warning);
    }
    warnings_.clear();
    return result;
  } catch (ad_utility::CancellationException& e) {
    e.setOperation("Query planning");
    throw;
  }
}

// _____________________________________________________________________________
std::vector<SubtreePlan> QueryPlanner::optimize(
    ParsedQuery::GraphPattern* rootPattern) {
  QueryPlanner::GraphPatternPlanner optimizer{*this, rootPattern};
  for (auto& child : rootPattern->_graphPatterns) {
    child.visit([&optimizer](auto& arg) {
      return optimizer.graphPatternOperationVisitor(arg);
    });
    checkCancellation();
  }
  // one last pass in case the last one was not an optional
  // if the last child was not an optional clause we still have unjoined
  // candidates. Do one last pass over them.
  optimizer.optimizeCommutatively();
  auto& candidatePlans = optimizer.candidatePlans_;

  // it might be, that we have not yet applied all the filters
  // (it might be, that the last join was optional and introduced new variables)
  if (!candidatePlans.empty()) {
    applyFiltersIfPossible<true>(candidatePlans[0], rootPattern->_filters);
    applyTextLimitsIfPossible(candidatePlans[0],
                              TextLimitVec{rootPattern->textLimits_.begin(),
                                           rootPattern->textLimits_.end()},
                              true);
    checkCancellation();
  }

  AD_CONTRACT_CHECK(candidatePlans.size() == 1 || candidatePlans.empty());
  if (candidatePlans.empty()) {
    // this case is needed e.g. if we have the empty graph pattern due to a
    // pattern trick
    return std::vector<SubtreePlan>{};
  } else {
    if (candidatePlans.at(0).empty()) {
      // This happens if either graph pattern is an empty group,
      // or it only consists of a MINUS clause (which then has no effect).
      std::vector neutralPlans{makeSubtreePlan<NeutralElementOperation>(_qec)};
      // Neutral element can potentially still get filtered out
      applyFiltersIfPossible<true>(neutralPlans, rootPattern->_filters);
      return neutralPlans;
    }
    return candidatePlans[0];
  }
}

// _____________________________________________________________________________
vector<SubtreePlan> QueryPlanner::getDistinctRow(
    const p::SelectClause& selectClause,
    const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (const auto& parent : previous) {
    SubtreePlan distinctPlan(_qec);
    vector<ColumnIndex> keepIndices;
    ad_utility::HashSet<ColumnIndex> indDone;
    const auto& colMap = parent._qet->getVariableColumns();
    for (const auto& var : selectClause.getSelectedVariables()) {
      // There used to be a special treatment for `?ql_textscore_` variables
      // which was considered a bug.
      if (auto it = colMap.find(var); it != colMap.end()) {
        auto ind = it->second.columnIndex_;
        if (indDone.count(ind) == 0) {
          keepIndices.push_back(ind);
          indDone.insert(ind);
        }
      }
    }
    const std::vector<ColumnIndex>& resultSortedOn =
        parent._qet->getRootOperation()->getResultSortedOn();
    // check if the current result is sorted on all columns of the distinct
    // with the order of the sorting
    bool isSorted = resultSortedOn.size() >= keepIndices.size();
    for (size_t i = 0; isSorted && i < keepIndices.size(); i++) {
      isSorted = isSorted && resultSortedOn[i] == keepIndices[i];
    }
    if (isSorted) {
      distinctPlan._qet =
          makeExecutionTree<Distinct>(_qec, parent._qet, keepIndices);
    } else {
      auto tree = makeExecutionTree<Sort>(_qec, parent._qet, keepIndices);
      distinctPlan._qet = makeExecutionTree<Distinct>(_qec, tree, keepIndices);
    }
    added.push_back(distinctPlan);
  }
  return added;
}

// _____________________________________________________________________________
vector<SubtreePlan> QueryPlanner::getPatternTrickRow(
    const p::SelectClause& selectClause,
    const vector<vector<SubtreePlan>>& dpTab,
    const checkUsePatternTrick::PatternTrickTuple& patternTrickTuple) {
  AD_CORRECTNESS_CHECK(!dpTab.empty());
  const vector<SubtreePlan>& previous = dpTab.back();
  auto aliases = selectClause.getAliases();

  vector<SubtreePlan> added;

  Variable predicateVariable = patternTrickTuple.predicate_;
  Variable countVariable =
      aliases.empty() ? generateUniqueVarName() : aliases[0]._target;
  // Pattern tricks always contain at least one triple, otherwise something
  // has gone wrong inside the `CheckUsePatternTrick` module.
  AD_CORRECTNESS_CHECK(!previous.empty());
  added.reserve(previous.size());
  for (const auto& parent : previous) {
    // Determine the column containing the subjects for which we are
    // interested in their predicates.
    // TODO<joka921> Move this lookup from subjects to columns
    // into the `CountAvailablePredicates` class where it belongs
    auto subjectColumn =
        parent._qet->getVariableColumn(patternTrickTuple.subject_);
    added.push_back(makeSubtreePlan<CountAvailablePredicates>(
        _qec, parent._qet, subjectColumn, predicateVariable, countVariable));
  }
  return added;
}

// _____________________________________________________________________________
vector<SubtreePlan> QueryPlanner::getHavingRow(
    const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (const auto& parent : previous) {
    SubtreePlan filtered = parent;
    for (const SparqlFilter& filter : pq._havingClauses) {
      filtered =
          makeSubtreePlan<Filter>(_qec, filtered._qet, filter.expression_);
    }
    added.push_back(std::move(filtered));
  }
  return added;
}

// _____________________________________________________________________________
vector<SubtreePlan> QueryPlanner::getGroupByRow(
    const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (auto& parent : previous) {
    // Create a group by operation to determine on which columns the input
    // needs to be sorted
    SubtreePlan groupByPlan(_qec);
    groupByPlan._idsOfIncludedNodes = parent._idsOfIncludedNodes;
    groupByPlan._idsOfIncludedFilters = parent._idsOfIncludedFilters;
    groupByPlan.idsOfIncludedTextLimits_ = parent.idsOfIncludedTextLimits_;
    std::vector<Alias> aliases;
    if (pq.hasSelectClause()) {
      aliases = pq.selectClause().getAliases();
    }

    // Inside a `GRAPH ?var {....}` clause,  a `GROUP BY` must implicitly (also)
    // group by the graph variable.
    auto groupVariables = pq._groupByVariables;
    if (activeGraphVariable_.has_value()) {
      AD_CORRECTNESS_CHECK(
          !ad_utility::contains(groupVariables, activeGraphVariable_.value()),
          "Graph variable used inside the GRAPH clause, this "
          "should have thrown an exception earlier");
      groupVariables.push_back(activeGraphVariable_.value());
    }
    groupByPlan._qet = makeExecutionTree<GroupBy>(
        _qec, groupVariables, std::move(aliases), parent._qet);
    added.push_back(groupByPlan);
  }
  return added;
}

// _____________________________________________________________________________
vector<SubtreePlan> QueryPlanner::getOrderByRow(
    const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const {
  const vector<SubtreePlan>& previous = dpTab[dpTab.size() - 1];
  vector<SubtreePlan> added;
  added.reserve(previous.size());
  for (const auto& parent : previous) {
    SubtreePlan plan(_qec);
    auto& tree = plan._qet;
    plan._idsOfIncludedNodes = parent._idsOfIncludedNodes;
    plan._idsOfIncludedFilters = parent._idsOfIncludedFilters;
    plan.idsOfIncludedTextLimits_ = parent.idsOfIncludedTextLimits_;
    vector<pair<ColumnIndex, bool>> sortIndices;
    // Collect the variables of the ORDER BY or INTERNAL SORT BY clause. Ignore
    // variables that are not visible in the query body (according to the
    // SPARQL standard, they are allowed but have no effect).
    for (auto& ord : pq._orderBy) {
      auto idx = parent._qet->getVariableColumnOrNullopt(ord.variable_);
      if (!idx.has_value()) {
        continue;
      }
      sortIndices.emplace_back(idx.value(), ord.isDescending_);
    }

    // If none of these variables was bound, we can omit the whole ORDER BY
    // or INTERNAL SORT BY clause.
    if (sortIndices.empty()) {
      return previous;
    }

    if (pq._isInternalSort == IsInternalSort::True) {
      std::vector<ColumnIndex> sortColumns;
      for (auto& [index, isDescending] : sortIndices) {
        AD_CONTRACT_CHECK(!isDescending);
        sortColumns.push_back(index);
      }
      tree = QueryExecutionTree::createSortedTree(parent._qet, sortColumns);
    } else {
      AD_CONTRACT_CHECK(pq._isInternalSort == IsInternalSort::False);
      // Note: As the internal ordering is different from the semantic ordering
      // needed by `OrderBy`, we always have to instantiate the `OrderBy`
      // operation.
      tree = makeExecutionTree<OrderBy>(_qec, parent._qet, sortIndices);
    }
    added.push_back(plan);
  }
  return added;
}

// _____________________________________________________________________________
void QueryPlanner::addNodeToTripleGraph(const TripleGraph::Node& node,
                                        QueryPlanner::TripleGraph& tg) const {
  // TODO<joka921> This needs quite some refactoring: The IDs of the nodes have
  // to be ascending as an invariant, so we can store all the nodes in a
  // vector<unique_ptr> or even a plain vector.
  tg._nodeStorage.emplace_back(node);
  auto& addedNode = tg._nodeStorage.back();
  tg._nodeMap[addedNode.id_] = &addedNode;
  tg._adjLists.emplace_back();
  AD_CORRECTNESS_CHECK(tg._adjLists.size() == tg._nodeStorage.size());
  AD_CORRECTNESS_CHECK(tg._adjLists.size() == addedNode.id_ + 1);
  // Now add an edge between the added node and every node sharing a var.
  for (auto& addedNodevar : addedNode._variables) {
    for (size_t i = 0; i < addedNode.id_; ++i) {
      auto& otherNode = *tg._nodeMap[i];
      if (otherNode._variables.contains(addedNodevar)) {
        // There is an edge between *it->second and the node with id "id".
        tg._adjLists[addedNode.id_].push_back(otherNode.id_);
        tg._adjLists[otherNode.id_].push_back(addedNode.id_);
      }
    }
  }
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph QueryPlanner::createTripleGraph(
    const p::BasicGraphPattern* pattern) const {
  TripleGraph tg;
  size_t numNodesInTripleGraph = 0;
  ad_utility::HashMap<Variable, string> optTermForCvar;
  ad_utility::HashMap<Variable, vector<string>> potentialTermsForCvar;
  vector<const SparqlTriple*> entityTriples;
  // Add one or more nodes for each triple.
  for (auto& t : pattern->_triples) {
    if (t.p_.iri_ == CONTAINS_WORD_PREDICATE) {
      std::string buffer = t.o_.toString();
      std::string_view sv{buffer};
      // Add one node for each word
      for (const auto& term :
           absl::StrSplit(sv.substr(1, sv.size() - 2), ' ')) {
        std::string s{ad_utility::utf8ToLower(term)};
        potentialTermsForCvar[t.s_.getVariable()].push_back(s);
        if (activeGraphVariable_.has_value() ||
            activeDatasetClauses_.defaultGraphs_.has_value()) {
          AD_THROW(
              "contains-word is not allowed inside GRAPH clauses or in queries "
              "with FROM/FROM NAMED clauses.");
        }
        addNodeToTripleGraph(
            TripleGraph::Node{tg._nodeStorage.size(), t.s_.getVariable(), s, t},
            tg);
        numNodesInTripleGraph++;
      }
    } else if (t.p_.iri_ == CONTAINS_ENTITY_PREDICATE) {
      entityTriples.push_back(&t);
    } else {
      addNodeToTripleGraph(
          TripleGraph::Node{tg._nodeStorage.size(), t, activeGraphVariable_},
          tg);
      numNodesInTripleGraph++;
    }
  }
  for (const auto& [cvar, terms] : potentialTermsForCvar) {
    optTermForCvar[cvar] =
        terms[_qec->getIndex().getIndexOfBestSuitedElTerm(terms)];
  }
  for (const SparqlTriple* t : entityTriples) {
    Variable currentVar = t->s_.getVariable();
    if (!optTermForCvar.contains(currentVar)) {
      AD_THROW(
          "Missing ql:contains-word statement. A ql:contains-entity "
          "statement always also needs corresponding ql:contains-word "
          "statement.");
    }
    addNodeToTripleGraph(TripleGraph::Node(tg._nodeStorage.size(), currentVar,
                                           optTermForCvar[currentVar], *t),
                         tg);
    numNodesInTripleGraph++;
  }
  if (numNodesInTripleGraph > 64) {
    AD_THROW("At most 64 triples allowed at the moment.");
  }
  return tg;
}

namespace {
// A `TriplePosition` is a function that takes a triple and returns a
// `TripleComponent`, typically the subject, predicate, or object of the triple,
// hence the name.
template <typename Function>
CPP_concept TriplePosition =
    ad_utility::InvocableWithExactReturnType<Function, TripleComponent&,
                                             SparqlTripleSimple&>;

// Create a `SparqlFilter` that corresponds to the expression `var1==var2`.
// Used as a helper function below.
SparqlFilter createEqualFilter(const Variable& var1, const Variable& var2) {
  std::string filterString =
      absl::StrCat("FILTER ( ", var1.name(), "=", var2.name(), ")");
  return sparqlParserHelpers::ParserAndVisitor{filterString}
      .parseTypesafe(&SparqlAutomaticParser::filterR)
      .resultOfParse_;
};

// Helper function for `handleRepeatedVariables` below. Replace a single
// position of the `scanTriple`, denoted by the `rewritePosition` by a new
// variable, and add a filter, that checks the old and the new value for
// equality.
constexpr auto rewriteSingle = CPP_template_lambda()(typename T)(
    T rewritePosition, SparqlTripleSimple& scanTriple, const auto& addFilter,
    const auto& generateUniqueVarName)(requires TriplePosition<T>) {
  Variable filterVar = generateUniqueVarName();
  auto& target = std::invoke(rewritePosition, scanTriple).getVariable();
  addFilter(createEqualFilter(filterVar, target));
  target = filterVar;
};

// Replace the positions of the `triple` that are specified by the
// `rewritePositions` with a new variable, and add a filter, which checks the
// old and the new value for equality for each of these rewrites. Then also
// add an index scan for the rewritten triple.
constexpr auto handleRepeatedVariablesImpl =
    []<typename... T>(const auto& triple, auto& addIndexScan,
                      const auto& generateUniqueVarName, const auto& addFilter,
                      std::span<const Permutation::Enum> permutations,
                      T... rewritePositions)
    -> CPP_ret(void)(requires(TriplePosition<T>&&...)) {
  auto scanTriple = triple;
  (..., rewriteSingle(rewritePositions, scanTriple, addFilter,
                      generateUniqueVarName));
  for (const auto& permutation : permutations) {
    addIndexScan(permutation, scanTriple);
  }
};

}  // namespace

// _____________________________________________________________________________
template <typename AddedIndexScanFunction>
void QueryPlanner::indexScanSingleVarCase(
    const SparqlTripleSimple& triple,
    const AddedIndexScanFunction& addIndexScan) const {
  using enum Permutation::Enum;

  if (isVariable(triple.s_)) {
    addIndexScan(POS);
  } else if (isVariable(triple.p_)) {
    addIndexScan(SOP);
  } else {
    addIndexScan(PSO);
  }
}

// _____________________________________________________________________________
template <typename AddedIndexScanFunction>
void QueryPlanner::indexScanTwoVarsCase(
    const SparqlTripleSimple& triple,
    const AddedIndexScanFunction& addIndexScan, const auto& addFilter) {
  using enum Permutation::Enum;

  // Replace the position of the `triple` that is specified by the
  // `rewritePosition` with a new variable, and add a filter, that checks the
  // old and the new value for equality for this rewrite. Then also
  // add an index scan for the rewritten triple.
  auto generate = [this]() { return generateUniqueVarName(); };
  auto handleRepeatedVariables =
      [&triple, &addIndexScan, &addFilter, &generate]<typename... T>(
          std::span<const Permutation::Enum> permutations,
          T... rewritePositions)
      -> CPP_ret(void)(requires(TriplePosition<T>&&...)) {
    return handleRepeatedVariablesImpl(triple, addIndexScan, generate,
                                       addFilter, permutations,
                                       rewritePositions...);
  };

  const auto& [s, p, o, _] = triple;

  using Tr = SparqlTripleSimple;
  if (!isVariable(s)) {
    if (p == o) {
      handleRepeatedVariables({{SPO}}, &Tr::o_);
    } else {
      addIndexScan(SPO);
      addIndexScan(SOP);
    }
  } else if (!isVariable(p)) {
    if (s == o) {
      handleRepeatedVariables({{PSO}}, &Tr::o_);
    } else {
      addIndexScan(PSO);
      addIndexScan(POS);
    }
  } else {
    AD_CORRECTNESS_CHECK(!isVariable(o));
    if (s == p) {
      handleRepeatedVariables({{OPS}}, &Tr::s_);
    } else {
      addIndexScan(OSP);
      addIndexScan(OPS);
    }
  }
}

// _____________________________________________________________________________
template <typename AddedIndexScanFunction>
void QueryPlanner::indexScanThreeVarsCase(
    const SparqlTripleSimple& triple,
    const AddedIndexScanFunction& addIndexScan, const auto& addFilter) {
  using enum Permutation::Enum;
  AD_CONTRACT_CHECK(!_qec || _qec->getIndex().hasAllPermutations(),
                    "With only 2 permutations registered (no -a option), "
                    "triples should have at most two variables.");
  auto generate = [this]() { return generateUniqueVarName(); };

  // Replace the position of the `triple` that is specified by the
  // `rewritePosition` with a new variable, and add a filter, that checks the
  // old and the new value for equality for this rewrite. Then also
  // add an index scan for the rewritten triple.
  auto handleRepeatedVariables =
      [&triple, &addIndexScan, &addFilter, &generate]<typename... T>(
          std::span<const Permutation::Enum> permutations,
          T... rewritePositions)
      -> CPP_ret(void)(requires(TriplePosition<T>&&...)) {
    return handleRepeatedVariablesImpl(triple, addIndexScan, generate,
                                       addFilter, permutations,
                                       rewritePositions...);
  };

  using Tr = SparqlTripleSimple;
  const auto& [s, p, o, _] = triple;

  if (s == o) {
    if (s == p) {
      handleRepeatedVariables({{PSO}}, &Tr::o_, &Tr::s_);
    } else {
      handleRepeatedVariables({{POS, OPS}}, &Tr::s_);
    }
  } else if (s == p) {
    handleRepeatedVariables({{OPS, POS}}, &Tr::s_);
  } else if (o == p) {
    handleRepeatedVariables({{PSO, SPO}}, &Tr::o_);
  } else {
    // Three distinct variables
    // Add plans for all six permutations.
    addIndexScan(OPS);
    addIndexScan(OSP);
    addIndexScan(PSO);
    addIndexScan(POS);
    addIndexScan(SPO);
    addIndexScan(SOP);
  }
}

// _____________________________________________________________________________
template <typename AddedIndexScanFunction, typename AddFilter>
void QueryPlanner::seedFromOrdinaryTriple(
    const TripleGraph::Node& node, const AddedIndexScanFunction& addIndexScan,
    const AddFilter& addFilter) {
  auto triple = node.triple_.getSimple();
  const size_t numVars = static_cast<size_t>(isVariable(triple.s_)) +
                         static_cast<size_t>(isVariable(triple.p_)) +
                         static_cast<size_t>(isVariable(triple.o_));
  if (numVars == 0) {
    // We could read this from any of the permutations.
    addIndexScan(Permutation::Enum::PSO);
  } else if (numVars == 1) {
    indexScanSingleVarCase(triple, addIndexScan);
  } else if (numVars == 2) {
    indexScanTwoVarsCase(triple, addIndexScan, addFilter);
  } else {
    AD_CORRECTNESS_CHECK(numVars == 3);
    indexScanThreeVarsCase(triple, addIndexScan, addFilter);
  }
}

// _____________________________________________________________________________
auto QueryPlanner::seedWithScansAndText(
    const QueryPlanner::TripleGraph& tg,
    const vector<vector<SubtreePlan>>& children, TextLimitMap& textLimits)
    -> PlansAndFilters {
  PlansAndFilters result;
  vector<SubtreePlan>& seeds = result.plans_;
  // add all child plans as seeds
  uint64_t idShift = tg._nodeMap.size();
  for (const auto& vec : children) {
    AD_CONTRACT_CHECK(
        idShift < 64,
        absl::StrCat("Group graph pattern too large: QLever currently supports "
                     "at most 64 elements (like triples), but found ",
                     idShift));
    for (const SubtreePlan& plan : vec) {
      SubtreePlan newIdPlan = plan;
      // give the plan a unique id bit
      newIdPlan._idsOfIncludedNodes = uint64_t(1) << idShift;
      newIdPlan._idsOfIncludedFilters = 0;
      newIdPlan.idsOfIncludedTextLimits_ = 0;
      seeds.emplace_back(newIdPlan);
    }
    idShift++;
  }

  for (size_t i = 0; i < tg._nodeMap.size(); ++i) {
    const TripleGraph::Node& node = *tg._nodeMap.find(i)->second;

    auto pushPlan = [&seeds, i](SubtreePlan plan) {
      plan._idsOfIncludedNodes = (uint64_t(1) << i);
      seeds.push_back(std::move(plan));
    };

    using enum Permutation::Enum;

    if (node.isTextNode()) {
      seeds.push_back(getTextLeafPlan(node, textLimits));
      continue;
    }

    // Property paths must have been handled previously.
    AD_CORRECTNESS_CHECK(node.triple_.p_.operation_ ==
                         PropertyPath::Operation::IRI);
    // At this point, we know that the predicate is a simple IRI or a variable.

    if (_qec && !_qec->getIndex().hasAllPermutations() &&
        isVariable(node.triple_.p_.iri_)) {
      AD_THROW(
          "The query contains a predicate variable, but only the PSO "
          "and POS permutations were loaded. Rerun the server without "
          "the option --only-pso-and-pos-permutations and if "
          "necessary also rebuild the index.");
    }

    // Backward compatibility with spatial search predicates
    const auto& input = node.triple_.p_.iri_;
    if ((input.starts_with(MAX_DIST_IN_METERS) ||
         input.starts_with(NEAREST_NEIGHBORS)) &&
        input.ends_with('>')) {
      parsedQuery::SpatialQuery config{node.triple_};
      auto plan = makeSubtreePlan<SpatialJoin>(
          _qec, config.toSpatialJoinConfiguration(), std::nullopt,
          std::nullopt);
      if (input.starts_with(NEAREST_NEIGHBORS)) {
        plan._qet->getRootOperation()->addWarning(absl::StrCat(
            "The special predicate <nearest-neighbors:...> is deprecated due "
            "to confusing semantics. Please upgrade your query to the new "
            "syntax 'SERVICE ",
            SPATIAL_SEARCH_IRI,
            " { ... }'. For more information, please see the QLever Wiki."));
      }
      pushPlan(plan);
      continue;
    }

    if (node.triple_.p_.iri_ == HAS_PREDICATE_PREDICATE) {
      pushPlan(makeSubtreePlan<HasPredicateScan>(_qec, node.triple_));
      continue;
    }

    auto addIndexScan =
        [this, pushPlan, node,
         &relevantGraphs = activeDatasetClauses_.defaultGraphs_](
            Permutation::Enum permutation,
            std::optional<SparqlTripleSimple> triple = std::nullopt) {
          if (!triple.has_value()) {
            triple = node.triple_.getSimple();
          }

          // We are inside a `GRAPH ?var {...}` clause, so all index scans have
          // to add the graph variable as an additional column.
          auto& additionalColumns = triple.value().additionalScanColumns_;
          AD_CORRECTNESS_CHECK(!ad_utility::contains(
              additionalColumns | ql::views::keys, ADDITIONAL_COLUMN_GRAPH_ID));
          if (activeGraphVariable_.has_value()) {
            additionalColumns.emplace_back(ADDITIONAL_COLUMN_GRAPH_ID,
                                           activeGraphVariable_.value());
          }

          // TODO<joka921> Handle the case, that the Graph variable is also used
          // inside the `GRAPH` clause, e.g. by being used inside a triple.

          pushPlan(makeSubtreePlan<IndexScan>(
              _qec, permutation, std::move(triple.value()), relevantGraphs));
        };

    auto addFilter = [&filters = result.filters_](SparqlFilter filter) {
      filters.push_back(std::move(filter));
    };
    seedFromOrdinaryTriple(node, addIndexScan, addFilter);
  }

  // If there is no score variable, there is no ql:contains-entity for this text
  // variable, so we don't need a text limit and we can delete the object
  vector<Variable> toDelete;
  for (const auto& [textVar, textLimitMetaObject] : textLimits) {
    if (textLimitMetaObject.scoreVars_.empty()) {
      toDelete.push_back(textVar);
    }
  }
  for (const auto& var : toDelete) {
    textLimits.erase(var);
  }

  return result;
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromPropertyPath(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  switch (path.operation_) {
    case PropertyPath::Operation::ALTERNATIVE:
      return seedFromAlternative(left, path, right);
    case PropertyPath::Operation::INVERSE:
      return seedFromInverse(left, path, right);
    case PropertyPath::Operation::IRI:
      return seedFromIri(left, path, right);
    case PropertyPath::Operation::SEQUENCE:
      return seedFromSequence(left, path, right);
    case PropertyPath::Operation::ZERO_OR_MORE:
      return seedFromTransitive(left, path, right, 0,
                                std::numeric_limits<size_t>::max());
    case PropertyPath::Operation::ONE_OR_MORE:
      return seedFromTransitive(left, path, right, 1,
                                std::numeric_limits<size_t>::max());
    case PropertyPath::Operation::ZERO_OR_ONE:
      return seedFromTransitive(left, path, right, 0, 1);
  }
  AD_FAIL();
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromSequence(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  AD_CORRECTNESS_CHECK(path.children_.size() > 1);

  ParsedQuery::GraphPattern joinPattern{};
  TripleComponent innerLeft = left;
  TripleComponent innerRight = generateUniqueVarName();
  for (size_t i = 0; i < path.children_.size(); i++) {
    auto child = path.children_[i];

    if (i == path.children_.size() - 1) {
      innerRight = right;
    }

    auto pattern = seedFromPropertyPath(innerLeft, child, innerRight);
    joinPattern._graphPatterns.insert(joinPattern._graphPatterns.end(),
                                      pattern._graphPatterns.begin(),
                                      pattern._graphPatterns.end());
    innerLeft = innerRight;
    innerRight = generateUniqueVarName();
  }

  return joinPattern;
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromAlternative(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  if (path.children_.empty()) {
    AD_THROW(
        "Tried processing an alternative property path node without any "
        "children.");
  } else if (path.children_.size() == 1) {
    LOG(WARN)
        << "Processing an alternative property path that has only one child."
        << std::endl;
    return seedFromPropertyPath(left, path, right);
  }

  std::vector<ParsedQuery::GraphPattern> childPlans;
  childPlans.reserve(path.children_.size());
  for (const auto& child : path.children_) {
    childPlans.push_back(seedFromPropertyPath(left, child, right));
  }
  return uniteGraphPatterns(std::move(childPlans));
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromTransitive(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right, size_t min, size_t max) {
  Variable innerLeft = generateUniqueVarName();
  Variable innerRight = generateUniqueVarName();
  ParsedQuery::GraphPattern childPlan =
      seedFromPropertyPath(innerLeft, path.children_[0], innerRight);
  ParsedQuery::GraphPattern p{};
  p::TransPath transPath;
  transPath._left = left;
  transPath._right = right;
  transPath._innerLeft = innerLeft;
  transPath._innerRight = innerRight;
  transPath._min = min;
  transPath._max = max;
  transPath._childGraphPattern = std::move(childPlan);
  p._graphPatterns.emplace_back(std::move(transPath));
  return p;
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromInverse(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  return seedFromPropertyPath(right, path.children_[0], left);
}

// _____________________________________________________________________________
ParsedQuery::GraphPattern QueryPlanner::seedFromIri(
    const TripleComponent& left, const PropertyPath& path,
    const TripleComponent& right) {
  ParsedQuery::GraphPattern p{};
  p::BasicGraphPattern basic;
  basic._triples.push_back(SparqlTriple(left, path, right));
  p._graphPatterns.emplace_back(std::move(basic));

  return p;
}

ParsedQuery::GraphPattern QueryPlanner::uniteGraphPatterns(
    std::vector<ParsedQuery::GraphPattern>&& patterns) {
  using GraphPattern = ParsedQuery::GraphPattern;
  // Build a tree of union operations
  auto p = GraphPattern{};
  p._graphPatterns.emplace_back(
      p::Union{std::move(patterns[0]), std::move(patterns[1])});

  for (size_t i = 2; i < patterns.size(); i++) {
    GraphPattern next;
    next._graphPatterns.emplace_back(
        p::Union{std::move(p), std::move(patterns[i])});
    p = std::move(next);
  }
  return p;
}

// _____________________________________________________________________________
Variable QueryPlanner::generateUniqueVarName() {
  return Variable{absl::StrCat(QLEVER_INTERNAL_VARIABLE_QUERY_PLANNER_PREFIX,
                               _internalVarCount++)};
}

// _____________________________________________________________________________
SubtreePlan QueryPlanner::getTextLeafPlan(
    const QueryPlanner::TripleGraph::Node& node,
    TextLimitMap& textLimits) const {
  AD_CONTRACT_CHECK(node.wordPart_.has_value());
  string word = node.wordPart_.value();
  SubtreePlan plan(_qec);
  const auto& cvar = node.cvar_.value();
  if (!textLimits.contains(cvar)) {
    textLimits[cvar] = parsedQuery::TextLimitMetaObject{{}, {}, 0};
  }
  if (node.triple_.p_.iri_ == CONTAINS_ENTITY_PREDICATE) {
    if (node._variables.size() == 2) {
      // TODO<joka921>: This is not nice, refactor the whole TripleGraph class
      // to make these checks more explicitly.
      Variable evar = *(node._variables.begin()) == cvar
                          ? *(++node._variables.begin())
                          : *(node._variables.begin());
      plan = makeSubtreePlan<TextIndexScanForEntity>(_qec, cvar, evar, word);
      textLimits[cvar].entityVars_.push_back(evar);
      textLimits[cvar].scoreVars_.push_back(cvar.getEntityScoreVariable(evar));
    } else {
      // Fixed entity case
      AD_CORRECTNESS_CHECK(node._variables.size() == 1);
      plan = makeSubtreePlan<TextIndexScanForEntity>(
          _qec, cvar, node.triple_.o_.toString(), word);
      textLimits[cvar].scoreVars_.push_back(
          cvar.getEntityScoreVariable(node.triple_.o_.toString()));
    }
  } else {
    plan = makeSubtreePlan<TextIndexScanForWord>(_qec, cvar, word);
  }
  textLimits[cvar].idsOfMustBeFinishedOperations_ |= (size_t(1) << node.id_);
  plan._idsOfIncludedNodes |= (size_t(1) << node.id_);
  return plan;
}

// _____________________________________________________________________________
vector<SubtreePlan> QueryPlanner::merge(
    const vector<SubtreePlan>& a, const vector<SubtreePlan>& b,
    const QueryPlanner::TripleGraph& tg) const {
  // TODO: Add the following features:
  // If a join is supposed to happen, always check if it happens between
  // a scan with a relatively large result size
  // esp. with an entire relation but also with something like is-a Person
  // If that is the case look at the size estimate for the other side,
  // if that is rather small, replace the join and scan by a combination.
  ad_utility::HashMap<string, vector<SubtreePlan>> candidates;
  // Find all pairs between a and b that are connected by an edge.
  LOG(TRACE) << "Considering joins that merge " << a.size() << " and "
             << b.size() << " plans...\n";
  for (const auto& ai : a) {
    for (const auto& bj : b) {
      for (auto& plan : createJoinCandidates(ai, bj, tg)) {
        candidates[getPruningKey(plan, plan._qet->resultSortedOn())]
            .emplace_back(std::move(plan));
      }
      checkCancellation();
    }
  }

  // Duplicates are removed if the same triples are touched,
  // the ordering is the same. Only the best is kept then.

  // Therefore we mapped plans and use contained triples + ordering var
  // as key.
  LOG(TRACE) << "Pruning...\n";
  vector<SubtreePlan> prunedPlans;

  auto pruneCandidates = [&](auto& actualCandidates) {
    for (auto& [key, value] : actualCandidates) {
      (void)key;  // silence unused warning
      size_t minIndex = findCheapestExecutionTree(value);
      prunedPlans.push_back(std::move(value[minIndex]));
      checkCancellation();
    }
  };

  if (isInTestMode()) {
    std::vector<std::pair<string, vector<SubtreePlan>>> sortedCandidates{
        std::make_move_iterator(candidates.begin()),
        std::make_move_iterator(candidates.end())};
    std::sort(sortedCandidates.begin(), sortedCandidates.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    pruneCandidates(sortedCandidates);
  } else {
    pruneCandidates(candidates);
  }

  LOG(TRACE) << "Got " << prunedPlans.size() << " pruned plans from \n";
  return prunedPlans;
}

// _____________________________________________________________________________
string QueryPlanner::TripleGraph::asString() const {
  std::ostringstream os;
  for (size_t i = 0; i < _adjLists.size(); ++i) {
    if (!_nodeMap.find(i)->second->cvar_.has_value()) {
      os << i << " " << _nodeMap.find(i)->second->triple_.asString() << " : (";
    } else {
      os << i << " {TextOP for "
         << _nodeMap.find(i)->second->cvar_.value().name() << ", wordPart: \""
         << _nodeMap.find(i)->second->wordPart_.value() << "\"} : (";
    }

    for (size_t j = 0; j < _adjLists[i].size(); ++j) {
      os << _adjLists[i][j];
      if (j < _adjLists[i].size() - 1) {
        os << ", ";
      }
    }
    os << ')';
    if (i < _adjLists.size() - 1) {
      os << '\n';
    }
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t SubtreePlan::getCostEstimate() const { return _qet->getCostEstimate(); }

// _____________________________________________________________________________
size_t SubtreePlan::getSizeEstimate() const { return _qet->getSizeEstimate(); }

// _____________________________________________________________________________
bool QueryPlanner::connected(const SubtreePlan& a, const SubtreePlan& b,
                             const QueryPlanner::TripleGraph& tg) const {
  // Check if there is overlap.
  // If so, don't consider them as properly connected.
  if ((a._idsOfIncludedNodes & b._idsOfIncludedNodes) != 0) {
    return false;
  }

  if (a._idsOfIncludedNodes >= (size_t(1) << tg._nodeMap.size()) ||
      b._idsOfIncludedNodes >= (size_t(1) << tg._nodeMap.size())) {
    return getJoinColumns(a, b).size() > 0;
  }

  for (size_t i = 0; i < tg._nodeMap.size(); ++i) {
    if (((a._idsOfIncludedNodes >> i) & 1) == 0) {
      continue;
    }
    auto& connectedNodes = tg._adjLists[i];
    for (auto targetNodeId : connectedNodes) {
      if ((((a._idsOfIncludedNodes >> targetNodeId) & 1) == 0) &&
          (((b._idsOfIncludedNodes >> targetNodeId) & 1) != 0)) {
        return true;
      }
    }
  }
  return false;
}

// _____________________________________________________________________________
QueryPlanner::JoinColumns QueryPlanner::getJoinColumns(const SubtreePlan& a,
                                                       const SubtreePlan& b) {
  AD_CORRECTNESS_CHECK(a._qet && b._qet);
  return QueryExecutionTree::getJoinColumns(*a._qet, *b._qet);
}

// _____________________________________________________________________________
string QueryPlanner::getPruningKey(
    const SubtreePlan& plan,
    const vector<ColumnIndex>& orderedOnColumns) const {
  // Get the ordered var
  std::ostringstream os;
  const auto& varCols = plan._qet->getVariableColumns();
  for (ColumnIndex orderedOnCol : orderedOnColumns) {
    for (const auto& [variable, columnIndexWithType] : varCols) {
      if (columnIndexWithType.columnIndex_ == orderedOnCol) {
        os << variable.name() << ", ";
        break;
      }
    }
  }

  os << ' ' << plan._idsOfIncludedNodes;
  os << " f: ";
  os << ' ' << plan._idsOfIncludedFilters;
  os << " t: ";
  os << ' ' << plan.idsOfIncludedTextLimits_;

  return std::move(os).str();
}

// _____________________________________________________________________________
template <bool replace>
void QueryPlanner::applyFiltersIfPossible(
    vector<SubtreePlan>& row, const vector<SparqlFilter>& filters) const {
  // Apply every filter possible.
  // It is possible when,
  // 1) the filter has not already been applied
  // 2) all variables in the filter are covered by the query so far
  // New 06 May 2016:
  // There is a problem with the so-called (name may be changed)
  // TextOperationWithFilter ops: This method applies SPARQL filters
  // to all the leaf TextOperations (when feasible) and thus
  // prevents the special case from being applied when subtrees are merged.
  // Fix: Also copy (CHANGE not all plans but TextOperation) without applying
  // the filter. Problem: If the method gets called multiple times, plans with
  // filters May be duplicated. To prevent this, calling code has to ensure
  // That the method is only called once on each row. Similarly this affects
  // the (albeit rare) fact that a filter is directly applicable after a scan
  // of a huge relation where a subsequent join with a small result could be
  // translated into one or more scans directly. This also helps with cases
  // where applying the filter later is better. Finally, the replace flag can
  // be set to enforce that all filters are applied. This should be done for
  // the last row in the DPTab so that no filters are missed.

  // Note: we are first collecting the newly added plans and then adding them
  // in one go. Changing `row` inside the loop would invalidate the iterators.
  std::vector<SubtreePlan> addedPlans;
  for (auto& plan : row) {
    for (size_t i = 0; i < filters.size(); ++i) {
      if (((plan._idsOfIncludedFilters >> i) & 1) != 0) {
        continue;
      }

      if (ql::ranges::all_of(filters[i].expression_.containedVariables(),
                             [&plan](const auto& variable) {
                               return plan._qet->isVariableCovered(*variable);
                             })) {
        // Apply this filter.
        SubtreePlan newPlan =
            makeSubtreePlan<Filter>(_qec, plan._qet, filters[i].expression_);
        newPlan._idsOfIncludedFilters = plan._idsOfIncludedFilters;
        newPlan._idsOfIncludedFilters |= (size_t(1) << i);
        newPlan._idsOfIncludedNodes = plan._idsOfIncludedNodes;
        newPlan.type = plan.type;
        if constexpr (replace) {
          plan = std::move(newPlan);
        } else {
          addedPlans.push_back(std::move(newPlan));
        }
      }
    }
  }
  row.insert(row.end(), addedPlans.begin(), addedPlans.end());
}

// _____________________________________________________________________________
void QueryPlanner::applyTextLimitsIfPossible(vector<SubtreePlan>& row,
                                             const TextLimitVec& textLimits,
                                             bool replace) const {
  // Apply text limits if possible.
  // A text limit can be applied to a plan if:
  // 1) There is no text operation for the text record column left.
  // 2) The text limit has not already been applied to the plan.

  // Note: we are first collecting the newly added plans and then adding them
  // in one go. Changing `row` inside the loop would invalidate the iterators.
  if (!textLimit_.has_value()) {
    return;
  }
  std::vector<SubtreePlan> addedPlans;
  for (auto& plan : row) {
    size_t i = 0;
    for (const auto& [textVar, textLimit] : textLimits) {
      if (((plan.idsOfIncludedTextLimits_ >> i) & 1) != 0) {
        // The text limit has already been applied to the plan.
        i++;
        continue;
      }
      if (((plan._idsOfIncludedNodes &
            textLimit.idsOfMustBeFinishedOperations_) ^
           textLimit.idsOfMustBeFinishedOperations_) != 0) {
        // There is still an operation that needs to be finished before this
        // text limit can be applied
        i++;
        continue;
      }
      // TODO<C++23> simplify using ranges::to
      auto getVarColumns = [&plan](const std::vector<Variable>& vars) {
        std::vector<ColumnIndex> result;
        for (const auto& var : vars) {
          result.push_back(plan._qet->getVariableColumn(var));
        }
        return result;
      };
      SubtreePlan newPlan = makeSubtreePlan<TextLimit>(
          _qec, textLimit_.value(), plan._qet,
          plan._qet.get()->getVariableColumn(textVar),
          getVarColumns(textLimit.entityVars_),
          getVarColumns(textLimit.scoreVars_));
      newPlan.idsOfIncludedTextLimits_ = plan.idsOfIncludedTextLimits_;
      newPlan.idsOfIncludedTextLimits_ |= (size_t(1) << i);
      newPlan._idsOfIncludedNodes = plan._idsOfIncludedNodes;
      newPlan.type = plan.type;
      i++;
      if (replace) {
        plan = std::move(newPlan);
      } else {
        addedPlans.push_back(std::move(newPlan));
      }
    }
  }
  row.insert(row.end(), addedPlans.begin(), addedPlans.end());
}

// _____________________________________________________________________________
size_t QueryPlanner::findUniqueNodeIds(
    const std::vector<SubtreePlan>& connectedComponent) {
  ad_utility::HashSet<uint64_t> uniqueNodeIds;
  auto nodeIds = connectedComponent |
                 ql::views::transform(&SubtreePlan::_idsOfIncludedNodes);
  // Check that all the `_idsOfIncludedNodes` are one-hot encodings of a single
  // value, i.e. they have exactly one bit set.
  AD_CORRECTNESS_CHECK(ql::ranges::all_of(
      nodeIds, [](auto nodeId) { return std::popcount(nodeId) == 1; }));
  ql::ranges::copy(nodeIds, std::inserter(uniqueNodeIds, uniqueNodeIds.end()));
  return uniqueNodeIds.size();
}

// _____________________________________________________________________________
std::vector<SubtreePlan>
QueryPlanner::runDynamicProgrammingOnConnectedComponent(
    std::vector<SubtreePlan> connectedComponent,
    const vector<SparqlFilter>& filters, const TextLimitVec& textLimits,
    const TripleGraph& tg) const {
  vector<vector<SubtreePlan>> dpTab;
  // find the unique number of nodes in the current connected component
  // (there might be duplicates because we already have multiple candidates
  // for each index scan with different permutations.
  dpTab.push_back(std::move(connectedComponent));
  applyFiltersIfPossible<false>(dpTab.back(), filters);
  applyTextLimitsIfPossible(dpTab.back(), textLimits, false);
  size_t numSeeds = findUniqueNodeIds(dpTab.back());

  for (size_t k = 2; k <= numSeeds; ++k) {
    LOG(TRACE) << "Producing plans that unite " << k << " triples."
               << std::endl;
    dpTab.emplace_back();
    for (size_t i = 1; i * 2 <= k; ++i) {
      checkCancellation();
      auto newPlans = merge(dpTab[i - 1], dpTab[k - i - 1], tg);
      dpTab[k - 1].insert(dpTab[k - 1].end(), newPlans.begin(), newPlans.end());
      applyFiltersIfPossible<false>(dpTab.back(), filters);
      applyTextLimitsIfPossible(dpTab.back(), textLimits, false);
    }
    // As we only passed in connected components, we expect the result to always
    // be nonempty.
    AD_CORRECTNESS_CHECK(!dpTab[k - 1].empty());
  }
  auto& result = dpTab.back();
  applyFiltersIfPossible<true>(result, filters);
  applyTextLimitsIfPossible(result, textLimits, true);
  return std::move(result);
}

// _____________________________________________________________________________
size_t QueryPlanner::countSubgraphs(std::vector<const SubtreePlan*> graph,
                                    const std::vector<SparqlFilter>& filters,
                                    size_t budget) {
  // Remove duplicate plans from `graph`.
  auto getId = [](const SubtreePlan* v) { return v->_idsOfIncludedNodes; };
  ql::ranges::sort(graph, ql::ranges::less{}, getId);
  auto uniqueIter = ql::ranges::unique(graph, ql::ranges::equal_to{}, getId);
#ifdef QLEVER_CPP_17
  graph.erase(uniqueIter, graph.end());
#else
  graph.erase(uniqueIter.begin(), graph.end());
#endif

  // We also have to consider the `filters`. To make life easy, we temporarily
  // create simple `SubtreePlans` for them which just have the correct
  // variables. We only create one subtree plan for each set of variables that
  // is contained in the `filters`, because this will bring the estimate of this
  // function closer to the actual behavior of the DP query planner (it always
  // applies either all possible filters at once, or none of them).
  std::vector<SubtreePlan> dummyPlansForFilter;
  ad_utility::HashSet<ad_utility::HashSet<Variable>>
      deduplicatedFilterVariables;
  for (const auto& filter : filters) {
    const auto& vars = filter.expression_.containedVariables();
    ad_utility::HashSet<Variable> varSet;
    // We use a `VALUES` clause as the dummy because this operation is the
    // easiest to setup for a number of given variables.
    parsedQuery::SparqlValues values;
    for (auto* var : vars) {
      values._variables.push_back(*var);
      varSet.insert(*var);
    }
    if (deduplicatedFilterVariables.insert(std::move(varSet)).second) {
      dummyPlansForFilter.push_back(
          makeSubtreePlan<Values>(_qec, std::move(values)));
    }
  }

  const size_t numPlansWithoutFilters = graph.size();
  for (const auto& filterPlan : dummyPlansForFilter) {
    graph.push_back(&filterPlan);
  }

  // Qlever currently limits the number of triples etc. per group to be <= 64
  // anyway, so we can simply assert here.
  AD_CORRECTNESS_CHECK(graph.size() <= 64,
                       "Should qlever ever support more than 64 elements per "
                       "group graph pattern, then the `countSubgraphs` "
                       "functionality also has to be changed");

  // Compute the bit representation needed for the call to
  // `countConnectedSubgraphs::countSubgraphs` below.
  countConnectedSubgraphs::Graph g;
  for (size_t i = 0; i < graph.size(); ++i) {
    countConnectedSubgraphs::Node v{0};
    for (size_t k = 0; k < graph.size(); ++k) {
      // Don't connect nodes to themselves, don't connect filters with other
      // filters, otherwise connect `i` and `k` if they have at least one
      // variable in common.
      if ((k != i) &&
          (k < numPlansWithoutFilters || i < numPlansWithoutFilters) &&
          !QueryPlanner::getJoinColumns(*graph.at(k), *graph.at(i)).empty()) {
        v.neighbors_ |= (1ULL << k);
      }
    }
    g.push_back(v);
  }

  return countConnectedSubgraphs::countSubgraphs(g, budget);
}

// _____________________________________________________________________________
std::vector<SubtreePlan> QueryPlanner::runGreedyPlanningOnConnectedComponent(
    std::vector<SubtreePlan> connectedComponent,
    const vector<SparqlFilter>& filters, const TextLimitVec& textLimits,
    const TripleGraph& tg) const {
  applyFiltersIfPossible<true>(connectedComponent, filters);
  applyTextLimitsIfPossible(connectedComponent, textLimits, true);
  const size_t numSeeds = findUniqueNodeIds(connectedComponent);
  if (numSeeds <= 1) {
    // Only 0 or 1 nodes in the input, nothing to plan.
    return connectedComponent;
  }

  // Intermediate variables that will be filled by the `greedyStep` lambda
  // below.
  using Plans = std::vector<SubtreePlan>;

  // Perform a single step of greedy query planning.
  // `nextBestPlan` contains the result of the last step of greedy query
  // planning. `currentPlans` contains all plans that have been chosen/combined
  // so far (which might still be the initial start plans), except for the most
  // recently chosen plan, which is stored in `nextResult`. `cache` contains all
  // the plans that can be obtained by combining two plans in `input`. The
  // function then performs one additional step of greedy planning and
  // reinforces the above pre-/postconditions. Exception: if `isFirstStep` then
  // `cache` and `nextResult` must be empty, and the first step of greedy
  // planning is performed, which also establishes the pre-/postconditions.
  auto greedyStep = [this, &tg, &filters, &textLimits,
                     currentPlans = std::move(connectedComponent),
                     cache = Plans{}](Plans& nextBestPlan,
                                      bool isFirstStep) mutable {
    checkCancellation();
    // Normally, we already have all combinations of two nodes in `currentPlans`
    // in the cache, so we only have to add the combinations between
    // `currentPlans` and `nextResult`. In the first step, we need to initially
    // compute all possible combinations.
    auto newPlans = isFirstStep ? merge(currentPlans, currentPlans, tg)
                                : merge(currentPlans, nextBestPlan, tg);
    applyFiltersIfPossible<true>(newPlans, filters);
    applyTextLimitsIfPossible(newPlans, textLimits, true);
    AD_CORRECTNESS_CHECK(!newPlans.empty());
    ql::ranges::move(newPlans, std::back_inserter(cache));
    ql::ranges::move(nextBestPlan, std::back_inserter(currentPlans));

    // All candidates for the next greedy step are in the `cache`, choose the
    // cheapest one, remove it from the cache and make it the `nextResult`
    {
      auto smallestIdxNew = findSmallestExecutionTree(cache);
      auto& cheapestNewTree = cache.at(smallestIdxNew);
      std::swap(cheapestNewTree, cache.back());
      nextBestPlan.clear();
      nextBestPlan.push_back(std::move(cache.back()));
      cache.pop_back();
    }

    // All plans which have a node in common with the chosen plan have to be
    // deleted from the `currentPlans` and therefore also from the `cache`.
    auto shouldBeErased = [&nextTree = nextBestPlan.front()](const auto& plan) {
      return (nextTree._idsOfIncludedNodes & plan._idsOfIncludedNodes) != 0;
    };
    std::erase_if(currentPlans, shouldBeErased);
    std::erase_if(cache, shouldBeErased);
  };

  bool first = true;
  Plans result;
  for ([[maybe_unused]] size_t i : ad_utility::integerRange(numSeeds - 1)) {
    greedyStep(result, first);
    first = false;
  }
  // TODO<joka921> Assert that all seeds are covered by the result.
  return result;
}

// _____________________________________________________________________________
vector<vector<SubtreePlan>> QueryPlanner::fillDpTab(
    const QueryPlanner::TripleGraph& tg, vector<SparqlFilter> filters,
    TextLimitMap& textLimits, const vector<vector<SubtreePlan>>& children) {
  auto [initialPlans, additionalFilters] =
      seedWithScansAndText(tg, children, textLimits);
  ql::ranges::move(additionalFilters, std::back_inserter(filters));
  if (filters.size() > 64) {
    AD_THROW("At most 64 filters allowed at the moment.");
  }
  auto componentIndices = QueryGraph::computeConnectedComponents(initialPlans);
  ad_utility::HashMap<size_t, std::vector<SubtreePlan>> components;
  for (size_t i = 0; i < componentIndices.size(); ++i) {
    components[componentIndices.at(i)].push_back(std::move(initialPlans.at(i)));
  }
  vector<vector<SubtreePlan>> lastDpRowFromComponents;
  TextLimitVec textLimitVec(textLimits.begin(), textLimits.end());
  for (auto& component : components | ql::views::values) {
    std::vector<const SubtreePlan*> g;
    for (const auto& plan : component) {
      g.push_back(&plan);
    }
    const size_t budget = RuntimeParameters().get<"query-planning-budget">();
    bool useGreedyPlanning = countSubgraphs(g, filters, budget) > budget;
    if (useGreedyPlanning) {
      LOG(INFO)
          << "Using the greedy query planner for a large connected component"
          << std::endl;
    }
    auto impl = useGreedyPlanning
                    ? &QueryPlanner::runGreedyPlanningOnConnectedComponent
                    : &QueryPlanner::runDynamicProgrammingOnConnectedComponent;
    lastDpRowFromComponents.push_back(std::invoke(
        impl, this, std::move(component), filters, textLimitVec, tg));
    checkCancellation();
  }
  size_t numConnectedComponents = lastDpRowFromComponents.size();
  if (numConnectedComponents == 0) {
    // This happens for example if there is a BIND right at the beginning of the
    // query
    lastDpRowFromComponents.emplace_back();
    return lastDpRowFromComponents;
  }
  if (numConnectedComponents == 1) {
    // A Cartesian product is not needed if there is only one component.
    applyFiltersIfPossible<true>(lastDpRowFromComponents.back(), filters);
    applyTextLimitsIfPossible(lastDpRowFromComponents.back(), textLimitVec,
                              true);
    return lastDpRowFromComponents;
  }
  // More than one connected component, set up a Cartesian product.
  std::vector<std::vector<SubtreePlan>> result;
  result.emplace_back();
  std::vector<std::shared_ptr<QueryExecutionTree>> subtrees;
  // We need to manually inform the cartesian produce about
  // its included nodes and filters and text limits to make the
  // `applyTextLimitsIfPossible` call below work correctly.
  uint64_t nodes = 0;
  uint64_t filterIds = 0;
  uint64_t textLimitIds = 0;
  ql::ranges::for_each(
      lastDpRowFromComponents |
          ql::views::transform([this](auto& vec) -> decltype(auto) {
            return vec.at(findCheapestExecutionTree(vec));
          }),
      [&](SubtreePlan& plan) {
        nodes |= plan._idsOfIncludedNodes;
        filterIds |= plan._idsOfIncludedFilters;
        textLimitIds |= plan.idsOfIncludedTextLimits_;
        subtrees.push_back(std::move(plan._qet));
      });
  result.at(0).push_back(
      makeSubtreePlan<CartesianProductJoin>(_qec, std::move(subtrees)));
  auto& plan = result.at(0).back();
  plan._idsOfIncludedNodes = nodes;
  plan._idsOfIncludedFilters = filterIds;
  plan.idsOfIncludedTextLimits_ = textLimitIds;
  applyFiltersIfPossible<true>(result.at(0), filters);
  applyTextLimitsIfPossible(result.at(0), textLimitVec, true);
  return result;
}

// _____________________________________________________________________________
bool QueryPlanner::TripleGraph::isTextNode(size_t i) const {
  return _nodeMap.count(i) > 0 &&
         (_nodeMap.find(i)->second->triple_.p_.iri_ ==
              CONTAINS_ENTITY_PREDICATE ||
          _nodeMap.find(i)->second->triple_.p_.iri_ == CONTAINS_WORD_PREDICATE);
}

// _____________________________________________________________________________
vector<pair<QueryPlanner::TripleGraph, vector<SparqlFilter>>>
QueryPlanner::TripleGraph::splitAtContextVars(
    const vector<SparqlFilter>& origFilters,
    ad_utility::HashMap<string, vector<size_t>>& contextVarToTextNodes) const {
  vector<pair<QueryPlanner::TripleGraph, vector<SparqlFilter>>> retVal;
  // Recursively split the graph a context nodes.
  // Base-case: No no context nodes, return the graph itself.
  if (contextVarToTextNodes.size() == 0) {
    retVal.emplace_back(make_pair(*this, origFilters));
  } else {
    // Just take the first contextVar and split at it.
    ad_utility::HashSet<size_t> textNodeIds;
    textNodeIds.insert(contextVarToTextNodes.begin()->second.begin(),
                       contextVarToTextNodes.begin()->second.end());

    // For the next iteration / recursive call(s):
    // Leave out the first one because it has been worked on in this call.
    ad_utility::HashMap<string, vector<size_t>> cTMapNextIteration;
    cTMapNextIteration.insert(++contextVarToTextNodes.begin(),
                              contextVarToTextNodes.end());

    // Find a node to start the split.
    size_t startNode = 0;
    while (startNode < _adjLists.size() && textNodeIds.count(startNode) > 0) {
      ++startNode;
    }
    // If no start node was found, this means only text triples left.
    // --> don't enter code block below and return empty vector.
    if (startNode != _adjLists.size()) {
      // If we have a start node, do a BFS to obtain a set of reachable nodes
      auto reachableNodes = bfsLeaveOut(startNode, textNodeIds);
      if (reachableNodes.size() == _adjLists.size() - textNodeIds.size()) {
        // Case: cyclic or text operation was on the "outside"
        // -> only one split to work with further.
        // Recursively solve this split
        // (because there may be another context var in it)
        TripleGraph withoutText(*this, reachableNodes);
        vector<SparqlFilter> filters = pickFilters(origFilters, reachableNodes);
        auto recursiveResult =
            withoutText.splitAtContextVars(filters, cTMapNextIteration);
        retVal.insert(retVal.begin(), recursiveResult.begin(),
                      recursiveResult.end());
      } else {
        // Case: The split created two or more non-empty parts.
        // Find all parts so that the number of triples in them plus
        // the number of text triples equals the number of total triples.
        vector<vector<size_t>> setsOfReachablesNodes;
        ad_utility::HashSet<size_t> nodesDone;
        nodesDone.insert(textNodeIds.begin(), textNodeIds.end());
        nodesDone.insert(reachableNodes.begin(), reachableNodes.end());
        setsOfReachablesNodes.emplace_back(reachableNodes);
        assert(nodesDone.size() < _adjLists.size());
        while (nodesDone.size() < _adjLists.size()) {
          while (startNode < _adjLists.size() &&
                 nodesDone.count(startNode) > 0) {
            ++startNode;
          }
          reachableNodes = bfsLeaveOut(startNode, textNodeIds);
          nodesDone.insert(reachableNodes.begin(), reachableNodes.end());
          setsOfReachablesNodes.emplace_back(reachableNodes);
        }
        // Recursively split each part because there may be other context
        // vars.
        for (const auto& rNodes : setsOfReachablesNodes) {
          TripleGraph smallerGraph(*this, rNodes);
          vector<SparqlFilter> filters = pickFilters(origFilters, rNodes);
          auto recursiveResult =
              smallerGraph.splitAtContextVars(filters, cTMapNextIteration);
          retVal.insert(retVal.begin(), recursiveResult.begin(),
                        recursiveResult.end());
        }
      }
    }
  }
  return retVal;
}

// _____________________________________________________________________________
vector<size_t> QueryPlanner::TripleGraph::bfsLeaveOut(
    size_t startNode, ad_utility::HashSet<size_t> leaveOut) const {
  vector<size_t> res;
  ad_utility::HashSet<size_t> visited;
  std::list<size_t> queue;
  queue.push_back(startNode);
  visited.insert(startNode);
  while (!queue.empty()) {
    size_t n = queue.front();
    queue.pop_front();
    res.push_back(n);
    auto& neighbors = _adjLists[n];
    for (size_t v : neighbors) {
      if (visited.count(v) == 0 && leaveOut.count(v) == 0) {
        visited.insert(v);
        queue.push_back(v);
      }
    }
  }
  return res;
}

// _____________________________________________________________________________
vector<SparqlFilter> QueryPlanner::TripleGraph::pickFilters(
    const vector<SparqlFilter>& origFilters,
    const vector<size_t>& nodes) const {
  vector<SparqlFilter> ret;
  ad_utility::HashSet<Variable> coveredVariables;
  for (auto n : nodes) {
    auto& node = *_nodeMap.find(n)->second;
    coveredVariables.insert(node._variables.begin(), node._variables.end());
  }
  for (auto& f : origFilters) {
    if (ql::ranges::any_of(
            f.expression_.containedVariables(),
            [&](const auto* var) { return coveredVariables.contains(*var); })) {
      ret.push_back(f);
    }
  }
  return ret;
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph::TripleGraph(
    const std::vector<std::pair<Node, std::vector<size_t>>>& init) {
  for (const std::pair<Node, std::vector<size_t>>& p : init) {
    _nodeStorage.push_back(p.first);
    _nodeMap[p.first.id_] = &_nodeStorage.back();
    _adjLists.push_back(p.second);
  }
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph::TripleGraph(const QueryPlanner::TripleGraph& other,
                                       vector<size_t> keepNodes) {
  ad_utility::HashSet<size_t> keep;
  for (auto v : keepNodes) {
    keep.insert(v);
  }
  // Copy nodes to be kept and assign new node id's.
  // Keep information about the id change in a map.
  ad_utility::HashMap<size_t, size_t> idChange;
  for (size_t i = 0; i < other._nodeMap.size(); ++i) {
    if (keep.count(i) > 0) {
      _nodeStorage.push_back(*other._nodeMap.find(i)->second);
      idChange[i] = _nodeMap.size();
      _nodeStorage.back().id_ = _nodeMap.size();
      _nodeMap[idChange[i]] = &_nodeStorage.back();
    }
  }
  // Adjust adjacency lists accordingly.
  for (size_t i = 0; i < other._adjLists.size(); ++i) {
    if (keep.count(i) > 0) {
      vector<size_t> adjList;
      for (size_t v : other._adjLists[i]) {
        if (keep.count(v) > 0) {
          adjList.push_back(idChange[v]);
        }
      }
      _adjLists.push_back(adjList);
    }
  }
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph::TripleGraph(const TripleGraph& other)
    : _adjLists(other._adjLists), _nodeMap(), _nodeStorage() {
  for (auto it : other._nodeMap) {
    _nodeStorage.push_back(*it.second);
    _nodeMap[it.first] = &_nodeStorage.back();
  }
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph& QueryPlanner::TripleGraph::operator=(
    const TripleGraph& other) {
  _adjLists = other._adjLists;
  for (auto it : other._nodeMap) {
    _nodeStorage.push_back(*it.second);
    _nodeMap[it.first] = &_nodeStorage.back();
  }
  return *this;
}

// _____________________________________________________________________________
QueryPlanner::TripleGraph::TripleGraph()
    : _adjLists(), _nodeMap(), _nodeStorage() {}

// _____________________________________________________________________________
bool QueryPlanner::TripleGraph::isSimilar(
    const QueryPlanner::TripleGraph& other) const {
  // This method is very verbose as it is currently only intended for
  // testing
  if (_nodeStorage.size() != other._nodeStorage.size()) {
    LOG(INFO) << asString() << std::endl;
    LOG(INFO) << other.asString() << std::endl;
    LOG(INFO) << "The two triple graphs are not of the same size: "
              << _nodeStorage.size() << " != " << other._nodeStorage.size()
              << std::endl;
    return false;
  }
  ad_utility::HashMap<size_t, size_t> id_map;
  ad_utility::HashMap<size_t, size_t> id_map_reverse;
  for (const Node& n : _nodeStorage) {
    bool hasMatch = false;
    for (const Node& n2 : other._nodeStorage) {
      if (n.isSimilar(n2)) {
        id_map[n.id_] = n2.id_;
        id_map_reverse[n2.id_] = n.id_;
        hasMatch = true;
        break;
      } else {
      }
    }
    if (!hasMatch) {
      LOG(INFO) << asString() << std::endl;
      LOG(INFO) << other.asString() << std::endl;
      LOG(INFO) << "The node " << n << " has no match in the other graph"
                << std::endl;
      return false;
    }
  }
  if (id_map.size() != _nodeStorage.size() ||
      id_map_reverse.size() != _nodeStorage.size()) {
    LOG(INFO) << asString() << std::endl;
    LOG(INFO) << other.asString() << std::endl;
    LOG(INFO) << "Two nodes in this graph were matches to the same node in "
                 "the other graph"
              << std::endl;
    return false;
  }
  for (size_t id = 0; id < _adjLists.size(); ++id) {
    size_t other_id = id_map[id];
    ad_utility::HashSet<size_t> adj_set;
    ad_utility::HashSet<size_t> other_adj_set;
    for (size_t a : _adjLists[id]) {
      adj_set.insert(a);
    }
    for (size_t a : other._adjLists[other_id]) {
      other_adj_set.insert(a);
    }
    for (size_t a : _adjLists[id]) {
      if (other_adj_set.count(id_map[a]) == 0) {
        LOG(INFO) << asString() << std::endl;
        LOG(INFO) << other.asString() << std::endl;
        LOG(INFO) << "The node with id " << id << " is connected to " << a
                  << " in this graph graph but not to the equivalent "
                     "node in the other graph."
                  << std::endl;
        return false;
      }
    }
    for (size_t a : other._adjLists[other_id]) {
      if (adj_set.count(id_map_reverse[a]) == 0) {
        LOG(INFO) << asString() << std::endl;
        LOG(INFO) << other.asString() << std::endl;
        LOG(INFO) << "The node with id " << id << " is connected to " << a
                  << " in the other graph graph but not to the equivalent "
                     "node in this graph."
                  << std::endl;
        return false;
      }
    }
  }
  return true;
}

// _____________________________________________________________________________
void QueryPlanner::setEnablePatternTrick(bool enablePatternTrick) {
  _enablePatternTrick = enablePatternTrick;
}

// _________________________________________________________________________________
size_t QueryPlanner::findCheapestExecutionTree(
    const std::vector<SubtreePlan>& lastRow) const {
  AD_CONTRACT_CHECK(!lastRow.empty());
  auto compare = [this](const auto& a, const auto& b) {
    auto aCost = a.getCostEstimate(), bCost = b.getCostEstimate();
    if (aCost == bCost && isInTestMode()) {
      // Make the tiebreaking deterministic for the unit tests.
      return a._qet->getCacheKey() < b._qet->getCacheKey();
    } else {
      return aCost < bCost;
    }
  };
  return ql::ranges::min_element(lastRow, compare) - lastRow.begin();
};

// _________________________________________________________________________________
size_t QueryPlanner::findSmallestExecutionTree(
    const std::vector<SubtreePlan>& lastRow) {
  AD_CONTRACT_CHECK(!lastRow.empty());
  auto compare = [](const auto& a, const auto& b) {
    auto tie = [](const auto& x) {
      return std::tuple{x.getSizeEstimate(), x.getSizeEstimate()};
    };
    return tie(a) < tie(b);
  };
  return ql::ranges::min_element(lastRow, compare) - lastRow.begin();
};

// _____________________________________________________________________________
std::vector<SubtreePlan> QueryPlanner::createJoinCandidates(
    const SubtreePlan& ain, const SubtreePlan& bin,
    boost::optional<const TripleGraph&> tg) const {
  bool swapForTesting = isInTestMode() && bin.type != SubtreePlan::OPTIONAL &&
                        ain._qet->getCacheKey() < bin._qet->getCacheKey();
  const auto& a = !swapForTesting ? ain : bin;
  const auto& b = !swapForTesting ? bin : ain;
  std::vector<SubtreePlan> candidates;

  // TODO<joka921> find out, what is ACTUALLY the use case for the triple
  // graph. Is it only meant for (questionable) performance reasons
  // or does it change the meaning.
  JoinColumns jcs;
  if (tg) {
    if (connected(a, b, *tg)) {
      jcs = getJoinColumns(a, b);
    }
  } else {
    jcs = getJoinColumns(a, b);
  }

  return createJoinCandidates(ain, bin, jcs);
}

// _____________________________________________________________________________
std::vector<SubtreePlan> QueryPlanner::createJoinCandidates(
    const SubtreePlan& ain, const SubtreePlan& bin,
    const JoinColumns& jcs) const {
  bool swapForTesting = isInTestMode() && bin.type != SubtreePlan::OPTIONAL &&
                        ain._qet->getCacheKey() < bin._qet->getCacheKey();
  const auto& a = !swapForTesting ? ain : bin;
  const auto& b = !swapForTesting ? bin : ain;
  std::vector<SubtreePlan> candidates;

  if (jcs.empty()) {
    // The candidates are not connected
    return candidates;
  }

  // If both sides are spatial joins that are still missing children, return
  // immediately to prevent a regular join on the variables, which would lead to
  // the spatial join never having children.
  if (checkSpatialJoin(a, b) == std::pair<bool, bool>{true, true}) {
    return candidates;
  }

  // if one of the inputs is the spatial join and the other input is compatible
  // with the SpatialJoin, add it as a child to the spatialJoin. As unbound
  // SpatialJoin operations are incompatible with normal join operations, we
  // return immediately instead of creating a normal join below as well.
  // Note, that this if statement should be evaluated first, such that no other
  // join options get considered, when one of the candidates is a SpatialJoin.
  if (auto opt = createSpatialJoin(a, b, jcs)) {
    candidates.push_back(std::move(opt.value()));
    return candidates;
  }

  if (a.type == SubtreePlan::MINUS) {
    AD_THROW(
        "MINUS can only appear after"
        " another graph pattern.");
  }

  // This case shouldn't happen. If the first pattern is OPTIONAL, it
  // is made non optional earlier. If a minus occurs after an optional
  // further into the query that optional should be resolved by now.
  AD_CONTRACT_CHECK(a.type != SubtreePlan::OPTIONAL);
  if (b.type == SubtreePlan::MINUS) {
    return {makeSubtreePlan<Minus>(_qec, a._qet, b._qet)};
  }

  // OPTIONAL JOINS are not symmetric!
  if (b.type == SubtreePlan::OPTIONAL) {
    // Join the two optional columns using an optional join
    return {makeSubtreePlan<OptionalJoin>(_qec, a._qet, b._qet)};
  }

  if (auto opt = createJoinWithPathSearch(a, b, jcs)) {
    candidates.push_back(std::move(opt.value()));
    return candidates;
  }

  if (jcs.size() >= 2) {
    // If there are two or more join columns and we are not using the
    // TwoColumnJoin (the if part before this comment), use a multiColumnJoin.
    try {
      SubtreePlan plan = makeSubtreePlan<MultiColumnJoin>(_qec, a._qet, b._qet);
      mergeSubtreePlanIds(plan, a, b);
      return {plan};
    } catch (const std::exception& e) {
      return {};
    }
  }

  // CASE: JOIN ON ONE COLUMN ONLY.

  // Check if one of the two operations is a HAS_PREDICATE_SCAN.
  // If the join column corresponds to the has-predicate scan's
  // subject column we can use a specialized join that avoids
  // loading the full has-predicate predicate.
  if (auto opt = createJoinWithHasPredicateScan(a, b, jcs)) {
    candidates.push_back(std::move(opt.value()));
  }

  // Test if one of `a` or `b` is a union whose children can each have the joins
  // applied individually.
  for (SubtreePlan& plan : applyJoinDistributivelyToUnion(a, b, jcs)) {
    candidates.push_back(std::move(plan));
  }

  // Test if one of `a` or `b` is a transitive path to which we can bind the
  // other one.
  if (auto opt = createJoinWithTransitivePath(a, b, jcs)) {
    candidates.push_back(std::move(opt.value()));
  }

  // "NORMAL" CASE:
  // The join class takes care of sorting the subtrees if necessary
  SubtreePlan plan =
      makeSubtreePlan<Join>(_qec, a._qet, b._qet, jcs[0][0], jcs[0][1]);
  mergeSubtreePlanIds(plan, a, b);
  candidates.push_back(std::move(plan));

  return candidates;
}

// _____________________________________________________________________________
std::pair<bool, bool> QueryPlanner::checkSpatialJoin(const SubtreePlan& a,
                                                     const SubtreePlan& b) {
  auto isIncompleteSpatialJoin = [](const SubtreePlan& sj) {
    auto sjCasted = std::dynamic_pointer_cast<const SpatialJoin>(
        sj._qet->getRootOperation());
    return sjCasted != nullptr && !sjCasted->isConstructed();
  };
  return {isIncompleteSpatialJoin(a), isIncompleteSpatialJoin(b)};
}

// _____________________________________________________________________________
auto QueryPlanner::createSpatialJoin(const SubtreePlan& a, const SubtreePlan& b,
                                     const JoinColumns& jcs)
    -> std::optional<SubtreePlan> {
  auto [aIs, bIs] = checkSpatialJoin(a, b);

  // Exactly one of the inputs must be a SpatialJoin.
  if (aIs == bIs) {
    return std::nullopt;
  }

  const SubtreePlan& spatialSubtreePlan = aIs ? a : b;
  const SubtreePlan& otherSubtreePlan = aIs ? b : a;

  std::shared_ptr<Operation> op = spatialSubtreePlan._qet->getRootOperation();
  auto spatialJoin = static_cast<SpatialJoin*>(op.get());

  if (spatialJoin->isConstructed()) {
    return std::nullopt;
  }

  if (jcs.size() > 1) {
    AD_THROW(
        "Currently, if both sides of a SpatialJoin are variables, then the"
        "SpatialJoin must be the only connection between these variables");
  }
  ColumnIndex ind = aIs ? jcs[0][1] : jcs[0][0];
  const Variable& var =
      otherSubtreePlan._qet->getVariableAndInfoByColumnIndex(ind).first;

  auto newSpatialJoin = spatialJoin->addChild(otherSubtreePlan._qet, var);

  SubtreePlan plan = makeSubtreePlan<SpatialJoin>(std::move(newSpatialJoin));
  mergeSubtreePlanIds(plan, a, b);
  return plan;
}

// _____________________________________________________________________________________________________________________

namespace {
// Helper function that maps the indices from the unions' columns to the
// children's columns if possible. Otherwise the entry in `jcs` is dropped.
std::pair<QueryPlanner::JoinColumns, QueryPlanner::JoinColumns>
mapColumnsInUnion(size_t columnIndex, const Union& unionOperation,
                  const QueryPlanner::JoinColumns& jcs) {
  QueryPlanner::JoinColumns leftMapping;
  leftMapping.reserve(jcs.size());
  QueryPlanner::JoinColumns rightMapping;
  rightMapping.reserve(jcs.size());
  auto mapColumns = [columnIndex, &unionOperation](
                        bool isLeft, std::array<ColumnIndex, 2> columns)
      -> std::optional<array<ColumnIndex, 2>> {
    ColumnIndex& column = columns.at(columnIndex);
    auto tmp = unionOperation.getOriginalColumn(isLeft, column);
    if (tmp.has_value()) {
      column = tmp.value();
      return columns;
    }
    return std::nullopt;
  };
  for (const auto& joinColumns : jcs) {
    if (auto mappedColumn = mapColumns(true, joinColumns)) {
      leftMapping.push_back(mappedColumn.value());
    }
    if (auto mappedColumn = mapColumns(false, joinColumns)) {
      rightMapping.push_back(mappedColumn.value());
    }
  }
  return {std::move(leftMapping), std::move(rightMapping)};
}

// Helper function that clones a SubtreePlan with a new QueryExecutionTree.
SubtreePlan cloneWithNewTree(const SubtreePlan& plan,
                             std::shared_ptr<QueryExecutionTree> newTree) {
  SubtreePlan newPlan = plan;
  newPlan._qet = std::move(newTree);
  return newPlan;
}

// Check if an unbound transitive path is somewhere in the tree. This is because
// the optimization with `Union` currently only makes sense if there is a
// transitive path in the tree that benefits from directly applying the join.
bool hasUnboundTransitivePathInTree(const Operation& operation) {
  if (auto* transitivePath =
          dynamic_cast<const TransitivePathBase*>(&operation)) {
    return !transitivePath->isBoundOrId();
  }
  // Only check `UNION`s for children.
  if (!dynamic_cast<const Union*>(&operation)) {
    return false;
  }
  return ql::ranges::any_of(
      operation.getChildren(), [](const QueryExecutionTree* child) {
        return hasUnboundTransitivePathInTree(*child->getRootOperation());
      });
}
}  // namespace

// _____________________________________________________________________________________________________________________
auto QueryPlanner::applyJoinDistributivelyToUnion(const SubtreePlan& a,
                                                  const SubtreePlan& b,
                                                  const JoinColumns& jcs) const
    -> std::vector<SubtreePlan> {
  AD_CORRECTNESS_CHECK(jcs.size() == 1);
  AD_CORRECTNESS_CHECK(a.type == SubtreePlan::BASIC &&
                       b.type == SubtreePlan::BASIC);
  std::vector<SubtreePlan> candidates{};
  auto findCandidates = [this, &candidates, &jcs](const SubtreePlan& thisPlan,
                                                  const SubtreePlan& other,
                                                  bool flipped) {
    auto unionOperation =
        std::dynamic_pointer_cast<Union>(thisPlan._qet->getRootOperation());
    if (!unionOperation || !hasUnboundTransitivePathInTree(*unionOperation)) {
      return;
    }

    auto findJoinCandidates = [this, flipped](const SubtreePlan& plan1,
                                              const SubtreePlan& plan2,
                                              const JoinColumns& jcs) {
      if (jcs.empty()) {
        return std::vector{makeSubtreePlan<CartesianProductJoin>(
            _qec, std::vector{plan1._qet, plan2._qet})};
      }
      return createJoinCandidates(flipped ? plan2 : plan1,
                                  flipped ? plan1 : plan2, jcs);
    };

    auto [leftMapping, rightMapping] =
        mapColumnsInUnion(flipped, *unionOperation, jcs);

    auto joinedLeft = findJoinCandidates(
        cloneWithNewTree(thisPlan, unionOperation->leftChild()), other,
        leftMapping);
    auto joinedRight = findJoinCandidates(
        cloneWithNewTree(thisPlan, unionOperation->rightChild()),
        cloneWithNewTree(other, other._qet->clone()), rightMapping);

    for (const auto& leftPlan : joinedLeft) {
      for (const auto& rightPlan : joinedRight) {
        SubtreePlan candidate =
            makeSubtreePlan<Union>(_qec, leftPlan._qet, rightPlan._qet);
        mergeSubtreePlanIds(candidate, thisPlan, other);
        candidates.push_back(std::move(candidate));
      }
    }
  };
  findCandidates(a, b, false);
  findCandidates(b, a, true);
  return candidates;
}

// __________________________________________________________________________________________________________________
auto QueryPlanner::createJoinWithTransitivePath(const SubtreePlan& a,
                                                const SubtreePlan& b,
                                                const JoinColumns& jcs)
    -> std::optional<SubtreePlan> {
  auto aTransPath = std::dynamic_pointer_cast<const TransitivePathBase>(
      a._qet->getRootOperation());
  auto bTransPath = std::dynamic_pointer_cast<const TransitivePathBase>(
      b._qet->getRootOperation());

  if (!(aTransPath || bTransPath)) {
    return std::nullopt;
  }
  std::shared_ptr<QueryExecutionTree> otherTree = aTransPath ? b._qet : a._qet;
  auto transPathOperation = aTransPath ? aTransPath : bTransPath;

  // TODO: Handle the case of two or more common variables
  if (jcs.size() > 1) {
    AD_THROW(
        "Transitive Path operation with more than"
        " two common variables is not supported");
  }
  const size_t otherCol = aTransPath ? jcs[0][1] : jcs[0][0];
  const size_t thisCol = aTransPath ? jcs[0][0] : jcs[0][1];
  // Do not bind the side of a path twice
  if (transPathOperation->isBoundOrId()) {
    return std::nullopt;
  }
  // An unbound transitive path has at most two columns.
  AD_CONTRACT_CHECK(thisCol <= 1);
  // The left or right side is a TRANSITIVE_PATH and its join column
  // corresponds to the left side of its input.
  SubtreePlan plan = [&]() {
    if (thisCol == 0) {
      return makeSubtreePlan(
          transPathOperation->bindLeftSide(otherTree, otherCol));
    } else {
      return makeSubtreePlan(
          transPathOperation->bindRightSide(otherTree, otherCol));
    }
  }();
  mergeSubtreePlanIds(plan, a, b);
  return plan;
}

// ______________________________________________________________________________________
auto QueryPlanner::createJoinWithHasPredicateScan(const SubtreePlan& a,
                                                  const SubtreePlan& b,
                                                  const JoinColumns& jcs)
    -> std::optional<SubtreePlan> {
  // Check if one of the two operations is a HAS_PREDICATE_SCAN.
  // If the join column corresponds to the has-predicate scan's
  // subject column we can use a specialized join that avoids
  // loading the full has-predicate predicate.
  auto isSuitablePredicateScan = [](const auto& tree, size_t joinColumn) {
    if (joinColumn == 0) {
      auto rootOperation = std::dynamic_pointer_cast<HasPredicateScan>(
          tree._qet->getRootOperation());
      return rootOperation &&
             rootOperation->getType() == HasPredicateScan::ScanType::FULL_SCAN;
    }
    return false;
  };

  const bool aIsSuitablePredicateScan = isSuitablePredicateScan(a, jcs[0][0]);
  const bool bIsSuitablePredicateScan = isSuitablePredicateScan(b, jcs[0][1]);
  if (!(aIsSuitablePredicateScan || bIsSuitablePredicateScan)) {
    return std::nullopt;
  }
  auto hasPredicateScanTree = aIsSuitablePredicateScan ? a._qet : b._qet;
  auto otherTree = aIsSuitablePredicateScan ? b._qet : a._qet;
  size_t otherTreeJoinColumn = aIsSuitablePredicateScan ? jcs[0][1] : jcs[0][0];
  auto qec = otherTree->getRootOperation()->getExecutionContext();
  // Note that this is a new operation.
  auto object = static_cast<HasPredicateScan*>(
                    hasPredicateScanTree->getRootOperation().get())
                    ->getObject()
                    .getVariable();
  auto plan = makeSubtreePlan<HasPredicateScan>(
      qec, std::move(otherTree), otherTreeJoinColumn, std::move(object));
  mergeSubtreePlanIds(plan, a, b);
  return plan;
}

// _____________________________________________________________________
auto QueryPlanner::createJoinWithPathSearch(const SubtreePlan& a,
                                            const SubtreePlan& b,
                                            const JoinColumns& jcs)
    -> std::optional<SubtreePlan> {
  auto aRootOp =
      std::dynamic_pointer_cast<PathSearch>(a._qet->getRootOperation());
  auto bRootOp =
      std::dynamic_pointer_cast<PathSearch>(b._qet->getRootOperation());

  // Exactly one of the two Operations can be a path search.
  if (static_cast<bool>(aRootOp) == static_cast<bool>(bRootOp)) {
    return std::nullopt;
  }

  auto pathSearch = aRootOp ? aRootOp : bRootOp;
  auto sibling = bRootOp ? a : b;

  auto decideColumns = [aRootOp](std::array<ColumnIndex, 2> joinColumns)
      -> std::pair<ColumnIndex, ColumnIndex> {
    auto thisCol = aRootOp ? joinColumns[0] : joinColumns[1];
    auto otherCol = aRootOp ? joinColumns[1] : joinColumns[0];
    return {thisCol, otherCol};
  };

  // Only source and target may be bound directly
  if (jcs.size() > 2) {
    return std::nullopt;
  }

  auto sourceColumn = pathSearch->getSourceColumn();
  auto targetColumn = pathSearch->getTargetColumn();

  // Either source or target column have to be a variable to create a join
  if (!sourceColumn && !targetColumn) {
    return std::nullopt;
  }

  // A join on an edge property column should not create any candidates
  auto isJoinOnSourceOrTarget = [sourceColumn,
                                 targetColumn](size_t joinColumn) {
    return ((sourceColumn && sourceColumn.value() == joinColumn) ||
            (targetColumn && targetColumn.value() == joinColumn));
  };

  if (jcs.size() == 2) {
    // To join source and target, both must be variables
    if (!sourceColumn || !targetColumn) {
      return std::nullopt;
    }

    auto [firstCol, firstOtherCol] = decideColumns(jcs[0]);

    auto [secondCol, secondOtherCol] = decideColumns(jcs[1]);

    if (!isJoinOnSourceOrTarget(firstCol) &&
        !isJoinOnSourceOrTarget(secondCol)) {
      return std::nullopt;
    }

    if (sourceColumn == firstCol && targetColumn == secondCol) {
      pathSearch->bindSourceAndTargetSide(sibling._qet, firstOtherCol,
                                          secondOtherCol);
    } else if (sourceColumn == secondCol && targetColumn == firstCol) {
      pathSearch->bindSourceAndTargetSide(sibling._qet, secondOtherCol,
                                          firstOtherCol);
    } else {
      return std::nullopt;
    }
  } else if (jcs.size() == 1) {
    auto [thisCol, otherCol] = decideColumns(jcs[0]);

    if (!isJoinOnSourceOrTarget(thisCol)) {
      return std::nullopt;
    }

    if (sourceColumn && sourceColumn == thisCol &&
        !pathSearch->isSourceBound()) {
      pathSearch->bindSourceSide(sibling._qet, otherCol);
    } else if (targetColumn && targetColumn == thisCol &&
               !pathSearch->isTargetBound()) {
      pathSearch->bindTargetSide(sibling._qet, otherCol);
    }
  } else {
    return std::nullopt;
  }

  SubtreePlan plan = makeSubtreePlan(pathSearch);
  mergeSubtreePlanIds(plan, a, b);
  return plan;
}

// _____________________________________________________________________
void QueryPlanner::QueryGraph::setupGraph(
    const std::vector<SubtreePlan>& leafOperations) {
  // Prepare the `nodes_` vector for the graph. We have one node for each leaf
  // of what later becomes the `QueryExecutionTree`.
  for (const auto& leafOperation : leafOperations) {
    nodes_.push_back(std::make_shared<Node>(&leafOperation));
  }

  // Set up a hash map from variables to nodes that contain this variable.
  const ad_utility::HashMap<Variable, std::vector<Node*>> varToNode = [this]() {
    ad_utility::HashMap<Variable, std::vector<Node*>> result;
    for (const auto& node : nodes_) {
      for (const auto& var :
           node->plan_->_qet->getVariableColumns() | ql::views::keys) {
        result[var].push_back(node.get());
      }
    }
    return result;
  }();
  // Set up a hash map from nodes to their adjacentNodes_. Two nodes are
  // adjacent if they share a variable. The adjacentNodes_ are stored as hash
  // sets so we don't need to worry about duplicates.
  ad_utility::HashMap<Node*, ad_utility::HashSet<Node*>> adjacentNodes =
      [&varToNode]() {
        ad_utility::HashMap<Node*, ad_utility::HashSet<Node*>> result;
        for (auto& nodesThatContainSameVar : varToNode | ql::views::values) {
          // TODO<C++23> Use ql::views::cartesian_product
          for (auto* n1 : nodesThatContainSameVar) {
            for (auto* n2 : nodesThatContainSameVar) {
              if (n1 != n2) {
                result[n1].insert(n2);
                result[n2].insert(n1);
              }
            }
          }
        }
        return result;
      }();
  // For each node move the set of adjacentNodes_ from the global hash map to
  // the node itself.
  for (const auto& node : nodes_) {
    if (adjacentNodes.contains(node.get())) {
      node->adjacentNodes_ = std::move(adjacentNodes.at(node.get()));
    }
  }
}

// _______________________________________________________________
void QueryPlanner::QueryGraph::dfs(Node* startNode, size_t componentIndex) {
  // Simple recursive DFS.
  if (startNode->visited_) {
    return;
  }
  startNode->componentIndex_ = componentIndex;
  startNode->visited_ = true;
  for (auto* adjacentNode : startNode->adjacentNodes_) {
    dfs(adjacentNode, componentIndex);
  }
}
// _______________________________________________________________
std::vector<size_t> QueryPlanner::QueryGraph::dfsForAllNodes() {
  std::vector<size_t> result;
  size_t nextIndex = 0;
  for (const auto& node : nodes_) {
    if (node->visited_) {
      // The node is part of a connected component that was already found.
      result.push_back(node->componentIndex_);
    } else {
      // The node is part of a yet unknown component, run a DFS.
      dfs(node.get(), nextIndex);
      result.push_back(node->componentIndex_);
      ++nextIndex;
    }
  }
  return result;
}

// _______________________________________________________________
void QueryPlanner::checkCancellation(
    ad_utility::source_location location) const {
  cancellationHandle_->throwIfCancelled(location);
}

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::visitGroupOptionalOrMinus(
    std::vector<SubtreePlan>&& candidates) {
  // Empty group graph patterns should have been handled previously.
  AD_CORRECTNESS_CHECK(!candidates.empty());

  // Optionals that occur before any of their variables have been bound,
  // actually behave like ordinary (Group)GraphPatterns.
  auto variables = candidates[0]._qet->getVariableColumns() | ql::views::keys;

  using enum SubtreePlan::Type;
  if (auto type = candidates[0].type;
      (type == OPTIONAL || type == MINUS) &&
      ql::ranges::all_of(variables, [this](const Variable& var) {
        return !boundVariables_.contains(var);
      })) {
    // A MINUS clause that doesn't share any variable with the preceding
    // patterns behaves as if it isn't there.
    if (type == MINUS) {
      return;
    }

    // All variables in the OPTIONAL are unbound so far, so this OPTIONAL
    // actually is not an OPTIONAL.
    for (auto& vec : candidates) {
      vec.type = SubtreePlan::BASIC;
    }
  }

  // All variables seen so far are considered bound and cannot appear as the
  // RHS of a BIND operation. This is also true for variables from OPTIONALs
  // and MINUS clauses (this used to be a bug in an old version of the code).
  ql::ranges::for_each(
      variables, [this](const Variable& var) { boundVariables_.insert(var); });

  // If our input is not OPTIONAL and not a MINUS, this means that we can still
  // arbitrarily optimize among our candidates and just append our new
  // candidates.
  if (candidates[0].type == SubtreePlan::BASIC) {
    candidatePlans_.push_back(std::move(candidates));
    return;
  }

  // For OPTIONAL or MINUS, optimization "across" the OPTIONAL or MINUS is
  // forbidden. Optimize all previously collected candidates, and then perform
  // an optional or minus join.
  optimizeCommutatively();
  AD_CORRECTNESS_CHECK(candidatePlans_.size() == 1);
  std::vector<SubtreePlan> nextCandidates;
  // For each candidate plan, and each plan from the OPTIONAL or MINUS, create
  // a new plan with an optional join. Note that `createJoinCandidates` will
  // whether `b` is from an OPTIONAL or MINUS.
  for (const auto& a : candidatePlans_.at(0)) {
    for (const auto& b : candidates) {
      auto vec = planner_.createJoinCandidates(a, b, boost::none);
      nextCandidates.insert(nextCandidates.end(),
                            std::make_move_iterator(vec.begin()),
                            std::make_move_iterator(vec.end()));
    }
  }

  // Keep the best found candidate, which can then be combined with potentially
  // following children, until we hit the next OPTIONAL or MINUS.
  // TODO<joka921> Also keep one candidate per ordering to make even
  // better plans at this step
  AD_CORRECTNESS_CHECK(
      !nextCandidates.empty(),
      "Could not find a single candidate join for two optimized graph "
      "patterns. Please report this to the developers");
  auto idx = planner_.findCheapestExecutionTree(nextCandidates);
  candidatePlans_.clear();
  candidatePlans_.push_back({std::move(nextCandidates[idx])});
}

// ____________________________________________________________
template <typename Arg>
void QueryPlanner::GraphPatternPlanner::graphPatternOperationVisitor(Arg& arg) {
  using T = std::decay_t<Arg>;
  if constexpr (std::is_same_v<T, p::Optional> ||
                std::is_same_v<T, p::GroupGraphPattern>) {
    // If this is a `GRAPH <graph> {...}` clause, then we have to overwrite the
    // default graphs while planning this clause, and reset them when leaving
    // the clause.
    std::optional<ParsedQuery::DatasetClauses> datasetBackup;
    std::optional<Variable> graphVariableBackup = planner_.activeGraphVariable_;
    if constexpr (std::is_same_v<T, p::GroupGraphPattern>) {
      if (std::holds_alternative<TripleComponent::Iri>(arg.graphSpec_)) {
        datasetBackup = planner_.activeDatasetClauses_;
        planner_.activeDatasetClauses_.defaultGraphs_.emplace(
            {std::get<TripleComponent::Iri>(arg.graphSpec_)});
      } else if (std::holds_alternative<Variable>(arg.graphSpec_)) {
        const auto& graphVar = std::get<Variable>(arg.graphSpec_);
        if (checkUsePatternTrick::isVariableContainedInGraphPattern(
                graphVar, arg._child, nullptr)) {
          throw std::runtime_error(
              "A variable that is used as the graph specifier of a `GRAPH ?var "
              "{...}` clause may not appear in the body of that clause");
        }
        datasetBackup = planner_.activeDatasetClauses_;
        planner_.activeDatasetClauses_.defaultGraphs_ =
            planner_.activeDatasetClauses_.namedGraphs_;
        // We already have backed up the `activeGraphVariable_`.
        planner_.activeGraphVariable_ = std::get<Variable>(arg.graphSpec_);
      } else {
        AD_CORRECTNESS_CHECK(
            std::holds_alternative<std::monostate>(arg.graphSpec_));
      }
    }

    auto candidates = planner_.optimize(&arg._child);
    if constexpr (std::is_same_v<T, p::Optional>) {
      for (auto& c : candidates) {
        c.type = SubtreePlan::OPTIONAL;
      }
    }
    visitGroupOptionalOrMinus(std::move(candidates));
    if (datasetBackup.has_value()) {
      planner_.activeDatasetClauses_ = std::move(datasetBackup.value());
    }
    planner_.activeGraphVariable_ = std::move(graphVariableBackup);
  } else if constexpr (std::is_same_v<T, p::Union>) {
    visitUnion(arg);
  } else if constexpr (std::is_same_v<T, p::Subquery>) {
    visitSubquery(arg);
  } else if constexpr (std::is_same_v<T, p::TransPath>) {
    return visitTransitivePath(arg);
  } else if constexpr (std::is_same_v<T, p::Values>) {
    SubtreePlan valuesPlan = makeSubtreePlan<Values>(qec_, arg._inlineValues);
    visitGroupOptionalOrMinus(std::vector{std::move(valuesPlan)});
  } else if constexpr (std::is_same_v<T, p::Service>) {
    SubtreePlan servicePlan = makeSubtreePlan<Service>(qec_, arg);
    visitGroupOptionalOrMinus(std::vector{std::move(servicePlan)});
  } else if constexpr (std::is_same_v<T, p::Bind>) {
    visitBind(arg);
  } else if constexpr (std::is_same_v<T, p::Minus>) {
    auto candidates = planner_.optimize(&arg._child);
    for (auto& c : candidates) {
      c.type = SubtreePlan::MINUS;
    }
    visitGroupOptionalOrMinus(std::move(candidates));
  } else if constexpr (std::is_same_v<T, p::PathQuery>) {
    visitPathSearch(arg);
  } else if constexpr (std::is_same_v<T, p::Describe>) {
    visitDescribe(arg);
  } else if constexpr (std::is_same_v<T, p::SpatialQuery>) {
    visitSpatialSearch(arg);
  } else {
    static_assert(std::is_same_v<T, p::BasicGraphPattern>);
    visitBasicGraphPattern(arg);
  }
};

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::visitBasicGraphPattern(
    const parsedQuery::BasicGraphPattern& v) {
  // A basic graph patterns consists only of triples. First collect all
  // the bound variables.
  for (const SparqlTriple& t : v._triples) {
    if (isVariable(t.s_)) {
      boundVariables_.insert(t.s_.getVariable());
    }
    if (isVariable(t.p_)) {
      boundVariables_.insert(Variable{t.p_.iri_});
    }
    if (isVariable(t.o_)) {
      boundVariables_.insert(t.o_.getVariable());
    }
  }

  // Then collect the triples. Transform each triple with a property path to
  // an equivalent form without property path (using `seedFromPropertyPath`).
  for (const auto& triple : v._triples) {
    if (triple.p_.operation_ == PropertyPath::Operation::IRI) {
      candidateTriples_._triples.push_back(triple);
    } else {
      auto children =
          planner_.seedFromPropertyPath(triple.s_, triple.p_, triple.o_);
      for (auto& child : children._graphPatterns) {
        std::visit([self = this](
                       auto& arg) { self->graphPatternOperationVisitor(arg); },
                   child);
      }
    }
  }
}

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::visitBind(const parsedQuery::Bind& v) {
  if (boundVariables_.contains(v._target)) {
    AD_THROW(
        "The target variable of a BIND must not be used before the "
        "BIND clause");
  }
  boundVariables_.insert(v._target);

  // Assumption for now: BIND does not commute. This is always safe.
  optimizeCommutatively();
  AD_CORRECTNESS_CHECK(candidatePlans_.size() == 1);
  auto lastRow = std::move(candidatePlans_.at(0));
  candidatePlans_.at(0).clear();
  for (const auto& a : lastRow) {
    // Add the query plan for the BIND.
    SubtreePlan plan = makeSubtreePlan<Bind>(qec_, a._qet, v);
    plan._idsOfIncludedFilters = a._idsOfIncludedFilters;
    plan.idsOfIncludedTextLimits_ = a.idsOfIncludedTextLimits_;
    candidatePlans_.back().push_back(std::move(plan));
  }
  // Handle the case where the BIND clause is the first clause (which is
  // equivalent to `lastRow` being empty).
  if (lastRow.empty()) {
    auto neutralElement = makeExecutionTree<NeutralElementOperation>(qec_);
    candidatePlans_.back().push_back(
        makeSubtreePlan<Bind>(qec_, std::move(neutralElement), v));
  }
}

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::visitTransitivePath(
    parsedQuery::TransPath& arg) {
  auto candidatesIn = planner_.optimize(&arg._childGraphPattern);
  std::vector<SubtreePlan> candidatesOut;

  for (auto& sub : candidatesIn) {
    TransitivePathSide left;
    TransitivePathSide right;
    auto getSideValue =
        [this](const TripleComponent& side) -> std::variant<Id, Variable> {
      if (isVariable(side)) {
        return side.getVariable();
      } else {
        if (auto opt = side.toValueId(planner_._qec->getIndex().getVocab());
            opt.has_value()) {
          return opt.value();
        } else {
          AD_THROW("No vocabulary entry for " + side.toString());
        }
      }
    };

    left.subCol_ = sub._qet->getVariableColumn(arg._innerLeft.getVariable());
    left.value_ = getSideValue(arg._left);
    right.subCol_ = sub._qet->getVariableColumn(arg._innerRight.getVariable());
    right.value_ = getSideValue(arg._right);
    size_t min = arg._min;
    size_t max = arg._max;
    auto transitivePath = TransitivePathBase::makeTransitivePath(
        qec_, std::move(sub._qet), std::move(left), std::move(right), min, max);
    auto plan = makeSubtreePlan<TransitivePathBase>(std::move(transitivePath));
    candidatesOut.push_back(std::move(plan));
  }
  visitGroupOptionalOrMinus(std::move(candidatesOut));
}

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::visitPathSearch(
    parsedQuery::PathQuery& pathQuery) {
  const auto& vocab = planner_._qec->getIndex().getVocab();
  auto config = pathQuery.toPathSearchConfiguration(vocab);

  // The path search requires a child graph pattern
  AD_CORRECTNESS_CHECK(pathQuery.childGraphPattern_.has_value());
  std::vector<SubtreePlan> candidatesIn =
      planner_.optimize(&pathQuery.childGraphPattern_.value());
  std::vector<SubtreePlan> candidatesOut;

  for (auto& sub : candidatesIn) {
    auto pathSearch =
        std::make_shared<PathSearch>(qec_, std::move(sub._qet), config);
    auto plan = makeSubtreePlan<PathSearch>(std::move(pathSearch));
    candidatesOut.push_back(std::move(plan));
  }
  visitGroupOptionalOrMinus(std::move(candidatesOut));
}

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::visitSpatialSearch(
    parsedQuery::SpatialQuery& spatialQuery) {
  auto config = spatialQuery.toSpatialJoinConfiguration();

  // If there is no child graph pattern, we need to construct a neutral element
  std::vector<SubtreePlan> candidatesIn;
  if (spatialQuery.childGraphPattern_.has_value()) {
    candidatesIn = planner_.optimize(&spatialQuery.childGraphPattern_.value());
  } else {
    candidatesIn = {makeSubtreePlan<NeutralElementOperation>(qec_)};
  }
  std::vector<SubtreePlan> candidatesOut;

  for (auto& sub : candidatesIn) {
    // This helper function adds a subtree plan to the output candidates, which
    // either has the child graph pattern as a right child or no child at all.
    // If it has no child at all, the query planner may look for the right child
    // of the SpatialJoin outside of the SERVICE. This is only allowed for max
    // distance joins.
    auto addCandidateSpatialJoin = [this, &sub, &config,
                                    &candidatesOut](bool rightVarOutside) {
      std::optional<std::shared_ptr<QueryExecutionTree>> right = std::nullopt;
      if (!rightVarOutside) {
        right = std::move(sub._qet);
      }
      auto spatialJoin =
          std::make_shared<SpatialJoin>(qec_, config, std::nullopt, right);
      auto plan = makeSubtreePlan<SpatialJoin>(std::move(spatialJoin));
      candidatesOut.push_back(std::move(plan));
    };

    if (spatialQuery.childGraphPattern_.has_value()) {
      // The version using the child graph pattern
      addCandidateSpatialJoin(false);
    } else {
      // The version without using the child graph pattern
      if (std::holds_alternative<MaxDistanceConfig>(config.task_)) {
        addCandidateSpatialJoin(true);
      }
    }
  }
  visitGroupOptionalOrMinus(std::move(candidatesOut));
}

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::visitUnion(parsedQuery::Union& arg) {
  // TODO<joka921> here we could keep all the candidates, and create a
  // "sorted union" by merging as additional candidates if the inputs
  // are presorted.
  SubtreePlan left = optimizeSingle(&arg._child1);
  SubtreePlan right = optimizeSingle(&arg._child2);

  // create a new subtree plan
  SubtreePlan candidate =
      makeSubtreePlan<Union>(planner_._qec, left._qet, right._qet);
  visitGroupOptionalOrMinus(std::vector{std::move(candidate)});
}

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::visitSubquery(
    parsedQuery::Subquery& arg) {
  ParsedQuery& subquery = arg.get();
  // TODO<joka921> We currently do not optimize across subquery borders
  // but abuse them as "optimization hints". In theory, one could even
  // remove the ORDER BY clauses of a subquery if we can prove that
  // the results will be reordered anyway.

  // For a subquery, make sure that one optimal result for each ordering
  // of the result (by a single column) is contained.
  auto candidatesForSubquery = planner_.createExecutionTrees(subquery, true);
  // Make sure that variables that are not selected by the subquery are
  // not visible.
  auto setSelectedVariables = [&](SubtreePlan& plan) {
    auto selectedVariables = arg.get().selectClause().getSelectedVariables();
    // TODO<C++23> Use `optional::transform`
    if (planner_.activeGraphVariable_.has_value()) {
      const auto& graphVar = planner_.activeGraphVariable_.value();
      AD_CORRECTNESS_CHECK(
          !ad_utility::contains(selectedVariables, graphVar),
          "This case (variable of GRAPH ?var {...} appears also in the body) "
          "should have thrown further up in the call stack");
      selectedVariables.push_back(graphVar);
    }
    plan._qet->getRootOperation()->setSelectedVariablesForSubquery(
        selectedVariables);
  };
  ql::ranges::for_each(candidatesForSubquery, setSelectedVariables);
  // A subquery must also respect LIMIT and OFFSET clauses
  ql::ranges::for_each(candidatesForSubquery, [&](SubtreePlan& plan) {
    plan._qet->setLimit(arg.get()._limitOffset);
  });
  visitGroupOptionalOrMinus(std::move(candidatesForSubquery));
}
// _______________________________________________________________

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::optimizeCommutatively() {
  auto tg = planner_.createTripleGraph(&candidateTriples_);
  auto lastRow = planner_
                     .fillDpTab(tg, rootPattern_->_filters,
                                rootPattern_->textLimits_, candidatePlans_)
                     .back();
  candidateTriples_._triples.clear();
  candidatePlans_.clear();
  candidatePlans_.push_back(std::move(lastRow));
  planner_.checkCancellation();
}

// _______________________________________________________________
void QueryPlanner::GraphPatternPlanner::visitDescribe(
    parsedQuery::Describe& describe) {
  auto tree = std::make_shared<QueryExecutionTree>(
      planner_.createExecutionTree(describe.whereClause_.get(), true));
  auto describeOp =
      makeSubtreePlan<Describe>(planner_._qec, std::move(tree), describe);
  candidatePlans_.push_back(std::vector{std::move(describeOp)});
  planner_.checkCancellation();
}
