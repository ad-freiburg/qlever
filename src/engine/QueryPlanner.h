// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_QUERYPLANNER_H
#define QLEVER_SRC_ENGINE_QUERYPLANNER_H

#include <boost/optional.hpp>
#include <vector>

#include "engine/CheckUsePatternTrick.h"
#include "engine/QueryExecutionTree.h"
#include "parser/GraphPattern.h"
#include "parser/GraphPatternOperation.h"
#include "parser/ParsedQuery.h"
#include "parser/data/Types.h"

class QueryPlanner {
  using TextLimitMap =
      ad_utility::HashMap<Variable, parsedQuery::TextLimitMetaObject>;
  using TextLimitVec =
      std::vector<std::pair<Variable, parsedQuery::TextLimitMetaObject>>;
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  template <typename T>
  using vector = std::vector<T>;

  ParsedQuery::DatasetClauses activeDatasetClauses_;
  // The variable of the innermost `GRAPH ?var` clause that the planner
  // currently is planning.
  // Note: The behavior of only taking the innermost graph variable into account
  // for nested `GRAPH` clauses is compliant with SPARQL 1.1.
  std::optional<Variable> activeGraphVariable_;
  // Store a flag that decides if only named graphs or all graphs including the
  // default graph are supposed to be bound to the graph variable.
  parsedQuery::GroupGraphPattern::GraphVariableBehaviour
      defaultGraphBehaviour_ =
          parsedQuery::GroupGraphPattern::GraphVariableBehaviour::ALL;

 public:
  using JoinColumns = std::vector<std::array<ColumnIndex, 2>>;

  explicit QueryPlanner(QueryExecutionContext* qec,
                        CancellationHandle cancellationHandle);

  virtual ~QueryPlanner() = default;

  // Create the best execution tree for the given query according to the
  // optimization algorithm and cost estimates of the QueryPlanner.
  QueryExecutionTree createExecutionTree(ParsedQuery& pq,
                                         bool isSubquery = false);

  class TripleGraph {
   public:
    TripleGraph();

    TripleGraph(const TripleGraph& other);

    TripleGraph& operator=(const TripleGraph& other);

    TripleGraph(const TripleGraph& other, vector<size_t> keepNodes);

    struct Node {
      Node(size_t id, SparqlTriple t,
           std::optional<Variable> graphVariable = std::nullopt)
          : id_(id), triple_(std::move(t)) {
        triple_.forEachVariable(
            [this](const auto& var) { _variables.insert(var); });
        if (graphVariable.has_value()) {
          _variables.insert(std::move(graphVariable).value());
        }
      }

      Node(size_t id, Variable cvar, std::string word, SparqlTriple t)
          : Node(id, std::move(t)) {
        cvar_ = std::move(cvar);
        wordPart_ = std::move(word);
      }

      Node(const Node& other) = default;

      Node& operator=(const Node& other) = default;

      // Returns true if the two nodes equal apart from the id
      // and the order of variables
      bool isSimilar(const Node& other) const {
        return triple_ == other.triple_ && cvar_ == other.cvar_ &&
               wordPart_ == other.wordPart_ && _variables == other._variables;
      }

      bool isTextNode() const { return cvar_.has_value(); }

      friend std::ostream& operator<<(std::ostream& out, const Node& n) {
        out << "id: " << n.id_ << " triple: " << n.triple_.asString()
            << " vars_ ";
        for (const auto& s : n._variables) {
          out << s.name() << ", ";
        }
        // TODO<joka921> Should the `cvar` and the `wordPart` be stored
        // together?
        if (n.cvar_.has_value()) {
          out << " cvar " << n.cvar_.value().name() << " wordPart "
              << n.wordPart_.value();
        }
        return out;
      }

      size_t id_;
      SparqlTriple triple_;
      ad_utility::HashSet<Variable> _variables;
      std::optional<Variable> cvar_ = std::nullopt;
      std::optional<std::string> wordPart_ = std::nullopt;
    };

    // Allows for manually building triple graphs for testing
    explicit TripleGraph(
        const std::vector<std::pair<Node, std::vector<size_t>>>& init);

    // Checks for id and order independent equality
    bool isSimilar(const TripleGraph& other) const;
    std::string asString() const;

    bool isTextNode(size_t i) const;

    vector<vector<size_t>> _adjLists;
    ad_utility::HashMap<size_t, Node*> _nodeMap;
    std::list<TripleGraph::Node> _nodeStorage;

    vector<size_t> bfsLeaveOut(size_t startNode,
                               ad_utility::HashSet<size_t> leaveOut) const;

   private:
    vector<std::pair<TripleGraph, vector<SparqlFilter>>> splitAtContextVars(
        const vector<SparqlFilter>& origFilters,
        ad_utility::HashMap<std::string, vector<size_t>>& contextVarTotextNodes)
        const;

    vector<SparqlFilter> pickFilters(const vector<SparqlFilter>& origFilters,
                                     const vector<size_t>& nodes) const;
  };

  class SubtreePlan {
   public:
    enum Type { BASIC, OPTIONAL, MINUS };

    explicit SubtreePlan(QueryExecutionContext* qec)
        : _qet(std::make_shared<QueryExecutionTree>(qec)) {}

    template <typename Operation>
    SubtreePlan(QueryExecutionContext* qec,
                std::shared_ptr<Operation> operation)
        : _qet{std::make_shared<QueryExecutionTree>(qec,
                                                    std::move(operation))} {}

    std::shared_ptr<QueryExecutionTree> _qet;
    std::shared_ptr<Result> _cachedResult;
    uint64_t _idsOfIncludedNodes = 0;
    uint64_t _idsOfIncludedFilters = 0;
    uint64_t idsOfIncludedTextLimits_ = 0;
    bool containsFilterSubstitute_ = false;
    Type type = Type::BASIC;

    size_t getCostEstimate() const;

    size_t getSizeEstimate() const;
  };

  // This struct represents a single SPARQL FILTER. Additionally, it has the
  // option to also store a subtree plan, which is semantically equivalent to
  // the filter, but might be cheaper to compute. This is currently used to
  // rewrite FILTERs on geof: functions to spatial join operations.
  struct FilterAndOptionalSubstitute {
    SparqlFilter filter_;
    std::optional<SubtreePlan> substitute_;

    bool hasSubstitute() const { return substitute_.has_value(); }
  };

  using FiltersAndOptionalSubstitutes =
      std::vector<FilterAndOptionalSubstitute>;

  // A helper class to find connected components of an RDF query using DFS.
  class QueryGraph {
   private:
    // A simple class to represent a graph node as well as some data for a DFS.
    class Node {
     public:
      const SubtreePlan* plan_;
      ad_utility::HashSet<Node*> adjacentNodes_{};
      // Was this node already visited during DFS.
      bool visited_ = false;
      // Index of the connected component of this node (will be set to a value
      // >= 0 by the DFS.
      int64_t componentIndex_ = -1;
      // Construct from a non-owning pointer.
      explicit Node(const SubtreePlan* plan) : plan_{plan} {}
    };
    // Storage for all the `Node`s that a graph contains.
    std::vector<std::shared_ptr<Node>> nodes_;

    // Default constructor
    QueryGraph() = default;

   public:
    // Return the indices of the connected component for each of the `node`s.
    // The return value will have exactly the same size as `node`s and
    // `result[i]` will be the index of the connected component of `nodes[i]`.
    // The connected components will be contiguous and start at 0.
    static std::vector<size_t> computeConnectedComponents(
        const std::vector<SubtreePlan>& nodes,
        const FiltersAndOptionalSubstitutes& filtersAndOptionalSubstitutes) {
      QueryGraph graph;
      graph.setupGraph(nodes, filtersAndOptionalSubstitutes);
      return graph.dfsForAllNodes();
    }

   private:
    // The actual implementation of `setupGraph`. First build a
    // graph from the `leafOperations` and then run DFS and return the result.
    void setupGraph(
        const std::vector<SubtreePlan>& leafOperations,
        const FiltersAndOptionalSubstitutes& filtersAndOptionalSubstitutes);

    // Run a single DFS startint at the `startNode`. All nodes that are
    // connected to this node (including the node itself) will have
    // `visited_ == true` and  `componentIndex_ == componentIndex` after the
    // call. Only works if `dfs` hasn't been called before on the `startNode` or
    // any node connected to it. (Exceptions to this rule are the recursive
    // calls from `dfs` itself).
    void dfs(Node* startNode, size_t componentIndex);

    // Run `dfs` repeatedly on nodes that were so far undiscovered until all
    // nodes are discovered, which means that all connected components have been
    // identified. Then return the indices of the connected components.
    std::vector<size_t> dfsForAllNodes();
  };

  TripleGraph createTripleGraph(
      const parsedQuery::BasicGraphPattern* pattern) const;

  void addNodeToTripleGraph(const TripleGraph::Node&, TripleGraph&) const;

  void setEnablePatternTrick(bool enablePatternTrick);

  // Create a set of possible execution trees for the given parsed query. The
  // best (cheapest) execution tree according to the QueryPlanner is part of
  // that set. When the query has no `ORDER BY` clause, the set contains one
  // optimal execution tree for each possible ordering (by one column) of the
  // result. This is relevant for subqueries, which are currently optimized
  // independently of the rest of the query, but where it depends on the rest
  // of the query, which ordering of the result is best.
  std::vector<SubtreePlan> createExecutionTrees(ParsedQuery& pq,
                                                bool isSubquery = false);

 protected:
  QueryExecutionContext* getQec() const { return _qec; }

 private:
  QueryExecutionContext* _qec;

  // Used to count the number of unique variables created using
  // generateUniqueVarName
  size_t _internalVarCount = 0;

  bool _enablePatternTrick = true;

  CancellationHandle cancellationHandle_;

  std::optional<size_t> textLimit_ = std::nullopt;

  // Used to collect warnings (created by the parser or the query planner) which
  // are then passed on to the created `QueryExecutionTree` such that they can
  // be reported as part of the query result if desired.
  std::vector<std::string> warnings_;

  std::vector<QueryPlanner::SubtreePlan> optimize(
      ParsedQuery::GraphPattern* rootPattern);

  // Add all the possible index scans for the triple represented by the node.
  // The triple is "ordinary" in the sense that it is neither a text triple with
  // ql:contains-word nor a special pattern trick triple.
  template <typename AddedIndexScanFunction, typename AddFilterFunction>
  void seedFromOrdinaryTriple(const TripleGraph::Node& node,
                              const AddedIndexScanFunction& addIndexScan,
                              const AddFilterFunction& addFilter);

  // Helper function used by the seedFromOrdinaryTriple function
  template <typename AddedIndexScanFunction>
  void indexScanSingleVarCase(const SparqlTripleSimple& triple,
                              const AddedIndexScanFunction& addIndexScan) const;

  // Helper function used by the seedFromOrdinaryTriple function
  template <typename AddedIndexScanFunction, typename AddedFilter>
  void indexScanTwoVarsCase(const SparqlTripleSimple& triple,
                            const AddedIndexScanFunction& addIndexScan,
                            const AddedFilter& addFilter);

  // Helper function used by the seedFromOrdinaryTriple function
  template <typename AddedIndexScanFunction, typename AddedFilter>
  void indexScanThreeVarsCase(const SparqlTripleSimple& triple,
                              const AddedIndexScanFunction& addIndexScan,
                              const AddedFilter& addFilter);

  /**
   * @brief Fills children with all operations that are associated with a single
   * node in the triple graph (e.g. IndexScans).
   */
  struct PlansAndFilters {
    std::vector<SubtreePlan> plans_;
    std::vector<SparqlFilter> filters_;
  };

  PlansAndFilters seedWithScansAndText(
      const TripleGraph& tg,
      const vector<vector<QueryPlanner::SubtreePlan>>& children,
      TextLimitMap& textLimits);

  // Function for optimization query rewrites: The function returns pairs of
  // filters with the corresponding substitute subtree plan. This is currently
  // used to translate GeoSPARQL filters to spatial join operations.
  virtual FiltersAndOptionalSubstitutes seedFilterSubstitutes(
      const std::vector<SparqlFilter>& filters) const;

  // TODO<RobinTF> Extract to dedicated module, this has little to do with
  // actual query planning.
  // Turn a generic `PropertyPath` into a `GraphPattern` that can be used for
  // further planning.
  ParsedQuery::GraphPattern seedFromPropertyPath(const TripleComponent& left,
                                                 const PropertyPath& path,
                                                 const TripleComponent& right);

  // Turn a sequence of `PropertyPath`s into a `GraphPattern` that can be used
  // for further planning. This handles the case for predicates separated by
  // `/`, for example in `SELECT ?x { ?x <a>/<b> ?y }`.
  ParsedQuery::GraphPattern seedFromSequence(
      const TripleComponent& left, const std::vector<PropertyPath>& paths,
      const TripleComponent& right);

  // Turn a union of `PropertyPath`s into a `GraphPattern` that can be used for
  // further planning. This handles the case for predicates separated by `|`,
  // for example in `SELECT ?x { ?x <a>|<b> ?y }`.
  ParsedQuery::GraphPattern seedFromAlternative(
      const TripleComponent& left, const std::vector<PropertyPath>& paths,
      const TripleComponent& right);

  // Create `GraphPattern` for property paths of the form `<a>+`, `<a>?` or
  // `<a>*`, where `<a>` can also be a complex `PropertyPath` (e.g. a sequence
  // or an alternative).
  ParsedQuery::GraphPattern seedFromTransitive(const TripleComponent& left,
                                               const PropertyPath& path,
                                               const TripleComponent& right,
                                               size_t min, size_t max);
  // Create `GraphPattern` for property paths of the form `!(<a> | ^<b>)` or
  // `!<a>` and similar.
  ParsedQuery::GraphPattern seedFromNegated(
      const TripleComponent& left, const std::vector<PropertyPath>& paths,
      const TripleComponent& right);
  static ParsedQuery::GraphPattern seedFromVarOrIri(
      const TripleComponent& left,
      const ad_utility::sparql_types::VarOrIri& varOrIri,
      const TripleComponent& right);

  Variable generateUniqueVarName();

  // Creates a tree of unions with the given patterns as the trees leaves
  static ParsedQuery::GraphPattern uniteGraphPatterns(
      std::vector<ParsedQuery::GraphPattern>&& patterns);

  /**
   * @brief Merges two rows of the dp optimization table using various types of
   * joins.
   * @return A new row for the dp table that contains plans created by joining
   * the result of a plan in a and a plan in b.
   */
  vector<SubtreePlan> merge(const vector<SubtreePlan>& a,
                            const vector<SubtreePlan>& b,
                            const TripleGraph& tg) const;

  // Create `SubtreePlan`s that join `a` and `b` together. The columns are
  // computed automatically.
  std::vector<SubtreePlan> createJoinCandidates(
      const SubtreePlan& a, const SubtreePlan& b,
      boost::optional<const TripleGraph&> tg) const;

  // Create `SubtreePlan`s that join `a` and `b` together. The columns are
  // configured by `jcs`.
  std::vector<SubtreePlan> createJoinCandidates(const SubtreePlan& a,
                                                const SubtreePlan& b,
                                                const JoinColumns& jcs) const;

  // Same as `createJoinCandidates(SubtreePlan, SubtreePlan, JoinColumns)`, but
  // creates a cartesian product when `jcs` is empty.
  std::vector<SubtreePlan> createJoinCandidatesAllowEmpty(
      const SubtreePlan& a, const SubtreePlan& b, const JoinColumns& jcs) const;

  // Whenever a join is applied to a `Union`, add candidates that try applying
  // join to the children of the union directly, which can be more efficient if
  // one of the children has an optimized join, which can happen for
  // `TransitivePath` for example.
  std::vector<SubtreePlan> applyJoinDistributivelyToUnion(
      const SubtreePlan& a, const SubtreePlan& b, const JoinColumns& jcs) const;

  // Return a pair of join columns (the first from the transitive path
  // operation, the second from the other operation with which the result of the
  // transitive path operation is joined). Otherwise return `std::nullopt`, in
  // which case the full transitive path will be computed, If the Boolean
  // `leftSideTransitivePath` is true, the column indices of the transitive path
  // are on the "left side" of the pairs from `jcs`, otherwise they are on the
  // "right side".
  static std::optional<std::tuple<size_t, size_t>>
  getJoinColumnsForTransitivePath(const JoinColumns& jcs,
                                  bool leftSideTransitivePath);

  // Used internally by `createJoinCandidates`. If `a` or `b` is a transitive
  // path operation and the other input can be bound to this transitive path
  // (see `TransitivePath.cpp` for details), then returns that bound transitive
  // path. Else returns `std::nullopt`.
  static std::optional<SubtreePlan> createJoinWithTransitivePath(
      const SubtreePlan& a, const SubtreePlan& b, const JoinColumns& jcs);

  // Used internally by `createJoinCandidates`. If  `a` or `b` is a
  // `HasPredicateScan` with a variable as a subject (`?x ql:has-predicate
  // <VariableOrIri>`) and `a` and `b` can be joined on that subject variable,
  // then returns a `HasPredicateScan` that takes the other input as a subtree.
  // Else returns `std::nullopt`.
  static std::optional<SubtreePlan> createJoinWithHasPredicateScan(
      const SubtreePlan& a, const SubtreePlan& b, const JoinColumns& jcs);

  static std::optional<SubtreePlan> createJoinWithPathSearch(
      const SubtreePlan& a, const SubtreePlan& b, const JoinColumns& jcs);

  // Helper that returns `true` for each of the subtree plans `a` and `b` iff
  // the subtree plan is a spatial join and it is not yet fully constructed
  // (it does not have both children set)
  static std::pair<bool, bool> checkSpatialJoin(const SubtreePlan& a,
                                                const SubtreePlan& b);

  // if one of the inputs is a spatial join which is compatible with the other
  // input, then add that other input to the spatial join as a child instead of
  // creating a normal join.
  static std::optional<SubtreePlan> createSpatialJoin(const SubtreePlan& a,
                                                      const SubtreePlan& b,
                                                      const JoinColumns& jcs);

  vector<SubtreePlan> getOrderByRow(
      const ParsedQuery& pq,
      const std::vector<std::vector<SubtreePlan>>& dpTab) const;

  vector<SubtreePlan> getGroupByRow(
      const ParsedQuery& pq,
      const std::vector<std::vector<SubtreePlan>>& dpTab) const;

  vector<SubtreePlan> getDistinctRow(
      const parsedQuery::SelectClause& selectClause,
      const vector<vector<SubtreePlan>>& dpTab) const;

  vector<SubtreePlan> getPatternTrickRow(
      const parsedQuery::SelectClause& selectClause,
      const vector<vector<SubtreePlan>>& dpTab,
      const checkUsePatternTrick::PatternTrickTuple& patternTrickTuple);

  vector<SubtreePlan> getHavingRow(
      const ParsedQuery& pq, const vector<vector<SubtreePlan>>& dpTab) const;

  // Apply the passed `VALUES` clause to the current plans.
  std::vector<SubtreePlan> applyPostQueryValues(
      const parsedQuery::Values& values,
      const std::vector<SubtreePlan>& currentPlans) const;

  JoinColumns connected(const SubtreePlan& a, const SubtreePlan& b,
                        boost::optional<const TripleGraph&> tg) const;

  static JoinColumns getJoinColumns(const SubtreePlan& a, const SubtreePlan& b);

  std::string getPruningKey(const SubtreePlan& plan,
                            const vector<ColumnIndex>& orderedOnColumns) const;

  // Configure the behavior of the `applyFiltersIfPossible` function below.
  enum class FilterMode {
    // Only apply matching filters, that is filters are only added to plans that
    // already bind all the variables that are used in the filter. The plans
    // with the added filters are added to the candidate set. This mode is used
    // in the dynamic programming approach, where we don't apply the filters
    // greedily. Applying filter substitutes is permitted in this mode.
    KeepUnfiltered,
    // Only apply matching filters (see above), but the plans with added filters
    // replace the plans without filters. This is used in the greedy approach,
    // where filters are always applied as early as possible. Applying filter
    // substitutes is permitted in this mode.
    ReplaceUnfiltered,
    // Same as ReplaceUnfiltered, but do not apply filter substitutes.
    ReplaceUnfilteredNoSubstitutes,
    // Apply all filters (also the nonmatching ones) and replace the unfiltered
    // plans. This has to be called at the end of parsing a group graph pattern
    // where we have to make sure that all filters are applied.
    ApplyAllFiltersAndReplaceUnfiltered,
  };
  template <FilterMode mode = FilterMode::KeepUnfiltered>
  void applyFiltersIfPossible(
      std::vector<SubtreePlan>& row,
      const FiltersAndOptionalSubstitutes& filters) const;

  // Apply text limits if possible.
  // A text limit can be applied to a plan if:
  // 1) There is no text operation for the text record column left.
  // 2) The text limit has not already been applied to the plan.
  void applyTextLimitsIfPossible(std::vector<SubtreePlan>& row,
                                 const TextLimitVec& textLimits,
                                 bool replaceInsteadOfAddPlans) const;

  /**
   * @brief Optimize a set of triples, filters and precomputed candidates
   * for child graph patterns
   *
   *
   * Optimize every GraphPattern starting with the leaves of the
   * GraphPattern tree.

   * Strategy:
   * Create a graph.
   * Each triple corresponds to a node, there is an edge between two nodes
   * iff they share a variable.

   * TripleGraph tg = createTripleGraph(&arg);

   * Each node/triple corresponds to a scan (more than one way possible),
   * each edge corresponds to a possible join.

   * Enumerate and judge possible query plans using a DP table.
   * Each ExecutionTree for a sub-problem gives an estimate:
   * There are estimates for cost and size ( and multiplicity per column).
   * Start bottom up, i.e. with the scans for triples.
   * Always merge two solutions from the table by picking one possible
   * join. A join is possible, if there is an edge between the results.
   * Therefore we keep track of all edges that touch a sub-result.
   * When joining two sub-results, the results edges are those that belong
   * to exactly one of the two input sub-trees.
   * If two of them have the same target, only one out edge is created.
   * All edges that are shared by both subtrees, are checked if they are
   * covered by the join or if an extra filter/select is needed.

   * The algorithm then creates all possible plans for 1 to n triples.
   * To generate a plan for k triples, all subsets between i and k-i are
   * joined.

   * Filters are now added to the mix when building execution plans.
   * Without them, a plan has an execution tree and a set of
   * covered triple nodes.
   * With them, it also has a set of covered filters.
   * A filter can be applied as soon as all variables that occur in the
   * filter Are covered by the query. This is also always the place where
   * this is done.

   * Text operations form cliques (all triples connected via the context
   * cvar). Detect them and turn them into nodes with stored word part and
   * edges to connected variables.

   * Each text operation has two ways how it can be used.
   * 1) As leave in the bottom row of the tab.
   * According to the number of connected variables, the operation creates
   * a cross product with n entities that can be used in subsequent joins.
   * 2) as intermediate unary (downwards) nodes in the execution tree.
   * This is a bit similar to sorts: they can be applied after each step
   * and will filter on one variable.
   * Cycles have to be avoided (by previously removing a triple and using
   * it as a filter later on).
   */
  vector<vector<SubtreePlan>> fillDpTab(
      const TripleGraph& graph, std::vector<SparqlFilter> fs,
      TextLimitMap& textLimits, const vector<vector<SubtreePlan>>& children);

  // Internal subroutine of `fillDpTab` that  only works on a single connected
  // component of the input. Throws if the subtrees in the `connectedComponent`
  // are not in fact connected (via their variables).
  std::vector<QueryPlanner::SubtreePlan>
  runDynamicProgrammingOnConnectedComponent(
      std::vector<SubtreePlan> connectedComponent,
      const FiltersAndOptionalSubstitutes& filters,
      const TextLimitVec& textLimits, const TripleGraph& tg) const;

  // Same as `runDynamicProgrammingOnConnectedComponent`, but uses a greedy
  // algorithm that always greedily chooses the smallest result of the possible
  // join operations using the "Greedy Operator Ordering (GOO)" algorithm.
  std::vector<QueryPlanner::SubtreePlan> runGreedyPlanningOnConnectedComponent(
      std::vector<SubtreePlan> connectedComponent,
      const FiltersAndOptionalSubstitutes& filters,
      const TextLimitVec& textLimits, const TripleGraph& tg) const;

  // Return the number of connected subgraphs is the `graph`, or `budget + 1`,
  // if the number of subgraphs is `> budget`. This is used to analyze the
  // complexity of the query graph and to choose between the DP and the greedy
  // query planner see above.
  // Note: We also need the added filters, because they behave like additional
  // graph nodes wrt the performance of the DP based query planner.
  size_t countSubgraphs(std::vector<const SubtreePlan*> graph,
                        const std::vector<SparqlFilter>& filters,
                        size_t budget);

  // Creates a SubtreePlan for the given text leaf node in the triple graph.
  // While doing this the TextLimitMetaObjects are created and updated according
  // to the text leaf node.
  SubtreePlan getTextLeafPlan(const TripleGraph::Node& node,
                              TextLimitMap& textLimits) const;

  // Given a `MaterializedViewQuery` construct a `SubtreePlan` for an
  // `IndexScan` operation on the requested materialized view.
  SubtreePlan getMaterializedViewIndexScanPlan(
      const parsedQuery::MaterializedViewQuery& viewQuery);

  // An internal helper class that encapsulates the functionality to optimize
  // a single graph pattern. It tightly interacts with the outer `QueryPlanner`
  // for example when optimizing a Subquery.
  struct GraphPatternPlanner {
    // References to the outer planner and the graph pattern that is being
    // optimized.
    QueryPlanner& planner_;
    ParsedQuery::GraphPattern* rootPattern_;
    QueryExecutionContext* qec_;

    // Used to store the set of candidate plans for the already processed parts
    // of the graph pattern. Each row stores different plans for the same graph
    // pattern, and plans from different rows can be joined in an arbitrary
    // order.
    std::vector<std::vector<SubtreePlan>> candidatePlans_{};

    // Triples from BasicGraphPatterns that can be joined arbitrarily
    // with each other and with the contents of  `candidatePlans_`
    parsedQuery::BasicGraphPattern candidateTriples_{};

    // The variables that have been bound by the children of the `rootPattern_`
    // which we have dealt with so far.
    // TODO<joka921> verify that we get no false positives with plans that
    // create no single binding for a variable "by accident".
    ad_utility::HashSet<Variable> boundVariables_{};

    // We remember the potential filter substitutions so we can avoid
    // unnecessarily recomputing them.
    FiltersAndOptionalSubstitutes filtersAndSubst_;

    // ________________________________________________________________________
    GraphPatternPlanner(QueryPlanner& planner,
                        ParsedQuery::GraphPattern* rootPattern)
        : planner_{planner},
          rootPattern_{rootPattern},
          qec_{planner._qec},
          filtersAndSubst_{
              planner.seedFilterSubstitutes(rootPattern->_filters)} {}

    // This function is called for each of the graph patterns that are contained
    // in the `rootPattern_`. It dispatches to the various `visit...`functions
    // below depending on the type of the pattern.
    template <typename T>
    void graphPatternOperationVisitor(T& arg);

    // The following functions all handle a single type of graph pattern.
    // Typically, they create a set of candidate plans for the individual
    // patterns and then add them to the `candidatePlans_` s.t. they can be
    // commutatively joined with other plans.
    void visitBasicGraphPattern(const parsedQuery::BasicGraphPattern& pattern);
    void visitBind(const parsedQuery::Bind& bind);
    void visitTransitivePath(parsedQuery::TransPath& transitivePath);
    void visitPathSearch(parsedQuery::PathQuery& config);
    void visitSpatialSearch(parsedQuery::SpatialQuery& config);
    void visitTextSearch(const parsedQuery::TextSearchQuery& config);
    void visitNamedCachedResult(const parsedQuery::NamedCachedResult& config);
    void visitMaterializedViewQuery(
        const parsedQuery::MaterializedViewQuery& viewQuery);
    void visitUnion(parsedQuery::Union& un);
    void visitSubquery(parsedQuery::Subquery& subquery);
    void visitDescribe(parsedQuery::Describe& describe);

    // Helper function for `visitGroupOptionalOrMinus`. SPARQL queries like
    // `SELECT * { OPTIONAL { ?a ?b ?c }}`, `SELECT * { MINUS { ?a ?b ?c }}` or
    // `SELECT * { ?x ?y ?z . OPTIONAL { ?a ?b ?c }}` need special handling.
    template <typename Variables>
    bool handleUnconnectedMinusOrOptional(std::vector<SubtreePlan>& candidates,
                                          const Variables& variables);

    // This function is called for groups, optional, or minus clauses.
    // The `candidates` are the result of planning the pattern inside the
    // braces. This leads to all of those clauses currently being an
    // optimization border (The braces are planned individually).
    // The distinction between "normal" groups, OPTIONALs and MINUS clauses
    // is made via the type member of the `SubtreePlan`s.
    void visitGroupOptionalOrMinus(std::vector<SubtreePlan>&& candidates);

    // This function finds a set of candidates that unite all the different
    // `candidatePlans_` and `candidateTriples_`. It then replaces the contents
    // of `candidatePlans_` with those plans and clears the `candidateTriples_`.
    // It is called when a non-commuting pattern (like OPTIONAL or BIND) is
    // encountered. We then first optimize the previous candidates using this
    // function, and then combine the result with the OPTIONAL etc. clause.
    void optimizeCommutatively();

    // Find a single best candidate for a given graph pattern.
    template <typename Pattern>
    SubtreePlan optimizeSingle(const Pattern& pattern) {
      auto v = planner_.optimize(pattern);
      auto idx = planner_.findCheapestExecutionTree(v);
      return std::move(v[idx]);
    };
  };

  /**
   * @brief return the index of the cheapest execution tree in the argument.
   *
   * If we are in the unit test mode, this is deterministic by additionally
   * sorting by the cache key when comparing equally cheap indices, else the
   * first element that has the minimum index is returned.
   */
  size_t findCheapestExecutionTree(
      const std::vector<SubtreePlan>& lastRow) const;
  static size_t findSmallestExecutionTree(
      const std::vector<SubtreePlan>& lastRow);
  static size_t findUniqueNodeIds(
      const std::vector<SubtreePlan>& connectedComponent);

  /// if this Planner is not associated with a queryExecutionContext we are only
  /// in the unit test mode
  bool isInTestMode() const { return _qec == nullptr; }

  /// Helper function to check if the assigned `cancellationHandle_` has
  /// been cancelled yet and throw an exception if this is the case.
  void checkCancellation(
      ad_utility::source_location location = AD_CURRENT_SOURCE_LOC()) const;

  // Return a filter that filters the active graphs. Outside of `GRAPH` clauses,
  // the default graphs (implicit, or specified via `FROM`) are active, and
  // inside a `GRAPH` clause, the named graphs are active (specified via an
  // explicit IRI in the `GRAPH` clause, or via `FROM NAMED`).
  qlever::index::GraphFilter<TripleComponent> getActiveGraphs() const;
};

#endif  // QLEVER_SRC_ENGINE_QUERYPLANNER_H
