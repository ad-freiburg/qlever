// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H

#include <cstdint>
#include <utility>

#include "engine/TransitivePathBase.h"
#include "util/Iterators.h"
#include "util/Timer.h"

using IdWithGraphs = absl::InlinedVector<std::pair<Id, Id>, 1>;

namespace detail {

// Helper struct that allows to group a read-only view of a column of a table
// with a reference to the table itself and a local vocabulary (used to ensure
// the correct lifetime).
template <typename ColumnType>
struct TableColumnWithVocab {
  PayloadTable payload_;
  ColumnType startNodes_;
  LocalVocab vocab_;

  using column_type = ColumnType;

  // Explicit to prevent issues with co_yield and lifetime.
  // See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103909 for more info.
  TableColumnWithVocab(std::optional<IdTableView<0>> payload,
                       ColumnType startNodes, LocalVocab vocab)
      : payload_{std::move(payload)},
        startNodes_{std::move(startNodes)},
        vocab_{std::move(vocab)} {}

  // Return a range substituting undefined values with all corresponding values
  // from `edges`. If `tuple` doesn't contain any undefined values, then
  // the range returned by this function just returns the tuple itself.
  static decltype(auto) expandUndef(const auto& tuple,
                                    [[maybe_unused]] const auto& edges,
                                    bool checkGraph) {
    // This is the unbound case, e.g. `?x wdt:P279+ ?y` where the left side of
    // the P279+ is guaranteed to be defined.
    if constexpr (std::is_same_v<ColumnType, SetWithGraph>) {
      return ql::views::single(tuple);
    } else {
      const auto& [startId, graphId] = tuple;
      if (startId.isUndefined() || (checkGraph && graphId.isUndefined()))
          [[unlikely]] {
        return edges.getEquivalentIdAndMatchingGraphs(startId);
      } else {
        return IdWithGraphs{tuple};
      }
    }
  }
};
};  // namespace detail

/**
 * @class TransitivePathImpl
 * @brief This class implements common functions for the concrete TransitivePath
 * classes TransitivePathBinSearch and TransitivePathHashMap. The template can
 * be set to a map data structure which is used for the transitive hull
 * computation.
 *
 * @tparam T A map data structure for the transitive hull computation.
 */
template <typename T>
class TransitivePathImpl : public TransitivePathBase {
  // Tuple-like class
  using ZippedType = ql::ranges::range_value_t<
      ::ranges::zip_view<ql::span<const Id>, ::ranges::repeat_view<Id>>>;
  using TableColumnWithVocab = detail::TableColumnWithVocab<
      ad_utility::InputRangeTypeErased<ZippedType>>;

 public:
  using TransitivePathBase::TransitivePathBase;

  /**
   * @brief Compute the transitive hull with a bound side.
   * This function is called when the startSide is bound and
   * it is a variable. The other IdTable contains the result
   * of the start side and will be used to get the start nodes.
   *
   * @param sub A shared pointer to the sub result. Needs to be kept alive for
   * the lifetime of this generator.
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   * @param startSideResult The Result of the startSide
   * @param yieldOnce If true, the generator will yield only a single time.
   */
  Result::Generator computeTransitivePathBound(
      std::shared_ptr<const Result> sub, const TransitivePathSide& startSide,
      const TransitivePathSide& targetSide,
      std::shared_ptr<const Result> startSideResult, bool yieldOnce) const {
    ad_utility::Timer timer{ad_utility::Timer::Started};

    auto edges = setupEdgesMap(sub->idTable(), startSide, targetSide);
    auto nodes = setupNodes(startSide, std::move(startSideResult));
    // Setup nodes returns a generator, so this time measurement won't include
    // the time for each iteration, but every iteration step should have
    // constant overhead, which should be safe to ignore.
    runtimeInfo().addDetail("Initialization time", timer.msecs());

    NodeGenerator hull = transitiveHull(
        std::move(edges), sub->getCopyOfLocalVocab(), std::move(nodes),
        startSide.value_, targetSide.value_, yieldOnce);

    const auto& [tree, joinColumn] = startSide.treeAndCol_.value();
    size_t numberOfPayloadColumns =
        tree->getResultWidth() - numJoinColumnsWith(tree, joinColumn);
    auto result = fillTableWithHull(std::move(hull), startSide.outputCol_,
                                    targetSide.outputCol_, yieldOnce,
                                    numberOfPayloadColumns);

    // Iterate over generator to prevent lifetime issues
    for (auto& pair : result) {
      co_yield pair;
    }
  };

  /**
   * @brief Compute the transitive hull.
   * This function is called when no side is bound (or an id).
   *
   * @param sub A shared pointer to the sub result. Needs to be kept alive for
   * the lifetime of this generator.
   * @param startSide The start side for the transitive hull
   * @param targetSide The target side for the transitive hull
   * @param yieldOnce If true, the generator will yield only a single time.
   */

  Result::Generator computeTransitivePath(std::shared_ptr<const Result> sub,
                                          const TransitivePathSide& startSide,
                                          const TransitivePathSide& targetSide,
                                          bool yieldOnce) const {
    ad_utility::Timer timer{ad_utility::Timer::Started};

    auto edges = setupEdgesMap(sub->idTable(), startSide, targetSide);
    auto nodes = setupNodes(sub->idTable(), startSide, edges);

    runtimeInfo().addDetail("Initialization time", timer.msecs());

    // Technically we should pass the localVocab of `sub` here, but this will
    // just lead to a merge with itself later on in the pipeline.
    detail::TableColumnWithVocab<const decltype(nodes)&> tableInfo{
        std::nullopt, nodes, LocalVocab{}};

    NodeGenerator hull = transitiveHull(
        std::move(edges), sub->getCopyOfLocalVocab(), ql::span{&tableInfo, 1},
        startSide.value_, targetSide.value_, yieldOnce);

    // We don't pass a payload table, so our `inputWidth` is 0.
    auto result = fillTableWithHull(std::move(hull), startSide.outputCol_,
                                    targetSide.outputCol_, yieldOnce, 0);

    // Iterate over generator to prevent lifetime issues
    for (auto& pair : result) {
      co_yield pair;
    }
  }

 protected:
  /**
   * @brief Compute the result for this TransitivePath operation
   * This function chooses the start and target side for the transitive
   * hull computation. This choice of the start side has a large impact
   * on the time it takes to compute the hull. The set of nodes on the
   * start side should be as small as possible.
   *
   * @return Result The result of the TransitivePath operation
   */
  Result computeResult(bool requestLaziness) override {
    auto [startSide, targetSide] = decideDirection();
    // In order to traverse the graph represented by this result, we need random
    // access across the whole table, so it doesn't make sense to lazily compute
    // the result.
    std::shared_ptr<const Result> subRes = subtree_->getResult(false);

    if (startSide.isBoundVariable()) {
      std::shared_ptr<const Result> sideRes =
          startSide.treeAndCol_.value().first->getResult(true);

      auto gen =
          computeTransitivePathBound(std::move(subRes), startSide, targetSide,
                                     std::move(sideRes), !requestLaziness);

      return requestLaziness ? Result{std::move(gen), resultSortedOn()}
                             : Result{cppcoro::getSingleElement(std::move(gen)),
                                      resultSortedOn()};
    }
    auto gen = computeTransitivePath(std::move(subRes), startSide, targetSide,
                                     !requestLaziness);
    return requestLaziness ? Result{std::move(gen), resultSortedOn()}
                           : Result{cppcoro::getSingleElement(std::move(gen)),
                                    resultSortedOn()};
  }

  /**
   * @brief Depth-first search to find connected nodes in the graph.
   * @param edges The adjacency lists, mapping Ids (nodes) to their connected
   * Ids.
   * @param startNode The node to start the search from.
   * @param target Optional target Id. If supplied, only paths which end in this
   * Id are added to the result.
   * @return A set of connected nodes in the graph.
   */
  Set findConnectedNodes(const T& edges, Id startNode,
                         const std::optional<Id>& target) const {
    std::vector<std::pair<Id, size_t>> stack;
    ad_utility::HashSetWithMemoryLimit<Id> marks{allocator()};
    Set connectedNodes{allocator()};
    stack.emplace_back(startNode, 0);

    while (!stack.empty()) {
      checkCancellation();
      auto [node, steps] = stack.back();
      stack.pop_back();

      if (steps <= maxDist_ && !marks.contains(node)) {
        if (steps >= minDist_) {
          marks.insert(node);
          if (!target.has_value() || node == target.value()) {
            connectedNodes.insert(node);
          }
        }

        const auto& successors = edges.successors(node);
        for (auto successor : successors) {
          stack.emplace_back(successor, steps + 1);
        }
      }
    }
    return connectedNodes;
  }

  CPP_template(typename Node)(
      requires ql::ranges::range<Node>) class TransitiveHullLazyRange
      : public ad_utility::InputRangeMixin<TransitiveHullLazyRange<Node>> {
   private:
    using NodeWithTargetRange =
        ad_utility::InputRangeTypeErased<NodeWithTargets>;
    using EnumerateRangeType =
        std::invoke_result_t<decltype(::ranges::views::enumerate),
                             typename Node::value_type::column_type&>;

    // Input arguments
    const TransitivePathImpl<T>& parent_;
    T edges_;
    LocalVocab edgesVocab_;
    Node startNodes_;
    TripleComponent target_;
    bool yieldOnce_;
    // Runtime state
    ad_utility::Timer timer_{ad_utility::Timer::Stopped};
    LocalVocab targetHelper_;
    LocalVocab mergedVocab_;
    std::optional<Id> targetId_;
    bool sameVariableOnBothSides_;
    bool endsWithGraphVariable_;
    bool startsWithGraphVariable_;
    // Range state
    bool finished_{false};
    // The `resultRange_` yields results of this transitive hull
    // computation. Values from `TableColumnWithVocab::expandUndef` are
    // processed one by one. The range and iterator are reinitialised when the
    // next value from the `enumerateRange_` is yielded. The iterator is
    // incremented on each iteration of this object.
    std::optional<NodeWithTargetRange> resultRange_;
    NodeWithTargetRange::iterator result_{};
    // The `enumerateRange_` yields `pair<index, ZippedType>` for the current
    // table column's `startNodes_`.  The range and iterator are
    // reinitialised when the next table column is yielded. The iterator is
    // incremented only when the `resultRange_` is exhausted.
    std::optional<EnumerateRangeType> enumerateRange_;
    ql::ranges::iterator_t<EnumerateRangeType> enumerateRangeIt_{};
    // This iterator points to the table columns of `startNodes_` and only
    // incremented when the `enumerateRange_` is exhausted. The transitive hull
    // computation is completed when  this iterator reaches the end of
    // `startNodes_`.
    ql::ranges::iterator_t<Node> tableColumnIt_{};

   public:
    TransitiveHullLazyRange(const TransitivePathImpl<T>& parent, T edges,
                            LocalVocab edgesVocab, Node startNodes,
                            TripleComponent start, TripleComponent target,
                            bool yieldOnce)
        : parent_(parent),
          edges_(std::move(edges)),
          edgesVocab_(std::move(edgesVocab)),
          startNodes_(std::move(startNodes)),
          target_(std::move(target)),
          yieldOnce_(yieldOnce) {
      // `targetId` is only ever used for comparisons, and never stored in the
      // result, so we use a separate local vocabulary - `targetHelper_`.
      const auto& index = parent_.getIndex();
      targetId_ = target_.isVariable()
                      ? std::nullopt
                      : std::optional{std::move(target_).toValueId(
                            index.getVocab(), targetHelper_,
                            index.encodedIriManager())};
      sameVariableOnBothSides_ =
          !targetId_.has_value() && parent_.lhs_.value_ == parent_.rhs_.value_;
      endsWithGraphVariable_ = !targetId_.has_value() &&
                               parent_.graphVariable_ == target_.getVariable();
      startsWithGraphVariable_ =
          start.isVariable() && parent_.graphVariable_ == start.getVariable();
    }

    // Return the ID of the target node when searching for connected nodes.
    // Returns either `startNode`, `graphId` or `targetId_`,
    // where the selection logic covers the following case:
    // SELECT * {
    //   ?x <a>+ ?x . # sameVariableOnBothSides
    //   GRAPH ?g {
    //     ?y <b>+ ?g # endsWithGraphVariable
    //   }
    //   # else
    //   VALUES ?z { <d> }
    //   ?z <c>+ <e> # <e> would be the target id
    //   ?z <c>+ ?e  # std::nullopt would be the target id
    // }
    std::optional<Id> getTargetId(const Id& startNode,
                                  const Id& graphId) const {
      if (sameVariableOnBothSides_) {
        return startNode;
      } else if (endsWithGraphVariable_) {
        return graphId;
      } else {
        return targetId_;
      }
    };

    // Process an output ID pair from `TableColumnWithVocab::expandUndef`.
    // This method performs the core logic of the transitive hull computation
    // and runs the hull traversal with `findConnectedNodes`. It is called with
    // each iteration of the `resultRange_`.
    ad_utility::LoopControl<NodeWithTargets> process(ZippedType& idPair,
                                                     size_t currentRow,
                                                     PayloadTable& payload) {
      using namespace ad_utility;
      const auto& [startNode, graphId] = idPair;
      timer_.cont();
      // Skip generation of values for `SELECT * { GRAPH ?g { ?g a* ?x }}`
      // where both `?g` variables are not the same.
      if (startsWithGraphVariable_ && startNode != graphId) {
        return LoopControl<NodeWithTargets>::makeContinue();
      }
      edges_.setGraphId(graphId);
      Set connectedNodes = parent_.findConnectedNodes(
          edges_, startNode, getTargetId(startNode, graphId));
      if (!connectedNodes.empty()) {
        parent_.runtimeInfo().addDetail("Hull time", timer_.msecs());
        NodeWithTargets result{startNode,
                               graphId,
                               std::move(connectedNodes),
                               mergedVocab_.clone(),
                               payload,
                               currentRow};
        // Reset vocab to prevent merging the same vocab over and over
        // again.
        if (yieldOnce_) {
          mergedVocab_ = LocalVocab{};
        }
        timer_.stop();
        return LoopControl<NodeWithTargets>::yieldValue(std::move(result));
      }
      timer_.stop();
      return LoopControl<NodeWithTargets>::makeContinue();
    }

    // Return a range that yields `NodeWithTargets` for the current
    // `tableColumnIt_` and `enumerateRangeIt_`.
    NodeWithTargetRange buildResultRange() {
      auto& tableColumn = *tableColumnIt_;
      const auto& [currentRow, zippedType] = *enumerateRangeIt_;
      return NodeWithTargetRange(
          ad_utility::CachingContinuableTransformInputRange(
              tableColumn.expandUndef(zippedType, edges_,
                                      parent_.graphVariable_.has_value()),
              [this, currentRow = currentRow,
               payload = tableColumn.payload_](auto& idPair) mutable {
                return process(idPair, currentRow, payload);
              }));
    }

    // Initialise `enumerateRange_` and `enumerateRangeIt_` for the current
    // `tableColumnIt_`. If there are no more table columns, we are finished.
    void buildEnumerateRange() {
      if (tableColumnIt_ == ql::ranges::end(startNodes_)) {
        finished_ = true;
        return;
      }
      auto& tableColumn = *tableColumnIt_;
      mergedVocab_ = std::move(tableColumn.vocab_);
      mergedVocab_.mergeWith(edgesVocab_);

      enumerateRange_ = ::ranges::views::enumerate(tableColumn.startNodes_);
      enumerateRangeIt_ = ql::ranges::begin(*enumerateRange_);

      if (enumerateRangeIt_ == ql::ranges::end(*enumerateRange_)) {
        // if there are no start nodes in this table column, we move to the
        // next table column and recursively find the next valid range.
        ++tableColumnIt_;
        return buildEnumerateRange();
      }
    }

    // Advance `enumerateRangeIt_` to the next value, build the next
    // `resultRange_` and initialise `result_`.
    void getNextResultRange() {
      ++enumerateRangeIt_;
      if (enumerateRangeIt_ == ql::ranges::end(*enumerateRange_)) {
        ++tableColumnIt_;
        buildEnumerateRange();
      }

      if (finished_) {
        return;
      }

      resultRange_ = buildResultRange();
      result_ = resultRange_->begin();
      if (result_ == resultRange_->end()) {
        getNextResultRange();
      }
    }

    // This method is called when `begin()` is called on this object.
    // Initialises all ranges and prepares to yield the first value or signal
    // that we are finished.
    void start() {
      tableColumnIt_ = ql::ranges::begin(startNodes_);
      buildEnumerateRange();
      if (finished_) {
        return;
      }

      resultRange_ = buildResultRange();
      result_ = resultRange_->begin();
      if (result_ == resultRange_->end()) {
        getNextResultRange();
      }
    }

    // Advance to the next result value. If the current `resultRange_` is
    // exhausted, initialise the next one.
    void next() {
      ++result_;
      if (result_ == resultRange_->end()) {
        getNextResultRange();
      }
    }

    // Return true if there are no more values to yield.
    bool isFinished() { return finished_; }
    // Return the current result value.
    auto& get() { return *result_; }
    const auto& get() const { return *result_; }
  };

  /**
   * @brief Compute the transitive hull starting at the given nodes,
   * using the given Map.
   *
   * @param edges Adjacency lists, mapping Ids (nodes) to their connected
   * Ids.
   * @param edgesVocab The `LocalVocab` holding the vocabulary of the edges.
   * @param startNodes A range that yields an instantiation of
   * `TableColumnWithVocab` that can be consumed to create a transitive hull.
   * @param start Start `TripleComponent`. If it's a variable, and the same as
   * an optional graph variable, values where these values are not equal are
   * skipped.
   * @param target Target `TripleComponent`. If it's not a variable, paths that
   * don't end with a matching value are discarded.
   * @param yieldOnce This has to be set to the same value as the consuming
   * code. When set to true, this will prevent yielding the same LocalVocab over
   * and over again to make merging faster (because merging with an empty
   * LocalVocab is a no-op).
   * @return Map Maps each Id to its connected Ids in the transitive hull
   */
  CPP_template(typename Node)(requires ql::ranges::range<Node>) NodeGenerator
      transitiveHull(T edges, LocalVocab edgesVocab, Node startNodes,
                     TripleComponent start, TripleComponent target,
                     bool yieldOnce) const {
    return NodeGenerator(TransitiveHullLazyRange<Node>(
        *this, std::move(edges), std::move(edgesVocab), std::move(startNodes),
        std::move(start), std::move(target), yieldOnce));
  }

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @param sub The sub table result
   * @param startSide The TransitivePathSide where the edges start
   * @param edges Templated datastructure representing the edges of the graph
   * @return Set A set of starting nodes for the transitive hull computation
   */
  SetWithGraph setupNodes(const IdTable& sub,
                          const TransitivePathSide& startSide,
                          const T& edges) const {
    AD_CORRECTNESS_CHECK(minDist_ != 0,
                         "If minDist_ is 0 with a hardcoded side, we should "
                         "call the overload for a bound transitive path.");
    SetWithGraph result{allocator()};
    // var -> var
    if (startSide.isVariable()) {
      if (graphVariable_.has_value()) {
        for (const auto& [id, graph] : ::ranges::views::zip(
                 sub.getColumn(startSide.subCol_),
                 sub.getColumn(
                     subtree_->getVariableColumn(graphVariable_.value())))) {
          result.emplace(id, graph);
        }
      } else {
        for (Id id : sub.getColumn(startSide.subCol_)) {
          result.emplace(id, Id::makeUndefined());
        }
      }
      return result;
    }
    // id -> var|id
    LocalVocab helperVocab;
    Id startId = TripleComponent{startSide.value_}.toValueId(
        getIndex().getVocab(), helperVocab, getIndex().encodedIriManager());
    // Make sure we retrieve the Id from an IndexScan, so we don't have to pass
    // this LocalVocab around. If it's not present then no result needs to be
    // returned anyways. This also augments the id with matching graph ids.
    auto idAndGraphs = edges.getEquivalentIdAndMatchingGraphs(startId);
    result.insert(idAndGraphs.begin(), idAndGraphs.end());
    return result;
  }

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @param startSide The TransitivePathSide where the edges start
   * @param startSideResult A `Result` wrapping an `IdTable` containing the Ids
   * for the startSide
   * @return ad_utility::InputRangeTypeErased<TableColumnWithVocab> A range for
   * the transitive hull computation
   */
  ad_utility::InputRangeTypeErased<TableColumnWithVocab> setupNodes(
      const TransitivePathSide& startSide,
      std::shared_ptr<const Result> startSideResult) const {
    using namespace ad_utility;
    const auto& [tree, joinColumn] = startSide.treeAndCol_.value();
    size_t cols = tree->getResultWidth();
    std::optional<ColumnIndex> graphColumn = getActualGraphColumnIndex(tree);
    std::vector<ColumnIndex> columnsWithoutJoinColumns =
        computeColumnsWithoutJoinColumns(joinColumn, cols, graphColumn);
    auto columnsToRange = [graphColumn = std::move(graphColumn),
                           joinColumn](const auto& idTable) {
      ql::span<const Id> startNodes = idTable.getColumn(joinColumn);
      return graphColumn.has_value()
                 ? InputRangeTypeErased{zipColumns(
                       startNodes, idTable.getColumn(graphColumn.value()))}
                 : InputRangeTypeErased{padWithMissingGraph(startNodes)};
    };

    auto toView = [columnsWithoutJoinColumns = std::move(
                       columnsWithoutJoinColumns)](const IdTable& idTable) {
      return idTable.asColumnSubsetView(columnsWithoutJoinColumns);
    };

    if (startSideResult->isFullyMaterialized()) {
      return InputRangeTypeErased(lazySingleValueRange(
          [toView = std::move(toView),
           columnsToRange = std::move(columnsToRange),
           startSideResult = std::move(startSideResult)]() {
            const IdTable& idTable = startSideResult->idTable();
            return TableColumnWithVocab{toView(idTable),
                                        columnsToRange(idTable),
                                        startSideResult->getCopyOfLocalVocab()};
          }));
    }

    return InputRangeTypeErased(CachingTransformInputRange(
        startSideResult->idTables(),
        // the lambda uses a buffer to ensure the lifetime of the pointer to
        // the idTable, but releases ownership of the localVocab
        [toView = std::move(toView), columnsToRange = std::move(columnsToRange),
         buf = std::optional<Result::IdTableVocabPair>{std::nullopt}](
            auto& idTableAndVocab) mutable {
          buf = std::move(idTableAndVocab);
          auto& [idTable, localVocab] = buf.value();
          return TableColumnWithVocab{toView(idTable), columnsToRange(idTable),
                                      std::move(localVocab)};
        }));
  }

  virtual T setupEdgesMap(const IdTable& dynSub,
                          const TransitivePathSide& startSide,
                          const TransitivePathSide& targetSide) const = 0;

 private:
  // Helper function to filter the join column to not add it twice to the
  // result.
  static std::vector<ColumnIndex> computeColumnsWithoutJoinColumns(
      ColumnIndex joinColumn, size_t totalColumns,
      std::optional<ColumnIndex> graphColumn) {
    std::vector<ColumnIndex> columnsWithoutJoinColumn;
    uint8_t graphPadding = graphColumn.has_value() && joinColumn != graphColumn;
    AD_CORRECTNESS_CHECK(totalColumns > graphPadding);
    columnsWithoutJoinColumn.reserve(totalColumns - graphPadding - 1);
    ql::ranges::copy(
        ql::views::iota(static_cast<size_t>(0), totalColumns) |
            ql::views::filter([joinColumn, &graphColumn](size_t i) {
              return i != joinColumn && i != graphColumn;
            }),
        std::back_inserter(columnsWithoutJoinColumn));
    return columnsWithoutJoinColumn;
  }

  // Create a zipped view that returns `Id::makeUndefined()` for the graph
  // column.
  static auto padWithMissingGraph(ql::span<const Id> input) {
    return ::ranges::views::zip(input,
                                ::ranges::views::repeat(Id::makeUndefined()));
  }

  // Create a zipped view from two columns.
  static auto zipColumns(ql::span<const Id> input,
                         ql::span<const Id> graphInput) {
    return ::ranges::views::zip(input, graphInput);
  }
};

#endif  // QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H
