// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#ifndef QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H
#define QLEVER_SRC_ENGINE_TRANSITIVEPATHIMPL_H

#include <utility>

#include "engine/TransitivePathBase.h"
#include "engine/TransitivePathGraphSearch.h"
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
  ColumnType nodes_;
  LocalVocab vocab_;

  // Explicit to prevent issues with co_yield and lifetime.
  // See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=103909 for more info.
  TableColumnWithVocab(std::optional<IdTableView<0>> payload, ColumnType nodes,
                       LocalVocab vocab)
      : payload_{std::move(payload)},
        nodes_{std::move(nodes)},
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
      std::shared_ptr<const Result> startSideResult,
      std::shared_ptr<const Result> targetSideResult, bool yieldOnce) const {
    ad_utility::Timer timer{ad_utility::Timer::Started};

    auto edges = setupEdgesMap(sub->idTableView(), startSide, targetSide);

    auto startNodes = setupNodes(startSide, std::move(startSideResult));

    // Only fetch the target nodes if the target side is also bound.
    std::optional<decltype(startNodes)> targetNodes = std::nullopt;
    if (targetSideResult) {
      targetNodes =
          std::move(setupNodes(targetSide, std::move(targetSideResult)));
    }

    // Setup nodes returns a generator, so this time measurement won't include
    // the time for each iteration, but every iteration step should have
    // constant overhead, which should be safe to ignore.
    runtimeInfo().addDetail("Initialization time", timer.msecs());

    NodeGenerator hull = transitiveHull(
        std::move(edges), sub->getCopyOfLocalVocab(), std::move(startNodes),
        std::move(targetNodes), startSide.value_, targetSide.value_, yieldOnce);

    const auto& [tree, joinColumn] = startSide.treeAndCol_.value();
    size_t numberOfPayloadColumns =
        tree->getResultWidth() - numJoinColumnsWidth(tree, joinColumn);

    // Add the target side's payload columns as well.
    if (targetNodes.has_value()) {
      const auto& [targetTree, targetJoinColumns] =
          targetSide.treeAndCol_.value();
      numberOfPayloadColumns +=
          targetTree->getResultWidth() -
          numJoinColumnsWidth(targetTree, targetJoinColumns);
    }
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

    auto edges = setupEdgesMap(sub->idTableView(), startSide, targetSide);
    auto startNodes = setupNodes(sub->idTableView(), startSide, edges);
    auto targetNodes = setupNodes(sub->idTableView(), targetSide, edges);

    runtimeInfo().addDetail("Initialization time", timer.msecs());

    // Technically we should pass the localVocab of `sub` here, but this will
    // just lead to a merge with itself later on in the pipeline.
    detail::TableColumnWithVocab<const decltype(startNodes)&> startTableInfo{
        std::nullopt, startNodes, LocalVocab{}};
    detail::TableColumnWithVocab<const decltype(targetNodes)&> targetTableInfo{
        std::nullopt, targetNodes, LocalVocab{}};

    NodeGenerator hull =
        transitiveHull(std::move(edges), sub->getCopyOfLocalVocab(),
                       ql::span{&startTableInfo, 1},
                       std::optional(ql::span{&targetTableInfo, 1}),
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
      std::shared_ptr<const Result> startSideResult =
          startSide.treeAndCol_.value().first->getResult(true);
      std::shared_ptr<const Result> targetSideResult =
          targetSide.isBoundVariable()
              ? targetSide.treeAndCol_.value().first->getResult(true)
              : nullptr;

      auto gen = computeTransitivePathBound(
          std::move(subRes), startSide, targetSide, std::move(startSideResult),
          std::move(targetSideResult), !requestLaziness);

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

  // Compute the transitive Hull starting at the nodes given in the range
  // `startNodes` with optional targets given in `targetNodes`, using the map
  // given in `edges`. If no `targetNodes` are given, all nodes connected to the
  // current start node are returned. The `TripleComponent`s `start` and
  // `target` are used to specify the variables for the starting and target
  // side, which, if present, will be matched.
  // Yields matching results in `NodeWithGraph` objects.
  CPP_template(typename Node)(requires ql::ranges::range<Node>) NodeGenerator
      transitiveHull(T edges, LocalVocab edgesVocab, Node startNodes,
                     std::optional<Node> targetNodes, TripleComponent start,
                     TripleComponent target, bool yieldOnce) const {
    using namespace qlever::graphSearch;
    ad_utility::Timer timer{ad_utility::Timer::Stopped};
    // `targetId` is only ever used for comparisons, and never stored in the
    // result, so we use a separate local vocabulary.
    LocalVocab targetHelper;
    const auto& index = getIndex();

    std::optional<Id> targetId =
        target.isVariable()
            ? std::nullopt
            : std::optional{std::move(target).toValueId(index, targetHelper)};
    bool sameVariableOnBothSides =
        !targetId.has_value() && lhs_.value_ == rhs_.value_;
    bool endsWithGraphVariable =
        !targetId.has_value() && graphVariable_ == target.getVariable();
    bool startsWithGraphVariable =
        start.isVariable() && graphVariable_ == start.getVariable();
    // To bind the `targetId` to values, we have to ensure that both sides are
    // bound.
    bool targetNodesAreBound = targetNodes.has_value() &&
                               lhs_.isBoundVariable() && rhs_.isBoundVariable();

    auto expandUndef = [&](auto pair) {
      return TableColumnWithVocab::expandUndef(pair, edges,
                                               graphVariable_.has_value());
    };

    // Prepare nodes and run graph search. Return `NodeWithTargets` if graph
    // search was successful.
    auto runAndProcessGraphSearch =
        [&](Id startNode, Id graphId, std::optional<Id> matchedTargetNode,
            size_t currentRow, LocalVocab& mergedVocab, const auto& payload,
            const auto& targetPayload) -> std::optional<NodeWithTargets> {
      // Skip generation of values for `SELECT * { GRAPH ?g { ?g a* ?x } }`
      // where both `?g` variables are not the same.
      if (startsWithGraphVariable && startNode != graphId) {
        return std::nullopt;
      }
      if (sameVariableOnBothSides) {
        targetId = startNode;
      }
      if (endsWithGraphVariable) {
        targetId = graphId;
      }
      if (targetNodesAreBound) {
        targetId = matchedTargetNode;
      }

      edges.setGraphId(graphId);

      // Pick the appropriate graph search strategy and run it.
      GraphSearchProblem<T> gsp(edges, startNode, targetId, minDist_, maxDist_);
      GraphSearchExecutionParams ep(cancellationHandle_, allocator());
      Set connectedNodes = runOptimalGraphSearch(gsp, ep);
      if (connectedNodes.empty()) {
        return std::nullopt;
      }

      runtimeInfo().addDetail("Hull time", timer.msecs());
      timer.stop();
      return NodeWithTargets{
          startNode,           graphId, std::move(connectedNodes),
          mergedVocab.clone(), payload, targetPayload,
          currentRow};
    };

    // Bookkeeping that has to run right after every yield.
    auto postYieldCleanup = [&](LocalVocab& mergedVocab) {
      timer.cont();
      // Reset vocab to prevent merging the same vocab over and over again.
      if (yieldOnce) {
        mergedVocab = LocalVocab{};
      }
    };

    if (targetNodesAreBound) {
      // Iterate over both `startNodes` and `targetNodes`.
      for (auto&& [tableColumn, targetColumn] :
           ::ranges::views::zip(startNodes, *targetNodes)) {
        timer.cont();
        LocalVocab mergedVocab = std::move(tableColumn.vocab_);
        mergedVocab.mergeWith(edgesVocab);
        for (const auto& [currentRow, pairs] :
             ::ranges::views::enumerate(::ranges::views::zip(
                 tableColumn.nodes_, targetColumn.nodes_))) {
          const auto& [pair, targetPair] = pairs;
          // Position-match each expanded start pair with the corresponding
          // expanded target pair directly via `zip`, instead of scanning.
          for (auto&& [startNode, graphId] : expandUndef(pair)) {
            for (auto&& [targetNode, ignoredTargetGraphId] :
                 expandUndef(targetPair)) {
              if (auto node = runAndProcessGraphSearch(
                      startNode, graphId, targetNode,
                      static_cast<size_t>(currentRow), mergedVocab,
                      tableColumn.payload_, targetColumn.payload_)) {
                co_yield *node;
                postYieldCleanup(mergedVocab);
              }
            }
          }
        }
        timer.stop();
      }
    } else {
      for (auto&& tableColumn : startNodes) {
        timer.cont();
        LocalVocab mergedVocab = std::move(tableColumn.vocab_);
        mergedVocab.mergeWith(edgesVocab);
        for (const auto& [currentRow, pair] :
             ::ranges::views::enumerate(tableColumn.nodes_)) {
          for (const auto& [startNode, graphId] : expandUndef(pair)) {
            if (auto node = runAndProcessGraphSearch(
                    startNode, graphId, std::nullopt,
                    static_cast<size_t>(currentRow), mergedVocab,
                    tableColumn.payload_, std::nullopt)) {
              co_yield *node;
              postYieldCleanup(mergedVocab);
            }
          }
        }
        timer.stop();
      }
    }
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
  SetWithGraph setupNodes(const IdTableView<0>& sub,
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
    Id startId =
        TripleComponent{startSide.value_}.toValueId(getIndex(), helperVocab);
    // Make sure we retrieve the Id from an IndexScan, so we don't have to
    // pass this LocalVocab around. If it's not present then no result needs
    // to be returned anyways. This also augments the id with matching graph
    // ids.
    auto idAndGraphs = edges.getEquivalentIdAndMatchingGraphs(startId);
    result.insert(idAndGraphs.begin(), idAndGraphs.end());
    return result;
  }

  /**
   * @brief Prepare a Map and a nodes vector for the transitive hull
   * computation.
   *
   * @param startSide The TransitivePathSide where the edges start
   * @param startSideResult A `Result` wrapping an `IdTable` containing the
   * Ids for the startSide
   * @return ad_utility::InputRangeTypeErased<TableColumnWithVocab> A range
   * for the transitive hull computation
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

    // From two columns given by their column id, return an iterable range of
    // their contents.
    auto columnsToRange = [graphColumn = std::move(graphColumn),
                           joinColumn](const auto& idTable) {
      ql::span<const Id> startNodes = idTable.getColumn(joinColumn);
      return graphColumn.has_value()
                 ? InputRangeTypeErased{zipColumns(
                       startNodes, idTable.getColumn(graphColumn.value()))}
                 : InputRangeTypeErased{padWithMissingGraph(startNodes)};
    };

    // Get a view object of a column of an `IdTable`.
    auto toView = [columnsWithoutJoinColumns = std::move(
                       columnsWithoutJoinColumns)](const auto& idTable) {
      return idTable.asColumnSubsetView(columnsWithoutJoinColumns);
    };

    // For fully materialized result sides, create a lazily iterable
    // `TableColumnWithVocab` object.
    if (startSideResult->isFullyMaterialized()) {
      return InputRangeTypeErased(lazySingleValueRange(
          [toView = std::move(toView),
           columnsToRange = std::move(columnsToRange),
           startSideResult = std::move(startSideResult)]() {
            const IdTableView<0>& idTable = startSideResult->idTableView();
            return TableColumnWithVocab{toView(idTable),
                                        columnsToRange(idTable),
                                        startSideResult->getCopyOfLocalVocab()};
          }));
    }

    // For not fully materialized result sides, cache the `IdTable` of the
    // `startSideResult` and return a `TableColumnWithVocab` based on that
    // cached object.
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

  virtual T setupEdgesMap(const IdTableView<0>& dynSub,
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

#endif
