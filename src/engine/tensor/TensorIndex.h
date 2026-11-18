// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_SRC_ENGINE_TENSORINDEX_H
#define QLEVER_SRC_ENGINE_TENSORINDEX_H

#include <memory>
#include <optional>
#include <variant>

#include "TensorIndexConfig.h"
#include "engine/Operation.h"
#include "global/Id.h"
#include "rdfTypes/TensorData.h"
#include "rdfTypes/Variable.h"
#include "util/ContainersWithAllocator.h"

class TensorIndexCachedIndex;
class TensorIndex;

// helper struct to improve readability in prepareJoin()
struct PreparedTensorIndexParams {
  const IdTableView<0>* const idTableLeft_;
  std::shared_ptr<const Result> resultLeft_;
  const IdTableView<0>* const idTableRight_;
  std::shared_ptr<const Result> resultRight_;
  ColumnIndex leftJoinCol_;
  ColumnIndex rightJoinCol_;
  // The (visible) columns of the left and right input that are copied to the
  // result (in this order).
  qlm::vector<ColumnIndex> leftSelectedCols_;
  qlm::vector<ColumnIndex> rightSelectedCols_;
  // The key under which the FAISS index for the right side is cached (empty if
  // it must not be cached), whether the cache entry is pinned (named by the
  // user), and the fingerprint of the right input that is used to validate a
  // cached index.
  std::string indexCacheKey_;
  bool pinIndex_;
  std::string indexFingerprint_;
  size_t numColumns_;
  TensorIndexConfiguration config_;
};

// The actual algorithms of the `TensorIndex` operation.
class TensorIndexImpl {
 private:
  PreparedTensorIndexParams params_;
  QueryExecutionContext* qec_;
  // The operation, used for cancellation checks and warnings. May be `nullptr`.
  const TensorIndex* op_;

  // One match: a row of the left input, a row of the right input, and the
  // distance value.
  struct Match {
    size_t rowLeft_;
    size_t rowRight_;
    double dist_;
  };

  // Append the given `matches` to the `result`.
  void appendMatches(IdTable& result, const std::vector<Match>& matches) const;
  void checkCancellation() const;
  void addWarning(std::string warning) const;
  Result makeResult(IdTable result) const;

  Result computeTensorIndexResultFaiss();
  Result computeTensorIndexResultNaive();

 public:
  TensorIndexImpl(PreparedTensorIndexParams params, QueryExecutionContext* qec,
                  const TensorIndex* op = nullptr)
      : params_(std::move(params)), qec_(qec), op_(op) {}
  static size_t getNumThreads();
  Result computeTensorIndexResult();
  static void initializeGlobalRuntimeParameters();
};

// This class is implementing a Tensor Search operation. This operations
// performs (approximate) NN over either an index or naively.
class TensorIndex : public Operation {
 public:
  // creates a TensorIndex operation: the required configuration object
  // can for example be obtained from the `TensorIndexQuery` class. Children
  // are optional, because they may be added later using the addChild method.
  // The substitutesFilterOp parameter indicates whether the tensor search
  // was explicitly requested by the user (false) or has been created to
  // implicitly rewrite a filter (true, currently unused).
  TensorIndex(
      QueryExecutionContext* qec, TensorIndexConfiguration config,
      std::optional<std::shared_ptr<QueryExecutionTree>> childLeft,
      std::optional<std::shared_ptr<QueryExecutionTree>> childRight,
      bool substitutesFilterOp = false);

  std::string getCacheKeyImpl() const override;
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;
  uint64_t getSizeEstimateBeforeLimit() override;

  // get the boolean flag if this tensor search operation is used to substitute
  // a filter operation. This can be used in the future and ensure compatibility
  // with the existing spatial join implementation
  bool getSubstitutesFilterOp() const { return substitutesFilterOp_; }

  // this function assumes, that the complete cross product is build and
  // returned. If the TensorIndex does not have both children yet, it just
  // returns one as a dummy return. As no column gets joined in the
  // TensorIndex (both point columns stay in the result table in their row)
  // each row can have at most the same number of distinct elements as it had
  // before. If the size increases, the multiplicity increases. The assumption
  // is, that the distinctness doesn't change and only the multiplicity
  // changes.
  float getMultiplicity(size_t col) override;

  bool knownEmptyResult() override;
  Result computeResult(bool requestLaziness) override;

  // Depending on the amount of children the operation returns a different
  // VariableToColumnMap. If the operation doesn't have both children it needs
  // to aggressively push the queryplanner to add the children, because the
  // operation can't exist without them. If it has both children, it can
  // return the variable to column map, which will be present, after the
  // operation has computed its result
  VariableToColumnMap computeVariableToColumnMap() const override;

  // this method creates a new TensorIndex object, to which the child gets
  // added. The reason for this behavior is, that the QueryPlanner can then
  // still use the existing TensorIndex object, to try different orders
  std::shared_ptr<TensorIndex> addChild(
      std::shared_ptr<QueryExecutionTree> child,
      const Variable& varOfChild) const;

  // if the TensorIndex has both children its construction is done. Then true
  // is returned. This function is needed in the QueryPlanner, so that the
  // QueryPlanner stops trying to add children, after the TensorIndex is
  // already constructed
  bool isConstructed() const;

  // this function is used to give the maximum number of results
  std::optional<size_t> getMaxResults() const;

  // switch the algorithm set in the config parameter at construction time
  void selectAlgorithm(TensorIndexAlgorithm algo) { config_.algo_ = algo; }

  // retrieve the currently selected algorithm
  TensorIndexAlgorithm getAlgorithm() const { return config_.algo_; }

  // retrieve the currently selected spatial join type
  TensorDistanceAlgorithm getDistanceFunction() const { return config_.dist_; }

  // retrieve the variables the spatial join is joining on
  std::pair<Variable, Variable> getTensorIndexVariables() const {
    return {config_.left_, config_.right_};
  }

  const TensorIndexConfiguration& onlyForTestingGetConfig() const {
    return config_;
  }
  std::optional<size_t> onlyForTestingGetSearchK() const {
    return config_.searchK_;
  }
  std::optional<size_t> onlyForTestingGetNTrees() const {
    return config_.kIVF_;
  }
  std::optional<size_t> onlyForTestingGetNNeighbours() const {
    return config_.nNeighbours_;
  }
  ssize_t onlyForTestingGetMaxResults() const { return config_.maxResults_; }
  std::optional<Variable> onlyForTestingGetDistanceVariable() const {
    return config_.distanceVariable_;
  }
  TensorDistanceAlgorithm onlyForTestingGetDistanceFunction() const {
    return config_.dist_;
  }
  std::pair<Variable, Variable> onlyForTestingGetVariables() const {
    return std::pair{config_.left_, config_.right_};
  }

  PayloadVariables onlyForTestingGetPayloadVariables() const {
    return config_.payloadVariables_;
  }

  std::shared_ptr<QueryExecutionTree> onlyForTestingGetLeftChild() const {
    return childLeft_;
  }

  std::shared_ptr<QueryExecutionTree> onlyForTestingGetRightChild() const {
    return childRight_;
  }

  PreparedTensorIndexParams onlyForTestingGetPrepareJoin() const {
    return prepareJoin();
  }

  void checkCancellationWrapperForTensorIndexAlgorithms() const {
    checkCancellation();
  }

  // The key under which the FAISS index for the right side is stored in the
  // global cache of `TensorIndexCachedIndex`. This is the user-specified name
  // (`experimentalRightCacheName`) if given, otherwise a key derived from the
  // right child and the index parameters. Empty if the index can't be cached
  // (e.g. because caching is disabled).
  std::string getTensorIndexCacheKey() const;

  // Create a new `TensorIndex` with the same configuration but the given
  // children (`nullptr` represents a child that is not yet attached).
  std::unique_ptr<Operation> cloneWithNewChildren(
      qlm::vector<std::shared_ptr<QueryExecutionTree>> children) const override;

  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

 private:
  qlm::vector<QueryExecutionTree*> getChildrenImpl() const override;
  [[nodiscard]] qlm::vector<ColumnIndex> resultSortedOn() const override;
  std::unique_ptr<Operation> cloneImpl() const override;

  // The fingerprint of the right input of the FAISS index (see
  // `TensorIndexCachedIndex`).
  std::string getTensorIndexFingerprint() const;

  // helper function to generate a variable to column map from `childRight_`
  // that only contains the columns selected by `config_.payloadVariables_`
  // and (automatically added) the `config_.right_` variable.
  VariableToColumnMap getVarColMapPayloadVars() const;

  // helper function, to initialize various required objects for both algorithms
  PreparedTensorIndexParams prepareJoin() const;

  std::shared_ptr<QueryExecutionTree> childLeft_ = nullptr;
  std::shared_ptr<QueryExecutionTree> childRight_ = nullptr;

  TensorIndexConfiguration config_;

  bool substitutesFilterOp_ = false;
};

#endif  // QLEVER_SRC_ENGINE_TENSORINDEX_H
