//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_OPERATIONTESTHELPERS_H
#define QLEVER_OPERATIONTESTHELPERS_H

#include <chrono>
#include <vector>

#include "./GTestHelpers.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

using namespace std::chrono_literals;

class StallForeverOperation : public qlever::Operation {
  std::vector<qlever::QueryExecutionTree*> getChildren() override { return {}; }
  std::string getCacheKeyImpl() const override {
    return "StallForeverOperation";
  }
  std::string getDescriptor() const override {
    return "StallForeverOperationDescriptor";
  }
  size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  std::vector<qlever::ColumnIndex> resultSortedOn() const override {
    return {};
  }
  qlever::VariableToColumnMap computeVariableToColumnMap() const override {
    return {};
  }

 public:
  using qlever::Operation::Operation;
  // Do-nothing operation that runs for 100ms without computing anything, but
  // which can be cancelled.
  qlever::Result computeResult([[maybe_unused]] bool requestLaziness) override {
    auto end = std::chrono::steady_clock::now() + 100ms;
    while (std::chrono::steady_clock::now() < end) {
      checkCancellation();
    }
    throw std::runtime_error{"Loop was not interrupted for 100ms, aborting"};
  }

  // Provide public view of remainingTime for tests
  std::chrono::milliseconds publicRemainingTime() const {
    return remainingTime();
  }

  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  // _____________________________________________________________________________
  std::unique_ptr<qlever::Operation> cloneImpl() const override {
    AD_THROW("Clone not implemented");
  }
};
// _____________________________________________________________________________

// Dummy parent to test recursive application of a function
class ShallowParentOperation : public qlever::Operation {
  std::shared_ptr<qlever::QueryExecutionTree> child_;

  explicit ShallowParentOperation(
      std::shared_ptr<qlever::QueryExecutionTree> child)
      : qlever::Operation{child->getRootOperation()->getExecutionContext()},
        child_{std::move(child)} {}
  std::string getCacheKeyImpl() const override { return "ParentOperation"; }
  std::string getDescriptor() const override {
    return "ParentOperationDescriptor";
  }
  size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  std::vector<qlever::ColumnIndex> resultSortedOn() const override {
    return {};
  }
  qlever::VariableToColumnMap computeVariableToColumnMap() const override {
    return {};
  }

 public:
  template <typename ChildOperation, typename... Args>
  static ShallowParentOperation of(qlever::QueryExecutionContext* qec,
                                   Args&&... args) {
    return ShallowParentOperation{
        ad_utility::makeExecutionTree<ChildOperation>(qec, args...)};
  }

  std::vector<qlever::QueryExecutionTree*> getChildren() override {
    return {child_.get()};
  }

  qlever::Result computeResult([[maybe_unused]] bool requestLaziness) override {
    auto childResult = child_->getResult();
    return {childResult->cloneIdTable(), resultSortedOn(),
            childResult->getSharedLocalVocab()};
  }

  // Provide public view of remainingTime for tests
  std::chrono::milliseconds publicRemainingTime() const {
    return remainingTime();
  }

  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  // _____________________________________________________________________________
  std::unique_ptr<qlever::Operation> cloneImpl() const override {
    AD_THROW("Clone not implemented");
  }
};

// Operation that will throw on `computeResult` for testing.
class AlwaysFailOperation : public qlever::Operation {
  std::optional<qlever::Variable> variable_ = std::nullopt;

  std::vector<qlever::QueryExecutionTree*> getChildren() override { return {}; }
  std::string getCacheKeyImpl() const override {
    // Because this operation always fails, it should never be cached.
    return "AlwaysFailOperationCacheKey";
  }
  std::string getDescriptor() const override {
    return "AlwaysFailOperationDescriptor";
  }
  size_t getResultWidth() const override { return 1; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  std::vector<qlever::ColumnIndex> resultSortedOn() const override {
    return {0};
  }
  qlever::VariableToColumnMap computeVariableToColumnMap() const override {
    if (!variable_.has_value()) {
      return {};
    }
    return {
        {variable_.value(),
         qlever::ColumnIndexAndTypeInfo{
             0, qlever::ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined}}};
  }

 public:
  using qlever::Operation::Operation;
  AlwaysFailOperation(qlever::QueryExecutionContext* qec,
                      qlever::Variable variable)
      : qlever::Operation{qec}, variable_{std::move(variable)} {}
  qlever::Result computeResult(bool requestLaziness) override {
    if (!requestLaziness) {
      throw std::runtime_error{"AlwaysFailOperation"};
    }
    return {[]() -> qlever::Result::Generator {
              throw std::runtime_error{"AlwaysFailOperation"};
              // Required so that the exception only occurs within the generator
              co_return;
            }(),
            resultSortedOn()};
  }

  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  // _____________________________________________________________________________
  std::unique_ptr<qlever::Operation> cloneImpl() const override {
    AD_THROW("Clone not implemented");
  }
};

// Lazy operation that will yield a result with a custom generator you can
// provide via the constructor.
class CustomGeneratorOperation : public qlever::Operation {
  qlever::Result::Generator generator_;
  std::vector<qlever::QueryExecutionTree*> getChildren() override { return {}; }
  std::string getCacheKeyImpl() const override { AD_FAIL(); }
  std::string getDescriptor() const override {
    return "CustomGeneratorOperationDescriptor";
  }
  size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  std::vector<qlever::ColumnIndex> resultSortedOn() const override {
    return {};
  }
  qlever::VariableToColumnMap computeVariableToColumnMap() const override {
    return {};
  }

 public:
  CustomGeneratorOperation(qlever::QueryExecutionContext* context,
                           qlever::Result::Generator generator)
      : qlever::Operation{context}, generator_{std::move(generator)} {}
  qlever::Result computeResult(bool requestLaziness) override {
    AD_CONTRACT_CHECK(requestLaziness);
    return {std::move(generator_), resultSortedOn()};
  }

  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  // _____________________________________________________________________________
  std::unique_ptr<qlever::Operation> cloneImpl() const override {
    AD_THROW("Clone not implemented");
  }
};

MATCHER_P(SameTypeId, ptr, "has the same type id") {
  return typeid(*arg) == typeid(*ptr);
}

inline auto IsDeepCopy(const qlever::Operation& other) {
  using namespace ::testing;
  return AllOf(
      Address(SameTypeId(&other)),
      AD_PROPERTY(qlever::Operation, getChildren,
                  Pointwise(Ne(), other.getChildren())),
      AD_PROPERTY(qlever::Operation, getCacheKey, Eq(other.getCacheKey())),
      AD_PROPERTY(qlever::Operation, getLimitOffset,
                  Eq(other.getLimitOffset())),
      AD_PROPERTY(qlever::Operation, getExternallyVisibleVariableColumns,
                  Eq(other.getExternallyVisibleVariableColumns())),
      AD_PROPERTY(qlever::Operation, getResultWidth,
                  Eq(other.getResultWidth())),
      AD_PROPERTY(qlever::Operation, getDescriptor, Eq(other.getDescriptor())),
      AD_PROPERTY(qlever::Operation, getResultSortedOn,
                  Eq(other.getResultSortedOn())));
}

#endif  // QLEVER_OPERATIONTESTHELPERS_H
