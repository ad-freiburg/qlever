// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_

#include "engine/VariableToColumnMap.h"
#include "libqlever/Qlever.h"
#include "parser/MaterializedViewQuery.h"
#include "util/HashMap.h"

class QueryExecutionContext;
class QueryExecutionTree;

static constexpr size_t MATERIALIZED_VIEWS_VERSION = 1;

class MaterializedViewConfigException : public std::runtime_error {
  // Constructors have to be explicitly inherited
  using std::runtime_error::runtime_error;
};

// TODO
class MaterializedViewWriter {
 private:
  std::string name_;
  std::shared_ptr<QueryExecutionTree> qet_;
  std::shared_ptr<QueryExecutionContext> qec_;
  ParsedQuery parsedQuery_;

  using QueryPlan = qlever::Qlever::QueryPlan;

 public:
  // TODO enforce sane alphanum+dash view names
  MaterializedViewWriter(std::string name, QueryPlan queryPlan);

  std::string getFilenameBase() const;

  // TODO
  std::vector<ColumnIndex> getIdTableColumnPermutation() const;

  // Num cols and possibly col names should also be stored somewhere + maybe
  // list of views
  void writeViewToDisk();
};

// TODO
class MaterializedView {
 private:
  std::string onDiskBase_;
  std::string name_;
  std::shared_ptr<Permutation> permutation_;
  VariableToColumnMap varToColMap_;
  Variable indexedColVariable_;

  using AdditionalScanColumns = SparqlTripleSimple::AdditionalScanColumns;

 public:
  // Load a materialized view from disk given the filename components. The
  // constructor will throw an exception if the name is invalid or the view does
  // not exist.
  MaterializedView(std::string onDiskBase, std::string name);

  // Return the combined filename from the index' `onDiskBase` and the name of
  // the view. Note that this function does not check for validity or existence.
  static std::string getFilenameBase(const std::string& onDiskBase,
                                     const std::string& name);

  // Return a pointer to the open `Permutation` object for this view. Note that
  // this is always an SPO permutation because materialized views are indexed on
  // the first column.
  std::shared_ptr<const Permutation> getPermutation() const;

  // Checks if the given name is allowed for a materialized view. Currently only
  // alphanumerics and hyphens are allowed. This is relevant for safe filenames
  // and for correctly splitting the special predicate.
  static bool isValidName(const std::string& name);
  static void throwIfInvalidName(const std::string& name);

  // Given a `MaterializedViewQuery` obtained from a special `SERVICE` or
  // predicate, compute the `SparqlTripleSimple` to be passed to the constructor
  // of `IndexScan` such that the columns requested by the user are returned. If
  // the `viewQuery` does not request columns 1 and 2, the placeholders should
  // be set to unique variable names.
  SparqlTripleSimple makeScanConfig(
      const parsedQuery::MaterializedViewQuery& viewQuery,
      Variable placeholderPredicate = Variable{"?_internal_view_variable_p"},
      Variable placeholderObject = Variable{
          "?_internal_view_variable_o"}) const;
};

// TODO
class MaterializedViewsManager {
 private:
  std::string onDiskBase_;
  mutable ad_utility::HashMap<std::string, MaterializedView> loadedViews_;

 public:
  MaterializedViewsManager() = default;
  explicit MaterializedViewsManager(std::string onDiskBase)
      : onDiskBase_{std::move(onDiskBase)} {};

  void setOnDiskBase(const std::string& onDiskBase) {
    onDiskBase_ = onDiskBase;
  };

  // TODO const -> we don't want to break the const-ness of `Index` everywhere
  // just for this loading of views. The views themselves aren't changed.
  void loadView(const std::string& name) const;

  // Load if not already loaded and return
  MaterializedView getView(const std::string& name) const;
};

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEWS_H_
