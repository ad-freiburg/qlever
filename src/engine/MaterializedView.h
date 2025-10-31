// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_MATERIALIZEDVIEW_H_
#define QLEVER_SRC_ENGINE_MATERIALIZEDVIEW_H_

#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/ExternalSortFunctors.h"
#include "libqlever/Qlever.h"
#include "util/HashMap.h"

// TODO
class MaterializedViewWriter {
 private:
  std::string name_;
  std::shared_ptr<QueryExecutionTree> qet_;
  std::shared_ptr<QueryExecutionContext> qec_;
  ParsedQuery parsedQuery_;

  // SPO comparator
  using Comparator = SortTriple<0, 1, 2>;
  // Sorter with a dynamic number of columns (template argument `NumStaticCols
  // == 0`)
  using Sorter = ad_utility::CompressedExternalIdTableSorter<Comparator, 0>;
  using MetaData = IndexMetaDataMmap;

 public:
  MaterializedViewWriter(std::string name, qlever::Qlever::QueryPlan queryPlan);

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
  std::shared_ptr<Permutation> permutation_;

 public:
  MaterializedView(std::string_view onDiskBase, std::string_view name);

  static std::string getFilenameBase(std::string_view onDiskBase,
                                     std::string_view name);

  std::shared_ptr<const Permutation> getPermutation() const;
};

// TODO
class MaterializedViewManager {
 private:
  const Index& index_;
  ad_utility::HashMap<std::string, MaterializedView> loadedViews_;

 public:
  MaterializedViewManager(const Index& index) : index_{index} {};

  void loadView(const std::string& name);

  // Load if not already loaded and return
  MaterializedView getView(const std::string& name);
};

// TODO manager for the open permutations (should be kept in `Index` like
// deltatriples) + reader: get index scan op on custom permutation

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEW_H_
