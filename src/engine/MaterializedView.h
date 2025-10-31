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

  std::vector<ColumnIndex> getIdTableColumnPermutation() const;

  // Num cols and possibly col names should also be stored somewhere + maybe
  // list of views
  void writeViewToDisk();
};

class MaterializedView {
 private:
  Permutation permutation_;

 public:
  explicit MaterializedView(std::string filenameBase);
  // makeIndexScan? or get Permutation? or feed to index scan?
};

class MaterializedViewManager {
 private:
  const Index& index_;
  ad_utility::HashMap<std::string, MaterializedView> loadedViews_;

 public:
  MaterializedViewManager(const Index& index) : index_{index} {};

  void loadView(std::string name);

  // Load if not already loaded and get
  const MaterializedView& getView();
};

// TODO manager for the open permuations (should be kept in `Index` like
// deltatriples) + reader: get index scan op on custom permutation

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEW_H_
