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

  void writeViewToDisk();
};

// TODO writer, reader: get index scan op

#endif  // QLEVER_SRC_ENGINE_MATERIALIZEDVIEW_H_
