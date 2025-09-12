//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_SCANSPECIFICATION_H
#define QLEVER_SRC_INDEX_SCANSPECIFICATION_H

#include <optional>

#include "engine/LocalVocab.h"
#include "global/Id.h"
#include "index/GraphFilter.h"
#include "parser/TripleComponent.h"

// Forward declaration
class IndexImpl;
class Index;

// The specification of a scan operation for a given permutation.
// Can either be a full scan (all three elements are `std::nullopt`),
// a scan for a fixed `col0Id`, a scan for a fixed `col0Id` and `col1Id`,
// or even a scan for a single triple to check whether it is contained in
// the knowledge graph at all. The values which are `nullopt` become variables
// and are returned as columns in the result of the scan.
class ScanSpecification {
 public:
  using T = std::optional<Id>;
  using GraphFilter = qlever::index::GraphFilter<Id>;

 private:
  T col0Id_;
  T col1Id_;
  T col2Id_;
  // A local vocab that is needed in case at least one of the `colXIds_` has
  // type `LocalVocabIndex`. Note that this doesn't automatically mean that the
  // scan result will be empty, because local vocab entries might also be
  // created by SPARQL UPDATE requests.
  // Note: This `localVocab` keeps the `colXIds` alive in that case. It is a
  // serious bug, to copy the `colXIds` out of this class. The only valid usage
  // is to compare them with other IDs as long as the `ScanSpecification` is
  // still alive.
  // TODO<joka921> This can be enforced by the type system, but that requires
  // several intrusive changes in other parts of QLever.
  std::shared_ptr<const LocalVocab> localVocab_;

  // Filter specification of which graphs to include and which to omit.
  GraphFilter graphFilter_;
  friend class ScanSpecificationAsTripleComponent;

  void validate() const;

 public:
  ScanSpecification(T col0Id, T col1Id, T col2Id, LocalVocab localVocab = {},
                    GraphFilter graphFilter = GraphFilter::All())
      : col0Id_{col0Id},
        col1Id_{col1Id},
        col2Id_{col2Id},
        localVocab_{std::make_shared<LocalVocab>(std::move(localVocab))},
        graphFilter_{std::move(graphFilter)} {
    validate();
  }
  const T& col0Id() const { return col0Id_; }
  const T& col1Id() const { return col1Id_; }
  const T& col2Id() const { return col2Id_; }

  // Get the corresponding index to the first free `colXId_`.
  size_t firstFreeColIndex() const {
    if (!col0Id_) return 0;
    if (!col1Id_) return 1;
    if (!col2Id_) return 2;
    return 3;
  }

  const GraphFilter& graphFilter() const { return graphFilter_; }

  // Only used in tests.
  void setCol1Id(T col1Id) {
    col1Id_ = col1Id;
    validate();
  }
};

// Same as `ScanSpecification` (see above), but stores `TripleComponent`s
// instead of `Id`s.
class ScanSpecificationAsTripleComponent {
 public:
  using T = std::optional<TripleComponent>;
  using GraphFilter = qlever::index::GraphFilter<TripleComponent>;

 private:
  T col0_;
  T col1_;
  T col2_;
  GraphFilter graphFilter_;

 public:
  // Construct from three optional `TripleComponent`s. If any of the three
  // entries is unbound (`nullopt` or of type `Variable`), then all subsequent
  // entries also have to be unbound. For example if `col0` is bound, but `col1`
  // isn't, then `col2` also has to be unbound.
  ScanSpecificationAsTripleComponent(
      T col0, T col1, T col2, GraphFilter graphFilter = GraphFilter::All());

  // Convert to a `ScanSpecification`. The `index` is used to convert the
  // `TripleComponent` to `Id`s by looking them up in the vocabulary.
  ScanSpecification toScanSpecification(const IndexImpl& index) const;

  // The number of columns that the corresponding index scan will have.
  size_t numColumns() const;

  // Getter for testing.
  const GraphFilter& graphFilter() const { return graphFilter_; }
};

#endif  // QLEVER_SRC_INDEX_SCANSPECIFICATION_H
