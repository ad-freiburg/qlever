//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once
#include <optional>

#include "engine/LocalVocab.h"
#include "global/Id.h"
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
 private:
  using T = std::optional<Id>;
  using Graphs = std::optional<ad_utility::HashSet<Id>>;

  T col0Id_;
  T col1Id_;
  T col2Id_;
  std::shared_ptr<const LocalVocab> localVocab_;
  Graphs graphsToFilter_{};
  friend class ScanSpecificationAsTripleComponent;

  void validate() const;

 public:
  ScanSpecification(T col0Id, T col1Id, T col2Id, LocalVocab localVocab = {},
                    Graphs graphsToFilter = std::nullopt)
      : col0Id_{col0Id},
        col1Id_{col1Id},
        col2Id_{col2Id},
        localVocab_{std::make_shared<LocalVocab>(std::move(localVocab))},
        graphsToFilter_(std::move(graphsToFilter)) {
    validate();
  }
  const T& col0Id() const { return col0Id_; }
  const T& col1Id() const { return col1Id_; }
  const T& col2Id() const { return col2Id_; }

  const Graphs& graphsToFilter() const { return graphsToFilter_; }

  bool operator==(const ScanSpecification& s) const {
    auto tie = [](const ScanSpecification& x) {
      return std::tie(x.col0Id_, x.col1Id_, x.col2Id_, x.graphsToFilter_);
    };
    return tie(*this) == tie(s);
  }

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
  using Graphs = std::optional<ad_utility::HashSet<TripleComponent>>;

 private:
  T col0_;
  T col1_;
  T col2_;
  Graphs graphsToFilter_{};

 public:
  // Construct from three optional `TripleComponent`s. If any of the three
  // entries is unbound (`nullopt` or of type `Variable`), then all subsequent
  // entries also have to be unbound. For example if `col0` is bound, but `col1`
  // isn't, then `col2` also has to be unbound.
  ScanSpecificationAsTripleComponent(T col0, T col1, T col2,
                                     Graphs graphsToFilter = std::nullopt);

  // Convert to a `ScanSpecification`. The `index` is used to convert the
  // `TripleComponent` to `Id`s by looking them up in the vocabulary.
  ScanSpecification toScanSpecification(const IndexImpl& index) const;
  ScanSpecification toScanSpecification(const Index& index) const;

  // The number of columns that the corresponding index scan will have.
  size_t numColumns() const;
};
