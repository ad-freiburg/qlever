#pragma once

#include <array>
#include <memory>
#include <stxxl/vector>
#include <vector>

#include "../global/Id.h"

struct VocabularyData {
  using TripleVec = stxxl::vector<std::array<Id, 3>>;
  // The total number of distinct words in the complete Vocabulary
  size_t nofWords;
  // Id lower and upper bound of @lang@<predicate> predicates
  Id langPredLowerBound;
  Id langPredUpperBound;
  // The number of triples in the idTriples vec that each partial vocabulary is
  // responsible for (depends on the number of additional language filter
  // triples)
  std::vector<size_t> actualPartialSizes;
  // All the triples as Ids.
  std::unique_ptr<TripleVec> idTriples;
};
