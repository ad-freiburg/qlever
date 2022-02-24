#pragma once

#include "../global/Id.h"
#include "../util/File.h"
#include "../util/Generator.h"
#include "CompressedRelation.h"

/**
 * This allows iterating over one of the permutations of the index once.
 **/
cppcoro::generator<std::array<Id, 3>> TriplesView(const auto& permutation) {
  const auto& metaData = permutation._meta.data();
  for (auto it = metaData.ordered_begin(); it != metaData.ordered_end(); ++it) {
    uint64_t id = it.getId();
    std::vector<std::array<Id, 2>> col2And3;
    CompressedRelationMetaData::scan(id, &col2And3, permutation);
    for (const auto& [col2, col3] : col2And3) {
      std::array<Id, 3> triple{id, col2, col3};
      co_yield triple;
    }
  }
}
