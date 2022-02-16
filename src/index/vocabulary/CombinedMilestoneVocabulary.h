//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMBINEDMILESTONEVOCABULARY_H
#define QLEVER_COMBINEDMILESTONEVOCABULARY_H

#include "../../global/Id.h"
#include "../VocabularyOnDisk.h"
#include "./CombinedVocabulary.h"
#include "./VocabularyInMemory.h"

template <uint64_t distanceBetweenMilestones>
class MilestoneIndexConverter {
 private:
  ad_utility::MilestoneIdManager<distanceBetweenMilestones> _idManager;

 public:
  bool isInFirst(uint64_t id, const auto&) const {
    return _idManager.isMilestoneId(id);
  }

  uint64_t localFirstToGlobal(uint64_t id, const auto&) const {
    return _idManager.milestoneIdFromLocal(id);
    ;
  }

  uint64_t localSecondToGlobal(uint64_t id, const auto&) const { return id; }

  uint64_t globalToLocalFirst(uint64_t id, const auto&) const {
    return _idManager.milestoneIdToLocal(id);
  }

  uint64_t globalToLocalSecond(uint64_t id, const auto&) const {
    return _idManager.milestoneIdToLocal(id);
  }
};

#endif  // QLEVER_COMBINEDMILESTONEVOCABULARY_H
