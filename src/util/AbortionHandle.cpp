//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "AbortionHandle.h"

#include "util/Exception.h"

namespace ad_utility {

void AbortionHandle::abort(AbortionState reason) {
  AD_CONTRACT_CHECK(reason != AbortionState::NOT_ABORTED);

  AbortionState notAborted = AbortionState::NOT_ABORTED;
  abortionState_.compare_exchange_strong(notAborted, reason,
                                         std::memory_order_relaxed);
}
}  // namespace ad_utility
