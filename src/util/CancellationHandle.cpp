//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "CancellationHandle.h"

#include "util/Exception.h"

namespace ad_utility {

void CancellationHandle::cancel(CancellationState reason) {
  AD_CONTRACT_CHECK(reason != CancellationState::NOT_CANCELLED);

  CancellationState notAborted = CancellationState::NOT_CANCELLED;
  cancellationState_.compare_exchange_strong(notAborted, reason,
                                             std::memory_order_relaxed);
}
}  // namespace ad_utility
