//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "UpdateFetcher.h"

namespace ad_utility::websocket {

UpdateFetcher::payload_type UpdateFetcher::optionalFetchAndAdvance() {
  // FIXME access to data is not synchronized
  return nextIndex_ < distributor_->getData().size()
             ? distributor_->getData().at(nextIndex_++)
             : nullptr;
}
}  // namespace ad_utility::websocket
