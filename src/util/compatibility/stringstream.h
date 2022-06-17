//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <sstream>
#ifndef QLEVER_STRINGSTREAM_H
#define QLEVER_STRINGSTREAM_H

namespace ad_std {

inline uint64_t lenthOfStringstream(auto& stream) {
  long length = stream.tellp();
  return length < 0 ? 0ul: static_cast<uint64_t>(length);
}


}

#endif  // QLEVER_STRINGSTREAM_H
