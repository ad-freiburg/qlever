// Copyright 2012, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#ifndef SERVER_LISTIDPROVIDER_H_
#define SERVER_LISTIDPROVIDER_H_

#include <stdlib.h>
#include "./Globals.h"

namespace ad_semsearch {
class ListIdProvider {
  public:
    static size_t __nextListId;
};
}
#endif  // SERVER_LISTIDPROVIDER_H_
