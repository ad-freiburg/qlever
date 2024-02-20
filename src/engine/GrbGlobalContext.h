// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.roland.herrmann@mars.uni-freiburg.de)

extern "C" {
#include <GraphBLAS.h>
}

/**
 * @class GrbGlobalContext
 * @brief This Singleton class is based on the design by Scott Meyers. The basic
 * idea is that the singleton object exists in a 'magic' state within the
 * getContext() function. This is threadsafe.
 *
 * Reference:
 * https://laristra.github.io/flecsi/src/developer-guide/patterns/meyers_singleton.html
 *
 */
class GrbGlobalContext {
  GrbGlobalContext() { GrB_init(GrB_NONBLOCKING); }
  ~GrbGlobalContext() { GrB_finalize(); }

 public:
  static GrbGlobalContext& getContext() {
    static GrbGlobalContext context;
    return context;
  }

  GrbGlobalContext(const GrbGlobalContext&) = delete;
  GrbGlobalContext& operator=(const GrbGlobalContext&) = delete;
};
