//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "engine/idTable/IdTableCompressedWriter.h"

TEST(IdTableCompressedWriter, firstTest) {
  std::string filename = "idTableCompressedWriterFirstTest.dat";
  IdTableCompressedWriter{filename, 3};
}
