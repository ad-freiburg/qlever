//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_MMAPVECTORLEGACYFORMAT_H
#define QLEVER_TEST_UTIL_MMAPVECTORLEGACYFORMAT_H

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "util/File.h"
#include "util/MmapVector.h"

namespace ad_utility::testing {
// Write `elements` to `filename` in the exact on-disk format that a legacy
// `ad_utility::MmapVector<T>` produced: the raw object representation of the
// elements at the front of the file, followed by a region of unused capacity
// that is rounded up to a multiple of the page size, followed by the
// `MmapVectorMetaData` trailer at the very end. This reproduces files written
// by older QLever versions without depending on the (removed) `MmapVector`
// implementation.
template <typename T>
void writeLegacyMmapVectorFile(const std::string& filename,
                               const std::vector<T>& elements) {
  // A legacy `MmapVector` never used a capacity below `MinCapacity` (== 100)
  // and rounded the byte size up to a multiple of the page size, leaving a
  // region of unused capacity between the elements and the trailer.
  static constexpr size_t minCapacity = 100;
  const size_t pageSize = getpagesize();
  const size_t dataBytes = elements.size() * sizeof(T);
  size_t byteSize = std::max(elements.size(), minCapacity) * sizeof(T);
  byteSize = (byteSize / pageSize + 1) * pageSize;
  const size_t capacity = byteSize / sizeof(T);

  File file{filename, "w"};
  // The actual elements, stored as their raw object representation just like
  // `MmapVector` did via its memory mapping.
  file.write(elements.data(), dataBytes);
  // The unused capacity between the elements and the trailer, zero-filled (as
  // `truncate` would leave it).
  const std::vector<char> padding(byteSize - dataBytes, 0);
  file.write(padding.data(), padding.size());
  // The trailer at the very end of the file. Only `size_` is read back, but we
  // fill in `capacity_` and `bytesize_` with the values that a real
  // `MmapVector` would have stored.
  MmapVectorMetaData{elements.size(), capacity, byteSize}.writeToFile(file);
  file.close();
}

// Read a file back the way a legacy `MmapVectorView<T>` would have: take the
// element count from the `MmapVectorMetaData` trailer at the end of the file
// and read that many elements as their raw object representation from the
// front. Used to check that files written by current QLever remain readable by
// older versions.
template <typename T>
std::vector<T> readLegacyMmapVectorFile(const std::string& filename) {
  File file{filename, "r"};
  const size_t size = MmapVectorMetaData::readFromFile(file).size_;
  std::vector<T> elements(size);
  file.read(elements.data(), size * sizeof(T), 0);
  return elements;
}
}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_MMAPVECTORLEGACYFORMAT_H
