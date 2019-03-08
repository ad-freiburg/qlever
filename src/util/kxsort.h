/* The MIT License
   Copyright (c) 2016 Dinghua Li <voutcn@gmail.com>

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

// This version of the sorting algorithm was adapted to operate on IdTables

#ifndef KXSORT_H__
#define KXSORT_H__

#include <algorithm>
#include <iterator>

#include "../engine/IdTable.h"

namespace kx {

static constexpr int kRadixBits = 8;
static constexpr size_t kInsertSortThreshold = 64;
static constexpr int kRadixMask = (1 << kRadixBits) - 1;
static constexpr int kRadixBin = 1 << kRadixBits;

//================= HELPING FUNCTIONS ====================

template <int WIDTH>
inline void insert_sort_core_(IdTableStatic<WIDTH>& table, size_t beginRow,
                              size_t endRow, unsigned int compRow) {
  for (size_t i = beginRow + 1; i < endRow; i++) {
    if (table(i, compRow) < table(i - 1, compRow)) {
      size_t j = i - 1;
      for (; j > 0 && table(i, compRow) < table(j - 1, compRow); j--) {
      }
      // Move i to be the element right before j
      table.moveRow(i, j);
    }
  }
}

template <int WIDTH, int kWhichByte>
// inline void radix_sort_core_(RandomIt s, RandomIt e, RadixTraits
// radix_traits) {
inline void radix_sort_core_(IdTableStatic<WIDTH>& table, size_t beginRow,
                             size_t endRow, unsigned int compRow) {
  size_t last_[kRadixBin + 1];
  size_t* last = last_ + 1;
  size_t count[kRadixBin] = {0};

  for (size_t i = beginRow; i < endRow; ++i) {
    ++count[(table(i, compRow) >> (kWhichByte * kRadixBits)) & kRadixMask];
  }

  last_[0] = last_[1] = beginRow;

  for (int i = 1; i < kRadixBin; ++i) {
    last[i] = last[i - 1] + count[i - 1];
  }

  // Move every entry into its bin
  for (int i = 0; i < kRadixBin; ++i) {
    // check if the current bucket is already full
    size_t end = last[i - 1] + count[i];
    if (end == endRow) {
      last[i] = endRow;
      break;
    }
    // while the bucket is not full
    while (last[i] != end) {
      size_t swapper = last[i];
      // compute the bucket into which the row at i belongs into
      int tag =
          (table(swapper, compRow) >> (kWhichByte * kRadixBits)) & kRadixMask;
      if (tag != i) {
        // While our current value does not belong into the ith bucket switch
        // it with another value from the bucket it belongs into
        do {
          table.swapRows(swapper, last[tag]++);
        } while ((tag = (table(swapper, compRow) >> (kWhichByte * kRadixBits)) &
                        kRadixMask) != i);
        // this should already have been done by the swap
        // *last[i] = swapper;
      }
      ++last[i];
    }
  }

  if constexpr (kWhichByte > 0) {
    for (int i = 0; i < kRadixBin; ++i) {
      if (count[i] > kInsertSortThreshold) {
        radix_sort_core_<WIDTH, kWhichByte - 1>(table, last[i - 1], last[i],
                                                compRow);
      } else if (count[i] > 1) {
        insert_sort_core_<WIDTH>(table, last[i - 1], last[i], compRow);
      }
    }
  }
}

//================= INTERFACES ====================

template <int WIDTH>
inline void radix_sort(IdTableStatic<WIDTH>& table, unsigned int compRow) {
  if (table.size() <= (int)kInsertSortThreshold)
    insert_sort_core_(table, 0, table.size(), compRow);
  else
    radix_sort_core_<WIDTH, sizeof(Id) - 1>(table, 0, table.size(), compRow);
}

}  // namespace kx

#endif
