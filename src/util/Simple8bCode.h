// Copyright 2013, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#pragma once
#include <stdint.h>
#include <algorithm>

namespace ad_utility {

//! Selector mask,
//! see: Anh & Moffat: "Index compression using 64-bit words."
static const uint64_t SIMPLE8B_SELECTOR_MASK = 0x000000000000000F;

//! Selectors,
//! see: Anh & Moffat: "Index compression using 64-bit words."
static const struct {
    unsigned char _itemWidth;
    unsigned char _groupSize;
    unsigned char _wastedBits;
    uint64_t _mask;
} SIMPLE8B_SELECTORS[16] = {
    {0, 240, 60, 0x0000000000000000},  // selector 0
    {0, 120, 60, 0x0000000000000000},  // selector 1
    {1,  60,  0, 0x0000000000000001},  // selector 2
    {2,  30,  0, 0x0000000000000003},  // selector 3
    {3,  20,  0, 0x0000000000000007},  // selector 4
    {4,  15,  0, 0x000000000000000F},  // selector 5
    {5,  12,  0, 0x000000000000001F},  // selector 6
    {6,  10,  0, 0x000000000000003F},  // selector 7
    {7,   8,  4, 0x000000000000007F},  // selector 8
    {8,   7,  4, 0x00000000000000FF},  // selector 9
    {10,  6,  0, 0x00000000000003FF},  // selector 10
    {12,  5,  0, 0x0000000000000FFF},  // selector 11
    {15,  4,  0, 0x0000000000007FFF},  // selector 12
    {20,  3,  0, 0x00000000000FFFFF},  // selector 13
    {30,  2,  0, 0x000000003FFFFFFF},  // selector 14
    {60,  1,  0, 0x0FFFFFFFFFFFFFFF},  // selector 15
};

//! Simple8b Compression Scheme.
//! See: Anh & Moffat: Index compression using 64-bit words.
//! Changed the following:
//! Selectors 0 and 1 are used to encode long streaks of the most frequent
//! elements. In the original paper, this is 1 since doclists are encoded.
//! or contexts benefit less from this.
//! Especially, our frequency encoding benefits from 0's much more (esp. scores)
//! hence we use those selectors to encode 0's instead of 1's
class Simple8bCode {
  public:

    //! Encodes a list of elements that can be interpreted as numeric values
    //! using the Simple8b compression scheme.
    //! Returns the number of bytes in the encoded array, will always be a
    //! multiple of 8.
    //! Requires encoded to be preallocated with sufficient space.
    template<typename Numeric>
    static size_t encode(Numeric* plaintext, size_t nofElements,
        uint64_t* encoded) {
      size_t nofElementsEncoded = 0;
      size_t nofCodeWordsDone = 0;
      while (nofElementsEncoded < nofElements) {
        size_t itemsLeft = nofElements - nofElementsEncoded;
        // Count the number of consecutive 0's and decide if
        // selectors 0 or 1 can be used
        bool selector0 = true;
        bool selector1 = false;
        for (size_t i = 0; i < std::min<size_t>(240, itemsLeft); ++i) {
          if (plaintext[nofElementsEncoded + i] != 0) {
            if (i > 120) {
              selector0 = false;
              selector1 = true;
              break;
            } else {
              // Not possible to use selector 0 or 1
              selector0 = false;
              break;
            }
          }
        }
        if (selector0) {
          // Use selector 0 to compress 240 consecutive 0's.
          encoded[nofCodeWordsDone] = 0;
          nofElementsEncoded += 240;
          nofCodeWordsDone++;
          continue;
        }
        if (selector1) {
          // Use selector 1 to compress 120 consecutive 0's.
          encoded[nofCodeWordsDone] = 1;
          nofElementsEncoded += 120;
          nofCodeWordsDone++;
          continue;
        }
        // Try selectors for as many items per codeword as possible,
        // take the next selector whenever it is not possible.
        for (unsigned char selector = 2; selector < 16; ++selector) {
          uint64_t codeword = selector;
          size_t nofItemsInWord = 0;
          while (nofItemsInWord < std::min<size_t>(itemsLeft,
              SIMPLE8B_SELECTORS[selector]._groupSize)) {
            // Check that the max value (60 bit) is not exceeded.
            assert(plaintext[nofElementsEncoded + nofItemsInWord]
                             <= 0x0FFFFFFFFFFFFFFF);
            // If an element is too large, break the loop.
            // Later we recognize that not enough elements
            // were written for the specific selector, and hence try the next
            // selector.
            if (plaintext[nofElementsEncoded + nofItemsInWord]
                > SIMPLE8B_SELECTORS[selector]._mask) {
              break;
            }
            codeword |= (static_cast<uint64_t>(
                plaintext[nofElementsEncoded + nofItemsInWord])
                << (4 +  // Selector bits.
                nofItemsInWord * SIMPLE8B_SELECTORS[selector]._itemWidth));
            ++nofItemsInWord;
          }
          // Check if enough elements have been written to fit the selector
          // or if the loop was left earlier.
          if (nofItemsInWord == SIMPLE8B_SELECTORS[selector]._groupSize ||
              nofItemsInWord == itemsLeft) {
            encoded[nofCodeWordsDone] = codeword;
            nofElementsEncoded += nofItemsInWord;
            ++nofCodeWordsDone;
            break;
          }
        }
      }
      return sizeof(uint64_t) * nofCodeWordsDone;
    }

    //! Decodes a list of elements using the Simple8b compression scheme.
    //! Requires decoded to be preallocated with sufficient space,
    //! i.e. sizeof(Numeric) * (nofElements + 239).
    //! The overhead is included so that no check for noundaries
    //! is necessary inside the decoding of a single codeword.
    template<typename Numeric>
    static void decode(uint64_t* encoded, size_t nofElements,
        Numeric* decoded) {
      size_t nofElementsDone = 0;
      size_t nofCodeWordsDone = 0;
      // Loop over full 64bit codewords
      while (nofElementsDone < nofElements) {
        unsigned char selector = encoded[nofCodeWordsDone]
            & SIMPLE8B_SELECTOR_MASK;
        if (selector <= 1) {
          // Case: Long sequences of 1's compressed.
          for (size_t i = 0; i < SIMPLE8B_SELECTORS[selector]._groupSize;
              ++i) {
            decoded[nofElementsDone + i] = 0;
          }
        } else {
          // Case: Usual decompression.
          for (size_t i = 0; i < SIMPLE8B_SELECTORS[selector]._groupSize;
              ++i) {
            decoded[nofElementsDone + i] = (encoded[nofCodeWordsDone] >>
                (4 + SIMPLE8B_SELECTORS[selector]._itemWidth * i))
                & SIMPLE8B_SELECTORS[selector]._mask;
          }
        }
        nofElementsDone += SIMPLE8B_SELECTORS[selector]._groupSize;
        ++nofCodeWordsDone;
      }
    }
};
}
