// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Zhiwei Zhang <zhiwei.zhang@saturn.uni-freiburg.de>

#pragma once
#include <stdint.h>
#include <algorithm>
#include <assert.h>

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
  //! our contexts benefit less from this.
  //! Especially, our frequency encoding benefits from 0's much more (esp. scores)
  //! hence we use those selectors to encode 0's instead of 1's
  class Simple8bCode {
   public:

    Simple8bCode() {
      _encoded = new uint64_t[10];
      _cursor = 0;
      _word = 0;
      _selector = 0;
      _itemWidth = 0;
      _groupSize = 0;
    }

    template<typename Numeric>
    explicit Simple8bCode(const Numeric* plaintext, const size_t& nofElements) {
      _encoded = new uint64_t[nofElements];
      encode(plaintext, nofElements, _encoded);
      _cursor = 0;
      _word = _encoded[_cursor];
      _selector = _word & SIMPLE8B_SELECTOR_MASK;
      _itemWidth = SIMPLE8B_SELECTORS[_selector]._itemWidth & 15;
      _groupSize = SIMPLE8B_SELECTORS[_selector]._groupSize & 15;
      _word = _word >> 4;
    }

    ~Simple8bCode() {
      delete[] _encoded;
    }

    uint64_t getNextWord() {
      ++_cursor;
      _word = _encoded[_cursor];
      _selector = _word & SIMPLE8B_SELECTOR_MASK;
      _itemWidth = SIMPLE8B_SELECTORS[_selector]._itemWidth & 15;
      _groupSize = SIMPLE8B_SELECTORS[_selector]._groupSize & 15;
      _word = _word >> 4;
      return _word;
    }

    // ! the Successor function, to decode and return the next pointer.
    // ! This operation steps the file cursor (the current access point)
    // ! forwards a single pointer (or, depending on the exact context, a single integer).
    template<typename Numeric>
    void successor(Numeric* returnValue) {
      if (_groupSize == 0) {
        _word = getNextWord();
      }
      *returnValue = _word & SIMPLE8B_SELECTORS[_selector]._mask;
      _word = _word >> _itemWidth;
      _groupSize--;
    }

    // ! Forwards Search, to locate the next document pointer whose value
    // ! is greater than or equal to d, for some supplied value d.
    // ! This operation potentially shifts the file cursor to a large distance,
    // ! in terms of both pointers and stored integers.
    template<typename Numeric>
    void forwardSearch(const Numeric& reference) {
      Numeric currentNumeric = _word & SIMPLE8B_SELECTORS[_selector]._mask;
      while (currentNumeric < reference) {
        if (_groupSize <= 1) {
          _word = getNextWord();
          currentNumeric = _word & SIMPLE8B_SELECTORS[_selector]._mask;
          continue;
        }
        if (SIMPLE8B_SELECTORS[_selector]._mask < reference) {
          while (SIMPLE8B_SELECTORS[_selector]._mask < reference) {
            _word = getNextWord();
          }
          _itemWidth = SIMPLE8B_SELECTORS[_selector]._itemWidth & 15;
          _groupSize = SIMPLE8B_SELECTORS[_selector]._groupSize & 15;
          _word = _word >> 4;
          currentNumeric = _word & SIMPLE8B_SELECTORS[_selector]._mask;
          continue;
        }
        _word = _word >> _itemWidth;
        _groupSize--;
        currentNumeric = _word & SIMPLE8B_SELECTORS[_selector]._mask;
      }
    }

    // ! Forwards Seek, to shift the file cursor forwards by x integers,
    // ! for some supplied value x. In comparison with Forwards Search,
    // ! this operation steps a defined number of integers, rather than
    // ! until a particular value is located.
    void forwardSeek(const size_t& x) {
      size_t parameterofMovement = x;
      if (parameterofMovement > _groupSize) {
        while (parameterofMovement > _groupSize) {
          parameterofMovement -= _groupSize;
          ++_cursor;
          _groupSize = SIMPLE8B_SELECTORS[_encoded[_cursor]
                            & SIMPLE8B_SELECTOR_MASK]._groupSize & 15;
        }
        _word = _encoded[_cursor];
        _selector = _word & SIMPLE8B_SELECTOR_MASK;
        _itemWidth = SIMPLE8B_SELECTORS[_selector]._itemWidth & 15;
        _word = _word >> 4;
      }
      _word = _word >> (parameterofMovement * _itemWidth);
      _groupSize -= parameterofMovement;
    }

    // ! Encodes a list of elements that can be interpreted as numeric values
    // ! using the Simple8b compression scheme.
    // ! Returns the number of bytes in the encoded array, will always be a
    // ! multiple of 8.
    // ! Requires encoded to be preallocated with sufficient space.
    // ! the parameter show is used to controll the output.
    template<typename Numeric>
    static void encode(const Numeric* plaintext, const size_t& nofElements,
            uint64_t* encoded) {
      size_t nofElementsEncoded = 0;
      size_t nofCodeWordsDone = 0;
      while (nofElementsEncoded < nofElements) {
        // it's the lambda.
        size_t itemsLeft = nofElements - nofElementsEncoded;
        bool selector0 = true;
        bool selector1 = false;
        for (size_t i = 0; i < std::min<size_t>(240, itemsLeft); ++i) {
          if (plaintext[nofElementsEncoded + i] != 0) {
            selector0 = false;
            if (i > 120) selector1 = true;
            break;
          }
        }
        if (itemsLeft < 240 && selector0) {
          selector0 = false;
          if (itemsLeft >= 120)  selector1 = true;
        }
        if (selector0) {
          // Use selector 0 to compress 240 consecutive 0's.
          encoded[nofCodeWordsDone] = 0;
          nofElementsEncoded += 240;
          ++nofCodeWordsDone;
          continue;
        }
        if (selector1) {
          // Use selector 1 to compress 120 consecutive 0's.
          encoded[nofCodeWordsDone] = 1;
          nofElementsEncoded += 120;
          ++nofCodeWordsDone;
          continue;
        }

        for (unsigned char selector = 2; selector < 16; ++selector) {
          uint64_t codeword = selector;
          size_t nofItemsInWord = 0;

          if (itemsLeft < SIMPLE8B_SELECTORS[selector]._groupSize) {
            continue;
          }
          while (nofItemsInWord < SIMPLE8B_SELECTORS[selector]._groupSize) {
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
          if (nofItemsInWord == SIMPLE8B_SELECTORS[selector]._groupSize) {
            encoded[nofCodeWordsDone] = codeword;
            nofElementsEncoded += nofItemsInWord;
            ++nofCodeWordsDone;
            break;
          }
        }
      }
    }

    /*
    * This is the original encode function from Buchhold.
    *
    *   template<typename Numeric>
    *   static void encode(Numeric* plaintext, size_t nofElements,
    *       uint64_t* encoded) {
    *     size_t nofElementsEncoded = 0;
    *     size_t nofCodeWordsDone = 0;
    *     while (nofElementsEncoded < nofElements) {
    *       size_t itemsLeft = nofElements - nofElementsEncoded;
    *       // Count the number of consecutive 0's and decide if
    *       // selectors 0 or 1 can be used
    *       bool selector0 = true;
    *       bool selector1 = false;
    *       for (size_t i = 0; i < std::min<size_t>(240, itemsLeft); ++i) {
    *         if (plaintext[nofElementsEncoded + i] != 0) {
    *           if (i > 120) {
    *             selector0 = false;
    *             selector1 = true;
    *             break;
    *           } else {
    *             // Not possible to use selector 0 or 1
    *             selector0 = false;
    *             break;
    *           }
    *         }
    *       }
    *       if (selector0) {
    *         // Use selector 0 to compress 240 consecutive 0's.
    *         encoded[nofCodeWordsDone] = 0;
    *         nofElementsEncoded += 240;
    *         nofCodeWordsDone++;
    *         continue;
    *       }
    *       if (selector1) {
    *         // Use selector 1 to compress 120 consecutive 0's.
    *         encoded[nofCodeWordsDone] = 1;
    *         nofElementsEncoded += 120;
    *         nofCodeWordsDone++;
    *         continue;
    *       }
    *       // Try selectors for as many items per codeword as possible,
    *       // take the next selector whenever it is not possible.
    *       for (unsigned char selector = 2; selector < 16; ++selector) {
    *         uint64_t codeword = selector;
    *         size_t nofItemsInWord = 0;
    *         while (nofItemsInWord < std::min<size_t>(itemsLeft,
    *             SIMPLE8B_SELECTORS[selector]._groupSize)) {
    *           // Check that the max value (60 bit) is not exceeded.
    *           assert(plaintext[nofElementsEncoded + nofItemsInWord]
    *                            <= 0x0FFFFFFFFFFFFFFF);
    *           // If an element is too large, break the loop.
    *           // Later we recognize that not enough elements
    *           // were written for the specific selector, and hence try the next
    *           // selector.
    *           if (plaintext[nofElementsEncoded + nofItemsInWord]
    *               > SIMPLE8B_SELECTORS[selector]._mask) {
    *             break;
    *           }
    *           codeword |= (static_cast<uint64_t>(
    *               plaintext[nofElementsEncoded + nofItemsInWord])
    *               << (4 +  // Selector bits.
    *               nofItemsInWord * SIMPLE8B_SELECTORS[selector]._itemWidth));
    *           ++nofItemsInWord;
    *         }
    *         // Check if enough elements have been written to fit the selector
    *         // or if the loop was left earlier.
    *         if (nofItemsInWord == SIMPLE8B_SELECTORS[selector]._groupSize ||
    *             nofItemsInWord == itemsLeft) {
    *           encoded[nofCodeWordsDone] = codeword;
    *           nofElementsEncoded += nofItemsInWord;
    *           ++nofCodeWordsDone;
    *           break;
    *         }
    *       }
    *     }
    *   }
    */

    // ! Decodes a list of elements using the Simple8b compression scheme.
    // ! Requires decoded to be preallocated with sufficient space,
    // ! i.e. sizeof(Numeric) * (nofElements + 239).
    // ! The overhead is included so that no check for noundaries
    // ! is necessary inside the decoding of a single codeword.

    // ! This decode function is the optimal.
    template<typename Numeric>
    static void decode(const uint64_t* encoded, const size_t& nofElements,
                       Numeric* decoded) {
      size_t nofElementsDone = 0;
      size_t nofCodeWordsDone = 0;
      // Loop over full 64bit codewords
      while (nofElementsDone < nofElements) {
        unsigned char selector = encoded[nofCodeWordsDone]
            & SIMPLE8B_SELECTOR_MASK;
        if (selector > 1 && encoded[nofCodeWordsDone] != selector) {
          // Case: Usual decompression.
          // if encoded[nofCodeWordsDone] == selector, then we know that
          // there are selector consecutive 0's. We don't need to do the
          // assignment for 0, because the initial assignment of the normal
          // Numeric type is 0. We can save a lot of time to decompress the
          // code which constains not only 0.
          uint64_t word = encoded[nofCodeWordsDone] >> 4;
          for (size_t i = 0; i < SIMPLE8B_SELECTORS[selector]._groupSize;
               ++i) {
            decoded[nofElementsDone + i] = word
            & SIMPLE8B_SELECTORS[selector]._mask;
            word = word >> SIMPLE8B_SELECTORS[selector]._itemWidth;
          }
        }
        nofElementsDone += SIMPLE8B_SELECTORS[selector]._groupSize;
        ++nofCodeWordsDone;
      }
    }

    /*
    * This is the original decode function from Buchhold
    *
    *   template<typename Numeric>
    *   static void decode(uint64_t* encoded, size_t nofElements,
    *       Numeric* decoded, const bool& show = false) {
    *     size_t nofElementsDone = 0;
    *     size_t nofCodeWordsDone = 0;
    *     // Loop over full 64bit codewords
    *     while (nofElementsDone < nofElements) {
    *       unsigned char selector = encoded[nofCodeWordsDone]
    *           & SIMPLE8B_SELECTOR_MASK;
    *       if (selector > 1) {
    *         // Case: Usual decompression.
    *         for (size_t i = 0; i < SIMPLE8B_SELECTORS[selector]._groupSize;
    *              ++i) {
    *           decoded[nofElementsDone + i] = (encoded[nofCodeWordsDone] >>
    *                                           (4 + SIMPLE8B_SELECTORS[selector]._itemWidth * i))
    *                                          & SIMPLE8B_SELECTORS[selector]._mask;
    *         }
    *       } else {
    *         // Case: Long sequences of 1's (or 0's) compressed.
    *         for (size_t i = 0; i < SIMPLE8B_SELECTORS[selector]._groupSize; ++i) {
    *           decoded[nofElementsDone + i] = 0;
    *         }
    *       }
    *       nofElementsDone += SIMPLE8B_SELECTORS[selector]._groupSize;
    *       ++nofCodeWordsDone;
    *     }
    *   }
    */

   private:

    // the encoded code.
    uint64_t* _encoded;

    // the current word buffer.
    uint64_t _word;

    // the current selector of the word buffer in the encoded code.
    unsigned char _selector;

    // the current access position in the encoded code.
    size_t _cursor;

    // the current itemWidth of the word buffer.
    size_t _itemWidth;

    // the current number of the encoded word in the current word buffer.
    size_t _groupSize;
  };
}
