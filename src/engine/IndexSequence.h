// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_INDEXSEQUENCE_H
#define QLEVER_SRC_ENGINE_INDEXSEQUENCE_H

#include <cstdint>
#include <cstdlib>

#include "backports/type_traits.h"

// Code to generate index sequences used in the optimized, precompiled
// join methods, for example. For reference:
// http://stackoverflow.com/questions/27124920/

// This already exists in C++14 (index_sequence and make_index_sequence).
// The original is faster (and this can be made) by using
// a divide and conquer strategy to concat two lists of half the length in each
// step.
template <size_t...>
struct IndexSequence {};

template <size_t N, size_t... Is>
struct MakeIndexSequence : MakeIndexSequence<N - 1, N - 1, Is...> {};

// Base case
template <size_t... Is>
struct MakeIndexSequence<0u, Is...> : IndexSequence<Is...> {
  using type = IndexSequence<Is...>;
};

// Add the possibility to leave one out.
// GenSeq<N, I> generates a sequence from 0 to N-1 without I in it
// GenSeq<4, 2> -> {0, 1, 3}
// GenSeq<2, 2> -> {0, 1}

// Use a local concat that can generate sequences with gaps.
namespace ad_local {

template <typename Seq1, size_t Offset, typename Seq2>
struct ConcatSeq;

// Concats two index lists but uses an offset for the second list.
// Using sizeof...(Is1) would lead to a "normal" concat, but the
// offset allows skipping values in the sequence.
template <size_t... Is1, size_t Offset, size_t... Is2>
struct ConcatSeq<IndexSequence<Is1...>, Offset, IndexSequence<Is2...>> {
  using type = IndexSequence<Is1..., (Offset + Is2)...>;
};
}  // namespace ad_local

// GenSeqLo ("Generate Sequence with Leave out")
// now concats two lists: one with I elements (0 to I-1) and
// if necessary because there still is something after I, another
// one with size N - I - 1. I + 1 is then used as offset
// and hence the first part of the concat yields elements
// 0 ... I - 1 and the second part I + 1 ... N. Hence there is no
// element I.
template <size_t N, size_t I>
using GenSeqLo = typename ad_local::ConcatSeq<
    typename MakeIndexSequence<I>::type, I + 1,
    typename MakeIndexSequence<(N > I) ? (N - I - 1) : 0>::type>::type;

// GenSeq is the "normal" case that doesn't need a "leave-out"
template <size_t N>
using GenSeq = typename MakeIndexSequence<N>::type;

// Test the above, also demonstrates usage.
static_assert(std::is_same<IndexSequence<0, 1, 3, 4>, GenSeqLo<5, 2>>::value,
              "");
static_assert(std::is_same<IndexSequence<1, 2>, GenSeqLo<3, 0>>::value, "");
static_assert(std::is_same<IndexSequence<0, 1, 2, 3>, GenSeqLo<4, 4>>::value,
              "");
static_assert(
    std::is_same<IndexSequence<0, 1, 2, 3>, MakeIndexSequence<4>::type>::value,
    "");
static_assert(std::is_same<MakeIndexSequence<2>::type, GenSeqLo<2, 2>>::value,
              "");
static_assert(std::is_same<GenSeq<2>, GenSeqLo<2, 2>>::value, "");
static_assert(std::is_same<MakeIndexSequence<2>::type, GenSeq<2>>::value, "");

#endif  // QLEVER_SRC_ENGINE_INDEXSEQUENCE_H
