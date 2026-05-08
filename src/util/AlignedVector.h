//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
//  You may not use this file except in compliance with the Apache 2.0 License,
//  which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ALIGNEDVECTOR_H
#define QLEVER_SRC_UTIL_ALIGNEDVECTOR_H

#include <boost/align/aligned_allocator.hpp>
#include <new>
#include <vector>

// Helper aliases to provide `std::vector` with aligned memory buffers.
namespace qlever::util {
template <class T, std::size_t Alignment>
using AlignedVector =
    std::vector<T, boost::alignment::aligned_allocator<T, Alignment>>;

#ifdef __cpp_lib_hardware_interference_size
constexpr std::size_t L1_CACHE_LINE_SIZE =
    std::hardware_constructive_interference_size;
#else
// Fallback for systems that don't define this.
constexpr std::size_t L1_CACHE_LINE_SIZE = 64;
#endif

template <class T>
using CacheAlignedVector = AlignedVector<T, L1_CACHE_LINE_SIZE>;
}  // namespace qlever::util

#endif  // QLEVER_SRC_UTIL_ALIGNEDVECTOR_H
