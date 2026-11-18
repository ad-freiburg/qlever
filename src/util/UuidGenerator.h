// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_UUIDGENERATOR_H
#define QLEVER_SRC_UTIL_UUIDGENERATOR_H

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <random>
#include <string>

#include "util/Random.h"

namespace ad_utility {

// Generate random UUIDs as strings.
//
// NOTE: This class lives in its own header (and not in `util/Random.h`),
// because `Random.h` is included almost everywhere (via `EnumWithStrings.h`
// and `Log.h`) and the Boost.Uuid headers are expensive to compile.
class UuidGenerator {
 public:
  explicit UuidGenerator(
      RandomSeed seed = RandomSeed::make(std::random_device{}()))
      : randomEngine_{seed.get()}, gen_(randomEngine_) {}

  std::string operator()() { return boost::uuids::to_string(gen_()); }

 private:
  std::mt19937_64 randomEngine_;
  boost::uuids::basic_random_generator<std::mt19937_64> gen_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_UUIDGENERATOR_H
