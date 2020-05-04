//
// Created by johannes on 04.05.20.
//
#include<random>

#ifndef QLEVER_RANDOM_H
#define QLEVER_RANDOM_H

namespace ad_utility {

template <typename INT>
  class RandomIntGenerator {
  std::random_device r_;
  std::default_random_engine engine_;
  std::uniform_int_distribution<INT> dist_;
 public:
  RandomIntGenerator(INT min = std::numeric_limits<INT>::min(), INT max = std::numeric_limits<INT>::max()) :
    r_(), engine_(r_()), dist_(min, max) {}

  INT operator()() {
    return dist_(engine_);
  }

};
template <typename FLOAT>
class RandomFloatGenerator {
  std::random_device r_;
  std::default_random_engine engine_;
  std::uniform_real_distribution<FLOAT> dist_;

 public:
  RandomFloatGenerator(FLOAT min = std::numeric_limits<FLOAT>::min(), FLOAT max = std::numeric_limits<FLOAT>::max()) :
      r_(), engine_(r_()), dist_(min, max) {}

  FLOAT operator()() {
    return dist_(engine_);
  }
};

}

#endif  // QLEVER_RANDOM_H
