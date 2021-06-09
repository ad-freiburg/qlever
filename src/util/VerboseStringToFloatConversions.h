#pragma once

#include "./Log.h"
#include <string>

namespace ad_utility {
// ______________________________________________________________________
inline float stof(const std::string& input) {
   try {
     return std::stof(input);
   } catch (...) {
     LOG(ERROR) << "Could not convert string \"" + input + "\" to float using stof" << std::endl;
     //return std::numeric_limits<float>::quiet_NaN();
     return std::numeric_limits<float>::max();
   }
 }
// ______________________________________________________________________
inline double stod(const std::string& input) {
  try {
    return std::stod(input);
  } catch (...) {
    LOG(ERROR) << "Could not convert string \"" + input + "\" to double using stod" << std::endl;
    return std::numeric_limits<double>::max();
    //return std::numeric_limits<double>::max();
  }
}
}