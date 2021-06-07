#pragma once

#include "./Log.h"
#include <string>

namespace ad_utility {
// ______________________________________________________________________
 float stof(const std::string& input) {
   try {
     return std::stof(input);
   } catch (...) {
     LOG(ERROR) << "Could not convert string \"" + input + "\" to float using stof" << std::endl;
     return std::numeric_limits<float>::quiet_NaN();
   }
 }
// ______________________________________________________________________
double stod(const std::string& input) {
  try {
    return std::stod(input);
  } catch (...) {
    LOG(ERROR) << "Could not convert string \"" + input + "\" to double using stod" << std::endl;
    return std::numeric_limits<float>::quiet_NaN();
  }
}
}