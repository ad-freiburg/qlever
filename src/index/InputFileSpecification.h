//
// Created by kalmbacj on 10/2/24.
//

#pragma once

#include <string>
#include <optional>
namespace qlever {
enum class Filetype { Turtle, NQuad };
struct InputFileSpecification {
  std::string filename_;
  Filetype filetype_;
  std::optional<std::string> defaultGraph_;
};
}
