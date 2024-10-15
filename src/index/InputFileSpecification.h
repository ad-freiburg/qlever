//
// Created by kalmbacj on 10/2/24.
//

#pragma once

#include <optional>
#include <string>
namespace qlever {
enum class Filetype { Turtle, NQuad };
struct InputFileSpecification {
  std::string filename_;
  Filetype filetype_;
  std::optional<std::string> defaultGraph_;
  bool parseInParallel_ = false;

  bool operator==(const InputFileSpecification&) const = default;
};
}  // namespace qlever
