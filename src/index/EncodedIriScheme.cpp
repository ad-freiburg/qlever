// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/EncodedIriScheme.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include "backports/algorithm.h"

namespace qlever {

// ____________________________________________________________________________
nlohmann::json EncodedIriScheme::toJsonWithName() const {
  nlohmann::json j = toJson();
  AD_CONTRACT_CHECK(j.is_object(),
                    "The `toJson` function of an `EncodedIriScheme` has to "
                    "return a JSON object");
  AD_CONTRACT_CHECK(!j.contains(nameKey_),
                    "The `toJson` function of an `EncodedIriScheme` must not "
                    "use the key ",
                    nameKey_);
  j[nameKey_] = name();
  return j;
}

// ____________________________________________________________________________
EncodedIriSchemePtr EncodedIriScheme::fromJsonWithName(
    const nlohmann::json& j) {
  AD_CONTRACT_CHECK(j.contains(nameKey_),
                    "The JSON representation of an `EncodedIriScheme` is "
                    "missing the key ",
                    nameKey_);
  auto name = j[nameKey_].get<std::string>();
  auto factory = EncodedIriSchemeRegistry::get().getFactory(name);
  if (!factory.has_value()) {
    throw std::runtime_error{absl::StrCat(
        "The index uses the encoding scheme \"", name,
        "\" for IRIs, but no such scheme is registered in this process. Make "
        "sure that the corresponding class is compiled in and registered via "
        "`qlever::registerEncodedIriScheme`. The registered schemes are: [",
        absl::StrJoin(EncodedIriSchemeRegistry::get().registeredNames(), ", "),
        "]")};
  }
  auto scheme = std::invoke(factory.value(), j);
  AD_CORRECTNESS_CHECK(scheme != nullptr);
  AD_CONTRACT_CHECK(
      scheme->name() == name,
      "An `EncodedIriScheme` was registered under a name that differs from "
      "the name that it reports itself, this is a bug in the scheme");
  return scheme;
}

// ____________________________________________________________________________
EncodedIriSchemeRegistry& EncodedIriSchemeRegistry::get() {
  static EncodedIriSchemeRegistry registry;
  return registry;
}

// ____________________________________________________________________________
void EncodedIriSchemeRegistry::add(std::string name, std::type_index type,
                                   Factory factory) {
  AD_CONTRACT_CHECK(!name.empty(),
                    "The name of an `EncodedIriScheme` must not be empty");
  auto ptr = schemes_.wlock();
  auto it = ptr->find(name);
  if (it != ptr->end()) {
    // Registering the same class twice is a no-op, registering a different
    // class under the same name is an error.
    AD_CONTRACT_CHECK(
        it->second.type_ == type,
        "Two different classes were registered as an `EncodedIriScheme` with "
        "the same name \"",
        name, "\"");
    return;
  }
  ptr->emplace(std::move(name), Entry{type, std::move(factory)});
}

// ____________________________________________________________________________
std::optional<EncodedIriSchemeRegistry::Factory>
EncodedIriSchemeRegistry::getFactory(const std::string& name) const {
  auto ptr = schemes_.rlock();
  auto it = ptr->find(name);
  if (it == ptr->end()) {
    return std::nullopt;
  }
  return it->second.factory_;
}

// ____________________________________________________________________________
std::vector<std::string> EncodedIriSchemeRegistry::registeredNames() const {
  std::vector<std::string> result;
  {
    auto ptr = schemes_.rlock();
    result.reserve(ptr->size());
    for (const auto& [name, entry] : *ptr) {
      result.push_back(name);
    }
  }
  ql::ranges::sort(result);
  return result;
}

}  // namespace qlever
