// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "global/RuntimeParameters.h"
[[nodiscard]] ad_utility::HashMap<std::string, std::string>
RuntimeParameters::toMap() const {
  static ad_utility::HashMap<std::string, std::string> result = [this]() {
    ad_utility::HashMap<std::string, std::string> result;

    for (const auto& [name, parameter] : runtimeMap_) {
      result[name] = parameter->toString();
    }

    return result;
  }();

  return result;
}

void RuntimeParameters::setFromString(const std::string& name,
                                      const std::string& value) {
  if (!runtimeMap_.contains(name)) {
    throw std::runtime_error{"No parameter with name " + std::string{name} +
                             " exists"};
  }
  try {
    runtimeMap_.at(name)->setFromString(value);
  } catch (const std::exception& e) {
    throw std::runtime_error("Could not set parameter " + std::string{name} +
                             " to value " + value +
                             ". Exception was: " + e.what());
  }
}

std::vector<std::string> RuntimeParameters::getKeys() const {
  static std::vector<std::string> keys = [this]() {
    std::vector<std::string> result;
    result.reserve(runtimeMap_.size());
    for (const auto& [name, _] : runtimeMap_) {
      result.push_back(name);
    }
    return result;
  }();

  return keys;
}
