// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "util/json.h"

/*
 * A rather basic wrapper for nlohmann::json, which only allows basic adding
 * of key value pairs and returning of the json string.
 */
class BenchmarkMetadata{
    // No real reason, to really build everything ourselves, when the
    // nlohmann::json object already containes everything, that we could need.
    nlohmann::json data_;

  public:

    /*
     * @brief Adds a key value pair to the metadata.
     */
    template<typename T>
    void addKeyValuePair(const std::string& key, const T& value){
      data_[key] = value;
    }

    /*
     * @brief Returns the metadata in form of a json string.
     */
    std::string asJsonString() const{
      return data_.dump();
    }
};
