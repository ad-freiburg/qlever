// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (December of 2023,
// schlegea@informatik.uni-freiburg.de)

#ifndef QLEVER_TEST_UTIL_PRINTCONFIGURATIONDOCCOMPARISONSTRING_H
#define QLEVER_TEST_UTIL_PRINTCONFIGURATIONDOCCOMPARISONSTRING_H

/*
The comparison strings for the test `PrintConfigurationDocComparison` in file
`ConfigManagerTest`.

TODO Don't just copy the new output into the comparison strings, if you change
`printConfigurationDoc`! Actually check, if everything is formatted as it should
be!
*/

// The strings to compare against.
#include <absl/strings/str_cat.h>

#include <string_view>
constexpr std::string_view emptyConfigManagerExpectedString =
    "No configuration options were defined.";
constexpr std::string_view exampleConfigManagerExpectedNotDetailedString =
    R"--(Configuration:
{
  "booleanWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "booleanWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": true,
  "booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": true,
  "booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": true,
  "booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": true,
  "booleanWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "booleanWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": true,
  "booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": true,
  "booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": true,
  "booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": true,
  "stringWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "stringWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": "01",
  "stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": "01",
  "stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": "01",
  "stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": "01",
  "stringWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "stringWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": "01",
  "stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": "01",
  "stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": "01",
  "stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": "01",
  "integerWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "integerWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 3,
  "integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 3,
  "integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 3,
  "integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 3,
  "integerWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "integerWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 3,
  "integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 3,
  "integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 3,
  "integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 3,
  "unsignedintegerWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "unsignedintegerWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 3,
  "unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 3,
  "unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 3,
  "unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 3,
  "unsignedintegerWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "unsignedintegerWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 3,
  "unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 3,
  "unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 3,
  "unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 3,
  "floatWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "floatWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 43.70137405395508,
  "floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 43.70137405395508,
  "floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 43.70137405395508,
  "floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 43.70137405395508,
  "floatWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "floatWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 43.70137405395508,
  "floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 43.70137405395508,
  "floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 43.70137405395508,
  "floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 43.70137405395508,
  "listofbooleansWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listofbooleansWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    false,
    true
  ],
  "listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    false,
    true
  ],
  "listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    false,
    true
  ],
  "listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    false,
    true
  ],
  "listofbooleansWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listofbooleansWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    false,
    true
  ],
  "listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    false,
    true
  ],
  "listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    false,
    true
  ],
  "listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    false,
    true
  ],
  "listofstringsWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listofstringsWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    "0",
    "01"
  ],
  "listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    "0",
    "01"
  ],
  "listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    "0",
    "01"
  ],
  "listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    "0",
    "01"
  ],
  "listofstringsWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listofstringsWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    "0",
    "01"
  ],
  "listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    "0",
    "01"
  ],
  "listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    "0",
    "01"
  ],
  "listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    "0",
    "01"
  ],
  "listofintegersWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listofintegersWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    1,
    3
  ],
  "listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    1,
    3
  ],
  "listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    1,
    3
  ],
  "listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    1,
    3
  ],
  "listofintegersWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listofintegersWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    1,
    3
  ],
  "listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    1,
    3
  ],
  "listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    1,
    3
  ],
  "listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    1,
    3
  ],
  "listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    1,
    3
  ],
  "listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    1,
    3
  ],
  "listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    1,
    3
  ],
  "listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    1,
    3
  ],
  "listofunsignedintegersWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listofunsignedintegersWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    1,
    3
  ],
  "listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    1,
    3
  ],
  "listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    1,
    3
  ],
  "listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    1,
    3
  ],
  "listoffloatsWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listoffloatsWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    0.0,
    43.70137405395508
  ],
  "listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    0.0,
    43.70137405395508
  ],
  "listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    0.0,
    43.70137405395508
  ],
  "listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    0.0,
    43.70137405395508
  ],
  "listoffloatsWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
  "listoffloatsWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
  "listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
    0.0,
    43.70137405395508
  ],
  "listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
    0.0,
    43.70137405395508
  ],
  "listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
    0.0,
    43.70137405395508
  ],
  "listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
    0.0,
    43.70137405395508
  ],
  "subManager": {
    "booleanWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "booleanWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": true,
    "booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": true,
    "booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": true,
    "booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": true,
    "booleanWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "booleanWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": true,
    "booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": true,
    "booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": true,
    "booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": true,
    "stringWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "stringWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": "01",
    "stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": "01",
    "stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": "01",
    "stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": "01",
    "stringWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "stringWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": "01",
    "stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": "01",
    "stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": "01",
    "stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": "01",
    "integerWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "integerWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 3,
    "integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 3,
    "integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 3,
    "integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 3,
    "integerWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "integerWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 3,
    "integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 3,
    "integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 3,
    "integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 3,
    "unsignedintegerWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "unsignedintegerWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 3,
    "unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 3,
    "unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 3,
    "unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 3,
    "unsignedintegerWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "unsignedintegerWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 3,
    "unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 3,
    "unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 3,
    "unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 3,
    "floatWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "floatWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 43.70137405395508,
    "floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 43.70137405395508,
    "floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 43.70137405395508,
    "floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 43.70137405395508,
    "floatWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "floatWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": 43.70137405395508,
    "floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": 43.70137405395508,
    "floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": 43.70137405395508,
    "floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": 43.70137405395508,
    "listofbooleansWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listofbooleansWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      false,
      true
    ],
    "listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      false,
      true
    ],
    "listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      false,
      true
    ],
    "listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      false,
      true
    ],
    "listofbooleansWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listofbooleansWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      false,
      true
    ],
    "listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      false,
      true
    ],
    "listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      false,
      true
    ],
    "listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      false,
      true
    ],
    "listofstringsWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listofstringsWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      "0",
      "01"
    ],
    "listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      "0",
      "01"
    ],
    "listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      "0",
      "01"
    ],
    "listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      "0",
      "01"
    ],
    "listofstringsWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listofstringsWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      "0",
      "01"
    ],
    "listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      "0",
      "01"
    ],
    "listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      "0",
      "01"
    ],
    "listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      "0",
      "01"
    ],
    "listofintegersWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listofintegersWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      1,
      3
    ],
    "listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      1,
      3
    ],
    "listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      1,
      3
    ],
    "listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      1,
      3
    ],
    "listofintegersWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listofintegersWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      1,
      3
    ],
    "listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      1,
      3
    ],
    "listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      1,
      3
    ],
    "listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      1,
      3
    ],
    "listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      1,
      3
    ],
    "listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      1,
      3
    ],
    "listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      1,
      3
    ],
    "listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      1,
      3
    ],
    "listofunsignedintegersWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listofunsignedintegersWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      1,
      3
    ],
    "listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      1,
      3
    ],
    "listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      1,
      3
    ],
    "listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      1,
      3
    ],
    "listoffloatsWithoutDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listoffloatsWithoutDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      0.0,
      43.70137405395508
    ],
    "listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      0.0,
      43.70137405395508
    ],
    "listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      0.0,
      43.70137405395508
    ],
    "listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      0.0,
      43.70137405395508
    ],
    "listoffloatsWithDescriptionWithoutDefaultValueWithoutValidator": "[must be specified]",
    "listoffloatsWithDescriptionWithoutDefaultValueWithValidator": "[must be specified]",
    "listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator": [
      0.0,
      43.70137405395508
    ],
    "listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator": [
      0.0,
      43.70137405395508
    ],
    "listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator": [
      0.0,
      43.70137405395508
    ],
    "listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator": [
      0.0,
      43.70137405395508
    ],
    "doubleArgumentValidatorFirstArgument": "[must be specified]",
    "doubleArgumentValidatorSecondArgument": "[must be specified]"
  }
})--";

inline const std::string& exampleConfigManagerExpectedDetailedString =
    absl::StrCat(exampleConfigManagerExpectedNotDetailedString, R"--(

Option 'booleanWithoutDescriptionWithoutDefaultValueWithoutValidator' [boolean]
Value: [must be specified]

Option 'booleanWithoutDescriptionWithoutDefaultValueWithValidator' [boolean]
Value: [must be specified]
Required invariants:
    - Validator for configuration options booleanWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [boolean]
Value: true

Option 'booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [boolean]
Value: true
Default: false

Option 'booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [boolean]
Value: true
Required invariants:
    - Validator for configuration options booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [boolean]
Value: true
Default: false
Required invariants:
    - Validator for configuration options booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'booleanWithDescriptionWithoutDefaultValueWithoutValidator' [boolean]
Value: [must be specified]
Description: Description for type boolean.

Option 'booleanWithDescriptionWithoutDefaultValueWithValidator' [boolean]
Value: [must be specified]
Description: Description for type boolean.
Required invariants:
    - Validator for configuration options booleanWithDescriptionWithoutDefaultValueWithValidator.

Option 'booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [boolean]
Value: true
Description: Description for type boolean.

Option 'booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [boolean]
Value: true
Default: false
Description: Description for type boolean.

Option 'booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [boolean]
Value: true
Description: Description for type boolean.
Required invariants:
    - Validator for configuration options booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [boolean]
Value: true
Default: false
Description: Description for type boolean.
Required invariants:
    - Validator for configuration options booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'stringWithoutDescriptionWithoutDefaultValueWithoutValidator' [string]
Value: [must be specified]

Option 'stringWithoutDescriptionWithoutDefaultValueWithValidator' [string]
Value: [must be specified]
Required invariants:
    - Validator for configuration options stringWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [string]
Value: "01"

Option 'stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [string]
Value: "01"
Default: "0"

Option 'stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [string]
Value: "01"
Required invariants:
    - Validator for configuration options stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [string]
Value: "01"
Default: "0"
Required invariants:
    - Validator for configuration options stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'stringWithDescriptionWithoutDefaultValueWithoutValidator' [string]
Value: [must be specified]
Description: Description for type string.

Option 'stringWithDescriptionWithoutDefaultValueWithValidator' [string]
Value: [must be specified]
Description: Description for type string.
Required invariants:
    - Validator for configuration options stringWithDescriptionWithoutDefaultValueWithValidator.

Option 'stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [string]
Value: "01"
Description: Description for type string.

Option 'stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [string]
Value: "01"
Default: "0"
Description: Description for type string.

Option 'stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [string]
Value: "01"
Description: Description for type string.
Required invariants:
    - Validator for configuration options stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [string]
Value: "01"
Default: "0"
Description: Description for type string.
Required invariants:
    - Validator for configuration options stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'integerWithoutDescriptionWithoutDefaultValueWithoutValidator' [integer]
Value: [must be specified]

Option 'integerWithoutDescriptionWithoutDefaultValueWithValidator' [integer]
Value: [must be specified]
Required invariants:
    - Validator for configuration options integerWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [integer]
Value: 3

Option 'integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [integer]
Value: 3
Default: 1

Option 'integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [integer]
Value: 3
Required invariants:
    - Validator for configuration options integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [integer]
Value: 3
Default: 1
Required invariants:
    - Validator for configuration options integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'integerWithDescriptionWithoutDefaultValueWithoutValidator' [integer]
Value: [must be specified]
Description: Description for type integer.

Option 'integerWithDescriptionWithoutDefaultValueWithValidator' [integer]
Value: [must be specified]
Description: Description for type integer.
Required invariants:
    - Validator for configuration options integerWithDescriptionWithoutDefaultValueWithValidator.

Option 'integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [integer]
Value: 3
Description: Description for type integer.

Option 'integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [integer]
Value: 3
Default: 1
Description: Description for type integer.

Option 'integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [integer]
Value: 3
Description: Description for type integer.
Required invariants:
    - Validator for configuration options integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [integer]
Value: 3
Default: 1
Description: Description for type integer.
Required invariants:
    - Validator for configuration options integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'unsignedintegerWithoutDescriptionWithoutDefaultValueWithoutValidator' [unsigned integer]
Value: [must be specified]

Option 'unsignedintegerWithoutDescriptionWithoutDefaultValueWithValidator' [unsigned integer]
Value: [must be specified]
Required invariants:
    - Validator for configuration options unsignedintegerWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [unsigned integer]
Value: 3

Option 'unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [unsigned integer]
Value: 3
Default: 1

Option 'unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [unsigned integer]
Value: 3
Required invariants:
    - Validator for configuration options unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [unsigned integer]
Value: 3
Default: 1
Required invariants:
    - Validator for configuration options unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'unsignedintegerWithDescriptionWithoutDefaultValueWithoutValidator' [unsigned integer]
Value: [must be specified]
Description: Description for type unsigned integer.

Option 'unsignedintegerWithDescriptionWithoutDefaultValueWithValidator' [unsigned integer]
Value: [must be specified]
Description: Description for type unsigned integer.
Required invariants:
    - Validator for configuration options unsignedintegerWithDescriptionWithoutDefaultValueWithValidator.

Option 'unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [unsigned integer]
Value: 3
Description: Description for type unsigned integer.

Option 'unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [unsigned integer]
Value: 3
Default: 1
Description: Description for type unsigned integer.

Option 'unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [unsigned integer]
Value: 3
Description: Description for type unsigned integer.
Required invariants:
    - Validator for configuration options unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [unsigned integer]
Value: 3
Default: 1
Description: Description for type unsigned integer.
Required invariants:
    - Validator for configuration options unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'floatWithoutDescriptionWithoutDefaultValueWithoutValidator' [float]
Value: [must be specified]

Option 'floatWithoutDescriptionWithoutDefaultValueWithValidator' [float]
Value: [must be specified]
Required invariants:
    - Validator for configuration options floatWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [float]
Value: 43.701374

Option 'floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [float]
Value: 43.701374
Default: 0.000000

Option 'floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [float]
Value: 43.701374
Required invariants:
    - Validator for configuration options floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [float]
Value: 43.701374
Default: 0.000000
Required invariants:
    - Validator for configuration options floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'floatWithDescriptionWithoutDefaultValueWithoutValidator' [float]
Value: [must be specified]
Description: Description for type float.

Option 'floatWithDescriptionWithoutDefaultValueWithValidator' [float]
Value: [must be specified]
Description: Description for type float.
Required invariants:
    - Validator for configuration options floatWithDescriptionWithoutDefaultValueWithValidator.

Option 'floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [float]
Value: 43.701374
Description: Description for type float.

Option 'floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [float]
Value: 43.701374
Default: 0.000000
Description: Description for type float.

Option 'floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [float]
Value: 43.701374
Description: Description for type float.
Required invariants:
    - Validator for configuration options floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [float]
Value: 43.701374
Default: 0.000000
Description: Description for type float.
Required invariants:
    - Validator for configuration options floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listofbooleansWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of booleans]
Value: [must be specified]

Option 'listofbooleansWithoutDescriptionWithoutDefaultValueWithValidator' [list of booleans]
Value: [must be specified]
Required invariants:
    - Validator for configuration options listofbooleansWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of booleans]
Value: [false, true]

Option 'listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of booleans]
Value: [false, true]
Default: [false]

Option 'listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of booleans]
Value: [false, true]
Required invariants:
    - Validator for configuration options listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of booleans]
Value: [false, true]
Default: [false]
Required invariants:
    - Validator for configuration options listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listofbooleansWithDescriptionWithoutDefaultValueWithoutValidator' [list of booleans]
Value: [must be specified]
Description: Description for type list of booleans.

Option 'listofbooleansWithDescriptionWithoutDefaultValueWithValidator' [list of booleans]
Value: [must be specified]
Description: Description for type list of booleans.
Required invariants:
    - Validator for configuration options listofbooleansWithDescriptionWithoutDefaultValueWithValidator.

Option 'listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of booleans]
Value: [false, true]
Description: Description for type list of booleans.

Option 'listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of booleans]
Value: [false, true]
Default: [false]
Description: Description for type list of booleans.

Option 'listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of booleans]
Value: [false, true]
Description: Description for type list of booleans.
Required invariants:
    - Validator for configuration options listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of booleans]
Value: [false, true]
Default: [false]
Description: Description for type list of booleans.
Required invariants:
    - Validator for configuration options listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listofstringsWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of strings]
Value: [must be specified]

Option 'listofstringsWithoutDescriptionWithoutDefaultValueWithValidator' [list of strings]
Value: [must be specified]
Required invariants:
    - Validator for configuration options listofstringsWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of strings]
Value: ["0", "01"]

Option 'listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of strings]
Value: ["0", "01"]
Default: ["0"]

Option 'listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of strings]
Value: ["0", "01"]
Required invariants:
    - Validator for configuration options listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of strings]
Value: ["0", "01"]
Default: ["0"]
Required invariants:
    - Validator for configuration options listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listofstringsWithDescriptionWithoutDefaultValueWithoutValidator' [list of strings]
Value: [must be specified]
Description: Description for type list of strings.

Option 'listofstringsWithDescriptionWithoutDefaultValueWithValidator' [list of strings]
Value: [must be specified]
Description: Description for type list of strings.
Required invariants:
    - Validator for configuration options listofstringsWithDescriptionWithoutDefaultValueWithValidator.

Option 'listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of strings]
Value: ["0", "01"]
Description: Description for type list of strings.

Option 'listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of strings]
Value: ["0", "01"]
Default: ["0"]
Description: Description for type list of strings.

Option 'listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of strings]
Value: ["0", "01"]
Description: Description for type list of strings.
Required invariants:
    - Validator for configuration options listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of strings]
Value: ["0", "01"]
Default: ["0"]
Description: Description for type list of strings.
Required invariants:
    - Validator for configuration options listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listofintegersWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of integers]
Value: [must be specified]

Option 'listofintegersWithoutDescriptionWithoutDefaultValueWithValidator' [list of integers]
Value: [must be specified]
Required invariants:
    - Validator for configuration options listofintegersWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of integers]
Value: [1, 3]

Option 'listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of integers]
Value: [1, 3]
Default: [1]

Option 'listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of integers]
Value: [1, 3]
Required invariants:
    - Validator for configuration options listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of integers]
Value: [1, 3]
Default: [1]
Required invariants:
    - Validator for configuration options listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listofintegersWithDescriptionWithoutDefaultValueWithoutValidator' [list of integers]
Value: [must be specified]
Description: Description for type list of integers.

Option 'listofintegersWithDescriptionWithoutDefaultValueWithValidator' [list of integers]
Value: [must be specified]
Description: Description for type list of integers.
Required invariants:
    - Validator for configuration options listofintegersWithDescriptionWithoutDefaultValueWithValidator.

Option 'listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of integers]
Value: [1, 3]
Description: Description for type list of integers.

Option 'listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of integers]
Value: [1, 3]
Default: [1]
Description: Description for type list of integers.

Option 'listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of integers]
Value: [1, 3]
Description: Description for type list of integers.
Required invariants:
    - Validator for configuration options listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of integers]
Value: [1, 3]
Default: [1]
Description: Description for type list of integers.
Required invariants:
    - Validator for configuration options listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of unsigned integers]
Value: [must be specified]

Option 'listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithValidator' [list of unsigned integers]
Value: [must be specified]
Required invariants:
    - Validator for configuration options listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of unsigned integers]
Value: [1, 3]

Option 'listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of unsigned integers]
Value: [1, 3]
Default: [1]

Option 'listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of unsigned integers]
Value: [1, 3]
Required invariants:
    - Validator for configuration options listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of unsigned integers]
Value: [1, 3]
Default: [1]
Required invariants:
    - Validator for configuration options listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listofunsignedintegersWithDescriptionWithoutDefaultValueWithoutValidator' [list of unsigned integers]
Value: [must be specified]
Description: Description for type list of unsigned integers.

Option 'listofunsignedintegersWithDescriptionWithoutDefaultValueWithValidator' [list of unsigned integers]
Value: [must be specified]
Description: Description for type list of unsigned integers.
Required invariants:
    - Validator for configuration options listofunsignedintegersWithDescriptionWithoutDefaultValueWithValidator.

Option 'listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of unsigned integers]
Value: [1, 3]
Description: Description for type list of unsigned integers.

Option 'listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of unsigned integers]
Value: [1, 3]
Default: [1]
Description: Description for type list of unsigned integers.

Option 'listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of unsigned integers]
Value: [1, 3]
Description: Description for type list of unsigned integers.
Required invariants:
    - Validator for configuration options listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of unsigned integers]
Value: [1, 3]
Default: [1]
Description: Description for type list of unsigned integers.
Required invariants:
    - Validator for configuration options listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listoffloatsWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of floats]
Value: [must be specified]

Option 'listoffloatsWithoutDescriptionWithoutDefaultValueWithValidator' [list of floats]
Value: [must be specified]
Required invariants:
    - Validator for configuration options listoffloatsWithoutDescriptionWithoutDefaultValueWithValidator.

Option 'listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of floats]
Value: [0.000000, 43.701374]

Option 'listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of floats]
Value: [0.000000, 43.701374]
Default: [0.000000]

Option 'listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of floats]
Value: [0.000000, 43.701374]
Required invariants:
    - Validator for configuration options listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of floats]
Value: [0.000000, 43.701374]
Default: [0.000000]
Required invariants:
    - Validator for configuration options listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Option 'listoffloatsWithDescriptionWithoutDefaultValueWithoutValidator' [list of floats]
Value: [must be specified]
Description: Description for type list of floats.

Option 'listoffloatsWithDescriptionWithoutDefaultValueWithValidator' [list of floats]
Value: [must be specified]
Description: Description for type list of floats.
Required invariants:
    - Validator for configuration options listoffloatsWithDescriptionWithoutDefaultValueWithValidator.

Option 'listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of floats]
Value: [0.000000, 43.701374]
Description: Description for type list of floats.

Option 'listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of floats]
Value: [0.000000, 43.701374]
Default: [0.000000]
Description: Description for type list of floats.

Option 'listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of floats]
Value: [0.000000, 43.701374]
Description: Description for type list of floats.
Required invariants:
    - Validator for configuration options listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.

Option 'listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of floats]
Value: [0.000000, 43.701374]
Default: [0.000000]
Description: Description for type list of floats.
Required invariants:
    - Validator for configuration options listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.

Sub manager 'subManager'
    Option 'booleanWithoutDescriptionWithoutDefaultValueWithoutValidator' [boolean]
    Value: [must be specified]
    
    Option 'booleanWithoutDescriptionWithoutDefaultValueWithValidator' [boolean]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options booleanWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [boolean]
    Value: true
    
    Option 'booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [boolean]
    Value: true
    Default: false
    
    Option 'booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [boolean]
    Value: true
    Required invariants:
        - Validator for configuration options booleanWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [boolean]
    Value: true
    Default: false
    Required invariants:
        - Validator for configuration options booleanWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'booleanWithDescriptionWithoutDefaultValueWithoutValidator' [boolean]
    Value: [must be specified]
    Description: Description for type boolean.
    
    Option 'booleanWithDescriptionWithoutDefaultValueWithValidator' [boolean]
    Value: [must be specified]
    Description: Description for type boolean.
    Required invariants:
        - Validator for configuration options booleanWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [boolean]
    Value: true
    Description: Description for type boolean.
    
    Option 'booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [boolean]
    Value: true
    Default: false
    Description: Description for type boolean.
    
    Option 'booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [boolean]
    Value: true
    Description: Description for type boolean.
    Required invariants:
        - Validator for configuration options booleanWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [boolean]
    Value: true
    Default: false
    Description: Description for type boolean.
    Required invariants:
        - Validator for configuration options booleanWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'stringWithoutDescriptionWithoutDefaultValueWithoutValidator' [string]
    Value: [must be specified]
    
    Option 'stringWithoutDescriptionWithoutDefaultValueWithValidator' [string]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options stringWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [string]
    Value: "01"
    
    Option 'stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [string]
    Value: "01"
    Default: "0"
    
    Option 'stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [string]
    Value: "01"
    Required invariants:
        - Validator for configuration options stringWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [string]
    Value: "01"
    Default: "0"
    Required invariants:
        - Validator for configuration options stringWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'stringWithDescriptionWithoutDefaultValueWithoutValidator' [string]
    Value: [must be specified]
    Description: Description for type string.
    
    Option 'stringWithDescriptionWithoutDefaultValueWithValidator' [string]
    Value: [must be specified]
    Description: Description for type string.
    Required invariants:
        - Validator for configuration options stringWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [string]
    Value: "01"
    Description: Description for type string.
    
    Option 'stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [string]
    Value: "01"
    Default: "0"
    Description: Description for type string.
    
    Option 'stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [string]
    Value: "01"
    Description: Description for type string.
    Required invariants:
        - Validator for configuration options stringWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [string]
    Value: "01"
    Default: "0"
    Description: Description for type string.
    Required invariants:
        - Validator for configuration options stringWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'integerWithoutDescriptionWithoutDefaultValueWithoutValidator' [integer]
    Value: [must be specified]
    
    Option 'integerWithoutDescriptionWithoutDefaultValueWithValidator' [integer]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options integerWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [integer]
    Value: 3
    
    Option 'integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [integer]
    Value: 3
    Default: 1
    
    Option 'integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [integer]
    Value: 3
    Required invariants:
        - Validator for configuration options integerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [integer]
    Value: 3
    Default: 1
    Required invariants:
        - Validator for configuration options integerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'integerWithDescriptionWithoutDefaultValueWithoutValidator' [integer]
    Value: [must be specified]
    Description: Description for type integer.
    
    Option 'integerWithDescriptionWithoutDefaultValueWithValidator' [integer]
    Value: [must be specified]
    Description: Description for type integer.
    Required invariants:
        - Validator for configuration options integerWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [integer]
    Value: 3
    Description: Description for type integer.
    
    Option 'integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [integer]
    Value: 3
    Default: 1
    Description: Description for type integer.
    
    Option 'integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [integer]
    Value: 3
    Description: Description for type integer.
    Required invariants:
        - Validator for configuration options integerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [integer]
    Value: 3
    Default: 1
    Description: Description for type integer.
    Required invariants:
        - Validator for configuration options integerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'unsignedintegerWithoutDescriptionWithoutDefaultValueWithoutValidator' [unsigned integer]
    Value: [must be specified]
    
    Option 'unsignedintegerWithoutDescriptionWithoutDefaultValueWithValidator' [unsigned integer]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options unsignedintegerWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [unsigned integer]
    Value: 3
    
    Option 'unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [unsigned integer]
    Value: 3
    Default: 1
    
    Option 'unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [unsigned integer]
    Value: 3
    Required invariants:
        - Validator for configuration options unsignedintegerWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [unsigned integer]
    Value: 3
    Default: 1
    Required invariants:
        - Validator for configuration options unsignedintegerWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'unsignedintegerWithDescriptionWithoutDefaultValueWithoutValidator' [unsigned integer]
    Value: [must be specified]
    Description: Description for type unsigned integer.
    
    Option 'unsignedintegerWithDescriptionWithoutDefaultValueWithValidator' [unsigned integer]
    Value: [must be specified]
    Description: Description for type unsigned integer.
    Required invariants:
        - Validator for configuration options unsignedintegerWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [unsigned integer]
    Value: 3
    Description: Description for type unsigned integer.
    
    Option 'unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [unsigned integer]
    Value: 3
    Default: 1
    Description: Description for type unsigned integer.
    
    Option 'unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [unsigned integer]
    Value: 3
    Description: Description for type unsigned integer.
    Required invariants:
        - Validator for configuration options unsignedintegerWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [unsigned integer]
    Value: 3
    Default: 1
    Description: Description for type unsigned integer.
    Required invariants:
        - Validator for configuration options unsignedintegerWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'floatWithoutDescriptionWithoutDefaultValueWithoutValidator' [float]
    Value: [must be specified]
    
    Option 'floatWithoutDescriptionWithoutDefaultValueWithValidator' [float]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options floatWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [float]
    Value: 43.701374
    
    Option 'floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [float]
    Value: 43.701374
    Default: 0.000000
    
    Option 'floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [float]
    Value: 43.701374
    Required invariants:
        - Validator for configuration options floatWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [float]
    Value: 43.701374
    Default: 0.000000
    Required invariants:
        - Validator for configuration options floatWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'floatWithDescriptionWithoutDefaultValueWithoutValidator' [float]
    Value: [must be specified]
    Description: Description for type float.
    
    Option 'floatWithDescriptionWithoutDefaultValueWithValidator' [float]
    Value: [must be specified]
    Description: Description for type float.
    Required invariants:
        - Validator for configuration options floatWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [float]
    Value: 43.701374
    Description: Description for type float.
    
    Option 'floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [float]
    Value: 43.701374
    Default: 0.000000
    Description: Description for type float.
    
    Option 'floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [float]
    Value: 43.701374
    Description: Description for type float.
    Required invariants:
        - Validator for configuration options floatWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [float]
    Value: 43.701374
    Default: 0.000000
    Description: Description for type float.
    Required invariants:
        - Validator for configuration options floatWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listofbooleansWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of booleans]
    Value: [must be specified]
    
    Option 'listofbooleansWithoutDescriptionWithoutDefaultValueWithValidator' [list of booleans]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options listofbooleansWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of booleans]
    Value: [false, true]
    
    Option 'listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of booleans]
    Value: [false, true]
    Default: [false]
    
    Option 'listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of booleans]
    Value: [false, true]
    Required invariants:
        - Validator for configuration options listofbooleansWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of booleans]
    Value: [false, true]
    Default: [false]
    Required invariants:
        - Validator for configuration options listofbooleansWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listofbooleansWithDescriptionWithoutDefaultValueWithoutValidator' [list of booleans]
    Value: [must be specified]
    Description: Description for type list of booleans.
    
    Option 'listofbooleansWithDescriptionWithoutDefaultValueWithValidator' [list of booleans]
    Value: [must be specified]
    Description: Description for type list of booleans.
    Required invariants:
        - Validator for configuration options listofbooleansWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of booleans]
    Value: [false, true]
    Description: Description for type list of booleans.
    
    Option 'listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of booleans]
    Value: [false, true]
    Default: [false]
    Description: Description for type list of booleans.
    
    Option 'listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of booleans]
    Value: [false, true]
    Description: Description for type list of booleans.
    Required invariants:
        - Validator for configuration options listofbooleansWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of booleans]
    Value: [false, true]
    Default: [false]
    Description: Description for type list of booleans.
    Required invariants:
        - Validator for configuration options listofbooleansWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listofstringsWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of strings]
    Value: [must be specified]
    
    Option 'listofstringsWithoutDescriptionWithoutDefaultValueWithValidator' [list of strings]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options listofstringsWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of strings]
    Value: ["0", "01"]
    
    Option 'listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of strings]
    Value: ["0", "01"]
    Default: ["0"]
    
    Option 'listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of strings]
    Value: ["0", "01"]
    Required invariants:
        - Validator for configuration options listofstringsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of strings]
    Value: ["0", "01"]
    Default: ["0"]
    Required invariants:
        - Validator for configuration options listofstringsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listofstringsWithDescriptionWithoutDefaultValueWithoutValidator' [list of strings]
    Value: [must be specified]
    Description: Description for type list of strings.
    
    Option 'listofstringsWithDescriptionWithoutDefaultValueWithValidator' [list of strings]
    Value: [must be specified]
    Description: Description for type list of strings.
    Required invariants:
        - Validator for configuration options listofstringsWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of strings]
    Value: ["0", "01"]
    Description: Description for type list of strings.
    
    Option 'listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of strings]
    Value: ["0", "01"]
    Default: ["0"]
    Description: Description for type list of strings.
    
    Option 'listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of strings]
    Value: ["0", "01"]
    Description: Description for type list of strings.
    Required invariants:
        - Validator for configuration options listofstringsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of strings]
    Value: ["0", "01"]
    Default: ["0"]
    Description: Description for type list of strings.
    Required invariants:
        - Validator for configuration options listofstringsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listofintegersWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of integers]
    Value: [must be specified]
    
    Option 'listofintegersWithoutDescriptionWithoutDefaultValueWithValidator' [list of integers]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options listofintegersWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of integers]
    Value: [1, 3]
    
    Option 'listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of integers]
    Value: [1, 3]
    Default: [1]
    
    Option 'listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of integers]
    Value: [1, 3]
    Required invariants:
        - Validator for configuration options listofintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of integers]
    Value: [1, 3]
    Default: [1]
    Required invariants:
        - Validator for configuration options listofintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listofintegersWithDescriptionWithoutDefaultValueWithoutValidator' [list of integers]
    Value: [must be specified]
    Description: Description for type list of integers.
    
    Option 'listofintegersWithDescriptionWithoutDefaultValueWithValidator' [list of integers]
    Value: [must be specified]
    Description: Description for type list of integers.
    Required invariants:
        - Validator for configuration options listofintegersWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of integers]
    Value: [1, 3]
    Description: Description for type list of integers.
    
    Option 'listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of integers]
    Value: [1, 3]
    Default: [1]
    Description: Description for type list of integers.
    
    Option 'listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of integers]
    Value: [1, 3]
    Description: Description for type list of integers.
    Required invariants:
        - Validator for configuration options listofintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of integers]
    Value: [1, 3]
    Default: [1]
    Description: Description for type list of integers.
    Required invariants:
        - Validator for configuration options listofintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of unsigned integers]
    Value: [must be specified]
    
    Option 'listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithValidator' [list of unsigned integers]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options listofunsignedintegersWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of unsigned integers]
    Value: [1, 3]
    
    Option 'listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of unsigned integers]
    Value: [1, 3]
    Default: [1]
    
    Option 'listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of unsigned integers]
    Value: [1, 3]
    Required invariants:
        - Validator for configuration options listofunsignedintegersWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of unsigned integers]
    Value: [1, 3]
    Default: [1]
    Required invariants:
        - Validator for configuration options listofunsignedintegersWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listofunsignedintegersWithDescriptionWithoutDefaultValueWithoutValidator' [list of unsigned integers]
    Value: [must be specified]
    Description: Description for type list of unsigned integers.
    
    Option 'listofunsignedintegersWithDescriptionWithoutDefaultValueWithValidator' [list of unsigned integers]
    Value: [must be specified]
    Description: Description for type list of unsigned integers.
    Required invariants:
        - Validator for configuration options listofunsignedintegersWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of unsigned integers]
    Value: [1, 3]
    Description: Description for type list of unsigned integers.
    
    Option 'listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of unsigned integers]
    Value: [1, 3]
    Default: [1]
    Description: Description for type list of unsigned integers.
    
    Option 'listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of unsigned integers]
    Value: [1, 3]
    Description: Description for type list of unsigned integers.
    Required invariants:
        - Validator for configuration options listofunsignedintegersWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of unsigned integers]
    Value: [1, 3]
    Default: [1]
    Description: Description for type list of unsigned integers.
    Required invariants:
        - Validator for configuration options listofunsignedintegersWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listoffloatsWithoutDescriptionWithoutDefaultValueWithoutValidator' [list of floats]
    Value: [must be specified]
    
    Option 'listoffloatsWithoutDescriptionWithoutDefaultValueWithValidator' [list of floats]
    Value: [must be specified]
    Required invariants:
        - Validator for configuration options listoffloatsWithoutDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of floats]
    Value: [0.000000, 43.701374]
    
    Option 'listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of floats]
    Value: [0.000000, 43.701374]
    Default: [0.000000]
    
    Option 'listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of floats]
    Value: [0.000000, 43.701374]
    Required invariants:
        - Validator for configuration options listoffloatsWithoutDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of floats]
    Value: [0.000000, 43.701374]
    Default: [0.000000]
    Required invariants:
        - Validator for configuration options listoffloatsWithoutDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'listoffloatsWithDescriptionWithoutDefaultValueWithoutValidator' [list of floats]
    Value: [must be specified]
    Description: Description for type list of floats.
    
    Option 'listoffloatsWithDescriptionWithoutDefaultValueWithValidator' [list of floats]
    Value: [must be specified]
    Description: Description for type list of floats.
    Required invariants:
        - Validator for configuration options listoffloatsWithDescriptionWithoutDefaultValueWithValidator.
    
    Option 'listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithoutValidator' [list of floats]
    Value: [0.000000, 43.701374]
    Description: Description for type list of floats.
    
    Option 'listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithoutValidator' [list of floats]
    Value: [0.000000, 43.701374]
    Default: [0.000000]
    Description: Description for type list of floats.
    
    Option 'listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator' [list of floats]
    Value: [0.000000, 43.701374]
    Description: Description for type list of floats.
    Required invariants:
        - Validator for configuration options listoffloatsWithDescriptionWithDefaultValueWithKeepDefaultValueWithValidator.
    
    Option 'listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator' [list of floats]
    Value: [0.000000, 43.701374]
    Default: [0.000000]
    Description: Description for type list of floats.
    Required invariants:
        - Validator for configuration options listoffloatsWithDescriptionWithDefaultValueWithoutKeepDefaultValueWithValidator.
    
    Option 'doubleArgumentValidatorFirstArgument' [boolean]
    Value: [must be specified]
    
    Option 'doubleArgumentValidatorSecondArgument' [boolean]
    Value: [must be specified]
    
    Required invariants:
        - Validator for configuration options doubleArgumentValidatorFirstArgument, doubleArgumentValidatorSecondArgument.)--");

#endif  // QLEVER_TEST_UTIL_PRINTCONFIGURATIONDOCCOMPARISONSTRING_H
