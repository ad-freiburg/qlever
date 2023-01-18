#  // Copyright 2023, University of Freiburg,
#   //                Chair of Algorithms and Data Structures.
#  // Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

"""
This script takes the output of `ctest --show-only=json-v1` and extracts the
names of the binaries that have to be executed to run the tests. It writes
the output to a file where each second line is "-object". An example output
might be

/path/to/tests/firstTestBinary
-object
/path/to/tests/secondTestBinary
-object
/path/to/tests/thirdTestBinary

Usage: python3 ctest-output-to-executables.py <filenameOfCtestJSON> <outfile>

This script is used by the Github action `native-build.yml` when aggregating coverage data.
The result of this script is fed to `llvm-cov` which needs to explicitly know, which binaries
were run by `ctest`.
"""

import sys
import json

with open(sys.argv[1], "r") as f:
        j = json.load(f)
        commands = set()
        for test in j["tests"]:
            if "command" in test:
                commands.add(test["command"][0])

with open(sys.argv[2], "w") as out_file:
    first = True
    for command in commands:
        if (not first):
            # This is required by the llvm-cov binary.
            print("-object", file=out_file)
        print(command, file=out_file)
        first = False
