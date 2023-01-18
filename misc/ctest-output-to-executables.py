#  // Copyright 2023, University of Freiburg,
#   //                Chair of Algorithms and Data Structures.
#  // Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

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





