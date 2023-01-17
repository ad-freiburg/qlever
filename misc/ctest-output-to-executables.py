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
    for command in commands:
        print(command, file=out_file)





