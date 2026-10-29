// Copyright 2024 - 2026, The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <range/v3/view/zip.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/FsstCompressor.h"
#include "util/GTestHelpers.h"

TEST(FsstEncoder, firstTest) {
  std::vector<std::string> s{
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur "
      "sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et "
      "dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam "
      "et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea "
      "takimata sanctus est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor "};
  s = absl::StrSplit(s.front(), " ");

  // First test the manual interface.
  FsstEncoder encoder{s};
  std::vector<std::string> s2;
  size_t original = 0;
  size_t compressed = 0;
  for (auto& str : s) {
    s2.push_back(encoder.compress(str));
    original += str.size();
    compressed += s2.back().size();
  }
  EXPECT_LT(compressed, original);

  auto decoder = encoder.makeDecoder();
  for (auto& str : s2) {
    str = decoder.decompress(str);
  }
  EXPECT_THAT(s2, ::testing::ElementsAreArray(s));

  // Now test the `compressAll` interface.
  {
    auto [buffer, compressedViews, decoder] = FsstEncoder::compressAll(s);
    std::vector<std::string> s3;
    for (auto compressedView : compressedViews) {
      s3.push_back(decoder.decompress(compressedView));
    }
    EXPECT_THAT(s3, ::testing::ElementsAreArray(s));
  }
}

// _____________________________________________________________________________
// A deterministic string of `n` pseudo-random printable characters, which FSST
// compresses poorly (so that it stays long after every compression stage).
static std::string pseudoRandomString(size_t n, uint32_t seed = 12345) {
  std::string result(n, ' ');
  uint32_t state = seed;
  for (char& c : result) {
    state = state * 1664525u + 1013904223u;
    c = static_cast<char>('!' + (state >> 24) % 90);
  }
  return result;
}

// _____________________________________________________________________________
// Goal: `decompress` decodes into a stack buffer of
// `FsstDecoder::decodeStackBufferSize` bytes and falls back to a heap buffer
// for longer strings. Method: decode words just below, at and above that size
// (and much longer ones) and compare them with the originals.
TEST(FsstEncoder, DecompressAroundTheStackBufferSize) {
  constexpr size_t bufferSize = FsstDecoder::decodeStackBufferSize;
  const std::vector<std::string> words{pseudoRandomString(bufferSize - 1, 1),
                                       pseudoRandomString(bufferSize, 2),
                                       pseudoRandomString(bufferSize + 1, 3),
                                       pseudoRandomString(3 * bufferSize, 4),
                                       std::string(5 * bufferSize, 'x'),
                                       "short",
                                       ""};
  auto [buffer, compressedViews, decoder] = FsstEncoder::compressAll(words);
  for (const auto& [word, compressed] :
       ::ranges::views::zip(words, compressedViews)) {
    EXPECT_EQ(decoder.decompress(compressed), word);
  }
}

// _____________________________________________________________________________
// Goal: `decompressInto` (the buffer-based interface) must produce
// byte-for-byte the same output as the string-returning `decompress`.
// Method: compress the words, decode each compressed word through both
// interfaces, and compare the results against each other and the original.
TEST(FsstEncoder, DecompressIntoMatchesDecompress) {
  const std::vector<std::string> words{"alpha", "", "beta", "", "gamma"};
  auto [buffer, compressedViews, decoder] = FsstEncoder::compressAll(words);
  for (const auto& [word, compressed] :
       ::ranges::views::zip(words, compressedViews)) {
    const std::string decompressed = decoder.decompress(compressed);
    const size_t bound = decoder.maxDecompressedSize(compressed);
    // The buffer has exactly the minimal size.
    std::string output(bound, '\0');
    const size_t size = decoder.decompressInto(
        compressed, ql::span<char>{output.data(), output.size()});
    EXPECT_EQ(std::string_view(output.data(), size), decompressed);
    EXPECT_EQ(decompressed, word);

    // A buffer that is larger than required is allowed.
    std::string largerOutput(bound + 16, '\0');
    const size_t sizeInLargerBuffer = decoder.decompressInto(
        compressed, ql::span<char>{largerOutput.data(), largerOutput.size()});
    EXPECT_EQ(std::string_view(largerOutput.data(), sizeInLargerBuffer), word);

    if (bound > 0) {
      std::string undersized(bound - 1, '\0');
      AD_EXPECT_THROW_WITH_MESSAGE(
          (void)decoder.decompressInto(
              compressed, ql::span<char>{undersized.data(), undersized.size()}),
          ::testing::HasSubstr("out.size() >= bound"));
    }
  }
}

// _____________________________________________________________________________
// Goal: `maxDecompressedSize` rejects inputs whose worst-case expansion
// overflows `size_t`, for one stage and for three stages of a repeated decoder
// (where the input passes the first two checks and fails the third).
// Method: `maxDecompressedSize` only reads `str.size()`, so a view that claims
// more bytes than exist (and is never dereferenced) reaches the guards.
TEST(FsstEncoder, MaxDecompressedSizeRejectsOverflow) {
  constexpr size_t factor = FsstDecoder::maxExpansionFactor;
  constexpr size_t max = std::numeric_limits<size_t>::max();
  const char dummy = 'x';
  auto viewOfSize = [&dummy](size_t size) {
    return std::string_view{&dummy, size};
  };

  EXPECT_EQ(FsstDecoder::maxDecompressedSize(viewOfSize(max / factor)),
            max / factor * factor);
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)FsstDecoder::maxDecompressedSize(viewOfSize(max / factor + 1)),
      ::testing::HasSubstr("maxExpansionFactor"));

  // Three stages: the input passes the first two checks and fails the third.
  constexpr size_t limit3 = max / factor / factor / factor;
  EXPECT_EQ(FsstRepeatedDecoder<3>::maxDecompressedSize(viewOfSize(limit3)),
            limit3 * factor * factor * factor);
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)FsstRepeatedDecoder<3>::maxDecompressedSize(viewOfSize(limit3 + 1)),
      ::testing::HasSubstr("maxExpansionFactor"));
}

// _____________________________________________________________________________
class FsstRepeatedDecoderTest : public ::testing::Test {
 protected:
  // ___________________________________________________________________________
  // Return a word that stays short in the intermediate stages but is longer
  // than the stack buffer of `FsstDecoder::decompress` after the last stage.
  static std::string longWord() {
    return std::string(5 * FsstDecoder::decodeStackBufferSize, 'x');
  }

  // ___________________________________________________________________________
  // Return a deterministic word whose intermediate stages do not fit the stack
  // buffers of `FsstRepeatedDecoder::decompress` either.
  static std::string randomWord() {
    return pseudoRandomString(3 * FsstDecoder::decodeStackBufferSize);
  }

  // ___________________________________________________________________________
  // Return the words that all stage counts are tested with.
  static std::vector<std::string> corpus() {
    return {"alpha", "",         "beta",      "gamma-gamma-gamma",
            "",      longWord(), randomWord()};
  }

  // ___________________________________________________________________________
  // Compress `words` through `N` cascaded FSST stages, build the
  // `FsstRepeatedDecoder<N>` from the stage decoders, and expect that
  // `decompress` and `decompressInto` both return the original words and that
  // `decompressInto` rejects an undersized output buffer.
  template <size_t N>
  static void expectRepeatedDecompressIntoMatches(
      const std::vector<std::string>& words) {
    std::vector<std::string_view> compressed;
    compressed.reserve(words.size());
    for (const auto& w : words) {
      compressed.emplace_back(w);
    }

    std::array<FsstDecoder, N> decoders{};
    // The views in `compressed` point into the buffer of the stage that
    // produced them, so the buffers must stay alive while `compressed` is used.
    std::vector<std::shared_ptr<std::string>> buffers;
    buffers.reserve(N);
    for (size_t stage = 0; stage < N; ++stage) {
      auto [buffer, nextViews, decoder] = FsstEncoder::compressAll(compressed);
      compressed.assign(nextViews.begin(), nextViews.end());
      decoders[stage] = std::move(decoder);
      buffers.push_back(std::move(buffer));
    }

    FsstRepeatedDecoder<N> repeated{std::move(decoders)};
    std::string scratch;
    for (size_t i = 0; i < words.size(); ++i) {
      const std::string viaString = repeated.decompress(compressed[i]);
      const size_t bound = repeated.maxDecompressedSize(compressed[i]);
      std::string intoBuf(bound, '\0');
      const size_t size = repeated.decompressInto(
          compressed[i], ql::span<char>{intoBuf.data(), intoBuf.size()},
          scratch);
      EXPECT_EQ(size, viaString.size());
      EXPECT_EQ(std::string_view(intoBuf.data(), size), viaString);
      EXPECT_EQ(viaString, words[i]);

      if (bound > 0) {
        std::string undersized(bound - 1, '\0');
        AD_EXPECT_THROW_WITH_MESSAGE(
            (void)repeated.decompressInto(
                compressed[i],
                ql::span<char>{undersized.data(), undersized.size()}, scratch),
            ::testing::HasSubstr("out.size() >= bound"));
      }
    }

    if constexpr (N >= 2) {
      size_t expectedScratchSize = 0;
      for (std::string_view word : compressed) {
        expectedScratchSize =
            std::max(expectedScratchSize, repeated.maxDecompressedSize(word) /
                                              FsstDecoder::maxExpansionFactor);
      }
      EXPECT_EQ(scratch.size(), expectedScratchSize);
    }
  }
};

// _____________________________________________________________________________
// Goal: the repeated-decoder variant (FSST stages applied `N` times) must also
// satisfy `decompressInto` == `decompress` byte-for-byte. Method: the helper
// `expectRepeatedDecompressIntoMatches<N>` compresses words through `N`
// cascaded stages, decodes via both interfaces, and compares all three.
TEST_F(FsstRepeatedDecoderTest, decompressIntoMatchesDecompressOneStage) {
  expectRepeatedDecompressIntoMatches<1>(corpus());
}

// _____________________________________________________________________________
// See above, with two cascaded FSST stages.
TEST_F(FsstRepeatedDecoderTest, decompressIntoMatchesDecompressTwoStages) {
  expectRepeatedDecompressIntoMatches<2>(corpus());
}

// _____________________________________________________________________________
// See above, with three cascaded FSST stages (deeper than the two stages of
// `FsstSquaredCompressionWrapper`, to cover the alternation of both buffers).
TEST_F(FsstRepeatedDecoderTest, decompressIntoMatchesDecompressThreeStages) {
  expectRepeatedDecompressIntoMatches<3>(corpus());
}

// _____________________________________________________________________________
// Goal: `FsstRepeatedDecoder::decompress` decides per intermediate stage
// whether the result fits its stack buffer. Method: sweep the word length over
// a range in which the intermediate results grow past
// `FsstDecoder::decodeStackBufferSize` bytes (the words are nearly
// incompressible), and expect the original words back.
TEST_F(FsstRepeatedDecoderTest, decompressAroundIntermediateStackBufferSize) {
  std::vector<std::string> words;
  constexpr size_t bufferSize = FsstDecoder::decodeStackBufferSize;
  for (size_t length = bufferSize / 2; length <= 2 * bufferSize; length += 13) {
    words.push_back(pseudoRandomString(length, static_cast<uint32_t>(length)));
  }
  expectRepeatedDecompressIntoMatches<2>(words);
  expectRepeatedDecompressIntoMatches<3>(words);
}
