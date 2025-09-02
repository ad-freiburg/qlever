//
// Created by kalmbacj on 9/2/25.
//

#include <gmock/gmock.h>

#include "global/Constants.h"
#include "global/TypedIndex.h"
#include "util/Algorithm.h"
#include "util/AllocatorWithLimit.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Random.h"
#include "util/StringUtils.h"
#include "util/Synchronized.h"
#include "util/TypeTraits.h"

// TODO<joka921> The AsioHelpers heavily rely on coroutines and only are used in
// the server module.
// #include "util/AsioHelpers.h"

#include "util/AsyncStream.h"
#include "util/BatchedPipeline.h"
#include "util/BitUtils.h"
// #include "util/BlankNodeManager.h"

TEST(HeaderCompilation, Dummy) {}
