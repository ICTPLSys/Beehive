#pragma once

#ifdef NO_REMOTE

#include "no_remote.hpp"  // IWYU pragma: export

#else

#include "accessor.hpp"  // IWYU pragma: export

#endif

#define __define_oms__(__scope) \
    FarLib::cache::OnMissScope oms(__entry__, &__scope);
