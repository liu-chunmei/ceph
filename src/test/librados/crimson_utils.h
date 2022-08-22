// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdlib>

static inline bool is_crimson_cluster() {
  static int i = 0;
  if (i == 0) {
    setenv("CRIMSON_COMPAT", "on", 1); //for test
    i++;
  }
  return getenv("CRIMSON_COMPAT") != nullptr;
}

#define SKIP_IF_CRIMSON()             \
  if (is_crimson_cluster()) {         \
    GTEST_SKIP() << "Not supported by crimson yet. Skipped"; \
  }
