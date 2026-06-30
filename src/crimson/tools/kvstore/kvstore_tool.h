// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <string>
#include <optional>
#include <vector>
#include <map>
#include <set>

#include <seastar/core/future.hh>

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"

namespace crimson::tools::kvstore {

using TransactionRef = std::unique_ptr<crimson::os::seastore::Transaction>;

class KVStoreTool {
public:
  // All operations return seastar::future
  using init_ret = seastar::future<>;
  using exists_ret = seastar::future<bool>;
  using get_ret = seastar::future<ceph::bufferlist>;
  using set_ret = seastar::future<>;
  using rm_ret = seastar::future<>;
  using rm_prefix_ret = seastar::future<uint64_t>;
  using list_ret = seastar::future<std::vector<std::string>>;
  using stats_ret = seastar::future<std::string>;
  using histogram_ret = seastar::future<>;
  using traverse_ret = seastar::future<uint32_t>;
  using get_size_ret = seastar::future<uint64_t>;

  explicit KVStoreTool(
    std::unique_ptr<crimson::os::seastore::TransactionManager> tm,
    std::unique_ptr<crimson::os::seastore::OMapManager> omap_mgr
  );
  ~KVStoreTool();

  KVStoreTool(const KVStoreTool&) = delete;
  KVStoreTool& operator=(const KVStoreTool&) = delete;

  // Core operations
  init_ret init();
  exists_ret exists(const std::string& prefix, const std::string& key);
  get_ret get(const std::string& prefix, const std::string& key);
  set_ret set(const std::string& prefix, const std::string& key,
              const ceph::bufferlist& value);
  rm_ret rm(const std::string& prefix, const std::string& key);
  rm_prefix_ret rm_prefix(const std::string& prefix);
  list_ret list(const std::string& prefix = "");
  stats_ret stats();
  histogram_ret histogram(const std::string& prefix = "");
  traverse_ret traverse(
    const std::string& prefix = "",
    bool do_crc = false,
    bool do_value_dump = false,
    std::ostream* out = nullptr
  );
  get_size_ret get_size(const std::string& prefix = "", const std::string& key = "");

  static std::unique_ptr<KVStoreTool> create(
    const std::string& device_path,
    bool ephemeral = true
  );

private:
  std::unique_ptr<crimson::os::seastore::TransactionManager> tm;
  std::unique_ptr<crimson::os::seastore::OMapManager> omap_mgr;
  crimson::os::seastore::omap_root_t root;
  bool initialized = false;

  static seastar::logger& logger();
};

} // namespace crimson::tools::kvstore