// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "kvstore_tool.h"

#include <iostream>
#include <iomanip>
#include <sstream>
#include <ostream>
#include <bits/stdc++.h>

#include <seastar/core/print.hh>
#include <seastar/core/sleep.hh>

#include "common/url_escape.h"      // url_escape (全局函数)
#include "include/buffer.h"
#include "crimson/common/log.h"
#include "crimson/common/errorator.h"
#include "os/ObjectStore.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::tools::kvstore {

using crimson::os::seastore::Transaction;
using crimson::os::seastore::omap_root_t;
using crimson::os::seastore::omap_type_t;
using omap_iter_seek_t = ObjectStore::omap_iter_seek_t;
using omap_iter_ret_t  = ObjectStore::omap_iter_ret_t;

static seastar::logger& logger() {
  static seastar::logger logger{"kvstore-tool"};
  return logger;
}

KVStoreTool::KVStoreTool(
    std::unique_ptr<crimson::os::seastore::TransactionManager> tm,
    std::unique_ptr<crimson::os::seastore::OMapManager> omap_mgr)
  : tm(std::move(tm)), omap_mgr(std::move(omap_mgr)) {}

KVStoreTool::~KVStoreTool() = default;

// ==================== Helper ====================

static TransactionRef make_transaction(crimson::os::seastore::TransactionManager& tm) {
  return tm.create_transaction(
      Transaction::src_t::MUTATE,
      "kvstore-tool",
      crimson::os::seastore::CACHE_HINT_NOCACHE,
      false);
}

// ==================== init ====================

KVStoreTool::init_ret KVStoreTool::init() {
  auto t = make_transaction(*tm);
  return omap_mgr->initialize_omap(
      *t,
      crimson::os::seastore::L_ADDR_MIN,
      omap_type_t::OMAP
  ).si_then([this, t = std::move(t)](omap_root_t new_root) mutable {
    root = new_root;
    return tm->submit_transaction(*t);
  }).si_then([this] {
    initialized = true;
    logger().info("KVStoreTool initialized with root {}", root);
  }).handle_error_interruptible(
    crimson::ct_error::assert_all{"init failed"}
  );
}

// ==================== exists ====================

KVStoreTool::exists_ret KVStoreTool::exists(
    const std::string& prefix,
    const std::string& key) {
  if (!initialized) {
    return seastar::make_ready_future<bool>(false);
  }
  std::string full_key = prefix + key;
  auto t = make_transaction(*tm);
  return omap_mgr->omap_get_value(root, *t, full_key)
    .si_then([](std::optional<ceph::bufferlist> val) {
      return val.has_value();
    })
    .handle_error_interruptible(
      crimson::ct_error::enoent::handle([](auto) { return false; }),
      crimson::ct_error::pass_further_all{}
    );
}

// ==================== get ====================

KVStoreTool::get_ret KVStoreTool::get(
    const std::string& prefix,
    const std::string& key) {
  if (!initialized) {
    return seastar::make_ready_future<ceph::bufferlist>(ceph::bufferlist());
  }
  std::string full_key = prefix + key;
  auto t = make_transaction(*tm);
  return omap_mgr->omap_get_value(root, *t, full_key)
    .si_then([](std::optional<ceph::bufferlist> val) -> ceph::bufferlist {
      if (val) {
        return *val;
      } else {
        throw std::runtime_error("Key not found");
      }
    })
    .handle_error_interruptible(
      crimson::ct_error::enoent::handle([](auto) -> ceph::bufferlist {
        throw std::runtime_error("Key not found");
      }),
      crimson::ct_error::pass_further_all{}
    );
}

// ==================== set ====================

KVStoreTool::set_ret KVStoreTool::set(
    const std::string& prefix,
    const std::string& key,
    const ceph::bufferlist& value) {
  if (!initialized) {
    return seastar::make_ready_future<>();
  }
  std::string full_key = prefix + key;
  auto t = make_transaction(*tm);
  return omap_mgr->omap_set_key(root, *t, full_key, value).si_then(
    [this, t = std::move(t)]() mutable {
      return tm->submit_transaction(*t);
    }
  ).handle_error_interruptible(
    crimson::ct_error::assert_all{"set failed"}
  );
}

// ==================== rm ====================

KVStoreTool::rm_ret KVStoreTool::rm(
    const std::string& prefix,
    const std::string& key) {
  if (!initialized) {
    return seastar::make_ready_future<>();
  }
  std::string full_key = prefix + key;
  auto t = make_transaction(*tm);
  return omap_mgr->omap_rm_key(root, *t, full_key).si_then(
    [this, t = std::move(t)]() mutable {
      return tm->submit_transaction(*t);
    }
  ).handle_error_interruptible(
    crimson::ct_error::assert_all{"rm failed"}
  );
}

// ==================== rm_prefix ====================

KVStoreTool::rm_prefix_ret KVStoreTool::rm_prefix(const std::string& prefix) {
  if (!initialized) {
    return seastar::make_ready_future<uint64_t>(0);
  }

  return seastar::do_with(std::set<std::string>(), TransactionRef{},
    [this, prefix](std::set<std::string>& keys_to_remove, TransactionRef& t) {
      t = make_transaction(*tm);
      omap_iter_seek_t seek;
      seek.seek_position = prefix;
      seek.seek_type = omap_iter_seek_t::LOWER_BOUND;

      return omap_mgr->omap_iterate(
          root,
          *t,
          seek,
          [prefix, &keys_to_remove](std::string_view key, std::string_view val) {
            if (key.compare(0, prefix.size(), prefix) != 0)
              return omap_iter_ret_t::STOP;
            keys_to_remove.insert(std::string(key));
            return omap_iter_ret_t::NEXT;
          }
      ).si_then([this, &keys_to_remove, &t]() mutable {
        if (keys_to_remove.empty())
          return seastar::make_ready_future<uint64_t>(0);

        auto t2 = make_transaction(*tm);
        return omap_mgr->omap_rm_keys(root, *t2, keys_to_remove)
          .si_then([this, t2 = std::move(t2)]() mutable {
            return tm->submit_transaction(*t2);
          })
          .si_then([&keys_to_remove] {
            return seastar::make_ready_future<uint64_t>(keys_to_remove.size());
          });
      }).handle_error_interruptible(
        crimson::ct_error::assert_all{"rm_prefix failed"}
      );
    }
  );
}

// ==================== list ====================

KVStoreTool::list_ret KVStoreTool::list(const std::string& prefix) {
  if (!initialized) {
    return seastar::make_ready_future<std::vector<std::string>>(
        std::vector<std::string>{});
  }

  return seastar::do_with(std::vector<std::string>(), TransactionRef{},
    [this, prefix](std::vector<std::string>& keys, TransactionRef& t) {
      t = make_transaction(*tm);
      omap_iter_seek_t seek;
      seek.seek_position = prefix;
      seek.seek_type = omap_iter_seek_t::LOWER_BOUND;

      return omap_mgr->omap_iterate(
          root,
          *t,
          seek,
          [prefix, &keys](std::string_view key, std::string_view val) {
            if (!prefix.empty() && key.compare(0, prefix.size(), prefix) != 0)
              return omap_iter_ret_t::STOP;
            keys.push_back(std::string(key));
            return omap_iter_ret_t::NEXT;
          }
      ).si_then([&keys] {
        return seastar::make_ready_future<std::vector<std::string>>(std::move(keys));
      }).handle_error_interruptible(
        crimson::ct_error::assert_all{"list failed"}
      );
    }
  );
}

// ==================== stats ====================

KVStoreTool::stats_ret KVStoreTool::stats() {
  if (!initialized) {
    return seastar::make_ready_future<std::string>("Not initialized");
  }

  return seastar::do_with(uint64_t(0), uint64_t(0), TransactionRef{},
    [this](uint64_t& count, uint64_t& total_size, TransactionRef& t) {
      t = make_transaction(*tm);
      omap_iter_seek_t seek;
      seek.seek_position = "";
      seek.seek_type = omap_iter_seek_t::LOWER_BOUND;

      return omap_mgr->omap_iterate(
          root,
          *t,
          seek,
          [&count, &total_size](std::string_view key, std::string_view val) {
            ++count;
            total_size += key.size() + val.size();
            return omap_iter_ret_t::NEXT;
          }
      ).si_then([&count, &total_size] {
        std::ostringstream oss;
        oss << "Total keys: " << count << "\n"
            << "Total size: " << total_size << " bytes\n"
            << "Average key+value size: "
            << (count > 0 ? total_size / count : 0) << " bytes";
        return seastar::make_ready_future<std::string>(oss.str());
      }).handle_error_interruptible(
        crimson::ct_error::assert_all{"stats failed"}
      );
    }
  );
}

// ==================== histogram ====================

KVStoreTool::histogram_ret KVStoreTool::histogram(const std::string& prefix) {
  if (!initialized) {
    return seastar::make_ready_future<>();
  }

  struct Histogram {
    uint64_t total_keys = 0;
    uint64_t total_size = 0;
    size_t max_key_size = 0;
    size_t max_value_size = 0;
    std::map<size_t, uint64_t> size_buckets;
  };

  return seastar::do_with(Histogram{}, TransactionRef{},
    [this, prefix](Histogram& hist, TransactionRef& t) {
      t = make_transaction(*tm);
      omap_iter_seek_t seek;
      seek.seek_position = prefix;
      seek.seek_type = omap_iter_seek_t::LOWER_BOUND;

      return omap_mgr->omap_iterate(
          root,
          *t,
          seek,
          [&hist, prefix](std::string_view key, std::string_view val) {
            if (!prefix.empty() && key.compare(0, prefix.size(), prefix) != 0)
              return omap_iter_ret_t::STOP;

            size_t key_sz = key.size(), val_sz = val.size();
            hist.total_keys++;
            hist.total_size += key_sz + val_sz;
            hist.max_key_size = std::max(hist.max_key_size, key_sz);
            hist.max_value_size = std::max(hist.max_value_size, val_sz);
            size_t bucket = val_sz > 0 ? (1 << (32 - __builtin_clz(val_sz))) : 0;
            hist.size_buckets[bucket]++;

            return omap_iter_ret_t::NEXT;
          }
      ).si_then([&hist] {
        std::cout << "Key-Value Size Histogram:" << std::endl;
        std::cout << "  Total keys: " << hist.total_keys << std::endl;
        std::cout << "  Total size: " << hist.total_size << " bytes" << std::endl;
        std::cout << "  Max key size: " << hist.max_key_size << " bytes" << std::endl;
        std::cout << "  Max value size: " << hist.max_value_size << " bytes" << std::endl;
        std::cout << "  Value size distribution:" << std::endl;
        for (const auto& [bucket, count] : hist.size_buckets)
          std::cout << "    " << bucket << " bytes: " << count << " keys" << std::endl;
        return seastar::make_ready_future<>();
      }).handle_error_interruptible(
        crimson::ct_error::assert_all{"histogram failed"}
      );
    }
  );
}

// ==================== traverse ====================

KVStoreTool::traverse_ret KVStoreTool::traverse(
    const std::string& prefix,
    bool do_crc,
    bool do_value_dump,
    std::ostream* out) {
  if (!initialized) {
    return seastar::make_ready_future<uint32_t>(0);
  }

  return seastar::do_with(uint32_t(0),
    [this, prefix, do_crc, do_value_dump, out](uint32_t& crc) {
      auto t = make_transaction(*tm);
      omap_iter_seek_t seek;
      seek.seek_position = prefix;
      seek.seek_type = omap_iter_seek_t::LOWER_BOUND;

      return omap_mgr->omap_iterate(
          root,
          *t,
          seek,
          [prefix, do_crc, do_value_dump, out, &crc]
          (std::string_view key, std::string_view val) {
            if (!prefix.empty() && key.compare(0, prefix.size(), prefix) != 0)
              return omap_iter_ret_t::STOP;

            if (out) {
              *out << url_escape(std::string(key));
              if (do_crc) {
                ceph::bufferlist bl;
                bl.append(key.data(), key.size());
                bl.append(val.data(), val.size());
                crc = bl.crc32c(crc);
                *out << "\t" << bl.crc32c(0);
              }
              *out << std::endl;
              if (do_value_dump) {
                std::ostringstream os;
                ceph::bufferlist val_bl;
                val_bl.append(val.data(), val.size());
                val_bl.hexdump(os);
                *out << os.str() << std::endl;
              }
            }
            return omap_iter_ret_t::NEXT;
          }
      ).handle_error_interruptible(
        crimson::ct_error::assert_all{"omap_iterate failed"}
      ).then([&crc] {
        return seastar::make_ready_future<uint32_t>(crc);
      });
    }
  );
}

// ==================== get_size ====================

KVStoreTool::get_size_ret KVStoreTool::get_size(
    const std::string& prefix,
    const std::string& key) {
  if (!initialized) {
    return seastar::make_ready_future<uint64_t>(0);
  }

  if (prefix.empty() && key.empty()) {
    return seastar::do_with(uint64_t(0),
      [this](uint64_t& total) {
        auto t = make_transaction(*tm);
        omap_iter_seek_t seek;
        seek.seek_position = "";
        seek.seek_type = omap_iter_seek_t::LOWER_BOUND;

        return omap_mgr->omap_iterate(
            root,
            *t,
            seek,
            [&total](std::string_view key, std::string_view val) {
              total += key.size() + val.size();
              return omap_iter_ret_t::NEXT;
            }
        ).si_then([&total] {
          return seastar::make_ready_future<uint64_t>(total);
        }).handle_error_interruptible(
          crimson::ct_error::assert_all{"get_size total failed"}
        );
      }
    );
  } else if (!key.empty()) {
    return get(prefix, key).then([](ceph::bufferlist val) {
      return seastar::make_ready_future<uint64_t>(val.length());
    });
  } else {
    return seastar::do_with(uint64_t(0),
      [this, prefix](uint64_t& total) {
        auto t = make_transaction(*tm);
        omap_iter_seek_t seek;
        seek.seek_position = prefix;
        seek.seek_type = omap_iter_seek_t::LOWER_BOUND;

        return omap_mgr->omap_iterate(
            root,
            *t,
            seek,
            [prefix, &total](std::string_view key, std::string_view val) {
              if (key.compare(0, prefix.size(), prefix) != 0)
                return omap_iter_ret_t::STOP;
              total += key.size() + val.size();
              return omap_iter_ret_t::NEXT;
            }
        ).si_then([&total] {
          return total;
        }).handle_error_interruptible(
          crimson::ct_error::assert_all{"get_size prefix failed"}
        );
      }
    );
  }
}

// ==================== create ====================

std::unique_ptr<KVStoreTool> KVStoreTool::create(
    const std::string& device_path,
    bool ephemeral) {
  // TODO: implement proper initialization
  ceph_abort_msg("KVStoreTool::create not implemented");
  return nullptr;
}

} // namespace crimson::tools::kvstore