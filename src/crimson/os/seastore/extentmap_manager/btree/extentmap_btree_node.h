// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#pragma once

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/extentmap_manager.h"

namespace crimson::os::seastore::extentmap_manager{

struct ExtMapNode : LogicalCachedExtent {
  using ExtMapNodeRef = TCachedExtentRef<ExtMapNode>;
  depth_t depth = 0;

  ExtMapNode(ceph::bufferptr &&ptr) : LogicalCachedExtent(std::move(ptr)) {}
  ExtMapNode(const ExtMapNode &other)
  : LogicalCachedExtent(other),
    depth(other.depth) {}

  void set_depth(depth_t _depth) { depth = _depth; }

  LogicalCachedExtentRef make_duplicate(TransactionManager &tm, Transaction& t,
	  LogicalCachedExtentRef ref) {
    return tm.get_mutable_extent(t, ref);
  };

  using find_lextent_ertr = ExtentMapManager::find_lextent_ertr;
  using find_lextent_ret = ExtentMapManager::find_lextent_ret;
  virtual find_lextent_ret find_lextent(TransactionManager &tm, Transaction &t,
		                        objaddr_t lo, extent_len_t len) = 0;

  using insert_ertr = TransactionManager::read_extent_ertr;
  using insert_ret = insert_ertr::future<extent_mapping_t>;
  virtual insert_ret insert(TransactionManager &tm, Transaction &t, objaddr_t lo, lext_map_val_t val) = 0;

  using rm_lextent_ertr = TransactionManager::read_extent_ertr;
  using rm_lextent_ret = rm_lextent_ertr::future<bool>;
  virtual rm_lextent_ret rm_lextent(TransactionManager &tm, Transaction &t, objaddr_t lo, lext_map_val_t val) = 0;

  using split_children_ertr = TransactionManager::alloc_extent_ertr;
  using split_children_ret = split_children_ertr::future
	  <std::tuple<ExtMapNodeRef, ExtMapNodeRef, uint32_t>>;
  virtual split_children_ret make_split_children(TransactionManager &tm, Transaction &t) = 0;

  using full_merge_ertr = TransactionManager::alloc_extent_ertr;
  using full_merge_ret = full_merge_ertr::future<ExtMapNodeRef>;
  virtual full_merge_ret make_full_merge(TransactionManager &tm, Transaction &t, ExtMapNodeRef right) = 0;

  using make_balanced_ertr = TransactionManager::alloc_extent_ertr;
  using make_balanced_ret = make_balanced_ertr::future
	  <std::tuple<ExtMapNodeRef, ExtMapNodeRef, uint32_t>>;
  virtual make_balanced_ret
    make_balanced(TransactionManager &tm, Transaction &t, ExtMapNodeRef right, bool prefer_left) = 0;

  virtual bool at_max_capacity() const = 0;
  virtual bool at_min_capacity() const = 0;
  virtual ~ExtMapNode() = default;

  using alloc_ertr = TransactionManager::alloc_extent_ertr;
  template<class T>
  alloc_ertr::future<TCachedExtentRef<T>>
  extmap_alloc_extent(TransactionManager &tm, Transaction& txn, extent_len_t len) {
    return tm.alloc_extent<T>(txn, L_ADDR_MIN, len).safe_then(
      [this](auto&& extent) {
      return alloc_ertr::make_ready_future<TCachedExtentRef<T>>(std::move(extent));
    });
  }

  template<class T>
  alloc_ertr::future<std::pair<TCachedExtentRef<T>, TCachedExtentRef<T>>>
  extmap_alloc_2extents(TransactionManager &tm, Transaction &t, extent_len_t len) {
    return seastar::do_with(std::pair<TCachedExtentRef<T>, TCachedExtentRef<T>>(),
      [this, &tm, &t, len] (auto &extents) {
      return crimson::do_for_each(
		      boost::make_counting_iterator(0),
		      boost::make_counting_iterator(2),
        [this, &tm, &t, len, &extents] (auto i) {
        return tm.alloc_extent<T>(t, L_ADDR_MIN, len).safe_then(
          [this, i, &extents](auto &&node) {
	  if (i == 0)
	    extents.first = node;
	  if (i == 1)
	    extents.second = node;
	});
      }).safe_then([&extents] {
        return alloc_ertr::make_ready_future
	  <std::pair<TCachedExtentRef<T>, TCachedExtentRef<T>>>(std::move(extents));
      });
    });
  }

  using retire_ertr = crimson::errorator<
  crimson::ct_error::enoent,
  crimson::ct_error::input_output_error>;
  using retire_ret = retire_ertr::future<std::list<unsigned>>;
  retire_ret
  extmap_retire_node(TransactionManager &tm, Transaction& t, std::list<laddr_t> dec_laddrs) {
    return seastar::do_with(std::move(dec_laddrs), std::list<unsigned>(),
      [this, &tm, &t] (auto &&dec_laddrs, auto &refcnt) {
      return crimson::do_for_each(dec_laddrs.begin(), dec_laddrs.end(),
        [this, &tm, &t, &refcnt] (auto &laddr) {
        return tm.dec_ref(t, laddr).safe_then([&refcnt] (auto ref) {
          refcnt.push_back(ref);
        });
      }).safe_then([&refcnt] {
        return retire_ertr::make_ready_future<std::list<unsigned>>(std::move(refcnt));
      });
    });
  }

};

using ExtMapNodeRef = ExtMapNode::ExtMapNodeRef;

TransactionManager::read_extent_ertr::future<ExtMapNodeRef>
extmap_load_extent(TransactionManager &tm, Transaction& txn, laddr_t laddr, depth_t depth);

}


