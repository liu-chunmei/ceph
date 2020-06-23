// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#pragma once

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/extentmap_manager/extentmap_manager.h"

namespace crimson::os::seastore::extentmap_manager{

struct ExtMapNode : LogicalCachedExtent {
  TransactionManager* tm = nullptr;
  using ExtMapNodeRef = TCachedExtentRef<ExtMapNode>;
  depth_t depth = 0;

//  template <typename... T>
//  ExtMapNode(T&&... t) 
//  : LogicalCachedExtent(std::forward<T>(t)...) {}

  ExtMapNode(ceph::bufferptr &&ptr) : LogicalCachedExtent(std::move(ptr)) {}
  ExtMapNode(const ExtMapNode &other)
  : LogicalCachedExtent(other),
    tm(other.tm),
    depth(other.depth) {}

  void set_depth(depth_t _depth) { depth = _depth; }
  void set_tm(TransactionManager* _tm) { tm = _tm; }

  LogicalCachedExtentRef make_duplicate(Transaction& t,
	  LogicalCachedExtentRef ref) {
    return tm->get_mutable_extent(t, ref);
  };

  using seek_lextent_ertr = ExtentMapManager::seek_lextent_ertr;
  using seek_lextent_ret = ExtentMapManager::seek_lextent_ret;
  virtual seek_lextent_ret seek_lextent(Transaction &t,
		                        uint32_t lo, uint32_t len) = 0;
  using insert_ertr = TransactionManager::read_extent_ertr;
  using insert_ret = insert_ertr::future<ExtentRef>;
  virtual insert_ret insert(Transaction &t, uint32_t lo, lext_map_val_t val) = 0;

  using punch_lextent_ertr = ExtentMapManager::punch_lextent_ertr;
  using punch_lextent_ret = ExtentMapManager::punch_lextent_ret;
  virtual punch_lextent_ret punch_lextent(Transaction &t, uint32_t lo, uint32_t len) = 0;

  using find_hole_ertr = TransactionManager::read_extent_ertr;
  using find_hole_ret = find_hole_ertr::future<ExtentRef>;
  virtual find_hole_ret find_hole(Transaction &t, uint32_t lo, uint32_t len) = 0;

  using rm_lextent_ertr = TransactionManager::read_extent_ertr;
  using rm_lextent_ret = rm_lextent_ertr::future<bool>;
  virtual rm_lextent_ret rm_lextent(Transaction &t, uint32_t lo, lext_map_val_t val) = 0;

  using mutate_mapping_ertr = TransactionManager::read_extent_ertr;
  using mutate_mapping_ret = mutate_mapping_ertr::future<
    std::optional<lext_map_val_t>>;
  using mutate_func_t = std::function<
    std::optional<lext_map_val_t>(const lext_map_val_t &v)>;
  virtual mutate_mapping_ret mutate_mapping(Transaction &transaction,
                                            uint32_t lo, mutate_func_t &&f) = 0;
  using split_children_ertr = TransactionManager::alloc_extent_ertr;
  using split_children_ret = split_children_ertr::future
	  <std::tuple<ExtMapNodeRef, ExtMapNodeRef, uint32_t>>;
  virtual split_children_ret make_split_children(Transaction &t) = 0;
  
  using full_merge_ertr = TransactionManager::alloc_extent_ertr;
  using full_merge_ret = full_merge_ertr::future<ExtMapNodeRef>;
  virtual full_merge_ret make_full_merge(Transaction &t, ExtMapNodeRef right) = 0;

  using make_balanced_ertr = TransactionManager::alloc_extent_ertr;
  using make_balanced_ret = make_balanced_ertr::future
	  <std::tuple<ExtMapNodeRef, ExtMapNodeRef, uint32_t>>;
  virtual make_balanced_ret
    make_balanced(Transaction &t, ExtMapNodeRef right, bool prefer_left) = 0;

  virtual bool at_max_capacity() const = 0;
  virtual bool at_min_capacity() const = 0;
  virtual ~ExtMapNode() = default;

  using alloc_ertr = TransactionManager::alloc_extent_ertr;
  template<class T>
  alloc_ertr::future<TCachedExtentRef<T>>
  extmap_alloc_extent(Transaction& txn, uint32_t len) {
    return tm->alloc_extent<T>(txn, L_ADDR_MIN, len).safe_then(
      [this](auto&& extent) {
      extent->set_tm(tm);
      return alloc_ertr::make_ready_future<TCachedExtentRef<T>>(std::move(extent));
    });
  }


};

using ExtMapNodeRef = ExtMapNode::ExtMapNodeRef;

TransactionManager::read_extent_ertr::future<ExtMapNodeRef>
extmap_load_extent(TransactionManager* tm, Transaction& txn, laddr_t laddr, depth_t depth);

}


