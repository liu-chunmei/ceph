// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/extentmap_manager/btree/btree_extentmap_manager.h"
#include "crimson/os/seastore/extentmap_manager/btree/extentmap_btree_node_impl.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::extentmap_manager {

BtreeExtentMapManager::BtreeExtentMapManager(
  TransactionManager &tm)
  : tm(tm) {}

BtreeExtentMapManager::BtreeExtentMapManager(
  TransactionManager &tm,  extmap_root_t extmap_root)
  : tm(tm),
    extmap_root(extmap_root) {}


BtreeExtentMapManager::initialize_extmap_ret
BtreeExtentMapManager::initialize_extmap(Transaction &t)
{

  logger().debug("BtreeExtentMapManager::initialize_extmap");
  if (extmap_root.extmap_root_laddr != L_ADDR_NULL)
    return initialize_extmap_ertr::make_ready_future<extmap_root_t>(extmap_root);

  return tm.alloc_extent<ExtMapLeafNode>(t, L_ADDR_MIN, EXTMAP_BLOCK_SIZE)
    .safe_then([this](auto&& root_extent) {
      root_extent->set_size(0);
      root_extent->set_depth(0);
      extmap_root = extmap_root_t(0, root_extent->get_laddr());
      return initialize_extmap_ertr::make_ready_future<extmap_root_t>(extmap_root);
  });
}

BtreeExtentMapManager::get_root_ret
BtreeExtentMapManager::get_extmap_root(Transaction &t)
{
  assert(extmap_root.extmap_root_laddr != L_ADDR_NULL);
  laddr_t laddr = extmap_root.extmap_root_laddr;
  return extmap_load_extent(tm, t, laddr, extmap_root.depth);
}

BtreeExtentMapManager::find_lextent_ret
BtreeExtentMapManager::find_lextent(Transaction &t, objaddr_t lo,
		                    extent_len_t len)
{
  logger().debug("BtreeExtentMapManager::find_lextent: {}, {}", lo, len);
  return get_extmap_root(t).safe_then([this, &t, lo, len](auto&& extent) {
    return extent->find_lextent(tm, t, lo, len);
  }).safe_then([](auto &&e) {
    logger().debug("BtreeExtentMapManager::find_lextent: found_lextent {}", e);
    return find_lextent_ret(
        find_lextent_ertr::ready_future_marker{},
	std::move(e));
  });

}

BtreeExtentMapManager::add_lextent_ret
BtreeExtentMapManager::add_lextent(Transaction &t, objaddr_t lo,
                                   lext_map_val_t val)
{
  assert(lo % PAGE_SIZE == 0 && val.length % PAGE_SIZE == 0);
  logger().debug("BtreeExtentMapManager::add_lextent: {}, {}, {}", lo, val.laddr, val.length);
  return get_extmap_root(t).safe_then([this, &t, lo, val](auto &&root) {
    return insert_lextent(t, root, lo, val);
  }).safe_then([](auto ret) {
      logger().debug("BtreeExtentMapManager::add_lextent:  {}", ret);
      return add_lextent_ret(
        add_lextent_ertr::ready_future_marker{},
        std::move(ret));
  });

}

BtreeExtentMapManager::insert_lextent_ret
BtreeExtentMapManager::insert_lextent(Transaction &t, ExtMapNodeRef root,
                    objaddr_t logical_offset, lext_map_val_t val)
{
  auto split = insert_lextent_ertr::make_ready_future<ExtMapNodeRef>(root);
  if (root->at_max_capacity()) {
      logger().debug("BtreeExtentMapManager::splitting root {}", *root);
    split =  root->extmap_alloc_extent<ExtMapInnerNode>(tm, t, EXTMAP_BLOCK_SIZE)
        .safe_then([this, root, &t, logical_offset](auto&& nroot) {
        nroot->set_depth(root->depth + 1);
	nroot->journal_insert(nroot->begin(), OBJ_ADDR_MIN,
			      root->get_laddr(), nullptr);
        extmap_root.extmap_root_laddr = nroot->get_laddr();
        extmap_root.depth = root->depth + 1;
        return nroot->split_entry(tm, t, logical_offset, nroot->begin(), root);
      });
  }
  return split.safe_then([this, &t, logical_offset, val](ExtMapNodeRef node) {
    return node->insert(tm, t, logical_offset, val);
  });
}

BtreeExtentMapManager::rm_lextent_ret
BtreeExtentMapManager::rm_lextent(Transaction &t, objaddr_t lo, lext_map_val_t val)
{
  assert(lo % PAGE_SIZE == 0 && val.length % PAGE_SIZE == 0);
  logger().debug("BtreeExtentMapManager::rm_lextent: {}, {}, {}", lo, val.laddr, val.length);
  return get_extmap_root(t).safe_then([this, &t, lo, val](auto extent) {
    return extent->rm_lextent(tm, t, lo, val);
  }).safe_then([](auto ret) {
    logger().debug("BtreeExtentMapManager::rm_lextent: {}", ret);
    return rm_lextent_ret(
      rm_lextent_ertr::ready_future_marker{},
      ret);
  });
}


}
