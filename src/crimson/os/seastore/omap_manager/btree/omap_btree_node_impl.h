// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string.h>

#include "include/buffer.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/omap_manager/btree/string_kv_node_layout.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node.h"

namespace crimson::os::seastore::omap_manager {

struct omap_node_meta_le_t {
  depth_le_t depth = init_les32(0);

  omap_node_meta_le_t() = default;
  omap_node_meta_le_t(const omap_node_meta_le_t &) = default;
  explicit omap_node_meta_le_t(const omap_node_meta_t &val)
    : depth(init_les32(val.depth)) {}

  operator omap_node_meta_t() const {
    return omap_node_meta_t{ depth };
  }
};

struct omap_inner_key_t {
  uint32_t key_off = 0;
  uint32_t key_len = 0;
  laddr_t laddr = 0;
  
  omap_inner_key_t() = default;
  omap_inner_key_t(uint32_t off, uint32_t len, laddr_t addr)
  : key_off(off), key_len(len), laddr(addr) {}
};

struct omap_inner_key_le_t {
  ceph_le32 key_off = init_le32(0);
  ceph_le32 key_len = init_le32(0);
  laddr_le_t laddr = laddr_le_t(0);

  omap_inner_key_le_t() = default;
  omap_inner_key_le_t(const omap_inner_key_le_t &) = default;
  explicit omap_inner_key_le_t(const omap_inner_key_t &key)
    : key_off(init_le32(key.key_off)),
      key_len(init_le32(key.key_len)),
      laddr(laddr_le_t(key.laddr)) {}

  operator omap_inner_key_t() const {
    return omap_inner_key_t{uint32_t(key_off), uint32_t(key_len), laddr_t(laddr)};
  }

  omap_inner_key_le_t& operator=(omap_inner_key_t key) {
    key_off = init_le32(key.key_off);
    key_len = init_le32(key.key_len);
    laddr = laddr_le_t(key.laddr);
    return *this;
  }
};

/**
 * OMapInnerNode
 *
 * Abstracts operations on and layout of internal nodes for the
 * omap Tree.
 *
 * Layout (4k):
 *   num_entries:   meta    :    keys    :  values  : 
 */

struct OMapInnerNode
  : OMapNode,
    StringKVInnerNodeLayout<
    omap_node_meta_t, omap_node_meta_le_t,
    omap_inner_key_t, omap_inner_key_le_t> {

  using internal_iterator_t = const_iterator;
  template <typename... T>
  OMapInnerNode(T&&... t) :
    OMapNode(std::forward<T>(t)...),
    StringKVInnerNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::OMAP_INNER;

  omap_node_meta_t get_node_meta() const final {return get_meta();}
  bool extent_is_overflow(size_t size) {return is_overflow(size);}
  bool extent_under_median() {return under_median();}

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new OMapInnerNode(*this));
  }

  delta_inner_buffer_t delta_buffer;
  delta_inner_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  get_value_ret get_value(omap_context_t oc, const std::string &key) final;

  insert_ret insert(omap_context_t oc, std::string &key, std::string &value) final;

  rm_key_ret rm_key(omap_context_t oc, const std::string &key) final;

  list_keys_ret list_keys(omap_context_t oc, std::vector<std::string> &result) final;

  list_ret list(omap_context_t oc, std::vector<std::pair<std::string, std::string>> &result) final;

  clear_ret clear(omap_context_t oc) final;

  split_children_ret make_split_children(omap_context_t oc) final;

  full_merge_ret make_full_merge(omap_context_t oc, OMapNodeRef right) final;

  make_balanced_ret
    make_balanced(omap_context_t oc, OMapNodeRef right, bool prefer_left) final;

  std::ostream &print_detail_l(std::ostream &out) const final;

  extent_types_t get_type() const final {
    return type;
  }

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::bufferlist bl;
    delta_buffer.encode(bl);
    return bl;
  }

  void apply_delta(const ceph::bufferlist &bl) final {
    assert(bl.length());
    delta_inner_buffer_t buffer;
    buffer.decode(bl);
    buffer.replay(*this);
  }

  using split_entry_ertr = TransactionManager::read_extent_ertr;
  using split_entry_ret = split_entry_ertr::future<OMapNodeRef>;
  split_entry_ret split_entry(omap_context_t oc, std::string &key,
                              internal_iterator_t, OMapNodeRef entry);

  using merge_entry_ertr = TransactionManager::read_extent_ertr;
  using merge_entry_ret = merge_entry_ertr::future<OMapNodeRef>;
  merge_entry_ret merge_entry(omap_context_t oc, const std::string &key,
                              internal_iterator_t iter, OMapNodeRef entry);

  internal_iterator_t get_containing_child(const std::string &key);

};

/**
 * OMapLeafNode
 *
 * Abstracts operations on and layout of leaf nodes for the
 * OMap Tree.
 *
 * Layout (4k):
 *   num_entries:   meta   :   keys   :  values  :
 */

struct omap_leaf_key_t {
  uint16_t key_off = 0;
  uint16_t key_len = 0;
  uint16_t val_off = 0;
  uint16_t val_len = 0;

  omap_leaf_key_t() = default;
  omap_leaf_key_t(uint16_t k_off, uint16_t k_len, uint16_t v_off, uint16_t v_len)
  : key_off(k_off), key_len(k_len), val_off(v_off), val_len(v_len) {}
};

struct omap_leaf_key_le_t {
  ceph_le16 key_off = init_le16(0);
  ceph_le16 key_len = init_le16(0);
  ceph_le16 val_off = init_le16(0);
  ceph_le16 val_len = init_le16(0);

  omap_leaf_key_le_t() = default;
  omap_leaf_key_le_t(const omap_leaf_key_le_t &) = default;
  explicit omap_leaf_key_le_t(const omap_leaf_key_t &key)
    : key_off(init_le16(key.key_off)),
      key_len(init_le16(key.key_len)),
      val_off(init_le16(key.val_off)),
      val_len(init_le16(key.val_len)) {}

  operator omap_leaf_key_t() const {
    return omap_leaf_key_t{uint16_t(key_off), uint16_t(key_len),
                           uint16_t(val_off), uint16_t(val_len)};
  }

  omap_leaf_key_le_t& operator=(omap_leaf_key_t key) {
    key_off = init_le16(key.key_off);
    key_len = init_le16(key.key_len);
    val_off = init_le16(key.val_off);
    val_len = init_le16(key.val_len);
    return *this;
  }
};

struct OMapLeafNode
  : OMapNode,
    StringKVLeafNodeLayout<
      omap_node_meta_t, omap_node_meta_le_t,
      omap_leaf_key_t, omap_leaf_key_le_t> {

 using internal_iterator_t = const_iterator;
  template <typename... T>
  OMapLeafNode(T&&... t) :
    OMapNode(std::forward<T>(t)...),
    StringKVLeafNodeLayout(get_bptr().c_str()) {}

  static constexpr extent_types_t type = extent_types_t::OMAP_LEAF;

  omap_node_meta_t get_node_meta() const final { return get_meta(); }
  bool extent_is_overflow(size_t size) {return is_overflow(size);}
  bool extent_under_median() {return under_median();}

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new OMapLeafNode(*this));
  }

  delta_leaf_buffer_t delta_buffer;
  delta_leaf_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  get_value_ret get_value(omap_context_t oc, const std::string &key) final;

  insert_ret insert(omap_context_t oc, std::string &key, std::string &value) final;

  rm_key_ret rm_key(omap_context_t oc, const std::string &key) final;

  list_keys_ret list_keys(omap_context_t oc, std::vector<std::string> &result) final;

  list_ret list(omap_context_t oc, std::vector<std::pair<std::string, std::string>> &result) final;

  clear_ret clear(omap_context_t oc) final;

  split_children_ret make_split_children(omap_context_t oc) final;

  full_merge_ret make_full_merge(omap_context_t oc, OMapNodeRef right) final;

  make_balanced_ret make_balanced(omap_context_t oc, OMapNodeRef _right, bool prefer_left) final;

  extent_types_t get_type() const final {
    return type;
  }

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::bufferlist bl;
    delta_buffer.encode(bl);
    return bl;
  }

  void apply_delta(const ceph::bufferlist &bl) final {
    assert(bl.length());
    delta_leaf_buffer_t buffer;
    buffer.decode(bl);
    buffer.replay(*this);
  }

  std::ostream &print_detail_l(std::ostream &out) const final;

  std::pair<internal_iterator_t, internal_iterator_t>
  get_leaf_entries(std::string &key);

};
using OMapLeafNodeRef = TCachedExtentRef<OMapLeafNode>;

std::ostream &operator<<(std::ostream &out, const omap_inner_key_t &rhs);
std::ostream &operator<<(std::ostream &out, const omap_leaf_key_t &rhs);
}

