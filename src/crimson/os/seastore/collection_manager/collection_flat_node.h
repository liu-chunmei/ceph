// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/collection_manager.h"

namespace crimson::os::seastore::collection_manager {
struct coll_context_t {
  TransactionManager &tm;
  Transaction &t;
};

using base_coll_map_t = std::map<denc_coll_t, uint32_t>;
struct coll_map_t : base_coll_map_t {
  void insert(coll_t coll, unsigned bits) {
    auto [iter, inserted] = emplace(
      std::make_pair(denc_coll_t{coll}, bits)
    );
    assert(inserted);
  }

  void update(coll_t coll, unsigned bits) {
    (*this)[denc_coll_t{coll}] = bits;
  }

  void remove(coll_t coll) {
    erase(denc_coll_t{coll});
  }
};

struct delta_t {
  enum class op_t : uint_fast8_t {
    INSERT,
    UPDATE,
    REMOVE,
    INVALID
  } op = op_t::INVALID;

  denc_coll_t coll;
  uint32_t bits = 0;

  delta_t() = default;

  DENC(delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.coll, p);
    denc(v.bits, p);
    DENC_FINISH(p);
  }

  void replay(coll_map_t &l) const;
};
}
WRITE_CLASS_DENC(crimson::os::seastore::collection_manager::delta_t)

namespace crimson::os::seastore::collection_manager {
class delta_buffer_t {
  std::vector<delta_t> buffer;
public:
  bool empty() const {
    return buffer.empty();
  }

  void insert(coll_t coll, uint32_t bits) {
    buffer.push_back(delta_t{delta_t::op_t::INSERT, denc_coll_t(coll), bits});
  }
  void update(coll_t coll, uint32_t bits) {
    buffer.push_back(delta_t{delta_t::op_t::UPDATE, denc_coll_t(coll), bits});
  }
  void remove(coll_t coll) {
    buffer.push_back(delta_t{delta_t::op_t::REMOVE, denc_coll_t(coll), 0});
  }
  void replay(coll_map_t &l) {
    for (auto &i: buffer) {
      i.replay(l);
    }
  }

  void clear() { buffer.clear(); }

  DENC(delta_buffer_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.buffer, p);
    DENC_FINISH(p);
  }
};
}
WRITE_CLASS_DENC(crimson::os::seastore::collection_manager::delta_buffer_t)

namespace crimson::os::seastore::collection_manager {

struct CollectionNode
  : LogicalCachedExtent {
  using CollectionNodeRef = TCachedExtentRef<CollectionNode>;

  template <typename... T>
  CollectionNode(T&&... t)
  : LogicalCachedExtent(std::forward<T>(t)...) {
     read_to_local();
  }

  static constexpr extent_types_t type = extent_types_t::COLL_BLOCK;

  coll_map_t decoded;
  delta_buffer_t delta_buffer;

  CachedExtentRef duplicate_for_write() final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new CollectionNode(*this));
  }
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  using list_ertr = CollectionManager::list_ertr;
  using list_ret = CollectionManager::list_ret;
  list_ret list();

  using create_ertr = CollectionManager::create_ertr;
  using create_ret = CollectionManager::create_ret;
  create_ret create(coll_context_t cc, coll_t coll, unsigned bits);

  using remove_ertr = CollectionManager::remove_ertr;
  using remove_ret = CollectionManager::remove_ret;
  remove_ret remove(coll_context_t cc, coll_t coll);

  using update_ertr = CollectionManager::update_ertr;
  using update_ret = CollectionManager::update_ret;
  update_ret update(coll_context_t cc, coll_t coll, unsigned bits);

  void read_to_local() {
    bufferlist bl;
    bl.append(get_bptr());
    auto iter = bl.cbegin();
    decode((base_coll_map_t&)decoded, iter);
  }

  void copy_to_node() {
    bufferlist bl;
    encode((base_coll_map_t&)decoded, bl);
    auto iter = bl.begin();
    auto size = encoded_sizeof((base_coll_map_t&)decoded);
    assert(size <= get_bptr().length());
    get_bptr().zero();
    iter.copy(size, get_bptr().c_str());

  }

  void copy_from_other(CollectionNodeRef other) {
    memcpy(get_bptr().c_str(), other->get_bptr().c_str(), other->get_length());
  }

  ceph::bufferlist get_delta() final {
    assert(!delta_buffer.empty());
    ceph::bufferlist bl;
    encode(delta_buffer, bl);
    delta_buffer.clear();
    return bl;
  }

  void apply_delta(const ceph::bufferlist &bl) final {
    assert(bl.length());
    delta_buffer_t buffer;
    auto bptr = bl.begin();
    decode(buffer, bptr);
    buffer.replay(decoded);
    copy_to_node();
  }

  extent_types_t get_type() const final {
    return type;
  }

  std::ostream &print_detail_l(std::ostream &out) const final;
};
using CollectionNodeRef = CollectionNode::CollectionNodeRef;
}
