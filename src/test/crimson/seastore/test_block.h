// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore {

struct test_extent_desc_t {
  size_t len = 0;
  unsigned checksum = 0;

  bool operator==(const test_extent_desc_t &rhs) const {
    return (len == rhs.len &&
	    checksum == rhs.checksum);
  }
  bool operator!=(const test_extent_desc_t &rhs) const {
    return !(*this == rhs);
  }
};

struct test_block_delta_t {
  uint8_t val = 0;
  uint16_t offset = 0;
  uint16_t len = 0;

  
  DENC(test_block_delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.val, p);
    denc(v.offset, p);
    denc(v.len, p);
    DENC_FINISH(p);
  }
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::test_block_delta_t)

namespace crimson::os::seastore {

inline std::ostream &operator<<(
  std::ostream &lhs, const test_extent_desc_t &rhs) {
  return lhs << "test_extent_desc_t(len=" << rhs.len
	     << ", checksum=" << rhs.checksum << ")";
}

struct TestBlock : crimson::os::seastore::LogicalCachedExtent {
  constexpr static segment_off_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<TestBlock>;

  std::vector<test_block_delta_t> delta = {};

  TestBlock(ceph::bufferptr &&ptr) : LogicalCachedExtent(std::move(ptr)) {}
  TestBlock(const TestBlock &other) : LogicalCachedExtent(other) {}

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new TestBlock(*this));
  };

  static constexpr extent_types_t TYPE = extent_types_t::TEST_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  ceph::bufferlist get_delta() final {
    ceph::bufferlist bl;
    ::encode(delta, bl);
    return bl;
  }

  void set_contents(char c, uint16_t offset, uint16_t len) {
    ::memset(get_bptr().c_str() + offset, c, len);
  }

  void set_contents(char c) {
    set_contents(c, 0, get_length());
  }

  int checksum() {
    return ceph_crc32c(
      1,
      (const unsigned char *)get_bptr().c_str(),
      get_length());
  }

  test_extent_desc_t get_desc() {
    return { get_length(), get_crc32c(1) };
  }

  void apply_delta(paddr_t delta_base, const ceph::bufferlist &bl) final {
    auto biter = bl.begin();
    decltype(delta) deltas;
    ::decode(deltas, biter);
    for (auto &&d : deltas) {
      set_contents(d.val, d.offset, d.len);
    }
    set_last_committed_crc(get_crc32c());
  }
};
using TestBlockRef = TCachedExtentRef<TestBlock>;

}
