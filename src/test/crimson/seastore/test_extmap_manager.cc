// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/extentmap_manager/extentmap_manager.h"

#include "test/crimson/seastore/test_block.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}


struct extentmap_manager_test_t : public seastar_test_suite_t {
  std::unique_ptr<SegmentManager> segment_manager;
  Journal journal;
  Cache cache;
  LBAManagerRef lba_manager;
  TransactionManager tm;
  ExtentMapManagerRef extmap_manager;

  extentmap_manager_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      journal(*segment_manager),
      cache(*segment_manager),
      lba_manager(
	lba_manager::create_lba_manager(*segment_manager, cache)),
      tm(*segment_manager, journal, cache, *lba_manager),
      extmap_manager(
        extentmap_manager::create_extentmap_manager(&tm)) {}

  seastar::future<> set_up_fut() final {
    return segment_manager->init().safe_then([this] {
      return tm.mkfs();
    }).safe_then([this] {
      return tm.mount();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to mount");
      })
    );
  }

  seastar::future<> tear_down_fut() final {
    return tm.close(
    ).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to close");
      })
    );
  }

  using test_extmap_t = std::map<uint32_t, lext_map_val_t>;
  test_extmap_t test_ext_mappings;

  TestBlockRef alloc_extent(
    Transaction &t,
    extent_len_t len,
    char contents) {
    auto extent = tm.alloc_extent<TestBlock>(
      t,
      L_ADDR_MIN,
      len).unsafe_get0();
    extent->set_contents(contents);
    EXPECT_EQ(len, extent->get_length());
    return extent;
  }

  ExtentRef insert_extent(
    Transaction &t,
    uint32_t lo,
    lext_map_val_t val) {
    auto extent = extmap_manager->add_lextent(t, lo, val).unsafe_get0();
    EXPECT_EQ(lo, extent->logical_offset);
    EXPECT_EQ(val.laddr, extent->laddr);
    EXPECT_EQ(val.lextent_offset, extent->lextent_offset);
    EXPECT_EQ(val.length, extent->length);
    test_ext_mappings.emplace(std::make_pair(extent->logical_offset,
      lext_map_val_t{extent->laddr, extent->lextent_offset, extent->length}));
    return extent;
  }

  extent_map_list_t find_extent(
    Transaction &t,
    uint32_t lo,
    uint32_t len) {
    auto extent = extmap_manager->seek_lextent(t, lo, len).unsafe_get0();
    EXPECT_EQ(lo, extent.front()->logical_offset);
    EXPECT_EQ(len, extent.front()->length);
    return extent;
  }
  extent_map_list_t findno_extent(
    Transaction &t,
    uint32_t lo,
    uint32_t len) {
    auto extent = extmap_manager->seek_lextent(t, lo, len).unsafe_get0();
    EXPECT_EQ(extent.empty(), true);
    return extent;
  }
  extent_map_list_t find_any(
    Transaction &t,
    uint32_t lo,
    uint32_t len) {
    auto extent = extmap_manager->seek_lextent(t, lo, len).unsafe_get0();
    EXPECT_EQ(extent.size(), 1);
    return extent;
  }

  bool rm_extent(
    Transaction &t,
    uint32_t lo,
    lext_map_val_t val ) {
    auto ret = extmap_manager->rm_lextent(t, lo, val).unsafe_get0();
    EXPECT_EQ(ret, true);
    test_ext_mappings.erase(test_ext_mappings.find(lo));
    return ret;
  }

  extent_map_list_t punch_extent(
    Transaction &t,
    uint32_t lo,
    uint32_t offset,
    uint32_t len) {
    auto extent = extmap_manager->punch_lextent(t, lo + offset, len).unsafe_get0();
    if (extent.empty())
      return extent;

    auto it = test_ext_mappings.find(lo);
    auto val = it->second;
    if (it != test_ext_mappings.end())
      test_ext_mappings.erase(it);
    if (offset == 0) {
      test_ext_mappings.insert(std::make_pair(lo+len, 
			       lext_map_val_t{val.laddr, len, val.length-len}));
      EXPECT_EQ(lo, extent.front()->logical_offset);
      EXPECT_EQ(len, extent.front()->length);
    }
    if (offset == len){
      test_ext_mappings.insert(std::make_pair(lo, lext_map_val_t{val.laddr, 0, offset}));
      test_ext_mappings.insert(std::make_pair(lo + offset + len, 
		       lext_map_val_t{val.laddr, offset+len, val.length-len-offset}));
      EXPECT_EQ(lo+len, extent.front()->logical_offset);
      EXPECT_EQ(len, extent.front()->length);
    }
    if (offset == 2*len){
      test_ext_mappings.insert(std::make_pair(lo,
			       lext_map_val_t{val.laddr, 0,  val.length-len}));	    
      EXPECT_EQ(lo + 2*len, extent.front()->logical_offset);
      EXPECT_EQ(len, extent.front()->length);
    }
    return extent;
  }

  void check_mappings(Transaction &t) {
    for (auto &&i: test_ext_mappings){
      auto ret_list = find_extent(t, i.first, i.second.length);
      EXPECT_EQ(ret_list.size(), 1);
      auto &ret = *ret_list.begin();
      EXPECT_EQ(i.second.laddr, ret->laddr);
      EXPECT_EQ(i.second.lextent_offset, ret->lextent_offset);
      EXPECT_EQ(i.second.length, ret->length);
    }
  }

  void check_mappings() {
    auto t = tm.create_transaction();
    check_mappings(*t);
  }

};

TEST_F(extentmap_manager_test_t, basic)
{
  run_async([this] {
    uint32_t len = 4096;
    uint32_t lo = 0x1 * len;
    {
      auto t = tm.create_transaction();
      logger().debug("first transaction");
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      [[maybe_unused]] auto extent = alloc_extent(*t, len, 'a');
      [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
      [[maybe_unused]] auto seekref = find_extent(*t, lo, len);
      tm.submit_transaction(std::move(t)).unsafe_get();

      auto t2 = tm.create_transaction();
      logger().debug("second transaction");
      [[maybe_unused]] auto seekref2 = find_extent(*t2, lo, len);
      [[maybe_unused]] auto seekref5 = find_any(*t2, 0, len);
      [[maybe_unused]] auto rmret = rm_extent(*t2, lo, {extent->get_laddr(), 0, len});
      [[maybe_unused]] auto seekref3 = findno_extent(*t2, lo, len);
      tm.submit_transaction(std::move(t2)).unsafe_get();

      auto t3 = tm.create_transaction();
      logger().debug("third transaction");
      [[maybe_unused]] auto seekref4 = findno_extent(*t3, lo, len);
      tm.submit_transaction(std::move(t3)).unsafe_get();

    }
  });
} 

TEST_F(extentmap_manager_test_t, force_split)
{
  run_async([this] {
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 40; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 10; ++j) {
       // auto extent = alloc_extent(*t, len, 'a');
       // [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
        [[maybe_unused]] auto addref = insert_extent(*t, lo, {lo, 0, len});
        lo += len;
        if ((i % 20 == 0) && (j == 5)) {
          check_mappings(*t);
        }
      }
      tm.submit_transaction(std::move(t)).unsafe_get();
      check_mappings();
    }
  });

}


TEST_F(extentmap_manager_test_t, force_split_merge)
{
  run_async([this] {
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 80; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
      //  auto extent = alloc_extent(*t, len, 'a');
      //  [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
        [[maybe_unused]] auto addref = insert_extent(*t, lo, {lo, 0, len});
        lo += len;
	if ((i % 10 == 0) && (j == 3)) {
	  check_mappings(*t);
	}
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
	check_mappings();
      }
    }
    auto t = tm.create_transaction();
    int i = 0;
    for (auto &e: test_ext_mappings) {
      if (i % 3 != 0) {
	[[maybe_unused]] auto rmref= rm_extent(*t, e.first, e.second);
      }

      if (i % 10 == 0) {
      logger().debug("submitting transaction i= {}", i);
	tm.submit_transaction(std::move(t)).unsafe_get();
	t = tm.create_transaction();
      }
      if (i % 100 == 0) {
      logger().debug("check_mappings  i= {}", i);
	check_mappings(*t);
	check_mappings();
      }
      i++;
    }
    logger().debug("finally submitting transaction ");
    tm.submit_transaction(std::move(t)).unsafe_get();
  });
}

TEST_F(extentmap_manager_test_t, force_split_balanced)
{
  run_async([this] {
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 50; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
      //  auto extent = alloc_extent(*t, len, 'a');
      //  [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
        [[maybe_unused]] auto addref = insert_extent(*t, lo, {lo, 0, len});
        lo += len;
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(*t);
        }
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
        check_mappings();
      }
    }
    auto t = tm.create_transaction();
    int i = 0;
    for (auto &e: test_ext_mappings) {
      if (i < 100) {
        [[maybe_unused]] auto rmref= rm_extent(*t, e.first, e.second);
      }

      if (i % 10 == 0) {
      logger().debug("submitting transaction i= {}", i);
        tm.submit_transaction(std::move(t)).unsafe_get();
        t = tm.create_transaction();
      }
      if (i % 50 == 0) {
      logger().debug("check_mappings  i= {}", i);
        check_mappings(*t);
        check_mappings();
      }
      i++;
      if (i = 100)
	break;
    }
    logger().debug("finally submitting transaction ");
    tm.submit_transaction(std::move(t)).unsafe_get();
    check_mappings();
  });
}

TEST_F(extentmap_manager_test_t, force_split_punch)
{
  run_async([this] {
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 50; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
      //  auto extent = alloc_extent(*t, len, 'a');
      //  [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
        [[maybe_unused]] auto addref = insert_extent(*t, lo, {lo, 0, 3*len});
        lo += 3*len;
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(*t);
        }
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
        check_mappings();
      }
    }
    auto t = tm.create_transaction();
    int i = 1;
    for (auto &e: test_ext_mappings) {
      if (i % 9 == 0) {
        [[maybe_unused]] auto punchref= punch_extent(*t, e.first, 0, len);
        i++;
	continue;
      }

      if ((i % 9 != 0) && (i % 6 == 0)) {
        [[maybe_unused]] auto punchref= punch_extent(*t, e.first, len, len);
        i++;
	continue;
      }

      if ((i % 9 != 0) && (i % 6 != 0) && (i % 3 == 0)) {
        [[maybe_unused]] auto punchref= punch_extent(*t, e.first, 2*len, len);
        i++;
	continue;
      }

      if (i % 20 == 0) {
        logger().debug("submitting transaction i= {}", i);
        tm.submit_transaction(std::move(t)).unsafe_get();
        t = tm.create_transaction();
      }
      if (i % 50 == 0) {
      logger().debug("check_mappings  i= {}", i);
        check_mappings(*t);
      }
      i++;
    }
    logger().debug("finally submitting transaction ");
    tm.submit_transaction(std::move(t)).unsafe_get();
    check_mappings();
  });
}
