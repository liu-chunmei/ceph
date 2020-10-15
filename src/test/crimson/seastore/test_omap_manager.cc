// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/omap_manager.h"

#include "test/crimson/seastore/test_block.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace std;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

struct omap_manager_test_t : public seastar_test_suite_t {
  std::unique_ptr<SegmentManager> segment_manager;
  SegmentCleaner segment_cleaner;
  Journal journal;
  Cache cache;
  LBAManagerRef lba_manager;
  TransactionManager tm;
  OMapManagerRef omap_manager;

  omap_manager_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      segment_cleaner(SegmentCleaner::config_t::default_from_segment_manager(
         *segment_manager)),
      journal(*segment_manager),
      cache(*segment_manager),
      lba_manager(
        lba_manager::create_lba_manager(*segment_manager, cache)),
        tm(*segment_manager, segment_cleaner, journal, cache, *lba_manager),
        omap_manager(omap_manager::create_omap_manager(tm)) {
         journal.set_segment_provider(&segment_cleaner);
         segment_cleaner.set_extent_callback(&tm);
        }

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
    return tm.close().handle_error(
      crimson::ct_error::all_same_way([] {
        ASSERT_FALSE("Unable to close");
      })
    );
  }

  using test_omap_t = std::map<std::string, std::string>;
  test_omap_t test_omap_mappings;

  std::pair<string, string> set_key(
    omap_root_t &omap_root,
    Transaction &t,
    string &key,
    string &val) {
    auto ret = omap_manager->omap_set_key(omap_root, t, key, val).unsafe_get0();
    EXPECT_EQ(key, ret.first);
    EXPECT_EQ(val, ret.second);
    test_omap_mappings[key] = val;
  //  test_omap_mappings.emplace(ret);
    return ret;
  }

  std::pair<string, string> get_value(
    omap_root_t &omap_root,
    Transaction &t,
    const string &key) {
    auto ret = omap_manager->omap_get_value(omap_root, t, key).unsafe_get0();
    EXPECT_EQ(key, ret.first);
    return ret;
  }

  bool rm_key(
    omap_root_t &omap_root,
    Transaction &t,
    const string &key) {
    auto ret = omap_manager->omap_rm_key(omap_root, t, key).unsafe_get0();
    EXPECT_EQ(ret, true);
    test_omap_mappings.erase(test_omap_mappings.find(key));
    return ret;
  }

  std::vector<string> list_keys(
    omap_root_t &omap_root,
    Transaction &t) {
    auto ret = omap_manager->omap_list_keys(omap_root, t).unsafe_get0();
    for ( auto &i : ret) {
      auto it = test_omap_mappings.find(i);
      EXPECT_NE(it, test_omap_mappings.end());
      EXPECT_EQ(i, it->first);
      EXPECT_EQ(test_omap_mappings.size(), ret.size());
    }
    return ret;
  }

  std::vector<pair<string, string>> list(
    omap_root_t &omap_root,
    Transaction &t) {
    auto ret = omap_manager->omap_list(omap_root, t).unsafe_get0();
    for ( auto &i : ret) {
      auto it = test_omap_mappings.find(i.first);
      EXPECT_EQ(i.second, it->second);
      EXPECT_EQ(test_omap_mappings.size(), ret.size());
    }
    return ret;
  }
  
  void clear(
    omap_root_t &omap_root,
    Transaction &t) {
    omap_manager->omap_clear(omap_root, t).unsafe_get0();
    EXPECT_EQ(omap_root.omap_root_laddr, L_ADDR_NULL);
  }

  void check_mappings(omap_root_t &omap_root, Transaction &t) {
    for (const auto &i: test_omap_mappings){
      auto ret = get_value(omap_root, t, i.first);
      EXPECT_EQ(i.first, ret.first);
      EXPECT_EQ(i.second, ret.second);
    }
  }

  void check_mappings(omap_root_t &omap_root) {
    auto t = tm.create_transaction();
    check_mappings(omap_root, *t);
  }

};

char* rand_string(char* str, const int len)
{
  int i;
  for (i = 0; i < len; ++i) {
    switch (rand() % 3) {
      case 1:
        str[i] = 'A' + rand() % 26;
        break;
      case 2:
        str[i] = 'a' +rand() % 26;
        break;
      case 0:
        str[i] = '0' + rand() % 10;
        break;
    }
  }
  str[++i] = '\0';
  return str;
}
/*
TEST_F(omap_manager_test_t, basic)    //worked
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }

    string key = "owner";
    string val = "test";
    {
      auto t = tm.create_transaction();
      logger().debug("first transaction");
      [[maybe_unused]] auto setret = set_key(omap_root, *t, key, val);
      [[maybe_unused]] auto getret = get_value(omap_root, *t, key);
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    {
      auto t = tm.create_transaction();
      logger().debug("second transaction");
      [[maybe_unused]] auto getret = get_value(omap_root, *t, key);
      [[maybe_unused]] auto rmret = rm_key(omap_root, *t, key);
      [[maybe_unused]] auto getret2 = get_value(omap_root, *t, key);
      EXPECT_EQ(getret2.second, "");
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    {
      auto t = tm.create_transaction();
      logger().debug("third transaction");
      [[maybe_unused]] auto getret = get_value(omap_root, *t, key);
      EXPECT_EQ(getret.second, "");
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
  });
}
TEST_F(omap_manager_test_t, force_split)  //worked for leaf node
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 50;
    char str[STR_LEN + 1];
    for (unsigned i = 0; i < 40; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 10; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 20 == 0) && (j == 5)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("force split submit transaction i = {}", i);
      tm.submit_transaction(std::move(t)).unsafe_get();
      check_mappings(omap_root);
    }
  });
}


TEST_F(omap_manager_test_t, force_split_merge_fullandbalance) //worked for leaf node
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 50;
    char str[STR_LEN + 1];

    for (unsigned i = 0; i < 80; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
        check_mappings(omap_root);
      }
    }
    auto t = tm.create_transaction();
    int i = 0;
    for (auto &e: test_omap_mappings) {
      if (i % 3 != 0) {
        [[maybe_unused]] auto rmref= rm_key(omap_root, *t, e.first);
      }

      if (i % 10 == 0) {
        logger().debug("submitting transaction i= {}", i);
        tm.submit_transaction(std::move(t)).unsafe_get();
        t = tm.create_transaction();
      }
      if (i % 100 == 0) {
        logger().debug("check_mappings  i= {}", i);
        check_mappings(omap_root, *t);
        check_mappings(omap_root);
      }
      i++;
    }
    logger().debug("finally submitting transaction ");
    tm.submit_transaction(std::move(t)).unsafe_get();
  });
}

TEST_F(omap_manager_test_t, force_split_merge_fullandbalanced) //worked for leaf node
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 50;
    char str[STR_LEN + 1];

    for (unsigned i = 0; i < 50; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
        check_mappings(omap_root);
      }
    }
    auto t = tm.create_transaction();
    int i = 0;
    for (auto &e: test_omap_mappings) {
      if (30 < i && i < 100) {
        auto val = e;
        [[maybe_unused]] auto rmref= rm_key(omap_root, *t, e.first);
      }

      if (i % 10 == 0) {
      logger().debug("submitting transaction i= {}", i);
        tm.submit_transaction(std::move(t)).unsafe_get();
        t = tm.create_transaction();
      }
      if (i % 50 == 0) {
      logger().debug("check_mappings  i= {}", i);
        check_mappings(omap_root, *t);
        check_mappings(omap_root);
      }
      i++;
      if (i == 100)
 break;
    }
    logger().debug("finally submitting transaction ");
    tm.submit_transaction(std::move(t)).unsafe_get();
    check_mappings(omap_root);
  });
}

TEST_F(omap_manager_test_t, force_split_listkeys_list_clear) //worked
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 50;
    char str[STR_LEN + 1];
    for (unsigned i = 0; i < 40; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 10; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 20 == 0) && (j == 5)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("force split submit transaction i = {}", i);
      tm.submit_transaction(std::move(t)).unsafe_get();
      check_mappings(omap_root);
    }
    auto t = tm.create_transaction();
    [[maybe_unused]] auto keyv = list_keys(omap_root, *t);
     tm.submit_transaction(std::move(t)).unsafe_get();

     t = tm.create_transaction();
    [[maybe_unused]] auto ls = list(omap_root, *t);
    tm.submit_transaction(std::move(t)).unsafe_get();

    t = tm.create_transaction();
    clear(omap_root, *t);
    tm.submit_transaction(std::move(t)).unsafe_get();
  });
}

*/
TEST_F(omap_manager_test_t, force_split) //
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 300;
    char str[STR_LEN + 1];

    for (unsigned i = 0; i < 8; i++) {
      logger().debug("opened split transaction");
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret1 = list_keys(omap_root, *t);

      for (unsigned j = 0; j < 80; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 2 == 0) && (j % 50 == 0)) {
          check_mappings(omap_root, *t);
        }
      }
      [[maybe_unused]] auto ret2 = list_keys(omap_root, *t);
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    check_mappings(omap_root);
  });
}
