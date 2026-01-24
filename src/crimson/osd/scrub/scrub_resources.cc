// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "./scrub_resources.h"

#include "include/ceph_assert.h"
#include "osd/osd_types_fmt.h"
#include "crimson/common/log.h"

SET_SUBSYS(osd);
namespace crimson::osd::scrub {
using ShardScrubResources = crimson::osd::scrub::ShardScrubResources;
using LocalResourceWrapper = crimson::osd::scrub::LocalResourceWrapper;


// ------------------------- scrubbing as primary on this OSD -----------------

// can we increase the number of concurrent scrubs performed by Primaries
// on this OSD? note that counted separately from the number of scrubs
// performed by replicas.
bool ShardScrubResources::can_inc_scrubs() const
{
  LOG_PREFIX(ShardScrubResources::can_inc_scrubs);
  if (shard_scrubs_local < crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs")/seastar::smp::count) {
    return true;
  }
  DEBUG(
      "Cannot add local scrubs. Current counter ({}) >= max ({})", "",
      shard_scrubs_local,
      crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs")/seastar::smp::count);
  return false;
}

std::unique_ptr<LocalResourceWrapper> ShardScrubResources::inc_scrubs_local(
    bool is_high_priority)
{
  LOG_PREFIX(ShardScrubResources::inc_scrubs_local);
  if (is_high_priority || can_inc_scrubs()) {
    ++shard_scrubs_local;
    DEBUG(
        "{} -> {} (max {})", "", (shard_scrubs_local - 1),
        shard_scrubs_local,
        crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs")/seastar::smp::count);
    return std::make_unique<LocalResourceWrapper>(*this);
  }
  return nullptr;
}


void ShardScrubResources::dec_scrubs_local()
{
  LOG_PREFIX(ShardScrubResources::dec_scrubs_local);
  
  DEBUG(
      "{} -> {} (max {})", "", shard_scrubs_local,
      (shard_scrubs_local - 1),
      crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs")/seastar::smp::count);
  --shard_scrubs_local;
  ceph_assert(shard_scrubs_local >= 0);
}


void ShardScrubResources::dump_scrub_reservations(ceph::Formatter* f) const
{
  f->dump_int("shard_scrubs_local", shard_scrubs_local);
  f->dump_int("osd_max_scrubs", crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs"));
}

// --------------- LocalResourceWrapper

LocalResourceWrapper::LocalResourceWrapper(
    ShardScrubResources& resource_bookkeeper)
    : m_resource_bookkeeper{resource_bookkeeper}
{}

LocalResourceWrapper::~LocalResourceWrapper()
{
  m_resource_bookkeeper.dec_scrubs_local();
}

}  // namespace crimson::osd::scrub