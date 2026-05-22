// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "osd/osd_types.h"

#include "osd/scrubber/scrub_resources.h"
#include "scrub_queue.h"

#include <map>

namespace crimson::common {
class CephContext;
class PerfCounters;
}

namespace crimson::osd {
class ShardServices;

static constexpr int SCRUB_TICK_INTERVAL = 1; // seconds between scrub scheduler ticks

class ScrubScheduler {
  ShardServices &shard_services;
  /// resource reservation management
  scrub::ScrubResources m_resource_bookkeeper;
  /// the queue of PGs waiting to be scrubbed
  scrub::ScrubQueue m_queue;

  // Performance counters infrastructure (matching classic OSD)
  using pc_index_t = std::pair<scrub_level_t, int /*pool type*/>;
  std::map<pc_index_t, crimson::common::PerfCounters*> m_perf_counters;

  void create_scrub_perf_counters();
  void destroy_scrub_perf_counters();

  seastar::future<scrub::OSDRestrictions> restrictions_on_scrubbing(
    bool is_recovery_active,
    utime_t scrub_clock_now) const;
  seastar::future<scrub::schedule_result_t> initiate_a_scrub(
    const scrub::SchedEntry& candidate,
    scrub::OSDRestrictions restrictions);
  bool scrub_random_backoff() const;
  bool scrub_time_permit(utime_t now) const;
  bool scrub_load_below_threshold() const;

public:
  ScrubScheduler(ShardServices &shard_services);
  ~ScrubScheduler();

  seastar::future<> initiate_scrub(bool is_recovery_active);
  void enqueue_scrub_job(const scrub::ScrubJob& sjob);
  void enqueue_target(const scrub::SchedTarget& trgt);
  void dequeue_target(spg_t pgid, scrub_level_t s_or_d);
  void remove_from_osd_queue(spg_t pgid);

  static bool is_sched_target_eligible(
    const scrub::SchedEntry& e,
    const scrub::OSDRestrictions& r,
    utime_t time_now);
    // updating the resource counters
  std::unique_ptr<scrub::LocalResourceWrapper> inc_scrubs_local(
      bool is_high_priority, int scrubs_total);
  void dec_scrubs_local();
  int get_scrubs_local() const;

  scrub::ScrubQueue& get_queue() {
    return m_queue;
  }

  /// Get performance counters for a specific pool type and scrub level
  /// Matches the classic OSD OsdScrub::get_perf_counters interface
  crimson::common::PerfCounters* get_perf_counters(int pool_type, scrub_level_t level);
};
} // namespace crimson::osd
