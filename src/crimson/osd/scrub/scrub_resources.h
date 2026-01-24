// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <functional>
#include <string>

#include "common/Formatter.h"
#include "osd/osd_types.h"

namespace crimson::osd::scrub {

class LocalResourceWrapper;
/**
 * The number of concurrent scrub operations performed on an OSD is limited
 * by a configuration parameter. The 'ScrubResources' class is responsible for
 * maintaining a count of the number of scrubs currently performed by primary
 * PGs on this OSD, and for enforcing the limit.
 */
class ShardScrubResources {
  friend class LocalResourceWrapper;
  /**
   * the number of concurrent scrubs performed by Primaries on this OSD.
   *
   * Note that, as high priority scrubs are always allowed to proceed, this
   * counter may exceed the configured limit. When in this state - no new
   * regular scrubs will be allowed to start.
   */
  int shard_scrubs_local{0};

 public:
   ShardScrubResources() = default;


  /**
   * \returns true if the number of concurrent scrubs is
   *  below osd_max_scrubs pershard, false otherwise
   */
  bool can_inc_scrubs() const;

  /// increments the number of scrubs acting as a Primary
  std::unique_ptr<LocalResourceWrapper> inc_scrubs_local(bool is_high_priority);

  /// decrements the number of scrubs acting as a Primary
  void dec_scrubs_local();

  void dump_scrub_reservations(ceph::Formatter* f) const;
};

/**
 * a wrapper around a "local scrub resource". The resources bookkeeper
 * is handing these out to the PGs that acquired the local OSD's scrub
 * resources. The PGs use these to release the resources when they are
 * done scrubbing.
 */
class LocalResourceWrapper {
  ShardScrubResources& m_resource_bookkeeper;

 public:
  LocalResourceWrapper(
      ShardScrubResources& resource_bookkeeper);
  ~LocalResourceWrapper();
};

}  // namespace Scrub
