// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once
#include "include/utime.h"

namespace crimson::osd::scrub {
enum class scrub_prio_t : bool { low_priority = false, high_priority = true };

struct scrub_schedule_t {
  /**
   * the time at which we are allowed to start the scrub. Never
   * decreasing after 'scheduled_at' is set.
   */
  utime_t not_before{utime_t::max()};

  /**
   * the 'scheduled_at' is the time at which we intended the scrub to be scheduled.
   * For periodic (regular) scrubs, it is set to the time of the last scrub
   * plus the scrub interval (plus some randomization). Priority scrubs
   * have their own specific rules for the target time. E.g.:
   * - for operator-initiated scrubs: 'target' is set to 'scrub_must_stamp';
   * - same for re-scrubbing (deep scrub after a shallow scrub that ended with
   *   errors;
   * - when requesting a scrub after a repair (the highest priority scrub):
   *   the target is set to '0' (beginning of time);
   */
  utime_t scheduled_at{utime_t::max()};

  std::partial_ordering operator<=>(const scrub_schedule_t& rhs) const
  {
    // when compared - the 'not_before' is ignored, assuming
    // we never compare jobs with different eligibility status.
    return scheduled_at <=> rhs.scheduled_at;
  };

  bool operator==(const scrub_schedule_t& rhs) const = default;
};

enum class delay_cause_t {
  none,		    ///< scrub attempt was successful
  replicas,	    ///< failed to reserve replicas
  flags,	    ///< noscrub or nodeep-scrub
  pg_state,	    ///< not active+clean
  snap_trimming,    ///< snap-trimming is in progress
  restricted_time,  ///< time restrictions or busy CPU
  local_resources,  ///< too many scrubbing PGs
  aborted,	    ///< scrub was aborted w/ unspecified reason
  interval,	    ///< the interval had ended mid-scrub
  scrub_params,     ///< the specific scrub type is not allowed
};

struct OSDRestrictions {
  /// high local OSD concurrency. Thus - only high priority scrubs are allowed
  bool max_concurrency_reached{false};

  /// rolled a dice, and decided not to scrub in this tick
  bool random_backoff_active{false};

  /// the CPU load is high. No regular scrubs are allowed.
  bool cpu_overloaded:1{false};

  /// outside of allowed scrubbing hours/days
  bool restricted_time:1{false};

  /// the OSD is performing a recovery & osd_scrub_during_recovery is 'false'
  bool recovery_in_progress:1{false};
};
static_assert(sizeof(OSDRestrictions) <= sizeof(uint32_t));

/// concise passing of PG state affecting scrub to the
/// scrubber at the initiation of a scrub
struct ScrubPGPreconds {
  bool allow_shallow{true};
  bool allow_deep{true};
  bool can_autorepair{false};
};
static_assert(sizeof(ScrubPGPreconds) <= sizeof(uint32_t));

/// possible outcome when trying to select a PG and scrub it
enum class schedule_result_t {
  scrub_initiated,	    // successfully started a scrub
  target_specific_failure,  // failed to scrub this specific target
  osd_wide_failure	    // failed to scrub any target
};
inline utime_t scrub_must_stamp() { return utime_t(1, 1); }

} // namespace crimson::osd::scrub
#include <fmt/format.h>
namespace fmt {
template <>
struct formatter<crimson::osd::scrub::delay_cause_t> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(crimson::osd::scrub::delay_cause_t c,
              FormatContext& ctx) const {
    using T = crimson::osd::scrub::delay_cause_t;

    switch (c) {
      case T::none:             return format_to(ctx.out(), "none");
      case T::replicas:         return format_to(ctx.out(), "replicas");
      case T::flags:            return format_to(ctx.out(), "flags");
      case T::pg_state:         return format_to(ctx.out(), "pg_state");
      case T::snap_trimming:    return format_to(ctx.out(), "snap_trimming");
      case T::restricted_time:  return format_to(ctx.out(), "restricted_time");
      case T::local_resources:  return format_to(ctx.out(), "local_resources");
      case T::aborted:          return format_to(ctx.out(), "aborted");
      case T::interval:         return format_to(ctx.out(), "interval");
      case T::scrub_params:     return format_to(ctx.out(), "scrub_params");
    }

    return format_to(ctx.out(), "unknown");
  }
};

template <>
struct formatter<crimson::osd::scrub::OSDRestrictions> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const crimson::osd::scrub::OSDRestrictions& r, FormatContext& ctx) const {
    return format_to(
        ctx.out(),
        "restrictions[conc:{} rand:{} cpu:{} time:{} recov:{}]",
        r.max_concurrency_reached,
        r.random_backoff_active,
        r.cpu_overloaded,
        r.restricted_time,
        r.recovery_in_progress);
  }
};

template <>
struct formatter<crimson::osd::scrub::scrub_schedule_t> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const crimson::osd::scrub::scrub_schedule_t& s,
              FormatContext& ctx) const {

    const bool nb_is_max = (s.not_before == utime_t::max());
    const bool sa_is_max = (s.scheduled_at == utime_t::max());

    if (sa_is_max) {
      return format_to(ctx.out(), "sched[unscheduled]");
    }

    if (nb_is_max || s.not_before <= s.scheduled_at) {
      return format_to(ctx.out(),
                       "sched[at:{}]",
                       s.scheduled_at);
    }

    return format_to(ctx.out(),
                     "sched[at:{} nb:{}]",
                     s.scheduled_at,
                     s.not_before);
  }
};
}
