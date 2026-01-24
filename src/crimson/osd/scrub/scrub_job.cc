// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "./scrub_job.h"

#include "pg_scrubber.h"
#include "crimson/common/log.h"
#include "scrubber_common.h"

SET_SUBSYS(osd);

using must_scrub_t = crimson::osd::scrub::must_scrub_t;
using sched_params_t = crimson::osd::scrub::sched_params_t;
using OSDRestrictions = crimson::osd::scrub::OSDRestrictions;
using sched_conf_t = crimson::osd::scrub::sched_conf_t;
using scrub_schedule_t = crimson::osd::scrub::scrub_schedule_t;
using ScrubJob = crimson::osd::scrub::ScrubJob;
using namespace std::chrono;

using SchedEntry = crimson::osd::scrub::SchedEntry;

// ////////////////////////////////////////////////////////////////////////// //
// SchedTarget

using SchedTarget = crimson::osd::scrub::SchedTarget;

void SchedTarget::reset()
{
  // a bit convoluted, but the standard way to guarantee we keep the
  // same set of member defaults as the constructor
  *this = SchedTarget{sched_info.pgid, sched_info.level};
}

void SchedTarget::up_urgency_to(urgency_t u)
{
  sched_info.urgency = std::max(sched_info.urgency, u);
}


// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

ScrubJob::ScrubJob(const spg_t& pg, int node_id)
    : pgid{pg}
    , whoami{node_id}
    , shallow_target{pg, scrub_level_t::shallow}
    , deep_target{pg, scrub_level_t::deep}
    , random_gen{random_dev()}
{}

SchedTarget& ScrubJob::get_target(scrub_level_t s_or_d)
{
  return (s_or_d == scrub_level_t::deep) ? deep_target : shallow_target;
}


bool ScrubJob::is_queued() const
{
  return shallow_target.queued || deep_target.queued;
}


void ScrubJob::clear_both_targets_queued()
{
  shallow_target.queued = false;
  deep_target.queued = false;
}


void ScrubJob::set_both_targets_queued()
{
  shallow_target.queued = true;
  deep_target.queued = true;
}


void ScrubJob::adjust_shallow_schedule(
    utime_t last_scrub,
    const sched_conf_t& app_conf,
    utime_t scrub_clock_now)
{
  LOG_PREFIX(ScrubJob::adjust_shallow_schedule);
  DEBUG(
    "at entry: shallow target:{}, conf:{}, last-stamp:{}",
    shallow_target, app_conf, last_scrub);

  auto& sh_times = shallow_target.sched_info.schedule;	// shorthand

  if (ScrubJob::requires_randomization(shallow_target.urgency())) {
    utime_t adj_not_before = last_scrub;
    utime_t adj_target = last_scrub;

    // add a random delay to the proposed scheduled time
    adj_target += app_conf.shallow_interval;
    double r = rand() / (double)RAND_MAX;
    adj_target +=
	app_conf.shallow_interval * app_conf.interval_randomize_ratio * r;

    if (adj_not_before < adj_target) {
      adj_not_before = adj_target;
    }
    sh_times.scheduled_at = adj_target;
    sh_times.not_before = adj_not_before;
    sh_times.scheduled_at = scrub_clock_now;
    sh_times.not_before = scrub_clock_now;
  } else {

    // the target time is already set. Make sure to reset the n.b.
    sh_times.not_before = sh_times.scheduled_at;
    sh_times.not_before = scrub_clock_now;
  }

  DEBUG(
    "adjusted: shallow target:{}, scheduled_at:{}, not_before:{}",
      shallow_target, sh_times.scheduled_at, sh_times.not_before);
}


double ScrubJob::guaranteed_offset(
    scrub_level_t s_or_d,
    const sched_conf_t& app_conf)
{
  if (s_or_d == scrub_level_t::deep) {
    // use the sdv of the deep scrub distribution, times 3 (3-sigma...)
    const double sdv = app_conf.deep_interval * app_conf.deep_randomize_ratio;
  // note: the '+10.0' is there just to guarantee inequality if '._ratio' is 0
    return app_conf.deep_interval + abs(3 * sdv) + 10.0;
  }

  // shallow scrub
  return app_conf.shallow_interval * (2.0 + app_conf.interval_randomize_ratio);
}


void ScrubJob::operator_forced(scrub_level_t s_or_d, scrub_type_t scrub_type)
{
  auto& trgt = get_target(s_or_d);
  trgt.up_urgency_to(
      (scrub_type == scrub_type_t::do_repair) ? urgency_t::must_repair
					      : urgency_t::operator_requested);
  trgt.sched_info.schedule.scheduled_at = scrub_must_stamp();
  trgt.sched_info.schedule.not_before = scrub_must_stamp();
}


std::optional<std::reference_wrapper<SchedTarget>> ScrubJob::earliest_eligible(
    utime_t scrub_clock_now)
{
  std::weak_ordering compr = cmp_entries(
      scrub_clock_now, shallow_target.queued_element(),
      deep_target.queued_element());

  auto poss_ret = (compr == std::weak_ordering::less)
		      ? std::ref<SchedTarget>(shallow_target)
		      : std::ref<SchedTarget>(deep_target);
  if (poss_ret.get().sched_info.schedule.not_before <= scrub_clock_now) {
    return poss_ret;
  }
  return std::nullopt;
}

std::optional<std::reference_wrapper<const SchedTarget>>
ScrubJob::earliest_eligible(utime_t scrub_clock_now) const
{
  std::weak_ordering compr = cmp_entries(
      scrub_clock_now, shallow_target.queued_element(),
      deep_target.queued_element());

  auto poss_ret = (compr == std::weak_ordering::less)
		      ? std::cref<SchedTarget>(shallow_target)
		      : std::cref<SchedTarget>(deep_target);
  if (poss_ret.get().sched_info.schedule.not_before <= scrub_clock_now) {
    return poss_ret;
  }
  return std::nullopt;
}


SchedTarget& ScrubJob::earliest_target()
{
  std::weak_ordering compr = cmp_future_entries(
      shallow_target.queued_element(), deep_target.queued_element());
  return (compr == std::weak_ordering::less) ? shallow_target : deep_target;
}

const SchedTarget& ScrubJob::earliest_target() const
{
  std::weak_ordering compr = cmp_future_entries(
      shallow_target.queued_element(), deep_target.queued_element());
  return (compr == std::weak_ordering::less) ? shallow_target : deep_target;
}


SchedTarget& ScrubJob::earliest_target(utime_t scrub_clock_now)
{
  std::weak_ordering compr = cmp_entries(scrub_clock_now,
      shallow_target.queued_element(), deep_target.queued_element());
  return (compr == std::weak_ordering::less) ? shallow_target : deep_target;
}

const SchedTarget& ScrubJob::earliest_target(utime_t scrub_clock_now) const
{
  std::weak_ordering compr = cmp_entries(scrub_clock_now,
      shallow_target.queued_element(), deep_target.queued_element());
  return (compr == std::weak_ordering::less) ? shallow_target : deep_target;
}


utime_t ScrubJob::get_sched_time() const
{
  return earliest_target().sched_info.schedule.not_before;
}


void ScrubJob::adjust_deep_schedule(
    utime_t last_deep,
    const sched_conf_t& app_conf,
    utime_t scrub_clock_now)
{
  LOG_PREFIX(ScrubJob::adjust_deep_schedule);
  DEBUG(
    "at entry: deep target:{}, conf:{}, last-stamp:{:s}", deep_target,
    app_conf, last_deep);

  auto& dp_times = deep_target.sched_info.schedule;  // shorthand

  if (ScrubJob::requires_randomization(deep_target.urgency())) {
    utime_t adj_target = last_deep;

    // add a random delay to the proposed scheduled time
    const double sdv = app_conf.deep_interval * app_conf.deep_randomize_ratio;
    std::normal_distribution<double> normal_dist{app_conf.deep_interval, sdv};
    auto next_delay = std::clamp(
	normal_dist(random_gen), app_conf.deep_interval - 2 * sdv,
	app_conf.deep_interval + 2 * sdv);
    adj_target += next_delay;
    DEBUG("randomized deep scrub delay: {}s", next_delay);

    dp_times.scheduled_at = adj_target;
    dp_times.not_before = adj_target;
  } else {
    // the target time is already set. The n.b. is set to same
    dp_times.not_before = dp_times.scheduled_at;
  }

  DEBUG(
    "adjusted: deep target:{}, scheduled_at:{:s}, not_before:{:s}", deep_target,
    dp_times.scheduled_at,  dp_times.not_before);
}


SchedTarget& ScrubJob::delay_on_failure(
    scrub_level_t level,
    delay_cause_t delay_cause,
    utime_t scrub_clock_now)
{
  LOG_PREFIX(ScrubJob::delay_on_failure);
  seconds delay = seconds(crimson::common::local_conf().get_val<int64_t>("osd_scrub_retry_delay"));
  switch (delay_cause) {
    case delay_cause_t::flags:
      delay =
	  seconds(crimson::common::local_conf().get_val<int64_t>("osd_scrub_retry_after_noscrub"));
      break;
    case delay_cause_t::pg_state:
      delay = seconds(crimson::common::local_conf().get_val<int64_t>("osd_scrub_retry_pg_state"));
      break;
    case delay_cause_t::snap_trimming:
      delay = seconds(crimson::common::local_conf().get_val<int64_t>("osd_scrub_retry_trimming"));
      break;
    case delay_cause_t::interval:
      delay = seconds(crimson::common::local_conf().get_val<int64_t>("osd_scrub_retry_new_interval"));
      break;
    case delay_cause_t::local_resources:
    case delay_cause_t::aborted:
    default:
      // for all other possible delay causes: use the default delay
      break;
  }

  auto& delayed_target =
      (level == scrub_level_t::deep) ? deep_target : shallow_target;
  delayed_target.sched_info.schedule.not_before =
      std::max(scrub_clock_now, delayed_target.sched_info.schedule.not_before) +
      utime_t{delay};
  delayed_target.sched_info.last_issue = delay_cause;
  DEBUG(
    "delayed {} scrub target by {}s due to {}, new not_before:{:s}",
    (level == scrub_level_t::deep ? "deep" : "shallow"), delay.count(),
    static_cast<int>(delay_cause),
    delayed_target.sched_info.schedule.not_before);
  return delayed_target;
}


std::string ScrubJob::scheduling_state(utime_t now_is) const
{
  // if not registered, not a candidate for scrubbing on this OSD (or at all)
  if (!registered) {
    return "not registered for scrubbing";
  }
  if (!is_queued()) {
    // if not currently queued - we are being scrubbed
    return "scrubbing";
  }

  const auto first_ready = earliest_eligible(now_is);
  if (first_ready) {
    // the target is ready to be scrubbed
    return fmt::format(
	"queued for {}scrub at {:s}",
	(first_ready->get().is_deep() ? "deep " : ""),
	first_ready->get().sched_info.schedule.scheduled_at);
  } else {
    // both targets are in the future
    const auto& nearest = earliest_target();
    return fmt::format(
	"{}scrub scheduled @ {:s} ({:s})", (nearest.is_deep() ? "deep " : ""),
	nearest.sched_info.schedule.not_before,
	nearest.sched_info.schedule.scheduled_at);
  }
}


void ScrubJob::dump(ceph::Formatter* f) const
{
  const auto& entry = earliest_target().sched_info;
  const auto& sch = entry.schedule;
  Formatter::ObjectSection scrubjob_section{*f, "scrub"};
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << get_sched_time();
  f->dump_stream("orig_sched_time") << sch.scheduled_at;
  f->dump_bool("forced", entry.urgency >= urgency_t::operator_requested);
}

// a set of static functions to determine, given a scheduling target's urgency,
// what restrictions apply to that target (and what exemptions it has).

bool ScrubJob::observes_noscrub_flags(urgency_t urgency)
{
  return urgency < urgency_t::after_repair;
}

bool ScrubJob::observes_allowed_hours(urgency_t urgency)
{
  return urgency < urgency_t::operator_requested;
}

bool ScrubJob::observes_extended_sleep(urgency_t urgency)
{
  return urgency == urgency_t::periodic_regular;
}

bool ScrubJob::observes_load_limit(urgency_t urgency)
{
  return urgency < urgency_t::after_repair;
}

bool ScrubJob::requires_reservation(urgency_t urgency)
{
  return urgency < urgency_t::after_repair;
}

bool ScrubJob::requires_randomization(urgency_t urgency)
{
  return urgency == urgency_t::periodic_regular;
}

bool ScrubJob::observes_max_concurrency(urgency_t urgency)
{
  return urgency < urgency_t::operator_requested;
}

bool ScrubJob::observes_random_backoff(urgency_t urgency)
{
  return urgency < urgency_t::after_repair;
}

bool ScrubJob::observes_recovery(urgency_t urgency)
{
  return urgency < urgency_t::operator_requested;
}

bool ScrubJob::has_high_queue_priority(urgency_t urgency)
{
  return urgency >= urgency_t::operator_requested;
}

bool ScrubJob::is_repair_implied(urgency_t urgency)
{
  return urgency == urgency_t::repairing || urgency == urgency_t::must_repair;
}

bool ScrubJob::is_autorepair_allowed(urgency_t urgency)
{
  // note: 'after-repair' scrubs are not allowed to auto-repair
  return urgency == urgency_t::periodic_regular ||
	 urgency == urgency_t::operator_requested ||
	 urgency == urgency_t::repairing || urgency == urgency_t::must_repair;
}

bool ScrubJob::is_repairs_count_limited(urgency_t urgency)
{
  return urgency < urgency_t::operator_requested;
}
