// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/osd/pg.h"
#include "crimson/osd/pg_activation_blocker.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

void PGActivationBlocker::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg->get_pgid();
}

void PGActivationBlocker::unblock()
{
  std::cout<<"-------PGActivationBlocker::unblock------pg = "<<pg->get_pgid()<<std::endl;
  logger().debug("-------PGActivationBlocker::unblock------pg = {}", pg->get_pgid());
  p.set_value();
  p = {};
}

seastar::future<>
PGActivationBlocker::wait(PGActivationBlocker::BlockingEvent::TriggerI&& trigger)
{
  if (pg->get_peering_state().is_active()) {
    return seastar::now();
  } else {
  std::cout<<"-------PGActivationBlocker::wait------pg = "<<pg->get_pgid()<<std::endl;
  logger().debug("-------PGActivationBlocker::wait------pg = {}", pg->get_pgid());
    return trigger.maybe_record_blocking(p.get_shared_future(), *this);
  }
}

seastar::future<> PGActivationBlocker::stop()
{
  p.set_exception(crimson::common::system_shutdown_exception());
  return seastar::now();
}

} // namespace crimson::osd
