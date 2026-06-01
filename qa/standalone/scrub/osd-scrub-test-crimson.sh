#!/usr/bin/env bash
#
# Copyright (C) 2018 Red Hat <contact@redhat.com>
# Copyright (C) 2024 Red Hat <contact@redhat.com> - Crimson adaptation
#
# Author: David Zafman <dzafman@redhat.com>
# Crimson adaptation: Based on osd-scrub-test.sh
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/scrub/scrub-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7138" # git grep '\<7138\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # Crimson requires msgr2
    CEPH_ARGS+="--ms-bind-msgr2=true --ms-bind-msgr1=false "
    # Critical: Mark pools as crimson-compatible by default
    CEPH_ARGS+="--osd_pool_default_crimson=true "
    # Disable PG autoscale for crimson (not supported yet)
    CEPH_ARGS+="--osd_pool_default_pg_autoscale_mode=off "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        echo "-------------- Prepare Test $func -------------------"
        setup $dir || return 1
        echo "-------------- Run Test $func -----------------------"
        $func $dir || return 1
        echo "-------------- Teardown Test $func ------------------"
        teardown $dir || return 1
        echo "-------------- Complete Test $func ------------------"
    done
}

function dump_scrub_metrics() {
    local dir=$1
    local poolname=$2
    local poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')
    local pgid="${poolid}.0"
    
    echo "=========================================="
    echo "Dumping scrub metrics for PG $pgid"
    echo "=========================================="
    ceph pg $pgid scrub_metrics || echo "Failed to dump scrub metrics for $pgid"
}

function apply_crimson_config() {
    # Apply critical configurations that vstart.sh sets via config assimilate-conf
    # These are needed for proper Crimson operation and peering
    ceph config set mon mon_osd_reporter_subtree_level osd || return 1
    ceph config set mon mon_data_avail_warn 2 || return 1
    ceph config set mon mon_data_avail_crit 1 || return 1
    ceph config set mon mon_allow_pool_delete true || return 1
    ceph config set mon mon_allow_pool_size_one true || return 1
    ceph config set osd osd_scrub_load_threshold 2000 || return 1
    ceph config set osd osd_debug_op_order true || return 1
    ceph config set osd osd_debug_misdirected_ops true || return 1
}

function MANUAL_peering_check() {
    local dir=$1
    local poolname=test
    local OSDS=3

    echo "=========================================="
    echo "Starting MON..."
    echo "=========================================="
    run_mon $dir a --osd_pool_default_size=3 || return 1
    
    echo "=========================================="
    echo "Applying critical Crimson configurations..."
    echo "=========================================="
    apply_crimson_config || return 1
    
    echo "=========================================="
    echo "Starting MGR..."
    echo "=========================================="
    run_mgr $dir x --mgr_stats_period=1 || return 1
    
    # Crimson-specific OSD arguments
    local ceph_osd_args="--osd_objectstore=seastore "
    ceph_osd_args+="--osd-scrub-interval-randomize-ratio=0 --osd-deep-scrub-randomize-ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0 --osd_stats_update_period_not_scrubbing=3 "
    ceph_osd_args+="--osd_stats_update_period_scrubbing=2"
    
    echo "=========================================="
    echo "Starting $OSDS Crimson OSDs..."
    echo "=========================================="
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      echo "Starting OSD.$osd..."
      run_crimson_osd $dir $osd $ceph_osd_args || return 1
    done

    echo "=========================================="
    echo "Checking cluster status before pool creation..."
    echo "=========================================="
    ceph -s
    
    echo "=========================================="
    echo "Creating pool '$poolname' with 1 PG..."
    echo "=========================================="
    create_pool $poolname 1 1
    sleep 8
    ceph -s
    ceph pg dump pgs
    echo "=========================================="
    echo "Waiting for PGs to become clean (peering check)..."
    echo "=========================================="
    wait_for_clean || return 1
    
    echo "=========================================="
    echo "SUCCESS! PG peering completed successfully!"
    echo "=========================================="
    ceph -s
    ceph pg dump pgs
    
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')
    echo "Pool ID: $poolid"
    
    echo "=========================================="
    echo "Test stopped here for peering verification."
    echo "Press Ctrl+C to exit or the test will continue..."
    echo "=========================================="
    sleep 30
    
    return 0
}

function TEST_scrub_test() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    run_mon $dir a --osd_pool_default_size=3 || return 1
    apply_crimson_config || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1
    
    # Crimson-specific OSD arguments
    local ceph_osd_args="--osd_objectstore=seastore "
    ceph_osd_args+="--osd-scrub-interval-randomize-ratio=0 --osd-deep-scrub-randomize-ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0 --osd_stats_update_period_not_scrubbing=3 "
    ceph_osd_args+="--osd_stats_update_period_scrubbing=2"
    
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_crimson_osd $dir $osd $ceph_osd_args || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    local testdata_file=$(file_with_random_data 1032)
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $testdata_file || return 1
    done
    rm -f $testdata_file

    local primary=$(get_primary $poolname obj1)
    local otherosd=$(get_not_primary $poolname obj1)
    if [ "$otherosd" = "2" ];
    then
      local anotherosd="0"
    else
      local anotherosd="2"
    fi

    local corrupt_data_file=$(file_with_random_data 512)
    objectstore_tool $dir $anotherosd obj1 set-bytes $corrupt_data_file || return 1
    rm -f $corrupt_data_file

    local pgid="${poolid}.0"
    pg_deep_scrub "$pgid" || return 1

    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1
    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1

    ceph osd out $primary
    wait_for_clean || return 1

    pg_deep_scrub "$pgid" || return 1

    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "2" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -q -- +inconsistent || return 1

    ceph osd in $primary
    wait_for_clean || return 1

    repair "$pgid" || return 1
    wait_for_clean || return 1

    # This sets up the test after we've repaired with previous primary has old value
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "2" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1

    ceph osd out $primary
    wait_for_clean || return 1

    test "$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[0].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    test "$(ceph pg $pgid query | jq '.peer_info[1].stats.stat_sum.num_scrub_errors')" = "0" || return 1
    ceph pg dump pgs | grep ^${pgid} | grep -vq -- +inconsistent || return 1
    dump_scrub_metrics $dir $poolname
}

# Grab year-month-day
DATESED="s/\([0-9]*-[0-9]*-[0-9]*\).*/\1/"
DATEFORMAT="%Y-%m-%d"

function check_dump_scrubs() {
    local primary=$1
    local sched_time_check="$2"

    DS="$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) dump_scrubs)"
    # use eval to drop double-quotes
    eval SCHED_TIME=$(echo $DS | jq '.[0].sched_time')
    test $(echo $SCHED_TIME | sed $DATESED) = $(date +${DATEFORMAT} -d "now + $sched_time_check") || return 1
}

function TEST_interval_changes() {
    local poolname=test
    local OSDS=2
    local objects=10
    # Don't assume how internal defaults are set
    local day="$(expr 24 \* 60 \* 60)"
    local week="$(expr $day \* 7)"
    local min_interval=$day
    local max_interval=$week
    local WAIT_FOR_UPDATE=15

    TESTDATA="testdata.$$"

    # This min scrub interval results in 30 seconds backoff time
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    apply_crimson_config || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_crimson_osd $dir $osd --osd_objectstore=seastore --osd_scrub_min_interval=$min_interval --osd_scrub_max_interval=$max_interval --osd_scrub_interval_randomize_ratio=0 || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    local poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(get_primary $poolname obj1)

    # Check initial settings from above (min 1 day, min 1 week)
    check_dump_scrubs $primary "1 day" || return 1

    # Change global osd_scrub_min_interval to 2 days
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) config set osd_scrub_min_interval $(expr $day \* 2)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "2 days" || return 1

    # Change global osd_scrub_max_interval to 2 weeks
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) config set osd_scrub_max_interval $(expr $week \* 2)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "2 days" || return 1

    # Change pool osd_scrub_min_interval to 3 days
    ceph osd pool set $poolname scrub_min_interval $(expr $day \* 3)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "3 days" || return 1

    # Change pool osd_scrub_max_interval to 3 weeks
    ceph osd pool set $poolname scrub_max_interval $(expr $week \* 3)
    sleep $WAIT_FOR_UPDATE
    check_dump_scrubs $primary "3 days" || return 1
    dump_scrub_metrics $dir $poolname
}

function _scrub_abort() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=1000
    local type=$2

    TESTDATA="testdata.$$"
    if test $type = "scrub";
    then
      stopscrub="noscrub"
      check="noscrub"
    else
      stopscrub="nodeep-scrub"
      check="nodeep_scrub"
    fi

    run_mon $dir a --osd_pool_default_size=3 || return 1
    apply_crimson_config || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
        run_crimson_osd $dir $osd --osd_objectstore=seastore \
            --osd_pool_default_pg_autoscale_mode=off \
            --osd_deep_scrub_randomize_ratio=0.0 \
            --osd_scrub_sleep=5.0 \
            --osd_scrub_interval_randomize_ratio=0 \
            --debug|| return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local primary=$(get_primary $poolname obj1)
    local pgid="${poolid}.0"

    # Trigger scrub using pg command (not tell command)
    if [ "$type" = "scrub" ]; then
        ceph pg $pgid scrub || return 1
    else
        ceph pg $pgid deep-scrub || return 1
    fi

    # Wait for scrubbing to start
    set -o pipefail
    found="no"
    for i in $(seq 0 200)
    do
      flush_pg_stats
      if ceph pg dump pgs | grep  ^$pgid| grep -q "scrubbing"
      then
        found="yes"
        break
      fi
    done
    set +o pipefail

    if test $found = "no";
    then
      echo "Scrubbing never started"
      return 1
    fi

    ceph osd set $stopscrub
    if [ "$type" = "deep-scrub" ];
    then
      ceph osd set noscrub
    fi

    # Wait for scrubbing to end
    set -o pipefail
    for i in $(seq 0 200)
    do
      flush_pg_stats
      if ceph pg dump pgs | grep ^$pgid | grep -q "scrubbing"
      then
        continue
      fi
      break
    done
    set +o pipefail

    sleep 5

    if ! grep "$check set, aborting" $dir/osd.${primary}.log
    then
      echo "Abort not seen in log"
      return 1
    fi

    local last_scrub=$(get_last_scrub_stamp $pgid)
    ceph config set osd "osd_scrub_sleep" "0.1"

    ceph osd unset $stopscrub
    if [ "$type" = "deep-scrub" ];
    then
      ceph osd unset noscrub
    fi
    
    # Wait a bit for reservation cleanup to complete before triggering new scrub
    sleep 2
    
    # Trigger a new scrub after unsetting noscrub, it is different with classic, need check if support classic auto scrub
    if [ "$type" = "deep-scrub" ];
    then
        ceph pg $pgid deep-scrub || return 1
    else
        ceph pg $pgid scrub || return 1
    fi
    
    TIMEOUT=$(($objects / 2))
    wait_for_scrub $pgid "$last_scrub" || return 1
    dump_scrub_metrics $dir $poolname
}

function TEST_scrub_abort() {
    local dir=$1
    _scrub_abort $dir scrub
}

function TEST_deep_scrub_abort() {
    local dir=$1
    _scrub_abort $dir deep-scrub
}

function TEST_scrub_permit_time() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15

    TESTDATA="testdata.$$"

    run_mon $dir a --osd_pool_default_size=3 || return 1
    apply_crimson_config || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1
    local scrub_begin_hour=$(date -d '2 hour ago' +"%H" | sed 's/^0//')
    local scrub_end_hour=$(date -d '1 hour ago' +"%H" | sed 's/^0//')
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_crimson_osd $dir $osd --osd_objectstore=seastore \
	                --osd_deep_scrub_randomize_ratio=0.0 \
	                --osd_scrub_interval_randomize_ratio=0 \
                        --osd_scrub_begin_hour=$scrub_begin_hour \
                        --osd_scrub_end_hour=$scrub_end_hour || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1

    # Trigger a scrub on a PG
    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    local last_scrub=$(get_last_scrub_stamp $pgid)
    # If we don't specify an amount of time to subtract from
    # current time to set last_scrub_stamp, it sets the deadline
    # back by osd_max_interval which would cause the time permit checking
    # to be skipped.  Set back 1 day, the default scrub_min_interval.
    # Note: Crimson uses 'ceph pg' command instead of 'ceph tell' for scrub scheduling
    ceph pg $pgid schedule-scrub $(( 24 * 60 * 60 )) || return 1

    # Scrub should not run
    for ((i=0; i < 30; i++)); do
        if test "$(get_last_scrub_stamp $pgid)" '>' "$last_scrub" ; then
            return 1
        fi
        sleep 1
    done
    dump_scrub_metrics $dir $poolname
}

function TEST_pg_dump_objects_scrubbed() {
    local dir=$1
    local poolname=test
    local OSDS=3
    local objects=15
    local timeout=10

    TESTDATA="testdata.$$"

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    apply_crimson_config || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_crimson_osd $dir $osd --osd_objectstore=seastore --debug || return 1
    done

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $objects`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done
    rm -f $TESTDATA

    local pgid="${poolid}.0"
    #Trigger a scrub on a PG
    pg_scrub $pgid || return 1
    test "$(ceph pg $pgid query | jq '.info.stats.objects_scrubbed')" '=' $objects || return 1
    dump_scrub_metrics $dir $poolname

    teardown $dir || return 1
}

# Note: Some tests from the original osd-scrub-test.sh may not be fully compatible
# with Crimson yet. Tests that rely on features not yet implemented in Crimson
# have been omitted or will need adaptation.

main osd-scrub-test-crimson "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-test-crimson.sh"
# End: