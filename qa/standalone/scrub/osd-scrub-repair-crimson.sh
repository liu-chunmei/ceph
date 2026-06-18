#!/usr/bin/env bash
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
# Copyright (C) 2024 Red Hat <contact@redhat.com> - Crimson adaptation
#
# Author: Loic Dachary <loic@dachary.org>
# Crimson adaptation: Based on osd-scrub-repair.sh
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
set -x
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/scrub/scrub-helpers.sh

if [ `uname` = FreeBSD ]; then
    # erasure coding overwrites are only tested on Bluestore
    # erasure coding on filestore is unsafe
    # http://docs.ceph.com/en/latest/rados/operations/erasure-code/#erasure-coding-with-overwrites
    use_ec_overwrite=false
else
    use_ec_overwrite=true
fi

# Test development and debugging
# Set to "yes" in order to ignore diff errors and save results to update test
getjson="no"

# Filter out mtime and local_mtime dates, version, prior_version and last_reqid (client) from any object_info.
jqfilter='def walk(f):
  . as $in
  | if type == "object" then
      reduce keys[] as $key
        ( {}; . + { ($key):  ($in[$key] | walk(f)) } ) | f
    elif type == "array" then map( walk(f) ) | f
    else f
    end;
walk(if type == "object" then del(.mtime) else . end)
| walk(if type == "object" then del(.local_mtime) else . end)
| walk(if type == "object" then del(.last_reqid) else . end)
| walk(if type == "object" then del(.version) else . end)
| walk(if type == "object" then del(.prior_version) else . end)'

sortkeys='import json; import sys ; JSON=sys.stdin.read() ; ud = json.loads(JSON) ; print(json.dumps(ud, sort_keys=True, indent=2))'

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7147" # git grep '\<7147\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd-skip-data-digest=false "
    # Crimson requires msgr2
    CEPH_ARGS+="--ms-bind-msgr2=true --ms-bind-msgr1=false "
    # Critical: Mark pools as crimson-compatible by default
    CEPH_ARGS+="--osd_pool_default_crimson=true "
    # Disable PG autoscale for crimson (not supported yet)
    CEPH_ARGS+="--osd_pool_default_pg_autoscale_mode=off "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
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

function add_something() {
    local dir=$1
    local poolname=$2
    local obj=${3:-SOMETHING}
    local scrub=${4:-noscrub}

    if [ "$scrub" = "noscrub" ];
    then
        ceph osd set noscrub || return 1
        ceph osd set nodeep-scrub || return 1
    else
        ceph osd unset noscrub || return 1
        ceph osd unset nodeep-scrub || return 1
    fi

    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put $obj $dir/ORIGINAL || return 1
}

#
# Corrupt one copy of a replicated pool
#
function TES_corrupt_and_repair_replicated() {
    local dir=$1
    local poolname=rbd

    run_mon $dir a --osd_pool_default_size=2 || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    run_crimson_osd $dir 0 --osd_objectstore=seastore --debug || return 1
    run_crimson_osd $dir 1 --osd_objectstore=seastore --debug || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1

    add_something $dir $poolname || return 1
    corrupt_and_repair_one $dir $poolname $(get_not_primary $poolname SOMETHING) || return 1
    # Reproduces http://tracker.ceph.com/issues/8914
    corrupt_and_repair_one $dir $poolname $(get_primary $poolname SOMETHING) || return 1
}

#
# Allow operator-initiated scrubs to be scheduled even when some recovering is still
# undergoing on the same OSD
#
function TES_allow_oper_initiated_scrub_during_recovery() {
    local dir=$1
    local poolname=rbd

    run_mon $dir a --osd_pool_default_size=2 || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    
    local ceph_osd_args="--osd_objectstore=seastore \
        --osd_scrub_during_recovery=false \
        --osd_debug_pretend_recovery_active=true"
    
    run_crimson_osd $dir 0 $ceph_osd_args --debug|| return 1
    run_crimson_osd $dir 1 $ceph_osd_args --debug|| return 1
    
    create_rbd_pool || return 1
    wait_for_clean || return 1

    add_something $dir $poolname || return 1
    oper_scrub_and_schedule $dir $poolname $(get_not_primary $poolname SOMETHING) || return 1
}

#
# Allow repair to be scheduled when some recovering is still undergoing on the same OSD
#
function TES_allow_repair_during_recovery() {
    local dir=$1
    local poolname=rbd

    run_mon $dir a --osd_pool_default_size=2 || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    run_crimson_osd $dir 0 --osd_objectstore=seastore \
                   --osd_scrub_during_recovery=false \
                   --osd_debug_pretend_recovery_active=true --debug || return 1
    run_crimson_osd $dir 1 --osd_objectstore=seastore \
                   --osd_scrub_during_recovery=false \
                   --osd_debug_pretend_recovery_active=true --debug || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1

    add_something $dir $poolname || return 1
    corrupt_and_repair_one $dir $poolname $(get_not_primary $poolname SOMETHING) || return 1
}

#
# Skip non-repair scrub correctly during recovery
#
# Note: forgoing the automatic creation of a pool in standard_scrub_cluster as
#       the test requires a specific RBD pool.
function TES_skip_non_repair_during_recovery() {
    local dir=$1
    local -A cluster_conf=(
        ['osds_num']="2"
        ['pgs_in_pool']="4"
        ['pool_name']="nopool"
        ['pool_default_size']="2"
        ['extras']="--osd_scrub_during_recovery=false --osd_debug_pretend_recovery_active=true"
    )

    crimson_standard_scrub_cluster $dir cluster_conf
    local poolname=rbd
    create_rbd_pool || return 1
    wait_for_clean || return 1

    add_something $dir $poolname || return 1
    scrub_and_not_schedule $dir $poolname $(get_not_primary $poolname SOMETHING) || return 1
}


function oper_scrub_and_schedule() {
    local dir=$1
    local poolname=$2
    local osd=$3

    #
    # 1) start an operator-initiated scrub
    #
    local pg=$(get_pg $poolname SOMETHING)
    local last_scrub=$(get_last_scrub_stamp $pg)
    ceph pg $pg scrub

    #
    # 2) Assure the scrub was executed
    #
    sleep 3
    for ((i=0; i < 3; i++)); do
        if test "$(get_last_scrub_stamp $pg)" '>' "$last_scrub" ; then
            break
        fi
        if test "$(get_last_scrub_stamp $pg)" '==' "$last_scrub" ; then
            return 1
        fi
        sleep 1
    done

    #
    # 3) Access to the file must OK
    #
    objectstore_tool $dir $osd SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

function scrub_and_not_schedule() {
    local dir=$1
    local poolname=$2
    local osd=$3

    #
    # 1) start a non-repair scrub
    #
    local pg=$(get_pg $poolname SOMETHING)
    local last_scrub=$(get_last_scrub_stamp $pg)
    ceph pg $pg schedule-scrub

    #
    # 2) Assure the scrub is not scheduled
    #
    sleep 3
    for ((i=0; i < 3; i++)); do
        if test "$(get_last_scrub_stamp $pg)" '>' "$last_scrub" ; then
            return 1
        fi
        sleep 1
    done

    #
    # 3) Access to the file must OK
    #
    objectstore_tool $dir $osd SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

function corrupt_and_repair_two() {
    local dir=$1
    local poolname=$2
    local first=$3
    local second=$4

    #
    # 1) remove the corresponding file from the OSDs
    #
    pids=""
    run_in_background pids objectstore_tool $dir $first SOMETHING remove
    run_in_background pids objectstore_tool $dir $second SOMETHING remove
    wait_background pids
    return_code=$?
    if [ $return_code -ne 0 ]; then return $return_code; fi

    #
    # 2) repair the PG
    #
    local pg=$(get_pg $poolname SOMETHING)
    repair $pg
    #
    # 3) The files must be back
    #
    pids=""
    run_in_background pids objectstore_tool $dir $first SOMETHING list-attrs
    run_in_background pids objectstore_tool $dir $second SOMETHING list-attrs
    wait_background pids
    return_code=$?
    if [ $return_code -ne 0 ]; then return $return_code; fi

    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

#
# 1) add an object
# 2) remove the corresponding object from a designated OSD
# 3) repair the PG
# 4) check that the object has been restored in the designated OSD
#
function corrupt_and_repair_one() {
    local dir=$1
    local poolname=$2
    local osd=$3

    #
    # 1) remove the corresponding file from the OSD
    #
    objectstore_tool $dir $osd SOMETHING remove || return 1
    #
    # 2) repair the PG
    #
    local pg=$(get_pg $poolname SOMETHING)
    repair $pg
    #
    # 2.5) wait for recovery to complete (Crimson needs this)
    #
    wait_for_clean || return 1
    #
    # 3) The file must be back
    #
    objectstore_tool $dir $osd SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

function corrupt_and_repair_erasure_coded() {
    local dir=$1
    local poolname=$2

    add_something $dir $poolname || return 1

    local primary=$(get_primary $poolname SOMETHING)
    local -a osds=($(get_osds $poolname SOMETHING | sed -e "s/$primary//"))
    local not_primary_first=${osds[0]}
    local not_primary_second=${osds[1]}

    # Reproduces http://tracker.ceph.com/issues/10017
    corrupt_and_repair_one $dir $poolname $primary  || return 1
    # Reproduces http://tracker.ceph.com/issues/10409
    corrupt_and_repair_one $dir $poolname $not_primary_first || return 1
    corrupt_and_repair_two $dir $poolname $not_primary_first $not_primary_second || return 1
    corrupt_and_repair_two $dir $poolname $primary $not_primary_first || return 1

}

function auto_repair_erasure_coded() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd_objectstore=seastore \
            --osd-scrub-auto-repair=true \
            --osd-deep-scrub-interval=5 \
            --osd-scrub-max-interval=5 \
            --osd-scrub-min-interval=5 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 2) ; do
        run_crimson_osd $dir $id $ceph_osd_args --debug || return 1
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    # Create an EC pool
    create_ec_pool $poolname $allow_overwrites k=2 m=1 || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING remove || return 1
    # Wait for auto repair
    local pgid=$(get_pg $poolname SOMETHING)
    wait_for_scrub $pgid "$(get_last_scrub_stamp $pgid)"
    wait_for_clean || return 1
    # Verify - the file should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

function TES_auto_repair_erasure_coded_appends() {
    auto_repair_erasure_coded $1 false
}

function TES_auto_repair_erasure_coded_overwrites() {
    if [ "$use_ec_overwrite" = "true" ]; then
        auto_repair_erasure_coded $1 true
    fi
}

# initiate a scrub, then check for the (expected) 'scrubbing' and the
# (not expected until an error was identified) 'repair'
# Arguments: osd#, pg, sleep time
function initiate_and_fetch_state() {
    local the_osd="osd.$1"
    local pgid=$2
    local last_scrub=$(get_last_scrub_stamp $pgid)

    set_config "osd" "$1" "osd_scrub_sleep"  "$3"
    set_config "osd" "$1" "osd_scrub_auto_repair" "true"

    flush_pg_stats
    date  --rfc-3339=ns

    # note: must initiate a "regular" (periodic) deep scrub - not an operator-initiated one
    env CEPH_ARGS= ceph --format json daemon $(get_asok_path $the_osd) schedule-deep-scrub "$pgid"

    # wait for 'scrubbing' to appear
    for ((i=0; i < 80; i++)); do

        st=`ceph pg $pgid query --format json | jq '.state' `
        echo $i ") state now: " $st

        case "$st" in
            *scrubbing*repair* ) echo "found scrub+repair"; return 1;; # PR #41258 should have prevented this
            *scrubbing* ) echo "found scrub"; return 0;;
            *inconsistent* ) echo "Got here too late. Scrub has already finished"; return 1;;
            *recovery* ) echo "Got here too late. Scrub has already finished."; return 1;;
            * ) echo $st;;
        esac

        if [ $((i % 10)) == 4 ]; then
            echo "loop --------> " $i
        fi
    sleep 0.3
    done

    echo "Timeout waiting for deep-scrub of " $pgid " on " $the_osd " to start"
    return 1
}

function wait_end_of_scrub() { # osd# pg
    local the_osd="osd.$1"
    local pgid=$2

    for ((i=0; i < 40; i++)); do
        st=`ceph pg $pgid query --format json | jq '.state' `
        echo "wait-scrub-end state now: " $st
        [[ $st =~ (.*scrubbing.*) ]] || break
        if [ $((i % 5)) == 4 ] ; then
            flush_pg_stats
        fi
        sleep 0.3
    done

    if [[ $st =~ (.*scrubbing.*) ]]
    then
        # a timeout
        return 1
    fi
    return 0
}


function TES_auto_repair_seastore_basic() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 3 seconds scrub interval
    run_mon $dir a || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    # Set scheduler to "wpq" until there's a reliable way to query scrub states
    # with "--osd-scrub-sleep" set to 0. The "mclock_scheduler" overrides the
    # scrub sleep to 0 and as a result the checks in the test fail.
    local ceph_osd_args="--osd_objectstore=seastore \
            --osd-scrub-auto-repair=true \
            --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0 \
            --osd-op-queue=wpq"
    for id in $(seq 0 2) ; do
        run_crimson_osd $dir $id $ceph_osd_args --debug || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING remove || return 1

    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    echo "Affected PG " $pgid " w/ primary " $primary
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    initiate_and_fetch_state $primary $pgid "3.0"
    r=$?
    echo "initiate_and_fetch_state ret: " $r
    set_config "osd"  "$1"  "osd_scrub_sleep"  "0"
    if [ $r -ne 0 ]; then
        return 1
    fi

    wait_end_of_scrub "$primary" "$pgid" || return 1
    ceph pg dump pgs

    # Verify - the file should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING list-attrs || return 1
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING get-bytes $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    grep scrub_finish $dir/osd.${primary}.log
}

function TES_auto_repair_seastore_tag() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 3 seconds scrub interval
    run_mon $dir a || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    # Set scheduler to "wpq" until there's a reliable way to query scrub states
    # with "--osd-scrub-sleep" set to 0. The "mclock_scheduler" overrides the
    # scrub sleep to 0 and as a result the checks in the test fail.
    local ceph_osd_args="--osd_objectstore=seastore \
            --osd-scrub-auto-repair=true \
            --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0 \
            --osd-op-queue=wpq"
    for id in $(seq 0 2) ; do
        run_crimson_osd $dir $id $ceph_osd_args --debug || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING remove || return 1

    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    echo "Affected PG " $pgid " w/ primary " $primary
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    initiate_and_fetch_state $primary $pgid "3.0"
    r=$?
    echo "initiate_and_fetch_state ret: " $r
    set_config "osd"  "$1"  "osd_scrub_sleep"  "0"
    if [ $r -ne 0 ]; then
        return 1
    fi

    wait_end_of_scrub "$primary" "$pgid" || return 1
    ceph pg dump pgs

    # Verify - the file should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING list-attrs || return 1
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING get-bytes $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    grep scrub_finish $dir/osd.${primary}.log
}
function TES_auto_repair_seastore_scrub() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd_objectstore=seastore \
            --osd-scrub-auto-repair=true \
            --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0 \
            --osd-scrub-backoff-ratio=0"
    for id in $(seq 0 2) ; do
        run_crimson_osd $dir $id $ceph_osd_args --debug || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING remove || return 1

    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    ceph pg $pgid schedule-scrub

    # Wait for scrub -> auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    ceph pg dump pgs
    # Actually this causes 2 scrubs, so we better wait a little longer
    sleep 2
    ceph pg dump pgs
    sleep 2
    ceph pg dump pgs
    sleep 5
    wait_for_clean || return 1
    ceph pg dump pgs
    # Verify - the file should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    grep scrub_finish $dir/osd.${primary}.log

    # This should have caused 1 object to be repaired
    COUNT=$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_objects_repaired')
    test "$COUNT" = "1" || return 1
}
function TES_auto_repair_seastore_failed() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd_objectstore=seastore \
            --osd-scrub-auto-repair=true \
            --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0 \
            --osd-scrub-begin-hour=0 \
            --osd-scrub-end-hour=0 \
            --osd-scrub-backoff-ratio=0.0"
    for id in $(seq 0 2) ; do
        run_crimson_osd $dir $id $ceph_osd_args --debug || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    for i in $(seq 1 10)
    do
      rados --pool $poolname put obj$i $dir/ORIGINAL || return 1
    done

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) obj1 remove || return 1
    # obj2 can't be repaired
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) obj2 remove || return 1
    objectstore_tool $dir $(get_primary $poolname SOMETHING) obj2 rm-attr _ || return 1

    local pgid=$(get_pg $poolname obj1)
    local primary=$(get_primary $poolname obj1)
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    ceph pg $pgid schedule-deep-scrub

    # Wait for auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    wait_for_clean || return 1
    flush_pg_stats
    grep scrub_finish $dir/osd.${primary}.log
    grep -q "scrub_finish.*still present after re-scrub" $dir/osd.${primary}.log || return 1
    ceph pg dump pgs
    ceph pg dump pgs | grep -q "^${pgid}.*+failed_repair" || return 1

    # Verify - obj1 should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname obj1) obj1 list-attrs || return 1
    rados --pool $poolname get obj1 $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    grep scrub_finish $dir/osd.${primary}.log

    # Make it repairable
    objectstore_tool $dir $(get_primary $poolname SOMETHING) obj2 remove || return 1
    repair $pgid
    sleep 2

    flush_pg_stats
    ceph pg dump pgs
    ceph pg dump pgs | grep -q -e "^${pgid}.* active+clean " -e "^${pgid}.* active+clean+wait " || return 1
    grep scrub_finish $dir/osd.${primary}.log
}

function TEST_auto_repair_seastore_failed_norecov() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd_objectstore=seastore \
            --osd-scrub-auto-repair=true \
            --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0 \
            --osd-scrub-begin-hour=0 \
            --osd-scrub-end-hour=0 \
            --osd-scrub-backoff-ratio=0.0"
    for id in $(seq 0 2) ; do
        run_crimson_osd $dir $id $ceph_osd_args --debug || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    for i in $(seq 1 10)
    do
      rados --pool $poolname put obj$i $dir/ORIGINAL || return 1
    done

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    # obj1 can't be repaired
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) obj1 remove || return 1
    objectstore_tool $dir $(get_primary $poolname SOMETHING) obj1 rm-attr _ || return 1
    # obj2 can't be repaired
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) obj2 remove || return 1
    objectstore_tool $dir $(get_primary $poolname SOMETHING) obj2 rm-attr _ || return 1
    ceph config set osd osd_scrub_auto_repair true

    local pgid=$(get_pg $poolname obj1)
    local primary=$(get_primary $poolname obj1)
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    ceph pg $pgid schedule-deep-scrub

    # Wait for auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    wait_for_clean || return 1
    flush_pg_stats
    grep -q "scrub_finish.*present with no repair possible" $dir/osd.${primary}.log || return 1
    ceph pg dump pgs
    ceph pg dump pgs | grep -q "^${pgid}.*+failed_repair" || return 1
}




main osd-scrub-repair-crimson "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-repair-crimson.sh"
# End:

