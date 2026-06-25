#!/usr/bin/env bash
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
# Author: David Zafman <dzafman@redhat.com>
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

# Test development and debugging
# Set to "yes" in order to ignore diff errors and save results to update test
getjson="no"

jqfilter='.inconsistents'
sortkeys='import json; import sys ; JSON=sys.stdin.read() ; ud = json.loads(JSON) ; print ( json.dumps(ud, sort_keys=True, indent=2) )'

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7121" # git grep '\<7121\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
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
    # These are required for Crimson to work properly
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features crimson || return 1
    ceph osd set-allow-crimson --yes-i-really-mean-it || return 1
    
    # Set msgr2 requirements
    ceph config set global ms_bind_msgr2 true || return 1
    ceph config set global ms_bind_msgr1 false || return 1
    
    return 0
}

function create_scenario() {
    local dir=$1
    local poolname=$2
    local TESTDATA=$3
    local osd=$4

    SNAP=1
    rados -p $poolname mksnap snap${SNAP}
    dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
    rados -p $poolname put obj1 $TESTDATA
    rados -p $poolname put obj5 $TESTDATA
    rados -p $poolname put obj3 $TESTDATA
    for i in `seq 6 15`
     do rados -p $poolname put obj${i} $TESTDATA
    done

    SNAP=2
    rados -p $poolname mksnap snap${SNAP}
    dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
    rados -p $poolname put obj5 $TESTDATA

    SNAP=3
    rados -p $poolname mksnap snap${SNAP}
    dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
    rados -p $poolname put obj3 $TESTDATA

    SNAP=4
    rados -p $poolname mksnap snap${SNAP}
    dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
    rados -p $poolname put obj5 $TESTDATA
    rados -p $poolname put obj2 $TESTDATA

    SNAP=5
    rados -p $poolname mksnap snap${SNAP}
    SNAP=6
    rados -p $poolname mksnap snap${SNAP}
    dd if=/dev/urandom of=$TESTDATA bs=256 count=${SNAP}
    rados -p $poolname put obj5 $TESTDATA

    SNAP=7
    rados -p $poolname mksnap snap${SNAP}

    rados -p $poolname rm obj4
    rados -p $poolname rm obj2

    kill_daemons $dir TERM osd || return 1

    # Don't need to use ceph_objectstore_tool() function because osd stopped
    # Use crimson-objectstore-tool for Crimson OSDs and avoid BlueStore-only
    # snapmap/kvstore assumptions.

    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj1)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" --force remove || return 1

    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --op list obj5 | grep \"snapid\":2)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" remove || return 1

    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --op list obj5 | grep \"snapid\":1)"
    OBJ5SAVE="$JSON"
    # Skip removing obj5:1 - SeaStore doesn't support --rmtype nosnapmap
    # which was used in the original test to create orphaned snapmap entries

    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --op list obj5 | grep \"snapid\":4)"
    dd if=/dev/urandom of=$TESTDATA bs=256 count=18
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" set-bytes $TESTDATA || return 1

    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj3)"
    dd if=/dev/urandom of=$TESTDATA bs=256 count=15
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" set-bytes $TESTDATA || return 1

    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --op list obj4 | grep \"snapid\":7)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" remove || return 1

    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj2)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" rm-attr snapset || return 1

    # Create a clone which isn't in snapset and doesn't have object info
    JSON="$(echo "$OBJ5SAVE" | sed s/snapid\":1/snapid\":7/)"
    dd if=/dev/urandom of=$TESTDATA bs=256 count=7
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" set-bytes $TESTDATA || return 1

    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj6)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" clear-snapset clones || return 1
    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj7)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" clear-snapset corrupt || return 1
    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj8)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" clear-snapset seq || return 1
    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj9)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" clear-snapset clone_size || return 1
    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj10)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" clear-snapset clone_overlap || return 1
    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj11)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" clear-snapset clones || return 1
    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj13)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" clear-snapset snaps || return 1
    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj14)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" clear-snapset size || return 1

    echo "garbage" > $dir/bad
    JSON="$(crimson-objectstore-tool --data-path $dir/${osd} --head --op list obj15)"
    crimson-objectstore-tool --data-path $dir/${osd} "$JSON" set-attr snapset $dir/bad || return 1
    rm -f $dir/bad
    
    return 0
}

function TEST_scrub_snaps() {
    local dir=$1
    local poolname=test
    local OBJS=16
    local OSDS=1

    TESTDATA="testdata.$$"

    run_mon $dir a --osd_pool_default_size=$OSDS --mon_allow_pool_size_one=true || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_crimson_osd $dir $osd --osd_objectstore=seastore --debug || return 1
    done

    # All scrubs done manually.  Don't want any unexpected scheduled scrubs.
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']test[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $OBJS`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done

    local primary=$(get_primary $poolname obj1)

    create_scenario $dir $poolname $TESTDATA $primary || return 1

    rm -f $TESTDATA

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      activate_osd $dir $osd || return 1
    done
    ceph config set osd osd_shallow_scrub_chunk_max 25
    ceph config set osd osd_shallow_scrub_chunk_min 5
    ceph config set osd osd_pg_stat_report_interval_max_seconds 1
    ceph config set osd osd_pg_stat_report_interval_max_epochs 1


    wait_for_clean || return 1

    ceph config get osd osd_shallow_scrub_chunk_max
    ceph config get osd osd_shallow_scrub_chunk_min
    ceph config get osd osd_pg_stat_report_interval_max_seconds
    ceph config get osd osd_pg_stat_report_interval_max_epochs
    ceph config get osd osd_scrub_chunk_max
    ceph config get osd osd_scrub_chunk_min

    local pgid="${poolid}.0"
    if ! pg_scrub "$pgid" ; then
        return 1
    fi

    test "$(grep "_scan_snaps start" $dir/osd.${primary}.log | wc -l)" = "2" || return 1

    rados list-inconsistent-pg $poolname > $dir/json || return 1
    # Check pg count
    test $(jq '. | length' $dir/json) = "1" || return 1
    # Check pgid
    test $(jq -r '.[0]' $dir/json) = $pgid || return 1

    rados list-inconsistent-obj $pgid > $dir/json || return 1

    # The injected snapshot errors with a single copy pool doesn't
    # see object errors because all the issues are detected by
    # comparing copies.
    jq "$jqfilter" << EOF | python3 -c "$sortkeys" > $dir/checkcsjson
{
    "epoch": 17,
    "inconsistents": []
}
EOF

    jq "$jqfilter" $dir/json | python3 -c "$sortkeys" > $dir/csjson
    multidiff $dir/checkcsjson $dir/csjson || test $getjson = "yes" || return 1

    rados list-inconsistent-snapset $pgid > $dir/json || return 1

    jq "$jqfilter" << EOF | python3 -c "$sortkeys" > $dir/checkcsjson
{
  "inconsistents": [
    {
      "errors": [
        "headless"
      ],
      "snap": 1,
      "locator": "",
      "nspace": "",
      "name": "obj1"
    },
    {
      "errors": [
        "size_mismatch"
      ],
      "snap": 1,
      "locator": "",
      "nspace": "",
      "name": "obj10"
    },
    {
      "errors": [
        "headless"
      ],
      "snap": 1,
      "locator": "",
      "nspace": "",
      "name": "obj11"
    },
    {
      "errors": [
        "size_mismatch"
      ],
      "snap": 1,
      "locator": "",
      "nspace": "",
      "name": "obj14"
    },
    {
      "errors": [
        "headless"
      ],
      "snap": 1,
      "locator": "",
      "nspace": "",
      "name": "obj6"
    },
    {
      "errors": [
        "headless"
      ],
      "snap": 1,
      "locator": "",
      "nspace": "",
      "name": "obj7"
    },
    {
      "errors": [
        "size_mismatch"
      ],
      "snap": 1,
      "locator": "",
      "nspace": "",
      "name": "obj9"
    },
    {
      "errors": [
        "headless"
      ],
      "snap": 4,
      "locator": "",
      "nspace": "",
      "name": "obj2"
    },
    {
      "errors": [
        "size_mismatch"
      ],
      "snap": 4,
      "locator": "",
      "nspace": "",
      "name": "obj5"
    },
    {
      "errors": [
        "headless"
      ],
      "snap": 7,
      "locator": "",
      "nspace": "",
      "name": "obj2"
    },
    {
      "errors": [
        "info_missing",
        "headless"
      ],
      "snap": 7,
      "locator": "",
      "nspace": "",
      "name": "obj5"
    },
    {
      "name": "obj10",
      "nspace": "",
      "locator": "",
      "snap": "head",
      "snapset": {
        "seq": 1,
        "clones": [
          {
            "snap": 1,
            "size": 1032,
            "overlap": "????",
            "snaps": [
              1
            ]
          }
        ]
      },
      "errors": []
    },
    {
      "extra clones": [
        1
      ],
      "errors": [
        "extra_clones"
      ],
      "snap": "head",
      "locator": "",
      "nspace": "",
      "name": "obj11",
      "snapset": {
        "seq": 1,
        "clones": []
      }
    },
    {
      "name": "obj14",
      "nspace": "",
      "locator": "",
      "snap": "head",
      "snapset": {
        "seq": 1,
        "clones": [
          {
            "snap": 1,
            "size": 1033,
            "overlap": "[]",
            "snaps": [
              1
            ]
          }
        ]
      },
      "errors": []
    },
    {
      "errors": [
        "snapset_corrupted"
      ],
      "snap": "head",
      "locator": "",
      "nspace": "",
      "name": "obj15"
    },
    {
      "extra clones": [
        7,
        4
      ],
      "errors": [
        "snapset_missing",
        "extra_clones"
      ],
      "snap": "head",
      "locator": "",
      "nspace": "",
      "name": "obj2"
    },
    {
      "errors": [
        "size_mismatch"
      ],
      "snap": "head",
      "locator": "",
      "nspace": "",
      "name": "obj3",
      "snapset": {
        "seq": 3,
        "clones": [
          {
            "snap": 1,
            "size": 1032,
            "overlap": "[]",
            "snaps": [
              1
            ]
          },
          {
            "snap": 3,
            "size": 256,
            "overlap": "[]",
            "snaps": [
              3,
              2
            ]
          }
        ]
      }
    },
    {
      "missing": [
        7
      ],
      "errors": [
        "clone_missing"
      ],
      "snap": "head",
      "locator": "",
      "nspace": "",
      "name": "obj4",
      "snapset": {
        "seq": 7,
        "clones": [
          {
            "snap": 7,
            "size": 1032,
            "overlap": "[]",
            "snaps": [
              7,
              6,
              5,
              4,
              3,
              2,
              1
            ]
          }
        ]
      }
    },
    {
      "missing": [
        2
      ],
      "extra clones": [
        7
      ],
      "errors": [
        "extra_clones",
        "clone_missing"
      ],
      "snap": "head",
      "locator": "",
      "nspace": "",
      "name": "obj5",
      "snapset": {
        "seq": 6,
        "clones": [
          {
            "snap": 1,
            "size": 1032,
            "overlap": "[]",
            "snaps": [
              1
            ]
          },
          {
            "snap": 2,
            "size": 256,
            "overlap": "[]",
            "snaps": [
              2
            ]
          },
          {
            "snap": 4,
            "size": 512,
            "overlap": "[]",
            "snaps": [
              4,
              3
            ]
          },
          {
            "snap": 6,
            "size": 1024,
            "overlap": "[]",
            "snaps": [
              6,
              5
            ]
          }
        ]
      }
    },
    {
      "extra clones": [
        1
      ],
      "errors": [
        "extra_clones"
      ],
      "snap": "head",
      "locator": "",
      "nspace": "",
      "name": "obj6",
      "snapset": {
        "seq": 1,
        "clones": []
      }
    },
    {
      "extra clones": [
        1
      ],
      "errors": [
        "extra_clones"
      ],
      "snap": "head",
      "locator": "",
      "nspace": "",
      "name": "obj7",
      "snapset": {
        "seq": 0,
        "clones": []
      }
    },
    {
      "errors": [
        "snapset_error"
      ],
      "snap": "head",
      "locator": "",
      "nspace": "",
      "name": "obj8",
      "snapset": {
        "seq": 0,
        "clones": [
          {
            "snap": 1,
            "size": 1032,
            "overlap": "[]",
            "snaps": [
              1
            ]
          }
        ]
      }
    },
    {
      "name": "obj9",
      "nspace": "",
      "locator": "",
      "snap": "head",
      "snapset": {
        "seq": 1,
        "clones": [
          {
            "snap": 1,
            "size": "????",
            "overlap": "[]",
            "snaps": [
              1
            ]
          }
        ]
      },
      "errors": []
    }
  ],
  "epoch": 20
}
EOF

    jq "$jqfilter" $dir/json | python3 -c "$sortkeys" > $dir/csjson
    
    # Verify that checkcsjson and csjson have the same JSON format (same content, regardless of order)
    if ! python3 -c "
import json
import sys

with open('$dir/checkcsjson', 'r') as f:
    expected = json.load(f)
with open('$dir/csjson', 'r') as f:
    actual = json.load(f)

# Sort both lists by a consistent key to enable comparison
def sort_key(item):
    return (item.get('name', ''), str(item.get('snap', '')), item.get('nspace', ''), item.get('locator', ''))

expected_sorted = sorted(expected, key=sort_key)
actual_sorted = sorted(actual, key=sort_key)

if expected_sorted != actual_sorted:
    print('ERROR: checkcsjson and csjson have different content', file=sys.stderr)
    sys.exit(1)
print('SUCCESS: checkcsjson and csjson have the same format')
"; then
        echo "JSON format verification failed"
        multidiff $dir/checkcsjson $dir/csjson || test $getjson = "yes" || return 1
        if test $getjson != "yes"; then
            return 1
        fi
    fi
    if test $getjson = "yes"
    then
        jq '.' $dir/json > save1.json
    fi

    if test "$LOCALRUN" = "yes" && which jsonschema > /dev/null;
    then
      jsonschema -i $dir/json $CEPH_ROOT/doc/rados/command/list-inconsistent-snap.json || return 1
    fi

    pidfiles=$(find $dir 2>/dev/null | grep 'osd[^/]*\.pid')
    pids=""
    for pidfile in ${pidfiles}
    do
        pids+="$(cat $pidfile) "
    done

    ERRORS=0

    for i in `seq 1 7`
    do
        rados -p $poolname rmsnap snap$i
    done
    sleep 5
    local -i loop=0
    while ceph pg dump pgs | grep -q snaptrim;
    do
        if ceph pg dump pgs | grep -q snaptrim_error;
        then
            break
        fi
        sleep 2
        loop+=1
        if (( $loop >= 10 )) ; then
            ERRORS=$(expr $ERRORS + 1)
            break
        fi
    done
    ceph pg dump pgs

    for pid in $pids
    do
        if ! kill -0 $pid
        then
            echo "OSD Crash occurred"
            ERRORS=$(expr $ERRORS + 1)
        fi
    done

    kill_daemons $dir || return 1

    # Crimson uses structured error reporting in chunk_result_t messages
    # Check for expected error types and objects in the scrub results
    if test $getjson != "yes"; then
        # Extract scrub error messages
        if ! grep "emit_chunk_result: Scrub errors found" $dir/osd.${primary}.log > /dev/null; then
            echo "ERROR: No scrub errors found in log"
            ERRORS=$(expr $ERRORS + 1)
        else
            # Verify we have the expected error types and objects
            local scrub_output=$(grep "emit_chunk_result: Scrub errors found" $dir/osd.${primary}.log)
            
            # Check for expected error types
            declare -a expected_errors=(
                "CLONE_MISSING.*obj4"
                "CLONE_MISSING.*obj5"
                "SNAPSET_MISSING.*obj2"
                "EXTRA_CLONES.*obj2"
                "HEADLESS_CLONE.*obj2"
                "EXTRA_CLONES.*obj5"
                "HEADLESS_CLONE.*obj5"
                "SIZE_MISMATCH.*obj5"
                "SIZE_MISMATCH.*obj3"
                "EXTRA_CLONES.*obj6"
                "HEADLESS_CLONE.*obj6"
                "EXTRA_CLONES.*obj7"
                "HEADLESS_CLONE.*obj7"
                "SNAP_ERROR.*obj8"
                "SNAPSET_CORRUPTED.*obj15"
                "SIZE_MISMATCH.*obj10"
                "SIZE_MISMATCH.*obj14"
                "EXTRA_CLONES.*obj11"
                "HEADLESS_CLONE.*obj11"
                "SIZE_MISMATCH.*obj9"
                "HEADLESS_CLONE.*obj1"
            )
            
            for expected in "${expected_errors[@]}"; do
                if ! echo "$scrub_output" | grep -E "$expected" > /dev/null; then
                    echo "Missing expected error: $expected"
                    ERRORS=$(expr $ERRORS + 1)
                fi
            done
            
            # Verify total error count (should be 23 total: 14 + 9)
            local total_errors=$(echo "$scrub_output" | grep -oP 'num_scrub_errors: \K\d+' | awk '{sum+=$1} END {print sum}')
            if [ "$total_errors" != "23" ]; then
                echo "ERROR: Expected 23 total scrub errors, found $total_errors"
                ERRORS=$(expr $ERRORS + 1)
            fi
        fi

        if [ $ERRORS != "0" ]; then
            echo "TEST FAILED WITH $ERRORS ERRORS"
            return 1
        fi
    else
        echo "Skipping error string validation (getjson mode)"
    fi

    echo "TEST PASSED"
    return 0
}

function _scrub_snaps_multi() {
    local dir=$1
    local poolname=test
    local OBJS=16
    local OSDS=2
    local which=$2

    TESTDATA="testdata.$$"

    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    apply_crimson_config || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_crimson_osd $dir $osd --osd_objectstore=seastore || return 1
    done

    # All scrubs done manually.  Don't want any unexpected scheduled scrubs.
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1

    # Create a pool with a single pg
    create_pool $poolname 1 1
    wait_for_clean || return 1
    poolid=$(ceph osd dump | grep "^pool.*[']test[']" | awk '{ print $2 }')

    dd if=/dev/urandom of=$TESTDATA bs=1032 count=1
    for i in `seq 1 $OBJS`
    do
        rados -p $poolname put obj${i} $TESTDATA
    done

    local primary=$(get_primary $poolname obj1)
    local replica=$(get_not_primary $poolname obj1)

    eval create_scenario $dir $poolname $TESTDATA \$$which || return 1

    rm -f $TESTDATA

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      activate_osd $dir $osd || return 1
    done

    ceph config set osd osd_shallow_scrub_chunk_max 3
    ceph config set osd osd_shallow_scrub_chunk_min 3
    ceph config set osd osd_scrub_chunk_min 3
    ceph config set osd osd_pg_stat_report_interval_max_seconds 1
    ceph config set osd osd_pg_stat_report_interval_max_epochs 1
    wait_for_clean || return 1

    local pgid="${poolid}.0"
    if ! pg_scrub "$pgid" ; then
        return 1
    fi

    test "$(grep "_scan_snaps start" $dir/osd.${primary}.log | wc -l)" -gt "3" || return 1
    test "$(grep "_scan_snaps start" $dir/osd.${replica}.log | wc -l)" -gt "3" || return 1

    rados list-inconsistent-pg $poolname > $dir/json || return 1
    # Check pg count
    test $(jq '. | length' $dir/json) = "1" || return 1
    # Check pgid
    test $(jq -r '.[0]' $dir/json) = $pgid || return 1

    rados list-inconsistent-obj $pgid --format=json-pretty

    rados list-inconsistent-snapset $pgid > $dir/json || return 1

    # Since all of the snapshots on the primary is consistent there are no errors here
    if [ $which = "replica" ];
    then
        scruberrors="17"
        jq "$jqfilter" << EOF | python3 -c "$sortkeys" > $dir/checkcsjson
{
    "epoch": 23,
    "inconsistents": []
}
EOF

else
        scruberrors="29"
        jq "$jqfilter" << EOF | python3 -c "$sortkeys" > $dir/checkcsjson
{
    "epoch": 23,
    "inconsistents": [
        {
            "name": "obj10",
            "nspace": "",
            "locator": "",
            "snap": 1,
            "errors": [
                "size_mismatch"
            ]
        },
        {
            "name": "obj11",
            "nspace": "",
            "locator": "",
            "snap": 1,
            "errors": [
                "headless"
            ]
        },
        {
            "name": "obj14",
            "nspace": "",
            "locator": "",
            "snap": 1,
            "errors": [
                "size_mismatch"
            ]
        },
        {
            "name": "obj6",
            "nspace": "",
            "locator": "",
            "snap": 1,
            "errors": [
                "headless"
            ]
        },
        {
            "name": "obj7",
            "nspace": "",
            "locator": "",
            "snap": 1,
            "errors": [
                "headless"
            ]
        },
        {
            "name": "obj9",
            "nspace": "",
            "locator": "",
            "snap": 1,
            "errors": [
                "size_mismatch"
            ]
        },
        {
            "name": "obj5",
            "nspace": "",
            "locator": "",
            "snap": 7,
            "errors": [
                "info_missing",
                "headless"
            ]
        },
        {
            "name": "obj10",
            "nspace": "",
            "locator": "",
            "snap": "head",
            "snapset": {
                "seq": 1,
                "clones": [
                    {
                        "snap": 1,
                        "size": 1032,
                        "overlap": "????",
                        "snaps": [
                            1
                        ]
                    }
                ]
            },
            "errors": []
        },
        {
            "name": "obj11",
            "nspace": "",
            "locator": "",
            "snap": "head",
            "snapset": {
                "seq": 1,
                "clones": []
            },
            "errors": [
                "extra_clones"
            ],
            "extra clones": [
                1
            ]
        },
        {
            "name": "obj14",
            "nspace": "",
            "locator": "",
            "snap": "head",
            "snapset": {
                "seq": 1,
                "clones": [
                    {
                        "snap": 1,
                        "size": 1033,
                        "overlap": "[]",
                        "snaps": [
                            1
                        ]
                    }
                ]
            },
            "errors": []
        },
        {
            "name": "obj5",
            "nspace": "",
            "locator": "",
            "snap": "head",
            "snapset": {
                "seq": 6,
                "clones": [
                    {
                        "snap": 1,
                        "size": 1032,
                        "overlap": "[]",
                        "snaps": [
                            1
                        ]
                    },
                    {
                        "snap": 2,
                        "size": 256,
                        "overlap": "[]",
                        "snaps": [
                            2
                        ]
                    },
                    {
                        "snap": 4,
                        "size": 512,
                        "overlap": "[]",
                        "snaps": [
                            4,
                            3
                        ]
                    },
                    {
                        "snap": 6,
                        "size": 1024,
                        "overlap": "[]",
                        "snaps": [
                            6,
                            5
                        ]
                    }
                ]
            },
            "errors": [
                "extra_clones"
            ],
            "extra clones": [
                7
            ]
        },
        {
            "name": "obj6",
            "nspace": "",
            "locator": "",
            "snap": "head",
            "snapset": {
                "seq": 1,
                "clones": []
            },
            "errors": [
                "extra_clones"
            ],
            "extra clones": [
                1
            ]
        },
        {
            "name": "obj7",
            "nspace": "",
            "locator": "",
            "snap": "head",
            "snapset": {
                "seq": 0,
                "clones": []
            },
            "errors": [
                "extra_clones"
            ],
            "extra clones": [
                1
            ]
        },
        {
            "name": "obj8",
            "nspace": "",
            "locator": "",
            "snap": "head",
            "snapset": {
                "seq": 0,
                "clones": [
                    {
                        "snap": 1,
                        "size": 1032,
                        "overlap": "[]",
                        "snaps": [
                            1
                        ]
                    }
                ]
            },
            "errors": [
                "snapset_error"
            ]
        },
        {
            "name": "obj9",
            "nspace": "",
            "locator": "",
            "snap": "head",
            "snapset": {
                "seq": 1,
                "clones": [
                    {
                        "snap": 1,
                        "size": "????",
                        "overlap": "[]",
                        "snaps": [
                            1
                        ]
                    }
                ]
            },
            "errors": []
        }
    ]
}
EOF
fi

    jq "$jqfilter" $dir/json | python3 -c "$sortkeys" > $dir/csjson
    
    # Verify that checkcsjson and csjson have the same JSON format (same content, regardless of order)
    if ! python3 -c "
import json
import sys

with open('$dir/checkcsjson', 'r') as f:
    expected = json.load(f)
with open('$dir/csjson', 'r') as f:
    actual = json.load(f)

# Sort both lists by a consistent key to enable comparison
def sort_key(item):
    return (item.get('name', ''), str(item.get('snap', '')), item.get('nspace', ''), item.get('locator', ''))

expected_sorted = sorted(expected, key=sort_key)
actual_sorted = sorted(actual, key=sort_key)

if expected_sorted != actual_sorted:
    print('ERROR: checkcsjson and csjson have different content', file=sys.stderr)
    sys.exit(1)
print('SUCCESS: checkcsjson and csjson have the same format')
"; then
        echo "JSON format verification failed"
        multidiff $dir/checkcsjson $dir/csjson || test $getjson = "yes" || return 1
        if test $getjson != "yes"; then
            return 1
        fi
    fi
    if test $getjson = "yes"
    then
        jq '.' $dir/json > save1.json
    fi

    if test "$LOCALRUN" = "yes" && which jsonschema > /dev/null;
    then
      jsonschema -i $dir/json $CEPH_ROOT/doc/rados/command/list-inconsistent-snap.json || return 1
    fi

    pidfiles=$(find $dir 2>/dev/null | grep 'osd[^/]*\.pid')
    pids=""
    for pidfile in ${pidfiles}
    do
        pids+="$(cat $pidfile) "
    done

    ERRORS=0

    # When removing snapshots with a corrupt replica, it crashes.
    # See http://tracker.ceph.com/issues/23875
    if [ $which = "primary" ];
    then
        for i in `seq 1 7`
        do
            rados -p $poolname rmsnap snap$i
        done
        sleep 5
        local -i loop=0
        while ceph pg dump pgs | grep -q snaptrim;
        do
            if ceph pg dump pgs | grep -q snaptrim_error;
            then
                break
            fi
            sleep 2
            loop+=1
            if (( $loop >= 10 )) ; then
                ERRORS=$(expr $ERRORS + 1)
                break
            fi
        done
    fi
    ceph pg dump pgs

    for pid in $pids
    do
        if ! kill -0 $pid
        then
            echo "OSD Crash occurred"
            ERRORS=$(expr $ERRORS + 1)
        fi
    done

    kill_daemons $dir || return 1

    # Crimson uses structured error reporting in chunk_result_t messages
    # Check for expected error types and objects in the scrub results
    if test $getjson != "yes"; then
        # Extract scrub error messages
        if ! grep "emit_chunk_result: Scrub errors found" $dir/osd.${primary}.log > /dev/null; then
            echo "ERROR: No scrub errors found in log"
            ERRORS=$(expr $ERRORS + 1)
        else
            # Verify we have the expected error types and objects
            local scrub_output=$(grep "emit_chunk_result: Scrub errors found" $dir/osd.${primary}.log)
            
            # Check for expected error types (excluding obj1 and obj5:2 which were removed)
            # Note: Use specific patterns to ensure error and object are paired:
            # - For main error field: "error: TYPE, object: //name"
            # - For shard errors: "object: //name ... shard_info_t(error: TYPE"
            # - For snapset errors: "errors: TYPE, object: //name"
            declare -a expected_errors=(
                "obj4.*SHARD_MISSING"
                "error: SIZE_MISMATCH, object: //obj3"
                "error: SIZE_MISMATCH, object: //obj5"
                "HEADLESS_CLONE.*object: //obj5"
                "error: SNAPSET_INCONSISTENCY, object: //obj6"
                "errors: EXTRA_CLONES, object: //obj6"
                "errors: HEADLESS_CLONE, object: //obj6"
                "error: SNAPSET_INCONSISTENCY, object: //obj7"
                "errors: EXTRA_CLONES, object: //obj7"
                "errors: HEADLESS_CLONE, object: //obj7"
                "error: SNAPSET_INCONSISTENCY, object: //obj8"
                "errors: SNAP_ERROR, object: //obj8"
                "error: SNAPSET_INCONSISTENCY, object: //obj13"
                "error: SNAPSET_INCONSISTENCY, object: //obj10"
                "errors: SIZE_MISMATCH, object: //obj10"
                "error: SNAPSET_INCONSISTENCY, object: //obj14"
                "errors: SIZE_MISMATCH, object: //obj14"
                "error: SNAPSET_INCONSISTENCY, object: //obj11"
                "errors: EXTRA_CLONES, object: //obj11"
                "errors: HEADLESS_CLONE, object: //obj11"
                "error: SNAPSET_INCONSISTENCY, object: //obj9"
                "errors: SIZE_MISMATCH, object: //obj9"
            )
            
            for expected in "${expected_errors[@]}"; do
                if ! echo "$scrub_output" | grep -E "$expected" > /dev/null; then
                    echo "Missing expected error: $expected"
                    ERRORS=$(expr $ERRORS + 1)
                fi
            done
            
            # Verify total error count matches scruberrors variable
            local total_errors=$(echo "$scrub_output" | grep -oP 'num_scrub_errors: \K\d+' | awk '{sum+=$1} END {print sum}')
            if [ "$total_errors" != "${scruberrors}" ]; then
                echo "ERROR: Expected ${scruberrors} total scrub errors, found $total_errors"
                ERRORS=$(expr $ERRORS + 1)
            fi
        fi
    else
        echo "Skipping error string validation (getjson mode)"
    fi

    # Check replica specific messages
    declare -a rep_err_strings
    osd=$(eval echo \$$which)
    # No replica-specific error strings for Crimson after removing kvstore-tool checks
    for err_string in "${rep_err_strings[@]}"
    do
        if ! grep "$err_string" $dir/osd.${osd}.log > /dev/null;
        then
            echo "Missing log message '$err_string'"
            ERRORS=$(expr $ERRORS + 1)
        fi
    done

    if [ $ERRORS != "0" ];
    then
        echo "TEST FAILED WITH $ERRORS ERRORS"
        return 1
    fi

    echo "TEST PASSED"
    return 0
}

function TES_scrub_snaps_replica() {
    local dir=$1
    ORIG_ARGS=$CEPH_ARGS
    CEPH_ARGS+=" --osd_scrub_chunk_min=3 --osd_scrub_chunk_max=20 --osd_shallow_scrub_chunk_min=3 --osd_shallow_scrub_chunk_max=3 --osd_pg_stat_report_interval_max_seconds=1 --osd_pg_stat_report_interval_max_epochs=1"
    _scrub_snaps_multi $dir replica
    err=$?
    CEPH_ARGS=$ORIG_ARGS
    return $err
}

function TEST_scrub_snaps_primary() {
    local dir=$1
    ORIG_ARGS=$CEPH_ARGS
    CEPH_ARGS+=" --osd_scrub_chunk_min=3 --osd_scrub_chunk_max=20 --osd_shallow_scrub_chunk_min=3 --osd_shallow_scrub_chunk_max=3 --osd_pg_stat_report_interval_max_seconds=1 --osd_pg_stat_report_interval_max_epochs=1"
    _scrub_snaps_multi $dir primary
    err=$?
    CEPH_ARGS=$ORIG_ARGS
    return $err
}

main osd-scrub-snaps-crimson "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-snaps-crimson.sh"
# End:
