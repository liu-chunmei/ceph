# Crimson OSD Scrub Test

## Overview

`osd-scrub-test-crimson.sh` is a Crimson-compatible adaptation of `osd-scrub-test.sh` for testing scrub functionality with Crimson OSDs.

## Key Differences from Classic OSD Tests

### 1. OSD Initialization
- **Classic**: Uses `run_osd` function
- **Crimson**: Uses `run_crimson_osd` function which:
  - Enables experimental Crimson features
  - Sets `allow-crimson` flag
  - Configures `crimson_cpu_num` (required for standalone tests)
  - Uses `crimson-osd` binary instead of `ceph-osd`

### 2. Message Protocol
- **Crimson requires msgr2**: Added `--ms-bind-msgr2=true --ms-bind-msgr1=false` to CEPH_ARGS

### 3. Test Coverage
The Crimson version includes the following tests:
- `TEST_scrub_test`: Basic scrub functionality with error detection and repair
- `TEST_interval_changes`: Dynamic scrub interval configuration changes
- `TEST_scrub_abort`: Scrub abortion with noscrub flag
- `TEST_deep_scrub_abort`: Deep scrub abortion with nodeep-scrub flag
- `TEST_pg_dump_objects_scrubbed`: Verification of objects_scrubbed counter

### 4. Omitted Tests
Some tests from the original script are not included because they rely on features not yet fully implemented in Crimson:
- `TEST_scrub_permit_time`: Time-based scrub scheduling
- `TEST_just_deep_scrubs`: Complex noscrub/nodeep-scrub interaction
- `TEST_dump_scrub_schedule`: Detailed scrub scheduling state inspection
- `TEST_abort_periodic_for_operator`: Advanced scrub priority and abortion logic

## Usage

### Running All Tests
```bash
cd build
../qa/run-standalone.sh osd-scrub-test-crimson.sh
```

### Running Specific Tests
```bash
cd build
../qa/run-standalone.sh osd-scrub-test-crimson.sh TEST_scrub_test
```

### Running Multiple Specific Tests
```bash
cd build
../qa/run-standalone.sh osd-scrub-test-crimson.sh TEST_scrub_test TEST_interval_changes
```

### Running with Verbose Output and Logging
This command runs the test with verbose output (`-v`), debug mode (`-x`), timestamps, and saves to a log file:
```bash
cd build
SFS="/tmp/crimson_scrub_test_`date +'%d_%T'`"
echo $SFS
date
time ../qa/run-standalone.sh -x -v "osd-scrub-test-crimson.sh" 2>&1 | awk '{ print strftime(),$0 }' > $SFS
date
tail $SFS
```

This is equivalent to the classic OSD test command but uses `osd-scrub-test-crimson.sh` instead of `osd-scrub-test.sh`.

## Prerequisites

1. **Crimson Binary**: The `crimson-osd` binary must be built and available in `./bin/crimson-osd` or `$CEPH_ROOT/build/bin/crimson-osd`

2. **Build Crimson**:
   ```bash
   cd build
   cmake -DWITH_SEASTAR=ON ..
   make -j$(nproc) crimson-osd
   ```

3. **Dependencies**: Ensure all Crimson dependencies (Seastar, etc.) are installed

## Known Limitations

1. **CPU Configuration**: Crimson requires explicit CPU configuration. The test sets `crimson_cpu_num=1` for each OSD.

2. **Feature Parity**: Some scrub features available in classic OSDs may not be fully implemented in Crimson yet.

3. **Performance**: Crimson OSDs may have different performance characteristics compared to classic OSDs.

4. **Admin Socket Commands**: Some admin socket commands may have different behavior or availability in Crimson.

## Troubleshooting

### Crimson Binary Not Found
```
ERROR: crimson-osd binary not found
```
**Solution**: Build Crimson with `make crimson-osd` in the build directory.

### msgr2 Protocol Errors
```
ERROR: failed to bind to msgr2
```
**Solution**: Ensure msgr2 is properly configured in CEPH_ARGS (already done in the script).

### CPU Configuration Errors
```
ERROR: crimson_cpu_num or crimson_cpu_set must be set
```
**Solution**: The script automatically sets `crimson_cpu_num=1`. If this fails, check cluster configuration.

### Test Failures
If tests fail, check:
1. Crimson OSD logs in the test directory
2. Monitor logs for cluster health issues
3. Ensure no other Ceph processes are running

## Future Enhancements

As Crimson development progresses, additional tests from the original `osd-scrub-test.sh` can be adapted:
- Time-based scrub scheduling tests
- More complex scrub priority and abortion scenarios
- Advanced scrub state machine testing
- Performance-specific scrub tests

## Contributing

When adding new tests:
1. Ensure they use `run_crimson_osd` instead of `run_osd`
2. Verify Crimson supports the features being tested
3. Add appropriate comments for Crimson-specific behavior
4. Update this README with new test descriptions

## References

- Original test: `qa/standalone/scrub/osd-scrub-test.sh`
- Crimson OSD helper: `qa/standalone/ceph-helpers.sh::run_crimson_osd`
- Crimson scrub implementation: `src/crimson/osd/scrub/`