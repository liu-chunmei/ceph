# Crimson KVStore Tool

## Overview

The `crimson-kvstore-tool` is a command-line utility for directly accessing and manipulating Crimson/Seastore's internal key-value store. It is the Crimson equivalent of `ceph-kvstore-tool` for BlueStore.

## Architecture

### Key Components

1. **KVStoreTool Class** ([`kvstore_tool.h`](kvstore_tool.h), [`kvstore_tool.cc`](kvstore_tool.cc))
   - Core library providing async/Seastar-based KV operations
   - Wraps Seastore's TransactionManager and OMapManager
   - Provides operations: list, get, set, rm, exists, traverse, etc.

2. **Main Entry Point** ([`crimson_kvstore_tool.cc`](crimson_kvstore_tool.cc))
   - Command-line interface similar to ceph-kvstore-tool
   - Initializes Seastore components (segment manager, cache, journal, etc.)
   - Parses commands and invokes KVStoreTool operations

### Design Decisions

#### Async/Seastar Architecture
Unlike the synchronous `ceph-kvstore-tool`, this implementation uses Seastar's async model:
- All operations return `seastar::future<>`
- Uses Crimson's errorator for error handling
- Runs in Seastar reactor context

#### Seastore Integration
The tool directly accesses Seastore's internal structures:
- **TransactionManager**: Manages transactions and persistence
- **OMapManager**: Provides B-tree based key-value operations
- **omap_root_t**: Root structure for the KV tree

#### Key Differences from BlueStore Tool

| Feature | BlueStore (ceph-kvstore-tool) | Crimson (crimson-kvstore-tool) |
|---------|-------------------------------|--------------------------------|
| Backend | RocksDB via KeyValueDB | Seastore OMapManager |
| Architecture | Synchronous | Async (Seastar) |
| Compaction | Manual RocksDB compaction | Automatic segment cleaning |
| Transactions | KeyValueDB::Transaction | Seastore Transaction |
| Iteration | WholeSpaceIterator | omap_iterate callback |

## Usage

### Basic Commands

```bash
# List all keys
crimson-kvstore-tool <path> list

# List keys with prefix
crimson-kvstore-tool <path> list <prefix>

# List with CRC values
crimson-kvstore-tool <path> list-crc <prefix>

# Dump keys and values
crimson-kvstore-tool <path> dump <prefix>

# Check if key exists
crimson-kvstore-tool <path> exists <prefix> <key>

# Get value
crimson-kvstore-tool <path> get <prefix> <key>

# Get CRC of key-value
crimson-kvstore-tool <path> crc <prefix> <key>

# Set key-value
crimson-kvstore-tool <path> set <prefix> <key> <value>

# Remove key
crimson-kvstore-tool <path> rm <prefix> <key>

# Remove all keys with prefix
crimson-kvstore-tool <path> rm-prefix <prefix>

# Get store size estimate
crimson-kvstore-tool <path> get-size

# Print statistics
crimson-kvstore-tool <path> stats

# Build size histogram
crimson-kvstore-tool <path> histogram <prefix>

# Compact (automatic in Seastore)
crimson-kvstore-tool <path> compact
crimson-kvstore-tool <path> compact-prefix <prefix>
```

### Examples

```bash
# List all metadata keys
crimson-kvstore-tool /var/lib/ceph/osd/ceph-0 list M

# Get a specific onode
crimson-kvstore-tool /var/lib/ceph/osd/ceph-0 get O <object-key>

# Set a test key
crimson-kvstore-tool /var/lib/ceph/osd/ceph-0 set TEST mykey myvalue

# Check if key exists
crimson-kvstore-tool /var/lib/ceph/osd/ceph-0 exists TEST mykey

# Remove test keys
crimson-kvstore-tool /var/lib/ceph/osd/ceph-0 rm-prefix TEST
```

## Implementation Details

### Error Handling

The tool uses Crimson's errorator pattern for type-safe error handling:

```cpp
using get_ertr = crimson::errorator<
  crimson::ct_error::input_output_error,
  crimson::ct_error::enoent>;
using get_ret = get_ertr::future<ceph::bufferlist>;
```

### Transaction Management

All write operations are wrapped in Seastore transactions:

```cpp
create_transaction()
  .safe_then([](TransactionRef t) {
    return omap_mgr->omap_set_key(root, *t, key, value)
      .safe_then([t] {
        return submit_transaction(t);
      });
  });
```

### Iteration

The tool uses OMapManager's callback-based iteration:

```cpp
omap_mgr->omap_iterate(
  root, *t, prefix,
  [](const std::string& key, const ceph::bufferlist& val) {
    // Process key-value pair
    return seastar::make_ready_future<bool>(true); // continue
  }
);
```

## Comparison with ceph-kvstore-tool

### Similarities
- Command-line interface and command names
- URL escaping for keys with special characters
- Support for prefix-based operations
- CRC calculation for verification

### Differences

#### 1. Store Access
- **BlueStore**: Opens RocksDB directly via `BlueStore::open_db_environment()`
- **Crimson**: Initializes full Seastore stack (segment manager, cache, journal, etc.)

#### 2. Key Organization
- **BlueStore**: Uses RocksDB column families (prefixes)
- **Crimson**: Uses single OMap tree with prefix-based keys

#### 3. Compaction
- **BlueStore**: Manual RocksDB compaction commands
- **Crimson**: Automatic segment cleaning (compaction commands are no-ops)

#### 4. Performance
- **BlueStore**: Synchronous, blocking operations
- **Crimson**: Async, non-blocking with Seastar

## Building

The tool is built as part of the Crimson build:

```bash
cd build
cmake -DWITH_SEASTAR=ON ..
make crimson-kvstore-tool
```

The executable will be at: `build/bin/crimson-kvstore-tool`

## Testing

### Unit Tests
(To be implemented)

### Integration Tests
Test the tool with a Crimson OSD:

```bash
# Create test OSD
ceph-osd --mkfs -i 0 --osd-objectstore crimson

# Use tool to inspect
crimson-kvstore-tool /var/lib/ceph/osd/ceph-0 list

# Verify operations
crimson-kvstore-tool /var/lib/ceph/osd/ceph-0 set TEST key1 value1
crimson-kvstore-tool /var/lib/ceph/osd/ceph-0 get TEST key1
crimson-kvstore-tool /var/lib/ceph/osd/ceph-0 rm TEST key1
```

## Future Enhancements

1. **Read-only Mode**: Add support for read-only access (currently all operations require write access)
2. **Store Copy**: Implement `store-copy` command for migrating data
3. **Repair**: Add destructive repair capabilities
4. **Statistics**: Expose detailed Seastore statistics
5. **Batch Operations**: Support batch set/rm operations for efficiency
6. **Snapshot Support**: Add commands for working with Seastore snapshots

## References

- Original tool: [`src/tools/ceph_kvstore_tool.cc`](../../../tools/ceph_kvstore_tool.cc)
- BlueStore KV: [`src/tools/kvstore_tool.cc`](../../../tools/kvstore_tool.cc)
- Seastore OMap: [`src/crimson/os/seastore/omap_manager.h`](../../os/seastore/omap_manager.h)
- Transaction Manager: [`src/crimson/os/seastore/transaction_manager.h`](../../os/seastore/transaction_manager.h)