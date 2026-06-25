// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <string>
#include <vector>
#include <fstream>

#include <seastar/core/app-template.hh>
#include <seastar/core/print.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/log-cli.hh>

#include <boost/program_options.hpp>

#include "crimson/common/log.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"
#include "crimson/tools/kvstore/kvstore_tool.h"

namespace po = boost::program_options;

using crimson::tools::kvstore::KVStoreTool;

/**
 * Usage:
 *   crimson-kvstore-tool <device> <command> [args...]
 *
 * Commands:
 *   list [prefix]           - List all keys (or with prefix)
 *   list-crc [prefix]       - List keys with CRC32
 *   dump [prefix]           - Dump keys and values (hex)
 *   exists <prefix> <key>   - Check if key exists
 *   get <prefix> <key>      - Get value for key
 *   crc <prefix> <key>      - Get CRC32 of key+value
 *   set <prefix> <key> <value> - Set key-value pair
 *   rm <prefix> <key>       - Remove key
 *   rm-prefix <prefix>      - Remove all keys with prefix
 *   get-size [prefix] [key] - Get store size estimate (or key size)
 *   stats                   - Print statistics
 *   histogram [prefix]      - Build size histogram
 *   compact                 - Compact (automatic in SeaStore)
 *   compact-prefix <prefix> - Compact prefix
 */

static void print_usage(const char* argv0) {
  std::cerr << "Usage: " << argv0
            << " <device> <command> [args...]\n"
            << "\nCommands:\n"
            << "  list [prefix]           - List all keys (or with prefix)\n"
            << "  list-crc [prefix]       - List keys with CRC32\n"
            << "  dump [prefix]           - Dump keys and values (hex)\n"
            << "  exists <prefix> <key>   - Check if key exists\n"
            << "  get <prefix> <key>      - Get value for key\n"
            << "  crc <prefix> <key>      - Get CRC32 of key+value\n"
            << "  set <prefix> <key> <value> - Set key-value pair\n"
            << "  rm <prefix> <key>       - Remove key\n"
            << "  rm-prefix <prefix>      - Remove all keys with prefix\n"
            << "  get-size [prefix] [key] - Get store size estimate (or key size)\n"
            << "  stats                   - Print statistics\n"
            << "  histogram [prefix]      - Build size histogram\n"
            << "  compact                 - Compact (automatic in SeaStore)\n"
            << "  compact-prefix <prefix> - Compact prefix\n"
            << std::endl;
}

int main(int argc, char** argv) {
  seastar::app_template app;

  try {
    app.run(argc, argv, [&] {
      // Parse command-line arguments
      std::vector<std::string> args = {
        argv + 1, argv + argc
      };

      if (args.size() < 2) {
        print_usage(argv[0]);
        return seastar::make_ready_future<>();
      }

      std::string device_path = args[0];
      std::string command = args[1];

      // Create KVStoreTool instance
      bool ephemeral = (device_path == "ephemeral" || device_path == "memory");
      auto tool = KVStoreTool::create(device_path, ephemeral);

      // Initialize
      return tool->init().then([&, tool = std::move(tool)]() mutable {
        // Execute command
        if (command == "list" || command == "list-crc") {
          std::string prefix = (args.size() > 2) ? args[2] : "";
          bool do_crc = (command == "list-crc");

          return tool->traverse(prefix, do_crc, false, &std::cout)
            .then([](uint32_t crc) {
              return seastar::make_ready_future<>();
            });

        } else if (command == "dump") {
          std::string prefix = (args.size() > 2) ? args[2] : "";
          return tool->traverse(prefix, true, true, &std::cout)
            .then([](uint32_t crc) {
              return seastar::make_ready_future<>();
            });

        } else if (command == "exists") {
          if (args.size() < 4) {
            std::cerr << "Usage: exists <prefix> <key>" << std::endl;
            return seastar::make_ready_future<>();
          }
          std::string prefix = args[2];
          std::string key = args[3];
          return tool->exists(prefix, key).then([](bool exists) {
            std::cout << (exists ? "true" : "false") << std::endl;
            return seastar::make_ready_future<>();
          });

        } else if (command == "get") {
          if (args.size() < 4) {
            std::cerr << "Usage: get <prefix> <key>" << std::endl;
            return seastar::make_ready_future<>();
          }
          std::string prefix = args[2];
          std::string key = args[3];
          return tool->get(prefix, key).then([](ceph::bufferlist val) {
            std::cout << std::string(val.c_str(), val.length()) << std::endl;
            return seastar::make_ready_future<>();
          }).then_wrapped([](auto&& fut) {
            try {
              fut.get();
            } catch (const crimson::ct_error::enoent& e) {
              std::cerr << "Key not found" << std::endl;
            } catch (...) {
              std::cerr << "Unexpected error" << std::endl;
            }
            return seastar::make_ready_future<>();
          });

        } else if (command == "crc") {
          if (args.size() < 4) {
            std::cerr << "Usage: crc <prefix> <key>" << std::endl;
            return seastar::make_ready_future<>();
          }
          std::string prefix = args[2];
          std::string key = args[3];
          return tool->get(prefix, key).then([](ceph::bufferlist val) {
            uint32_t crc = val.crc32c(0);
            std::cout << "CRC32: " << crc << std::endl;
            return seastar::make_ready_future<>();
          }).then_wrapped([](auto&& fut) {
            try {
              fut.get();
            } catch (const crimson::ct_error::enoent& e) {
              std::cerr << "Key not found" << std::endl;
            } catch (...) {
              std::cerr << "Unexpected error" << std::endl;
            }
            return seastar::make_ready_future<>();
          });

        } else if (command == "set") {
          if (args.size() < 5) {
            std::cerr << "Usage: set <prefix> <key> <value>" << std::endl;
            return seastar::make_ready_future<>();
          }
          std::string prefix = args[2];
          std::string key = args[3];
          std::string value = args[4];
          ceph::bufferlist bl;
          bl.append(value);
          return tool->set(prefix, key, bl).then([] {
            std::cout << "OK" << std::endl;
            return seastar::make_ready_future<>();
          });

        } else if (command == "rm") {
          if (args.size() < 4) {
            std::cerr << "Usage: rm <prefix> <key>" << std::endl;
            return seastar::make_ready_future<>();
          }
          std::string prefix = args[2];
          std::string key = args[3];
          return tool->rm(prefix, key).then([] {
            std::cout << "OK" << std::endl;
            return seastar::make_ready_future<>();
          });

        } else if (command == "rm-prefix") {
          if (args.size() < 3) {
            std::cerr << "Usage: rm-prefix <prefix>" << std::endl;
            return seastar::make_ready_future<>();
          }
          std::string prefix = args[2];
          return tool->rm_prefix(prefix).then([](uint64_t count) {
            std::cout << "Removed " << count << " keys" << std::endl;
            return seastar::make_ready_future<>();
          });

        } else if (command == "stats") {
          return tool->stats().then([](std::string stats) {
            std::cout << stats << std::endl;
            return seastar::make_ready_future<>();
          });

        } else if (command == "histogram") {
          std::string prefix = (args.size() > 2) ? args[2] : "";
          return tool->histogram(prefix).then([] {
            return seastar::make_ready_future<>();
          });

        } else if (command == "get-size") {
          std::string prefix = (args.size() > 2) ? args[2] : "";
          std::string key = (args.size() > 3) ? args[3] : "";
          return tool->get_size(prefix, key).then([prefix, key](uint64_t size) {
            if (!prefix.empty() && !key.empty()) {
              std::cout << "(" << prefix << "," << key << ") size " << size << " bytes" << std::endl;
            } else {
              std::cout << "estimated store size: " << size << " bytes" << std::endl;
            }
            return seastar::make_ready_future<>();
          }).then_wrapped([](auto&& fut) {
            try {
              fut.get();
            } catch (const crimson::ct_error::enoent& e) {
              std::cerr << "Key not found" << std::endl;
            } catch (...) {
              std::cerr << "Unexpected error" << std::endl;
            }
            return seastar::make_ready_future<>();
          });

        } else if (command == "compact" || command == "compact-prefix") {
          // SeaStore performs automatic segment cleaning; no manual compaction needed
          std::cout << "SeaStore performs automatic segment cleaning; "
                    << "no manual compaction needed" << std::endl;
          return seastar::make_ready_future<>();

        } else {
          std::cerr << "Unknown command: " << command << std::endl;
          print_usage(argv[0]);
          return seastar::make_ready_future<>();
        }
      }).then_wrapped([](auto&& fut) {
        // Catch initialization errors
        try {
          fut.get();
        } catch (const std::exception& e) {
          std::cerr << "Initialization error: " << e.what() << std::endl;
        } catch (...) {
          std::cerr << "Unknown initialization error" << std::endl;
        }
        return seastar::make_ready_future<>();
      });
    });
  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}