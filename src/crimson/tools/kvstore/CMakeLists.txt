# Crimson KVStore Tool library
add_library(crimson-kvstore-tool STATIC
  kvstore_tool.cc
)

target_link_libraries(crimson-kvstore-tool
  crimson
  crimson-os
  crimson-seastore
)

# Executable
add_executable(crimson-kvstore-tool-bin
  crimson_kvstore_tool.cc
)

set_target_properties(crimson-kvstore-tool-bin
  PROPERTIES OUTPUT_NAME crimson-kvstore-tool
)

target_link_libraries(crimson-kvstore-tool-bin
  crimson-kvstore-tool
  crimson
  crimson-os
  crimson-seastore
  Seastar::seastar
  Boost::program_options
)

install(TARGETS crimson-kvstore-tool-bin DESTINATION bin)