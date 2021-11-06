cmake_minimum_required ( VERSION 2.8.5 )
cmake_policy ( SET CMP0054 NEW )
project ( Starter )

include ( etc/build_defs.cmake )
include ( etc/build_type.cmake )
include ( etc/cflags.cmake )
include ( etc/clang_format.cmake )
include ( etc/clang_tidy.cmake )

include ( FindOpenSSL )

file ( GLOB SOURCE
  src/util/*
  src/net/*
  src/remote/*
  src/schedulers/*
  src/simulator/*
  src/nat/*
  src/storage/*
)

include_directories ( src )

add_library( starter STATIC
  ${SOURCE}

)

set ( ALL_LIBS
  starter
  ${OPENSSL_LIBRARIES}
)

# Flags for building static binaries for AWS Lambda
set ( STATIC_LINK_FLAGS dl z unwind lzma -static -Wl,-allow-multiple-definition
                         -Wl,--whole-archive -lpthread -Wl,--no-whole-archive
                         -s )
# if ( CMAKE_BUILD_TYPE MATCHES Debug OR CMAKE_BUILD_TYPE MATCHES ASan )
#   set( STATIC_LINK_FLAGS "" )
# endif()

add_executable ( coordinator src/frontend/coordinator.cc )
target_link_libraries( coordinator ${ALL_LIBS} )

add_executable ( lambdafunc src/frontend/lambdafunc.cc )
target_link_libraries( lambdafunc ${ALL_LIBS} ${STATIC_LINK_FLAGS} )

add_executable ( storageserver src/frontend/storageserver.cc )
target_link_libraries( storageserver ${ALL_LIBS} ${STATIC_LINK_FLAGS} )

add_executable ( localclient src/frontend/localclient.cc )
target_link_libraries( localclient ${ALL_LIBS} ${STATIC_LINK_FLAGS} )
