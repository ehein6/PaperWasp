cmake_minimum_required (VERSION 3.3.1)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
project(PaperWasp C CXX)

# Use the 2011 standards for C and C++
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
# Turn warnings into errors
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")
# Enable Cilk
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fcilkplus")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcilkplus")

# Link with emu_c_utils
find_package(emu_c_utils REQUIRED)
link_libraries(${EMU_C_UTILS_LIBRARIES})
include_directories(${EMU_C_UTILS_INCLUDE_DIRS})

# Enable libc extenstions like getline
add_definitions(-D_XOPEN_SOURCE=700)

if (NOT CMAKE_SYSTEM_NAME STREQUAL "Emu1")
    add_subdirectory(generator)
endif()

include_directories(.)
add_executable(hybrid_bfs
    ack_control.h
    bitmap.h
    graph.h
    graph_from_edge_list.h
    graph_from_edge_list.c
    load_edge_list.h
    load_edge_list.c
    hybrid_bfs.h
    hybrid_bfs.c
    main.c
)
