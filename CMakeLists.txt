cmake_minimum_required(VERSION 3.25)

project(
    kura_metadata_store
    VERSION 0.1.0
    DESCRIPTION "A strongly consistent metadata store built from first principles"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(KURA_ENABLE_SANITIZERS "Enable address and undefined-behavior sanitizers" OFF)
option(KURA_ENABLE_THREAD_SANITIZER "Enable the thread sanitizer" OFF)

if(KURA_ENABLE_SANITIZERS AND KURA_ENABLE_THREAD_SANITIZER)
    message(FATAL_ERROR "Address and thread sanitizers cannot be enabled together")
endif()

find_package(Threads REQUIRED)

add_library(
    kura_metadata
    src/byte_sequence.cpp
    src/durable_storage.cpp
    src/figure2_spec.cpp
    src/raft_election.cpp
    src/raft_hard_state_store.cpp
    src/kura_client.cpp
    src/key_range.cpp
    src/in_memory_metadata_store.cpp
    src/raft_simulation.cpp)

target_include_directories(
    kura_metadata
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(kura_metadata PUBLIC Threads::Threads)
target_compile_features(kura_metadata PUBLIC cxx_std_23)

if(MSVC)
    target_compile_options(kura_metadata PRIVATE /W4 /permissive-)
else()
    target_compile_options(kura_metadata PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(KURA_ENABLE_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(kura_metadata PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(kura_metadata PUBLIC -fsanitize=address,undefined)
endif()

if(KURA_ENABLE_THREAD_SANITIZER AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(kura_metadata PUBLIC -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(kura_metadata PUBLIC -fsanitize=thread)
endif()

include(CTest)

if(BUILD_TESTING)
    add_executable(
        kura_metadata_tests
        tests/in_memory_metadata_store_test.cpp)

    target_link_libraries(kura_metadata_tests PRIVATE kura_metadata)

    if(MSVC)
        target_compile_options(kura_metadata_tests PRIVATE /W4 /permissive-)
    else()
        target_compile_options(kura_metadata_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    add_test(NAME kura_metadata_tests COMMAND kura_metadata_tests)

    add_executable(
        kura_durable_storage_tests
        tests/durable_storage_test.cpp)

    target_link_libraries(kura_durable_storage_tests PRIVATE kura_metadata)

    if(MSVC)
        target_compile_options(kura_durable_storage_tests PRIVATE /W4 /permissive-)
    else()
        target_compile_options(kura_durable_storage_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    add_test(
        NAME kura_durable_storage_tests
        COMMAND kura_durable_storage_tests)

    add_executable(
        kura_figure2_spec_tests
        tests/figure2_spec_test.cpp)

    target_link_libraries(kura_figure2_spec_tests PRIVATE kura_metadata)

    if(MSVC)
        target_compile_options(kura_figure2_spec_tests PRIVATE /W4 /permissive-)
    else()
        target_compile_options(kura_figure2_spec_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    add_test(
        NAME kura_figure2_spec_tests
        COMMAND kura_figure2_spec_tests)

    add_executable(
        kura_raft_simulation_tests
        tests/raft_simulation_test.cpp)

    target_link_libraries(kura_raft_simulation_tests PRIVATE kura_metadata)

    if(MSVC)
        target_compile_options(kura_raft_simulation_tests PRIVATE /W4 /permissive-)
    else()
        target_compile_options(kura_raft_simulation_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    add_test(
        NAME kura_raft_simulation_tests
        COMMAND kura_raft_simulation_tests)

    add_executable(
        kura_raft_hard_state_tests
        tests/raft_hard_state_store_test.cpp)

    target_link_libraries(kura_raft_hard_state_tests PRIVATE kura_metadata)

    if(MSVC)
        target_compile_options(kura_raft_hard_state_tests PRIVATE /W4 /permissive-)
    else()
        target_compile_options(kura_raft_hard_state_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    add_test(
        NAME kura_raft_hard_state_tests
        COMMAND kura_raft_hard_state_tests)

    add_executable(
        kura_raft_election_tests
        tests/raft_election_test.cpp)

    target_link_libraries(kura_raft_election_tests PRIVATE kura_metadata)

    if(MSVC)
        target_compile_options(kura_raft_election_tests PRIVATE /W4 /permissive-)
    else()
        target_compile_options(kura_raft_election_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    add_test(
        NAME kura_raft_election_tests
        COMMAND kura_raft_election_tests)

    add_executable(
        kura_client_tests
        tests/kura_client_test.cpp)

    target_link_libraries(kura_client_tests PRIVATE kura_metadata)

    if(MSVC)
        target_compile_options(kura_client_tests PRIVATE /W4 /permissive-)
    else()
        target_compile_options(kura_client_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    add_test(NAME kura_client_tests COMMAND kura_client_tests)
endif()
