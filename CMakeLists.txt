cmake_minimum_required(VERSION 3.16)
project(mod_event_kafka_outbox CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
find_package(SQLite3 REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka)

add_library(event_kafka_core STATIC
  src/kafka_outbox.cpp
  src/kafka_pipeline.cpp
)
target_include_directories(event_kafka_core PUBLIC include ${RDKAFKA_INCLUDE_DIRS})
target_link_directories(event_kafka_core PUBLIC ${RDKAFKA_LIBRARY_DIRS})
target_link_libraries(event_kafka_core PUBLIC SQLite::SQLite3 ${RDKAFKA_LIBRARIES} pthread)
target_compile_options(event_kafka_core PRIVATE ${RDKAFKA_CFLAGS_OTHER})

add_executable(test_outbox tests/test_outbox.cpp)
target_link_libraries(test_outbox PRIVATE event_kafka_core)
option(ENABLE_SANITIZERS "Instrument both the core and test executables" ON)
if(ENABLE_SANITIZERS)
  target_compile_options(event_kafka_core PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(event_kafka_core PUBLIC -fsanitize=address,undefined)
endif()

enable_testing()
add_test(NAME outbox COMMAND test_outbox)

add_executable(test_pipeline_smoke tests/test_pipeline_smoke.cpp)
target_link_libraries(test_pipeline_smoke PRIVATE event_kafka_core)

add_executable(test_pipeline_drill tests/test_pipeline_drill.cpp)
target_link_libraries(test_pipeline_drill PRIVATE event_kafka_core)

find_package(Python3 COMPONENTS Interpreter REQUIRED)
add_test(NAME drill_tools COMMAND ${Python3_EXECUTABLE} -m unittest discover
  -s ${CMAKE_SOURCE_DIR}/tests -p test_*.py -v)

add_executable(record_probe tests/record_probe.cpp)
target_include_directories(record_probe PRIVATE ${RDKAFKA_INCLUDE_DIRS})
target_link_directories(record_probe PRIVATE ${RDKAFKA_LIBRARY_DIRS})
target_link_libraries(record_probe PRIVATE ${RDKAFKA_LIBRARIES})

add_executable(test_ordering tests/test_ordering.cpp)
target_link_libraries(test_ordering PRIVATE event_kafka_core)
add_test(NAME ordering COMMAND test_ordering)

add_executable(test_ordering_drill tests/test_ordering_drill.cpp)
target_link_libraries(test_ordering_drill PRIVATE event_kafka_core)
add_executable(order_readback tests/order_readback.cpp)
target_include_directories(order_readback PRIVATE ${RDKAFKA_INCLUDE_DIRS})
target_link_directories(order_readback PRIVATE ${RDKAFKA_LIBRARY_DIRS})
target_link_libraries(order_readback PRIVATE ${RDKAFKA_LIBRARIES})

add_executable(test_ops_interop tests/test_ops_interop.cpp)
target_link_libraries(test_ops_interop PRIVATE event_kafka_core)
