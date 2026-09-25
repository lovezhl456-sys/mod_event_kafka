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
target_link_libraries(event_kafka_core PUBLIC SQLite::SQLite3 ${RDKAFKA_LIBRARIES} pthread)
target_compile_options(event_kafka_core PRIVATE ${RDKAFKA_CFLAGS_OTHER})

add_executable(test_outbox tests/test_outbox.cpp)
target_link_libraries(test_outbox PRIVATE event_kafka_core)
target_compile_options(test_outbox PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
target_link_options(test_outbox PRIVATE -fsanitize=address,undefined)

enable_testing()
add_test(NAME outbox COMMAND test_outbox)

add_executable(test_pipeline_smoke tests/test_pipeline_smoke.cpp)
target_link_libraries(test_pipeline_smoke PRIVATE event_kafka_core)
