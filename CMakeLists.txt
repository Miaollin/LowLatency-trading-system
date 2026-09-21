cmake_minimum_required(VERSION 3.10)

project(LowLatencyApp)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_COMPILER g++)
set(CMAKE_CXX_FLAGS "-std=c++2a -Wall -Wextra -Werror -Wpedantic")
set(CMAKE_VERBOSE_MAKEFILE on)

add_subdirectory(common)
add_subdirectory(exchange)
add_subdirectory(trading)
add_subdirectory(benchmarks)

list(APPEND LIBS libexchange)
list(APPEND LIBS libtrading)
list(APPEND LIBS libcommon)
list(APPEND LIBS pthread)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/exchange)
include_directories(${PROJECT_SOURCE_DIR}/trading)

add_executable(exchange_main exchange/exchange_main.cpp)
target_link_libraries(exchange_main PUBLIC ${LIBS})

add_executable(trading_main trading/trading_main.cpp)
target_link_libraries(trading_main PUBLIC ${LIBS})

add_executable(logger_benchmark benchmarks/logger_benchmark.cpp)
target_link_libraries(logger_benchmark PUBLIC ${LIBS})

add_executable(release_benchmark benchmarks/release_benchmark.cpp)
target_link_libraries(release_benchmark PUBLIC ${LIBS})

add_executable(hash_benchmark benchmarks/hash_benchmark.cpp)
target_link_libraries(hash_benchmark PUBLIC ${LIBS})

add_executable(tsc_measurement_benchmark benchmarks/tsc_measurement_benchmark.cpp)
target_link_libraries(tsc_measurement_benchmark PUBLIC pthread)

add_executable(matching_latency_benchmark
        benchmarks/matching_latency_benchmark.cpp
        exchange/matcher/matching_engine.cpp
        exchange/matcher/me_order_book.cpp
        exchange/matcher/me_order.cpp)
target_compile_definitions(matching_latency_benchmark PRIVATE
        LLT_BENCHMARK_MODE=1
        LLT_ME_MAX_TICKERS=1
        LLT_ME_MAX_NUM_CLIENTS=8
        LLT_ME_MAX_ORDER_IDS=65536
        LLT_ME_MAX_PRICE_LEVELS=256)
target_link_libraries(matching_latency_benchmark PUBLIC pthread)

add_executable(tick_to_trade_benchmark
        benchmarks/tick_to_trade_benchmark.cpp
        trading/strategy/trade_engine.cpp
        trading/strategy/market_order_book.cpp
        trading/strategy/market_order.cpp
        trading/strategy/market_maker.cpp
        trading/strategy/liquidity_taker.cpp
        trading/strategy/order_manager.cpp
        trading/strategy/risk_manager.cpp)
target_compile_definitions(tick_to_trade_benchmark PRIVATE
        LLT_BENCHMARK_MODE=1
        LLT_ME_MAX_TICKERS=1
        LLT_ME_MAX_NUM_CLIENTS=1
        LLT_ME_MAX_ORDER_IDS=65536
        LLT_ME_MAX_PRICE_LEVELS=256)
target_link_libraries(tick_to_trade_benchmark PUBLIC pthread)

add_executable(order_rtt_benchmark
        benchmarks/order_rtt_benchmark.cpp
        common/tcp_socket.cpp
        common/tcp_server.cpp
        exchange/order_server/order_server.cpp
        exchange/matcher/matching_engine.cpp
        exchange/matcher/me_order_book.cpp
        exchange/matcher/me_order.cpp
        trading/order_gw/order_gateway.cpp)
target_compile_definitions(order_rtt_benchmark PRIVATE
        LLT_BENCHMARK_MODE=1
        LLT_ORDER_RTT_BENCHMARK=1
        LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK=1
        LLT_TCP_BUFFER_SIZE=65536
        LLT_ME_MAX_TICKERS=1
        LLT_ME_MAX_NUM_CLIENTS=1
        LLT_ME_MAX_ORDER_IDS=65536
        LLT_ME_MAX_PRICE_LEVELS=256
        LLT_ME_MAX_CLIENT_UPDATES=4096
        LLT_ME_MAX_MARKET_UPDATES=4096)
target_link_libraries(order_rtt_benchmark PUBLIC pthread)

set(TICK_TO_KERNEL_SEND_BENCHMARK_SOURCES
        benchmarks/tick_to_kernel_send_benchmark.cpp
        common/mcast_socket.cpp
        common/tcp_socket.cpp
        common/tcp_server.cpp
        trading/market_data/market_data_consumer.cpp
        trading/order_gw/order_gateway.cpp
        trading/strategy/trade_engine.cpp
        trading/strategy/market_order_book.cpp
        trading/strategy/market_order.cpp
        trading/strategy/market_maker.cpp
        trading/strategy/liquidity_taker.cpp
        trading/strategy/order_manager.cpp
        trading/strategy/risk_manager.cpp)

set(TICK_TO_KERNEL_SEND_BENCHMARK_DEFINITIONS
        LLT_BENCHMARK_MODE=1
        LLT_TICK_TO_KERNEL_SEND_BENCHMARK=1
        LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK=1
        LLT_TCP_BUFFER_SIZE=65536
        LLT_MCAST_BUFFER_SIZE=65536
        LLT_ME_MAX_TICKERS=1
        LLT_ME_MAX_NUM_CLIENTS=1
        LLT_ME_MAX_ORDER_IDS=65536
        LLT_ME_MAX_PRICE_LEVELS=256
        LLT_ME_MAX_CLIENT_UPDATES=4096
        LLT_ME_MAX_MARKET_UPDATES=4096)

add_executable(tick_to_kernel_send_benchmark
        ${TICK_TO_KERNEL_SEND_BENCHMARK_SOURCES})
target_compile_definitions(tick_to_kernel_send_benchmark PRIVATE
        ${TICK_TO_KERNEL_SEND_BENCHMARK_DEFINITIONS})
target_link_libraries(tick_to_kernel_send_benchmark PUBLIC pthread)

add_executable(tick_to_kernel_send_stage_benchmark
        ${TICK_TO_KERNEL_SEND_BENCHMARK_SOURCES})
target_compile_definitions(tick_to_kernel_send_stage_benchmark PRIVATE
        ${TICK_TO_KERNEL_SEND_BENCHMARK_DEFINITIONS}
        LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC=1)
target_link_libraries(tick_to_kernel_send_stage_benchmark PUBLIC pthread)
