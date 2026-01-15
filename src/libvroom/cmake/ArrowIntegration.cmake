# Apache Arrow Output Integration for libvroom
# Include this file AFTER defining vroom

if(LIBVROOM_ENABLE_ARROW)
    message(STATUS "Apache Arrow output integration enabled")
    find_package(Arrow REQUIRED)
    set(LIBVROOM_HAS_ARROW TRUE PARENT_SCOPE)

    target_sources(vroom PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_output.cpp)
    target_link_libraries(vroom PUBLIC Arrow::arrow_shared)
    target_compile_definitions(vroom PUBLIC LIBVROOM_ENABLE_ARROW)

    # Optional Parquet support (requires Arrow Parquet library)
    # Parquet is optional - Feather/Arrow IPC format works without it
    find_package(Parquet QUIET)
    if(Parquet_FOUND)
        message(STATUS "Apache Parquet support enabled")
        target_link_libraries(vroom PUBLIC Parquet::parquet_shared)
        target_compile_definitions(vroom PUBLIC LIBVROOM_ENABLE_PARQUET)
        set(LIBVROOM_HAS_PARQUET TRUE PARENT_SCOPE)
    else()
        message(STATUS "Apache Parquet not found - Parquet export disabled, Feather export available")
    endif()

    # Note: arrow_output_test is defined separately after GoogleTest is configured.
    # This file only handles adding Arrow support to vroom.
endif()
