add_executable(memcached_client client.cpp)
target_link_libraries(memcached_client PRIVATE pthread)

# libsei paths (assumes libsei-gcc is checked out alongside Orthrus)
set(LIBSEI_GCC_ROOT ${CMAKE_CURRENT_LIST_DIR}/../../../libsei-gcc)
set(LIBSEI_INCLUDE ${LIBSEI_GCC_ROOT}/include)
set(LIBSEI_LIB ${LIBSEI_GCC_ROOT}/build/libsei.a)
set(LIBCRC_LIB ${LIBSEI_GCC_ROOT}/build/libcrc.a)

target_include_directories(memcached_client PRIVATE ${LIBSEI_INCLUDE})
target_link_libraries(memcached_client PRIVATE ${LIBCRC_LIB})

# Additional libsei builds for memcached SEI variants.
set(LIBSEI_BUILD_DYNAMIC_NWAY ${LIBSEI_GCC_ROOT}/build_dyn_nway_er5_rb)
set(LIBSEI_LIB_DYNAMIC_NWAY ${LIBSEI_BUILD_DYNAMIC_NWAY}/libsei.a)
set(LIBCRC_LIB_DYNAMIC_NWAY ${LIBSEI_BUILD_DYNAMIC_NWAY}/libcrc.a)

set(LIBSEI_BUILD_DYNAMIC_CORE ${LIBSEI_GCC_ROOT}/build_dyn_core_rb)
set(LIBSEI_LIB_DYNAMIC_CORE ${LIBSEI_BUILD_DYNAMIC_CORE}/libsei.a)
set(LIBCRC_LIB_DYNAMIC_CORE ${LIBSEI_BUILD_DYNAMIC_CORE}/libcrc.a)

# Static core redundancy build.
set(LIBSEI_BUILD_CORE ${LIBSEI_GCC_ROOT}/build_core1_only)
set(LIBSEI_LIB_CORE ${LIBSEI_BUILD_CORE}/libsei.a)
set(LIBCRC_LIB_CORE ${LIBSEI_BUILD_CORE}/libcrc.a)

# Static N-way SEI builds (no rollback, no core migration).
set(LIBSEI_BUILD_ER2_NOMIG ${LIBSEI_GCC_ROOT}/build_er2_nomig)
set(LIBSEI_LIB_ER2_NOMIG ${LIBSEI_BUILD_ER2_NOMIG}/libsei.a)
set(LIBCRC_LIB_ER2_NOMIG ${LIBSEI_BUILD_ER2_NOMIG}/libcrc.a)

set(LIBSEI_BUILD_ER5_NOMIG ${LIBSEI_GCC_ROOT}/build_er5_nomig)
set(LIBSEI_LIB_ER5_NOMIG ${LIBSEI_BUILD_ER5_NOMIG}/libsei.a)
set(LIBCRC_LIB_ER5_NOMIG ${LIBSEI_BUILD_ER5_NOMIG}/libcrc.a)

set(LIBSEI_BUILD_ER10_NOMIG ${LIBSEI_GCC_ROOT}/build_er10_nomig)
set(LIBSEI_LIB_ER10_NOMIG ${LIBSEI_BUILD_ER10_NOMIG}/libsei.a)
set(LIBCRC_LIB_ER10_NOMIG ${LIBSEI_BUILD_ER10_NOMIG}/libcrc.a)

add_executable(memcached_vanilla vanilla/server.cpp vanilla/hashmap.cpp)
target_link_libraries(memcached_vanilla PRIVATE ${LIBS_disabled})

add_executable(memcached_vanilla_mem vanilla/server.cpp vanilla/hashmap.cpp)
target_link_libraries(memcached_vanilla_mem PRIVATE ${LIBS_disabled} profile_mem)
target_compile_definitions(memcached_vanilla_mem PRIVATE PROFILE_MEM)

add_executable(memcached_sei sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB} ${LIBCRC_LIB} dl m)

add_executable(memcached_sei_mem sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_mem PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_mem PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_mem PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB} ${LIBCRC_LIB} dl m profile_mem)
target_compile_definitions(memcached_sei_mem PRIVATE PROFILE_MEM)

add_executable(memcached_sei_dynamic_nway sei/server_dynamicNway.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_dynamic_nway PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_dynamic_nway PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_dynamic_nway PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_DYNAMIC_NWAY} ${LIBCRC_LIB_DYNAMIC_NWAY} dl m)

add_executable(memcached_sei_dynamic_nway_mem sei/server_dynamicNway.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_dynamic_nway_mem PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_dynamic_nway_mem PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_dynamic_nway_mem PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_DYNAMIC_NWAY} ${LIBCRC_LIB_DYNAMIC_NWAY} dl m profile_mem)
target_compile_definitions(memcached_sei_dynamic_nway_mem PRIVATE PROFILE_MEM)

add_executable(memcached_sei_dynamic_core sei/server_dynamicCore.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_dynamic_core PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_dynamic_core PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_dynamic_core PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_DYNAMIC_CORE} ${LIBCRC_LIB_DYNAMIC_CORE} dl m)

add_executable(memcached_sei_dynamic_core_mem sei/server_dynamicCore.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_dynamic_core_mem PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_dynamic_core_mem PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_dynamic_core_mem PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_DYNAMIC_CORE} ${LIBCRC_LIB_DYNAMIC_CORE} dl m profile_mem)
target_compile_definitions(memcached_sei_dynamic_core_mem PRIVATE PROFILE_MEM)

add_executable(memcached_sei_core sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_core PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_core PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_core PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_CORE} ${LIBCRC_LIB_CORE} dl m)

add_executable(memcached_sei_core_mem sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_core_mem PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_core_mem PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_core_mem PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_CORE} ${LIBCRC_LIB_CORE} dl m profile_mem)
target_compile_definitions(memcached_sei_core_mem PRIVATE PROFILE_MEM)

add_executable(memcached_sei_er2 sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_er2 PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_er2 PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_er2 PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_ER2_NOMIG} ${LIBCRC_LIB_ER2_NOMIG} dl m)

add_executable(memcached_sei_er2_mem sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_er2_mem PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_er2_mem PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_er2_mem PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_ER2_NOMIG} ${LIBCRC_LIB_ER2_NOMIG} dl m profile_mem)
target_compile_definitions(memcached_sei_er2_mem PRIVATE PROFILE_MEM)

add_executable(memcached_sei_er5 sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_er5 PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_er5 PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_er5 PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_ER5_NOMIG} ${LIBCRC_LIB_ER5_NOMIG} dl m)

add_executable(memcached_sei_er5_mem sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_er5_mem PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_er5_mem PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_er5_mem PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_ER5_NOMIG} ${LIBCRC_LIB_ER5_NOMIG} dl m profile_mem)
target_compile_definitions(memcached_sei_er5_mem PRIVATE PROFILE_MEM)

add_executable(memcached_sei_er10 sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_er10 PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_er10 PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_er10 PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_ER10_NOMIG} ${LIBCRC_LIB_ER10_NOMIG} dl m)

add_executable(memcached_sei_er10_mem sei/server.cpp sei/hashmap.cpp sei/pthread_tm_wrappers.cpp)
target_include_directories(memcached_sei_er10_mem PRIVATE ${LIBSEI_INCLUDE})
target_compile_options(memcached_sei_er10_mem PRIVATE -fgnu-tm -fno-omit-frame-pointer -fno-builtin)
target_link_libraries(memcached_sei_er10_mem PRIVATE Threads::Threads sei_itm_shim ${LIBSEI_LIB_ER10_NOMIG} ${LIBCRC_LIB_ER10_NOMIG} dl m profile_mem)
target_compile_definitions(memcached_sei_er10_mem PRIVATE PROFILE_MEM)

add_library(memcached_orthrus_raw orthrus/closure.cpp)
target_compile_definitions(memcached_orthrus_raw PRIVATE NAMESPACE=raw)

add_library(memcached_orthrus_app orthrus/closure.cpp)
target_compile_definitions(memcached_orthrus_app PRIVATE NAMESPACE=app)

add_library(memcached_orthrus_val orthrus/closure.cpp)
target_compile_definitions(memcached_orthrus_val PRIVATE NAMESPACE=validator)

add_executable(memcached_orthrus orthrus/server.cpp)
target_link_libraries(memcached_orthrus PRIVATE ${LIBS}
    memcached_orthrus_raw
    memcached_orthrus_app
    memcached_orthrus_val
)

add_executable(memcached_orthrus_profile orthrus/server.cpp)
target_link_libraries(memcached_orthrus_profile PRIVATE ${LIBS_profile}
    memcached_orthrus_raw
    memcached_orthrus_app
    memcached_orthrus_val
)
target_compile_definitions(memcached_orthrus_profile PRIVATE PROFILE)

add_executable(memcached_orthrus_mem orthrus/server.cpp)
target_link_libraries(memcached_orthrus_mem PRIVATE ${LIBS}
    memcached_orthrus_raw
    memcached_orthrus_app
    memcached_orthrus_val
    profile_mem
)
target_compile_definitions(memcached_orthrus_mem PRIVATE PROFILE_MEM)

add_executable(memcached_orthrus_sync orthrus/server.cpp)
target_link_libraries(memcached_orthrus_sync PRIVATE ${LIBS}
    memcached_orthrus_raw
    memcached_orthrus_app
    memcached_orthrus_val
)
target_compile_definitions(memcached_orthrus_sync PRIVATE SCEE_SYNC_VALIDATE)

add_executable(memcached_orthrus_sync_mem orthrus/server.cpp)
target_link_libraries(memcached_orthrus_sync_mem PRIVATE ${LIBS}
    memcached_orthrus_raw
    memcached_orthrus_app
    memcached_orthrus_val
    profile_mem
)
target_compile_definitions(memcached_orthrus_sync_mem PRIVATE PROFILE_MEM SCEE_SYNC_VALIDATE)

add_executable(memcached_rbv_primary rbv/primary.cpp rbv/hashmap.cpp)
target_link_libraries(memcached_rbv_primary PRIVATE ${LIBS_disabled} rbv_primary_lib)
target_compile_definitions(memcached_rbv_primary PRIVATE RBV_PRIMARY)

add_executable(memcached_rbv_replica rbv/replica.cpp rbv/hashmap.cpp)
target_link_libraries(memcached_rbv_replica PRIVATE ${LIBS_disabled} rbv_replica_lib)

add_executable(memcached_rbv_primary_mem rbv/primary.cpp rbv/hashmap.cpp)
target_link_libraries(memcached_rbv_primary_mem PRIVATE ${LIBS_disabled} rbv_primary_lib profile_mem)
target_compile_definitions(memcached_rbv_primary_mem PRIVATE PROFILE_MEM RBV_PRIMARY)

add_executable(memcached_rbv_replica_mem rbv/replica.cpp rbv/hashmap.cpp)
target_link_libraries(memcached_rbv_replica_mem PRIVATE ${LIBS_disabled} rbv_replica_lib profile_mem)
target_compile_definitions(memcached_rbv_replica_mem PRIVATE PROFILE_MEM)

add_executable(memcached_rbv_replica_profile rbv/replica.cpp rbv/hashmap.cpp)
target_link_libraries(memcached_rbv_replica_profile PRIVATE ${LIBS_disabled_profile} rbv_replica_lib)
target_compile_definitions(memcached_rbv_replica_profile PRIVATE PROFILE)
