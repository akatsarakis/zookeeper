cmake_minimum_required(VERSION 2.8)
project(zookeeper)

set(KeyValueCache_VERSION_MAJOR 1)
set(KeyValueCache_VERSION_MINOR 0)

include_directories( include/libhrd include/zookeeper include/optik include/mica /usr/include/)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall ")

set(SOURCE_FILES
        #Cache
        src/zookeeper/util.c
        src/zookeeper/cache.c
        src/zookeeper/main.c
        src/zookeeper/follower.c
        src/zookeeper/leader.c
        src/zookeeper/stats.c
        #libhrd
        src/libhrd/hrd_util.c
        src/libhrd/hrd_conn.c
        #mica
        src/mica/city.c
        src/mica/mica.c
        ##### header files ####
        #armonia
        include/zookeeper/cache.h
        include/zookeeper/inline_util.h
        include/zookeeper/main.h
        include/zookeeper/util.h
        #libhrd
        include/libhrd/hrd.h
        include/libhrd/hrd_sizes.h
        #mica
        include/mica/city.h
        include/mica/mica.h
        #optik
        include/optik/optik_mod.h
        include/optik/utils.h
        /usr/include/infiniband/verbs.h)



add_executable(zookeeper ${SOURCE_FILES})

target_link_libraries(zookeeper pthread ibverbs rt memcached numa rdmacm)


