CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

SET(libgopherwood_VERSION_MAJOR 0)
SET(libgopherwood_VERSION_MINOR 0)
SET(libgopherwood_VERSION_PATCH 1)
SET(libgopherwood_VERSION_STRING "${libgopherwood_VERSION_MAJOR}.${libgopherwood_VERSION_MINOR}.${libgopherwood_VERSION_PATCH}")
SET(libgopherwood_VERSION_API 1)
SET(libgopherwood_ROOT_SOURCES_DIR ${CMAKE_SOURCE_DIR}/src)
SET(libgopherwood_COMMON_SOURCES_DIR ${libgopherwood_ROOT_SOURCES_DIR}/common)
SET(libgopherwood_NETWORK_SOURCES_DIR ${libgopherwood_ROOT_SOURCES_DIR}/network)

IF(ENABLE_DEBUG)
    SET(libgopherwood_VERSION_STRING "${libgopherwood_VERSION_STRING}d")
ENDIF(ENABLE_DEBUG)

CONFIGURE_FILE(platform.h.in platform.h)
CONFIGURE_FILE(doxyfile.in doxyfile)

AUTO_SOURCES(files "*.cpp" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libgopherwood_SOURCES ${files})

AUTO_SOURCES(files "*.cc" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libgopherwood_SOURCES ${files})

AUTO_SOURCES(files "*.c" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libgopherwood_SOURCES ${files})

AUTO_SOURCES(files "*.h" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libgopherwood_SOURCES ${files})

SET(HEADER 
    client/gopherwood.h)

ADD_LIBRARY(libgopherwood-static STATIC ${libgopherwood_SOURCES})
ADD_LIBRARY(libgopherwood-shared SHARED ${libgopherwood_SOURCES})

ADD_CUSTOM_COMMAND(
    TARGET libgopherwood-shared libgopherwood-static
    PRE_BUILD
    COMMAND echo ${libgopherwood_VERSION_STRING} > version
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

TARGET_LINK_LIBRARIES(libgopherwood-static pthread)
TARGET_LINK_LIBRARIES(libgopherwood-shared pthread)
       
IF(NEED_BOOST)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
    IF(OS_LINUX)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_thread)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_chrono)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_system)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_atomic)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_iostreams)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_thread)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_chrono)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_system)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_atomic)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_iostreams)
    	ELSE(OS_LINUX)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_thread-mt)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_chrono-mt)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_system-mt)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_atomic-mt)
    		TARGET_LINK_LIBRARIES(libgopherwood-static boost_iostreams-mt)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_thread-mt)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_chrono-mt)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_system-mt)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_atomic-mt)
    		TARGET_LINK_LIBRARIES(libgopherwood-shared boost_iostreams-mt)
    	ENDIF(OS_LINUX)
ENDIF(NEED_BOOST)

IF(NEED_GCCEH)
    TARGET_LINK_LIBRARIES(libgopherwood-static gcc_eh)
    TARGET_LINK_LIBRARIES(libgopherwood-shared gcc_eh)
ENDIF(NEED_GCCEH)

INCLUDE_DIRECTORIES(${libgopherwood_ROOT_SOURCES_DIR})
INCLUDE_DIRECTORIES(${libgopherwood_COMMON_SOURCES_DIR})
INCLUDE_DIRECTORIES(${libgopherwood_NETWORK_SOURCES_DIR})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${OSS_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/mock)

TARGET_LINK_LIBRARIES(libgopherwood-static ${OSS_LIBRARIES})
TARGET_LINK_LIBRARIES(libgopherwood-shared ${OSS_LIBRARIES})

SET_TARGET_PROPERTIES(libgopherwood-static PROPERTIES OUTPUT_NAME "gopherwood")
SET_TARGET_PROPERTIES(libgopherwood-shared PROPERTIES OUTPUT_NAME "gopherwood")

IF(NEED_BOOST)
    SET_TARGET_PROPERTIES(libgopherwood-shared libgopherwood-static PROPERTIES LINK_FLAGS "-L${Boost_LIBRARY_DIRS}")
ENDIF(NEED_BOOST)

SET_TARGET_PROPERTIES(libgopherwood-shared PROPERTIES 
    VERSION ${libgopherwood_VERSION_MAJOR}.${libgopherwood_VERSION_MINOR}.${libgopherwood_VERSION_PATCH} 
    SOVERSION ${libgopherwood_VERSION_API})

INSTALL(TARGETS libgopherwood-static libgopherwood-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
INSTALL(FILES ${HEADER} DESTINATION include/gopherwood)
INSTALL(FILES libgopherwood.pc DESTINATION lib/pkgconfig)
            
SET(libgopherwood_SOURCES ${libgopherwood_SOURCES} PARENT_SCOPE)
SET(libgopherwood_PLATFORM_HEADER_DIR ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)
SET(libgopherwood_ROOT_SOURCES_DIR ${libgopherwood_ROOT_SOURCES_DIR} PARENT_SCOPE)
SET(libgopherwood_COMMON_SOURCES_DIR ${libgopherwood_COMMON_SOURCES_DIR} PARENT_SCOPE)
SET(libgopherwood_VERSION_STRING ${libgopherwood_VERSION_STRING} PARENT_SCOPE)
