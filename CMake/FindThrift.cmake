# - Find thrift
# Find the native THRIFT includes and library
#
#  THRIFT_INCLUDE_DIR  - where to find thrift/protocol/TBinaryProtocol.h, etc.
#  THRIFT_LIBRARIES    - List of libraries when using thrift.
#  THRIFT_FOUND        - True if thrift found.

IF (THRIFT_INCLUDE_DIR AND THRIFT_LIBRARIES)
  # Already in cache, be silent
  SET(THRIFT_FIND_QUIETLY TRUE)
ENDIF (THRIFT_INCLUDE_DIR AND THRIFT_LIBRARIES)

FIND_PATH(THRIFT_INCLUDE_DIR thrift/protocol/TBinaryProtocol.h)

SET(THRIFT_NAMES thrift)
FIND_LIBRARY(THRIFT_LIBRARIES NAMES ${THRIFT_NAMES})

# handle the QUIETLY and REQUIRED arguments and set THRIFT_FOUND to TRUE if 
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(thrift DEFAULT_MSG THRIFT_LIBRARIES THRIFT_INCLUDE_DIR)

MARK_AS_ADVANCED(THRIFT_LIBRARIES THRIFT_INCLUDE_DIR)
