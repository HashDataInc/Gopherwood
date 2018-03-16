# - Find OSS
# Find the native OSS includes and library
#
#  OSS_INCLUDE_DIR  - where to find oss/oss.h, etc.
#  OSS_LIBRARIES    - List of libraries when using liboss.
#  OSS_FOUND        - True if OSS found.

IF (OSS_INCLUDE_DIR AND OSS_LIBRARIES)
  # Already in cache, be silent
  SET(OSS_FIND_QUIETLY TRUE)
ENDIF (OSS_INCLUDE_DIR AND OSS_LIBRARIES)

FIND_PATH(OSS_INCLUDE_DIR oss/oss.h)

SET(OSS_NAMES oss)
FIND_LIBRARY(OSS_LIBRARIES NAMES ${OSS_NAMES})

# handle the QUIETLY and REQUIRED arguments and set OSS_FOUND to TRUE if
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(oss DEFAULT_MSG OSS_LIBRARIES OSS_INCLUDE_DIR)

MARK_AS_ADVANCED(OSS_LIBRARIES OSS_INCLUDE_DIR)
