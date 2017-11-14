# - Find QingStor
# Find the native QingStor includes and library
#
#  QINGSTOR_INCLUDE_DIR  - where to find qingstor/qingstor.h, etc.
#  QINGSTOR_LIBRARIES    - List of libraries when using QingStor.
#  QINGSTOR_FOUND        - True if QingStor found.

IF (QINGSTOR_INCLUDE_DIR AND QINGSTOR_LIBRARIES)
  # Already in cache, be silent
  SET(QINGSTOR_FIND_QUIETLY TRUE)
ENDIF (QINGSTOR_INCLUDE_DIR AND QINGSTOR_LIBRARIES)

FIND_PATH(QINGSTOR_INCLUDE_DIR qingstor/qingstor.h)

SET(QINGSTOR_NAMES qingstor)
FIND_LIBRARY(QINGSTOR_LIBRARIES NAMES ${QINGSTOR_NAMES})

# handle the QUIETLY and REQUIRED arguments and set QINGSTOR_FOUND to TRUE if
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(qingstor DEFAULT_MSG QINGSTOR_LIBRARIES QINGSTOR_INCLUDE_DIR)

MARK_AS_ADVANCED(QINGSTOR_LIBRARIES QINGSTOR_INCLUDE_DIR)
