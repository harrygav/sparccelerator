################################################################
# A minimal CMake file that is compatible with sbt-jni         #
#                                                              #
# All settings required by sbt-jni have been marked so, please #
# add/modify/remove settings to build your specific library.   #
################################################################

cmake_minimum_required(VERSION 3.2.0)

# Define project and related variables
# (required by sbt-jni) please use semantic versioning
#
project (sparccelerator)
set(PROJECT_VERSION_MAJOR 1)
set(PROJECT_VERSION_MINOR 0)
set(PROJECT_VERSION_PATCH 0)

# Setup JNI
find_package(JNI REQUIRED)
if (JNI_FOUND)
    message (STATUS "JNI include directories: ${JNI_INCLUDE_DIRS}")
endif()

# Include directories
include_directories(.)
include_directories(include)
include_directories(${JNI_INCLUDE_DIRS})

# Sources
file(GLOB LIB_SRC
  "*.c"
  "*.cc"
  "*.cpp"
)

# Setup installation targets
# (required by sbt-jni) major version should always be appended to library name
#
set (LIB_NAME ${PROJECT_NAME}${PROJECT_VERSION_MAJOR})
add_library(${LIB_NAME} SHARED ${LIB_SRC})
install(TARGETS ${LIB_NAME} LIBRARY DESTINATION .)

#foreach( testsourcefile ${LIB_SRC} )
#    get_filename_component(barename ${testsourcefile} NAME)
#    string( REPLACE ".c" "" testname ${barename} )
#    set (LIB_NAME ${testname}${PROJECT_VERSION_MAJOR})
#    add_library(${LIB_NAME} SHARED ${testsourcefile})
#    install(TARGETS ${LIB_NAME} LIBRARY DESTINATION .)
#endforeach( testsourcefile ${LIB_SRC} )

set(CMAKE_BUILD_TYPE Release)
#set(CMAKE_CXX_FLAGS "-O3")
