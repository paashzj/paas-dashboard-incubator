cmake_minimum_required(VERSION 3.23)
project(paas-dashboard-cpp)

set(CMAKE_CXX_STANDARD 23)
set(TARGET paas-dashboard-cpp)

FILE(GLOB Sources *.cpp)
add_executable(${TARGET} ${Sources})
