cmake_minimum_required(VERSION 3.10)

# 프로젝트 이름 (원하는 대로 수정 가능)
project(kinesis_consumer)

# C++ 표준 설정 (AWS SDK는 11 이상 필수, 넉넉하게 17 추천)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# AWS SDK 찾기 (Kinesis 모듈만)
find_package(AWSSDK REQUIRED COMPONENTS kinesis)
# Protobuf 패키지 찾기
find_package(Protobuf REQUIRED)

# 헤더 파일 경로 설정 (include 폴더)
include_directories(${PROJECT_SOURCE_DIR}/include)

# 소스 파일들을 변수에 담기
file(GLOB SOURCES "src/*.cpp" "src/*.cc")

# 실행 파일 생성 (이름: my_app)
add_executable(my_app ${SOURCES})

# 라이브러리 연결 (AWS SDK, 쓰레드 등)
target_link_libraries(my_app ${AWSSDK_LINK_LIBRARIES} ${Protobuf_LIBRARIES} pthread)