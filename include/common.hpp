#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <set>
#include <numeric>

// 리눅스 전용 (CPU Pinning)
#ifdef __linux__
#include <pthread.h>
#endif

using namespace std;

// 전역 종료 신호 (main.cpp에서 정의)
// 현재 코드에서는 필요 없지만 확장성을 위해 보존
extern condition_variable cv_shutdown;
extern mutex mtx_shutdown;

// CPU 피닝 함수
inline void set_thread_affinity(thread& th, int core_id) {
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset); 
    int rc = pthread_setaffinity_np(th.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) cerr << "Pinning Error Core " << core_id << '\n';
#endif
}