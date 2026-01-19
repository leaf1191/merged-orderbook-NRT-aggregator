#include <iostream>
#include <limits>
#include <thread>
#include <atomic>
#include <memory>
#include <map>
#include <fstream>
#include <immintrin.h>
#include "common.hpp"
#include "clean_data.hpp"
#include "merged_order_book.hpp"
#include "iring_buffer.hpp"
#include "optimistic_ring_buffer.hpp"
#include "mutex_ring_buffer.hpp"
#include "spin_lock_ring_buffer.hpp"
#include "nrt_latch.hpp"
#include "aggregator_metrics.hpp"

// --- AWS SDK Headers ---
#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/SubscribeToShardRequest.h>
#include <aws/kinesis/model/SubscribeToShardHandler.h>
#include <aws/kinesis/model/SubscribeToShardEvent.h>
#include <aws/kinesis/model/ListShardsRequest.h>

using namespace std;
using namespace Aws::Kinesis;
using namespace Aws::Kinesis::Model;

condition_variable cv_shutdown;
mutex mtx_shutdown;

// --- 스레드 로직 (Reader, Processor, Publisher) ---
void reader_thread_func(
    const string& stream_name,    // Target Stream Name
    const string& consumer_arn,   // EFO Consumer ARN
    IRingBuffer<CleanData>& buffer, 
    atomic<bool>& running)
{
    cout << "[Reader] Connecting to Stream: " << stream_name << "..." << '\n';

    // 각 샤드의 현재 구독 상태를 추적
    struct ShardContext {
        string shardId;
        atomic<bool> is_active{false};
        shared_ptr<SubscribeToShardHandler> handler; // 가상 참조 방지(reader 스레드에서 관리)
        shared_ptr<SubscribeToShardRequest> request; // 가상 참조 방지(1초 단위 스케줄링)
    };
    // shared_ptr로 관리하여 람다 캡처 시 안전성 확보 (복사 방지)
    vector<shared_ptr<ShardContext>> shard_contexts;

    // AWS 클리언트 설정
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = "ap-northeast-2";
    KinesisClient kinesisClient(clientConfig);

    ListShardsRequest listShardsRequest;
    listShardsRequest.SetStreamName(stream_name.c_str());
    
    auto listShardsOutcome = kinesisClient.ListShards(listShardsRequest);
    if (!listShardsOutcome.IsSuccess()) {
        cerr << "[Reader] Failed to list shards: " << listShardsOutcome.GetError().GetMessage() << '\n';
        return;
    }

    const auto& shards = listShardsOutcome.GetResult().GetShards();
    cout << "[Reader] Found " << shards.size() << " shards in " << stream_name << '\n';
    for(const auto& s : shards) {
        auto ctx = make_shared<ShardContext>();
        ctx->shardId = s.GetShardId();
        // 핸들러 설정 로직
        ctx->handler = Aws::MakeShared<SubscribeToShardHandler>("KinesisReader"); // heap에 할당
        ShardContext* raw_ctx = ctx.get(); // 순환 참조 방지
        ctx->handler->SetSubscribeToShardEventCallback([&buffer, &running](const SubscribeToShardEvent& event) {
            if (!running.load(std::memory_order_relaxed)) return; // 좀비 콜백 방지
            const auto& records = event.GetRecords();
            for (const auto& record : records) {
                const auto& blob = record.GetData();
                string s(reinterpret_cast<const char*>(blob.GetUnderlyingData()), blob.GetLength());
                CleanData data = CleanData::from_proto(s);
                if (data.exchange != "unknown") {
                    buffer.push(data);
                }
            }
        });
        ctx->handler->SetOnErrorCallback([raw_ctx](const Aws::Client::AWSError<KinesisErrors>& error) {
            cerr << "[Reader] Shard " << raw_ctx->shardId << " Error: " << error.GetMessage() << '\n';
            raw_ctx->is_active = false; 
        });

        shard_contexts.push_back(ctx);
    }
    // [Helper Lambda] 구독 요청 함수
    auto subscribe_shard = [&](shared_ptr<ShardContext> ctx) {
        ctx->request = make_shared<SubscribeToShardRequest>();
        ctx->request->SetConsumerARN(consumer_arn.c_str());
        ctx->request->SetShardId(ctx->shardId.c_str());
        // NRT 시스템 특성상 끊겼던 과거 데이터보다 '최신'이 중요하므로 LATEST 유지.
        StartingPosition sp; sp.SetType(ShardIteratorType::LATEST);
        ctx->request->SetStartingPosition(sp);

        ctx->request->SetEventStreamHandler(*(ctx->handler));
        ctx->is_active = true; // 낙관적 활성화
        SubscribeToShardResponseReceivedHandler response_handler = 
            [ctx](const KinesisClient* /*client*/, 
                const SubscribeToShardRequest& /*request*/, 
                const auto& outcome,
                const shared_ptr<const Aws::Client::AsyncCallerContext>& /*context*/) 
        {
            if (!outcome.IsSuccess()) {
                cerr << "[Reader] Init Failed " << ctx->shardId << ": " 
                        << outcome.GetError().GetMessage() << '\n';
                ctx->is_active = false;
            }
        };
        kinesisClient.SubscribeToShardAsync(*(ctx->request), response_handler, nullptr);
    };
    // 초기 구독 시작
    for (auto& ctx : shard_contexts) {
        subscribe_shard(ctx);
    }

    while (running) {
        // CV로 대기하되, 타임아웃(예: 1초)을 두어 주기적으로 상태 체크
        std::unique_lock<std::mutex> lock(mtx_shutdown); // CV sleep의 동시성 핸들링
        if (cv_shutdown.wait_for(lock, chrono::seconds(1), [&](){ return !running.load(); })) {
            break; // running이 false
        }
        
        // 타임아웃으로 깨어남 -> 샤드 상태 점검
        for (auto& ctx : shard_contexts) {
            if (!ctx->is_active) {
                cout << "[Reader] Reconnecting to " << ctx->shardId << "..." << '\n';
                subscribe_shard(ctx);
            }
        }
    }
    
    cout << "[Reader] Terminated." << '\n';
}

void processor_thread_func(
    const string& product_name,
    IRingBuffer<CleanData>& input_buffer,
    NrtLatch<MergedOrderBook>& output_latch,
    atomic<bool>& running)
{
    cout << "[Processor-" << product_name << "] 시작." << '\n';

    const uint64_t WINDOW_SIZE_MS = 7;
    const uint64_t WATERMARK_DELAY_MS = 1;
    const uint64_t MY_CAPACITY = 3000;

    uint64_t my_last_processed_seq = input_buffer.get_write_cursor();

    // 윈도우 로직용 변수
    uint64_t max_event_time_seen = 0;
    uint64_t last_published_window = 0;
    map<uint64_t, vector<CleanData>> window_buffers;
    map<string, map<double, double>> current_bids;
    map<string, map<double, double>> current_asks;

    // 지표 측정
    AggregatorMetrics metrics;

    while (running) {
        const uint64_t latest_available_seq = input_buffer.get_write_cursor();
        const uint64_t pending_items = latest_available_seq - my_last_processed_seq;

        if (pending_items <= 0) {
            _mm_pause(); // 메모리 버스 병합 방지 딜레이(ns단위)
            continue;
        }
        auto cycle_start = chrono::steady_clock::now(); // 처리 시작 시간
        bool did_work = false; // 버퍼가 비었다면 체크 스킵

        // "건너뛰기" 로직
        const uint64_t seq_to_end = latest_available_seq;
        uint64_t start_seq;
        if (pending_items > MY_CAPACITY) {
            uint64_t jump_cnt = pending_items - MY_CAPACITY;
            metrics.jumped_count += jump_cnt;
            start_seq = seq_to_end - MY_CAPACITY;
        } else {
            start_seq = my_last_processed_seq;
        }

        for (uint64_t i = start_seq; i < seq_to_end; ++i) {
            CleanData data;
            bool read_success = false;

            int retry_count = 0; // reader가 죽었을 때 방지
            const int MAX_RETRIES = 30000000;
            // 데이터의 유실을 방지해 워터마크 딜레이만큼 오버헤드를 줄여 성능 개선
            while (true) {
                auto result = input_buffer.try_read(i, data);
                if (result == ReadResult::Success) {
                    read_success = true;
                    break;
                } 
                else if (result == ReadResult::NotReady) {
                    _mm_pause(); // flush 비용 및 L3 캐시 읽기 요청 개선
                    if (++retry_count > MAX_RETRIES) {
                        read_success = false;
                        break;
                    }
                    continue; 
                } 
                else { 
                    read_success = false;
                    break; 
                }
            }
            if(!read_success) continue;

            // 데이터가 유효함
            if (data.ts_event > max_event_time_seen) max_event_time_seen = data.ts_event; // 워터마크 업데이트 (ms 단위)

            // Skew 측정 (P99 워터마크 튜닝용)
            int64_t skew = (int64_t)max_event_time_seen - (int64_t)data.ts_event;
            if (skew >= 0) metrics.skew_hist.record(skew); // ms 단위 기록 가정

            uint64_t window = (data.ts_event / WINDOW_SIZE_MS) * WINDOW_SIZE_MS; // 윈도우 설정
            if (window <= last_published_window && last_published_window != 0) continue; // 발행된 윈도우 스킵 
            window_buffers[window].push_back(data);
        }
        
        my_last_processed_seq = seq_to_end;

        // 워터마크 계산
        uint64_t watermark = (max_event_time_seen > WATERMARK_DELAY_MS) ? (max_event_time_seen - WATERMARK_DELAY_MS) : 0;

        // 윈도우 마감 및 처리(신규)
        while (!window_buffers.empty()) {
            auto it = window_buffers.begin();
            uint64_t window_to_process = it->first;

            if (watermark < window_to_process + WINDOW_SIZE_MS) {
                break;
            }

            vector<CleanData>& events_to_process = it->second;
            set<string> updated_exchanges; // 스냅샷 완전성 체크용
            map<string, uint64_t> current_window_max_ts; // 이번 윈도우 내 거래소별 Max TS 추적

            // 메모리 상태 업데이트 - 래치 효과
            for (const auto& evt : events_to_process) {
                updated_exchanges.insert(evt.exchange); // 데이터가 온 거래소 기록
                if (!evt.bids.empty()) {
                    current_bids[evt.exchange].clear();
                    for (const auto& [p, s] : evt.bids) current_bids[evt.exchange][p] = s;
                }
                if (!evt.asks.empty()) {
                    current_asks[evt.exchange].clear();
                    for (const auto& [p, s] : evt.asks) current_asks[evt.exchange][p] = s;
                }
                // ts 기록
                uint64_t& ts = current_window_max_ts[evt.exchange];
                if (evt.ts_event > ts) ts = evt.ts_event;
            }

            metrics.total_windows++;
            if (updated_exchanges.size() < 3) {
                metrics.incomplete_windows++;
                //cout<< updated_exchanges.size() << '\n';
            }

            // 래치에 저장할 스냅샷 생성
            auto snapshot_ptr = make_shared<MergedOrderBook>();
            snapshot_ptr->window_start_time = window_to_process;

            // 윈도우 처리 로직
            for (auto const& [ex, ts] : current_window_max_ts) {
                snapshot_ptr->fresh_updates_ts.push_back(ts);
            }
            for (auto const& [ex, book] : current_bids) {
                for (auto const& [p, s] : book) snapshot_ptr->global_bids.emplace_back(p, s, ex);
            }
            sort(snapshot_ptr->global_bids.rbegin(), snapshot_ptr->global_bids.rend());
            for (auto const& [ex, book] : current_asks) {
                for (auto const& [p, s] : book) snapshot_ptr->global_asks.emplace_back(p, s, ex);
            }
            sort(snapshot_ptr->global_asks.begin(), snapshot_ptr->global_asks.end());

            // 래치 덮어쓰기
            if (!snapshot_ptr->global_bids.empty() || !snapshot_ptr->global_asks.empty()) {
                output_latch.store(snapshot_ptr);
            }

            last_published_window = window_to_process;
            // 처리 완료된 윈도우를 맵에서 삭제
            window_buffers.erase(it); 
            did_work = true; // 처리를 하지 않았다면 metric에 제외
        }

        if (did_work) {
            // 종료 시간 측정
            auto cycle_end = chrono::steady_clock::now();
            auto elapsed_us = chrono::duration_cast<chrono::microseconds>(cycle_end - cycle_start).count();
            
            metrics.cycle_time_hist.record(elapsed_us);
            metrics.report_counter++;

            if (metrics.report_counter >= 1000) {
                cout << "\n=== [Metrics: " << product_name << "] ===" << '\n';
                cout << " Cycle P99.9: " << metrics.cycle_time_hist.get_percentile(0.999) << " us" << '\n';
                cout << " Skew P99   : " << (double)metrics.skew_hist.get_percentile(0.99)/1000.0 << " ms" << '\n';
                //cout << " Incomplete : " << (double)metrics.incomplete_windows/metrics.total_windows*100.0 << "%" << '\n';
                cout << " Jumped     : " << metrics.jumped_count << '\n';
                metrics.reset();
            }
        }

    }
}
// csv 파일 기록용
struct LogEntry {
    uint64_t window_time;
    uint64_t latency;
};

// Publisher 스레드 (I/O-Bound)
void publisher_thread_func(
    const string& product_name,
    NrtLatch<MergedOrderBook>& input_latch,
    atomic<bool>& running)
{
    cout << "[Publisher-" << product_name << "] 시작." << '\n';
    shared_ptr<MergedOrderBook> last_processed_ptr = nullptr;

    // Metric 변수
    LatencyHistogram latency_hist(1000); // Max 1000ms
    uint64_t published_count = 0;
    auto last_report_time = chrono::steady_clock::now();
    const int REPORT_INTERVAL = 1000;

    vector<LogEntry> log_buffer;
    log_buffer.reserve(REPORT_INTERVAL * 3);

    string filename = "latency_log_" + product_name + ".csv";
    ofstream csv_file(filename);
    csv_file << "WindowStart,Latency_ms" << '\n'; 

    while (running) {
        auto current_ptr = input_latch.load();

        if (current_ptr && current_ptr != last_processed_ptr) {
            // Publisher의 I/O 병목 시뮬레이션
            this_thread::sleep_for(chrono::milliseconds(80));

            // 오더북 데이터 출력 로직
            // cout << "[Publisher-" << product_name << "] Window " << current_ptr->window_start_time << " 발행! (";
            // if (!current_ptr->global_bids.empty()) {
            //     const auto& best_bid = current_ptr->global_bids[0];
            //     cout << "Best Bid: " << get<0>(best_bid) << " (" << get<2>(best_bid) << ")";
            // } else { cout << "Best Bid: N/A"; }
            // cout << " / ";
            // if (!current_ptr->global_asks.empty()) {
            //     const auto& best_ask = current_ptr->global_asks[0];
            //     cout << "Best Ask: " << get<0>(best_ask) << " (" << get<2>(best_ask) << ")";
            // } else { cout << "Best Ask: N/A"; }
            // cout << ")" << '\n';

            // latency 측정
            uint64_t now_ms = chrono::duration_cast<chrono::milliseconds>(
                chrono::system_clock::now().time_since_epoch()
            ).count();
            if (!current_ptr->fresh_updates_ts.empty()) {
                for (uint64_t event_ts : current_ptr->fresh_updates_ts) {
                    uint64_t lat = (now_ms >= event_ts) ? (now_ms - event_ts) : 0;
                    latency_hist.record(lat);
                    log_buffer.push_back({current_ptr->window_start_time, lat});
                }
            }

            published_count++;
            last_processed_ptr = current_ptr;

            // 로그 출력
            if (published_count % REPORT_INTERVAL == 0) {
                auto now_t = chrono::steady_clock::now();
                auto duration_ms = chrono::duration_cast<chrono::milliseconds>(now_t - last_report_time).count();

                string chunk_data;
                chunk_data.reserve(log_buffer.size() * 40);
                // 문자열 생성
                for (const auto& entry : log_buffer) {
                    chunk_data += to_string(entry.window_time);
                    chunk_data += ",";
                    chunk_data += to_string(entry.latency);
                    chunk_data += "\n";
                }
                csv_file.write(chunk_data.c_str(), chunk_data.size()); // 단한번 덤프
                log_buffer.clear();

                // TPS 계산
                double tps = (double)REPORT_INTERVAL / (duration_ms / 1000.0);
                
                // Latency P99 계산
                auto [mean, std_dev] = latency_hist.get_stats();
                uint64_t p50 = latency_hist.get_percentile(0.50);
                uint64_t p99 = latency_hist.get_percentile(0.99);
                uint64_t max_lat = latency_hist.get_percentile(1.00);

                cout << ">>> [Metrics-" << product_name << "]"
                     << " TPS: " << tps 
                     << " | Latency(ms) [Avg: " << mean 
                     << ", P50: " << p50
                     << ", P99: " << p99 
                     << ", Max: " << max_lat 
                     << ", Jitter: " << std_dev << "]" 
                     << '\n';

                // 초기화
                last_report_time = now_t;
                latency_hist.reset(); 
            }
        } else {
            this_thread::yield(); // IO 바운드 -> 원활한 OS 스케줄링
        }
    }
    // 종료 시 남은 버퍼 잔량 처리
    if (!log_buffer.empty()) {
        for (const auto& entry : log_buffer) {
             csv_file << entry.window_time << "," << entry.latency << "\n";
        }
    }
    csv_file.close();
}


// --- 메인 함수 ---
int main() {
    cout << "Aggregator 시작... (9-스레드, CPU-Bound 피닝)" << '\n';
    cout << "(Cores 3,4,5 = Processors; 외에는 OS 스케줄러가 관리)" << '\n';
    
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    atomic<bool> running(true);

    // 스트림 이름과 EFO ARN (실제 환경 값 필요)
    string stream_btc = "BTC";
    string arn_btc = "arn:aws:kinesis:ap-northeast-2:337909762204:stream/BTC/consumer/MyEFOConsumer:1763975228";

    string stream_eth = "ETH";
    string arn_eth = "arn:aws:kinesis:ap-northeast-2:337909762204:stream/ETH/consumer/MyEFOConsumer:1763975236";

    string stream_sol = "SOL";
    string arn_sol = "arn:aws:kinesis:ap-northeast-2:337909762204:stream/SOL/consumer/MyEFOConsumer:1763975245";

    // 3개의 NRT 링 버퍼 (Reader -> Processor)
    // Case 1: Optimistic (Lock-Free)
    // OptimisticRingBuffer<CleanData> buffer_btc_impl(65536);
    // OptimisticRingBuffer<CleanData> buffer_eth_impl(65536);
    // OptimisticRingBuffer<CleanData> buffer_sol_impl(65536);

    // Case 2: Mutex (Blocking)
    // MutexRingBuffer<CleanData> buffer_btc_impl(65536);
    // MutexRingBuffer<CleanData> buffer_eth_impl(65536);
    // MutexRingBuffer<CleanData> buffer_sol_impl(65536);
    
    // Case 3: Spinlock
    SpinLockRingBuffer<CleanData> buffer_btc_impl(65536);
    SpinLockRingBuffer<CleanData> buffer_eth_impl(65536);
    SpinLockRingBuffer<CleanData> buffer_sol_impl(65536);


    // 인터페이스 참조로 변환
    IRingBuffer<CleanData>& buffer_btc = buffer_btc_impl;
    IRingBuffer<CleanData>& buffer_eth = buffer_eth_impl;
    IRingBuffer<CleanData>& buffer_sol = buffer_sol_impl;

    // 3개의 NRT 래치 (Processor -> Publisher)
    NrtLatch<MergedOrderBook> latch_btc;
    NrtLatch<MergedOrderBook> latch_eth;
    NrtLatch<MergedOrderBook> latch_sol;
    

    // --- 스레드 생성 (9개) ---
    thread reader_btc(reader_thread_func, stream_btc, arn_btc, ref(buffer_btc), ref(running));
    thread reader_eth(reader_thread_func, stream_eth, arn_eth, ref(buffer_eth), ref(running));
    thread reader_sol(reader_thread_func, stream_sol, arn_sol, ref(buffer_sol), ref(running));

    thread processor_btc(processor_thread_func, "BTC", ref(buffer_btc), ref(latch_btc), ref(running));
    thread processor_eth(processor_thread_func, "ETH", ref(buffer_eth), ref(latch_eth), ref(running));
    thread processor_sol(processor_thread_func, "SOL", ref(buffer_sol), ref(latch_sol), ref(running));

    thread publisher_btc(publisher_thread_func, "BTC", ref(latch_btc), ref(running));
    thread publisher_eth(publisher_thread_func, "ETH", ref(latch_eth), ref(running));
    thread publisher_sol(publisher_thread_func, "SOL", ref(latch_sol), ref(running));
    
    set_thread_affinity(processor_btc, 3);
    set_thread_affinity(processor_eth, 4);
    set_thread_affinity(processor_sol, 5);

    // 30초간 실행
    this_thread::sleep_for(chrono::seconds(150));

    {
        // Reader가 완전히 잠들도록 락 설정
        std::unique_lock<std::mutex> lock(mtx_shutdown);
        running = false; 
        cv_shutdown.notify_all(); 
    } 

    if (reader_btc.joinable()) reader_btc.join();
    if (reader_eth.joinable()) reader_eth.join();
    if (reader_sol.joinable()) reader_sol.join();

    if (processor_btc.joinable()) processor_btc.join();
    if (processor_eth.joinable()) processor_eth.join();
    if (processor_sol.joinable()) processor_sol.join();

    if (publisher_btc.joinable()) publisher_btc.join();
    if (publisher_eth.joinable()) publisher_eth.join();
    if (publisher_sol.joinable()) publisher_sol.join();

    Aws::ShutdownAPI(options);
    cout << "Aggregator 종료." << '\n';
    return 0;
}