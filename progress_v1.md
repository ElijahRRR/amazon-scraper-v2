# Project Progress

## Current Status
- Total: 11 features
- Passing: 11 / 11 (100%)
- Status: COMPLETED

## Test Results (Feature #11)
- Batch: optimization_test (50 ASINs)
- Success Rate: 100% (50/50)
- Throughput: 127 ASINs/min (TPS mode)
- p50 Latency: 2.3s
- p95 Latency: 2.8s
- Block Rate: 0%
- Gradient2 RTT tracking: working (g=1.32, stable)

## DPS Performance Test Results (v2 - 带宽感知优化)
- Batch: dps_bw_v2 (200 ASINs, 全部真实商品)
- Success Rate: 100% (200/200)
- Steady-state Throughput: 87 ASINs/min
- Overall Throughput: 71 ASINs/min (含预热期)
- p50 Latency: 6-7s (steady state)
- p95 Latency: 10-15s
- Block Rate: 0%
- AIMD 振荡区间: 11-15 concurrent
- 带宽利用率: 77-91% of 5 Mbps

### DPS v1 Results (无带宽感知)
- Batch: dps_500_test (500 ASINs)
- Throughput: 57-79 ASINs/min
- Optimal concurrency: 12-16

### Key DPS Optimization Findings
1. Global AIMD > per-channel AIMD (per-channel has too few samples)
2. TUNNEL_MAX_CONCURRENCY reduced from 48 → 20 (prevents over-expansion)
3. AIMD expansion gradient threshold narrowed 0.95 → 0.90 (faster convergence)
4. 带宽感知修复: resp_bytes 需除以压缩比(5:1)才能与网络带宽对比
5. Gradient reduction 需带宽守护: 带宽<50%时不降速(延迟来自远端非过载)
6. Gradient reduction 不低于初始并发(预防性降速不应过度收缩)
7. DPS理论上限: 5Mbps / 300KB/page × 60 = 125 ASINs/min，实测达到 69.6% 效率

## Session Log
### Session 1 - 2026-02-28 DPS+TPS 优化方案实施
- Completed: feature #1 (per-channel token bucket), #2 (DPS concurrency 48), #3 (per-channel AIMD), #4 (Gradient2-AIMD hybrid), #5 (batched session rebuild), #6 (proactive IP change on block)
- Issues: None

### Session 2 - 2026-02-28 (continued)
- Completed: feature #7 (CAPTCHA auto-solve), #8 (priority queue), #9 (smart UA rotation), #10 (settings page DPS params), #11 (end-to-end test)
- Issues: amazoncaptcha pip install required --break-system-packages workaround
- Test run confirmed 127 ASINs/min throughput, 100% success rate

### Session 3 - 2026-02-28 DPS 性能测试与优化
- Tested DPS mode with 快代理 TPS 代理链接
- Per-channel concurrency tuning: initial=2/max=4 optimal (initial=3/max=6 causes congestion)
- Discovered global AIMD outperforms per-channel AIMD for DPS
- AIMD dead zone fix: expansion threshold 0.95 → 0.90
- Capped TUNNEL_MAX_CONCURRENCY: 48 → 20
- 500 ASIN stress test: 100% success, 0 failures

### Session 4 - 2026-02-28 DPS 带宽感知优化
- 修复 metrics.py 带宽计算: resp_bytes 需除以压缩比(5:1)
- 修复 runtime_settings.json: proxy_bandwidth_mbps 从 0→5, request_timeout 30→15
- 修复 AIMD gradient reduction: 带宽<50%时不降速, 不低于初始并发
- 修复 _sync_controller_mode_profile: cooldown 从硬编码60→config值15
- 200 ASIN 测试: 100% success, 87 ASINs/min steady-state, 带宽利用率 77-91%

## Feature Summary
| # | Description | Status |
|---|-------------|--------|
| 1 | Per-channel token bucket (DPS) | PASS |
| 2 | DPS concurrency raised to 48 | PASS |
| 3 | Per-channel independent AIMD | PASS |
| 4 | Gradient2-AIMD hybrid limiter | PASS |
| 5 | Batched session rebuild | PASS |
| 6 | Proactive IP change on block | PASS |
| 7 | CAPTCHA auto-solve | PASS |
| 8 | Priority task queue | PASS |
| 9 | Smart User-Agent rotation | PASS |
| 10 | Settings page DPS params | PASS |
| 11 | End-to-end test verification | PASS |
