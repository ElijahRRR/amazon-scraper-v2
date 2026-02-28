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

## DPS Performance Test Results
- Batch: dps_500_test (500 ASINs)
- Success Rate: 100%
- Throughput: 57-79 ASINs/min (bandwidth-limited)
- p50 Latency: 6-11s (varies with concurrency)
- Block Rate: 0% steady state
- Optimal concurrency: 12-16 (AIMD auto-discovers)
- Proxy bandwidth: ~3.0 MB/s

### Key DPS Optimization Findings
1. Global AIMD > per-channel AIMD (per-channel has too few samples)
2. TUNNEL_MAX_CONCURRENCY reduced from 48 → 20 (prevents over-expansion)
3. AIMD expansion gradient threshold narrowed 0.95 → 0.90 (faster convergence)
4. DPS theoretical max: ~93 ASINs/min (bandwidth ceiling), achieved 80-85% efficiency

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
