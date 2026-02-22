118:| `MAX_CONCURRENCY` | 50 | 并发上限 |
276:> **关于多 Worker 并发**：`docker compose up -d --scale worker=3` 可以在一台机器上启动多个 Worker。但每个 Worker 默认并发 14，快代理单通道并发上限约 14 连接。**单代理通道下跑 1 个 Worker 是最优配置**。多 Worker 并行需要配合多条代理通道，否则会因代理限流导致失败率上升。
324:  --concurrency 10 \
345:| `--concurrency` | 50 | 最大并发上限（自适应控制器自动探索最优值） |
