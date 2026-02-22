rg -n -e '默认并发 14' -e '冷启动并发为 5' -e '--concurrency' -e '通常约为 14' README.md
276:> **关于多 Worker 并发**：`docker compose up -d --scale worker=3` 可以在一台机器上启动多个 Worker。每个 Worker 的**冷启动并发为 5**，默认会自适应探索到 `--concurrency` 上限（默认 50）；但快代理单通道可稳定承载的并发通常约为 14。**单代理通道下跑 1 个 Worker 是最优配置**。多 Worker 并行需要配合多条代理通道，否则会因代理限流导致失败率上升。
324:  --concurrency 10 \
345:| `--concurrency` | 50 | 最大并发上限（自适应控制器自动探索最优值） |
