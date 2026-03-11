# Project Progress — ASIN 主表合并模式 + 变动追踪 + 可选字段导出 + 定时自动采集

## Current Status
- Total: 13 features
- Passing: 13 / 13 (100%)
- Status: COMPLETED

## Session Log
### Session 1 - 2026-03-11 主表合并模式全面改造
- #1: Model+Config 新增变动追踪字段, EXPORTABLE_FIELDS, HEADER_MAP
- #2: 数据库 v2 迁移框架 - ASIN 主表唯一索引 + counters 表 + 变动字段
- #3: save_result ON CONFLICT DO UPDATE + 变动对比 + change_seq 计数器
- #4: batch_submit_results 批量变动对比 + change_seq 批量分配
- #5: get_results EXISTS 批次筛选 + 5种 change_filter 条件
- #6: iter_results/count_results/get_all_asins 支持 EXISTS + change_filter
- #7: delete_batch 仅删 tasks 保留 results + 清理悬空截图路径
- #8: DELETE /api/results 端点 + delete_results + 截图清理
- #9: 导出改造 - 路由顺序修正 + EXPORTABLE_FIELDS + change_filter + fields + 全局导出 + .db EXISTS
- #10: update_screenshot_path + get_result_by_asin 适配 ASIN 唯一索引
- #11: 定时自动采集 - 调度器 + 设置 + bool校验 + has_pending_auto_batch
- #12: results.html 变动筛选 + 变动标记 + 导出字段选择 Modal
- #13: settings.html 定时采集设置卡片
- Issues: None

### Session 2 - 2026-03-11 用户反馈修复
- Fix: results.html changeFilter 下拉框样式统一（移除 form-select-sm）
- Redesign: 定时采集从间隔模式改为多任务时间点模式
  - 支持多个 HH:MM 定时任务，CRUD 端点 /api/auto-scrape/schedules
  - settings.html 动态列表 UI（新增/删除/启用禁用）
  - 移除旧的 auto_scrape_enabled/interval/last_run 单任务设置
