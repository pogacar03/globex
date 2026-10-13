# Globex 实际验收记录

验收日期：2026-10-13（Asia/Shanghai）；原始进程日志保留UTC时间。基线 main：`baa675bd418f3ea751aaa2f6997d72005ef434a2`；`git ls-remote` 已核对远端 main 仍为该提交。当前改动在 `/workspace/globex` 工作目录，尚未提交或推送。

## 实际版本与安装

| 项目 | 实际版本 |
|---|---|
| Python | 3.11.16 |
| uv | 0.12.19 |
| Node.js / npm | 22.22.2 / 10.9.7 |
| AgentScope | 2.0.6 |
| FastAPI / SQLAlchemy | 0.141.1 / 2.0.52 |
| Redis Python client / MCP SDK | 8.1.0 / 1.29.0 |
| pytest / Vite | 9.1.1 / 5.4.21 |
| TypeScript / React | 5.9.3 / 18.3.1 |
| Docker daemon | 28.4.0 |

安装执行 `UV_CACHE_DIR=/tmp/globex-uv-cache uv sync --frozen --python /workspace/FinAgent/.venv/bin/python`，退出码 0；前端按 package-lock 执行 npm ci，Node 与缓存显式设置为 22.22.2 和 `/tmp` 可写目录。增加 MCP 显式依赖后更新锁文件，没有替换原有依赖版本。

## 最终结果

| 验证 | 实际结果 | 退出码与证据 |
|---|---|---|
| Python全部测试，含真实Redis Lua | **448 passed，无跳过**，22.19秒；2条第三方warning | 0；[命令与日志](../artifacts/final-verification/globex-full-suite-command.log)、[JUnit](../artifacts/final-verification/globex-final-tests.xml) |
| 原有测试保留核对 | 原有291个node ID全部存在且执行通过，新增157个 | [清单核对](../artifacts/final-verification/globex-original-tests-preserved.json) |
| 前端审批契约测试 | **12 passed**，包含刷新后继续已批准动作及禁止自动重试不确定动作 | 0；[日志](../artifacts/final-verification/globex-front-approval-green.log) |
| 前端类型检查与生产构建 | tsc -b 与 Vite均通过 | 0；[构建日志](../artifacts/final-verification/globex-final-frontend-build.log) |
| 前端生产页面与JS资源 | HTTP 200，实际脚本资源159KB以上 | 0；[HTTP证据](../artifacts/final-verification/globex-frontend-http.json)；浏览器点击仍UNVERIFIED |
| 独立API/Worker/真实Redis进程验收 | PASS；Worker强制终止后恢复同任务，完成的检索结果仅1条；持久幂等冲突返回409 | 0；[命令](../artifacts/final-verification/runtime/command.log)、[结果](../artifacts/final-verification/runtime/result.json) |
| WS与MCP实际协议 | 跨租户WS拒绝；事件背板成功；MCP异步research立即返回句柄，查询后done | 与上述进程验收同一次执行 |
| HTTP审批执行 | 未批准取消仅202且订单未变；替换参数403；获批准确动作执行并重放同一回执 | 与上述进程验收同一次执行 |
| 三次SYNTHETIC 1/2并发对照 | **9请求，0失败**；这是人工延迟的离线夹具 | 0；[原始JSON](../artifacts/final-verification/runtime/benchmark/loadtest-20261013T112248056386Z.json)、[CSV](../artifacts/final-verification/runtime/benchmark/loadtest-20261013T112248056386Z.csv) |
| Compose配置 | config --quiet通过 | 0；[记录](../artifacts/final-verification/globex-compose-config.json) |
| Docker镜像构建及容器API冒烟 | 最新镜像构建通过；容器health为mock/SQLite，未认证401，授权商品研究200 | 均0；[构建命令](../artifacts/final-verification/docker-build-command.json)、[镜像与冒烟结果](../artifacts/final-verification/globex-docker-smoke-result.json) |

进程日志记录了有意SIGKILL旧Worker的退出码 `-9`；最终清理API/新Worker退出码分别为 `-15`、`0`。验收主命令退出0并完整写出PASS记录，临时Redis和进程均已清理。三次原始记录保留了实际请求/响应、失败类别、工作目录dirty标记和源文件hash，不把基线commit SHA冒充已提交的新版本。

## 可复现命令与原始证据

```bash
uv sync --frozen --python 3.11
# 独立Redis数据库让opt-in Redis用例也实际执行；普通无Redis运行会显示skip。
GLOBEX_TEST_REDIS_URL=redis://127.0.0.1:6379/14 uv run --frozen pytest -q --junitxml=artifacts/tests.xml
cd frontend
npm ci
npm test
npm run build
cd ..
uv run --frozen python scripts/accept_runtime.py --benchmark
```

`accept_runtime.py` 使用临时身份、独立 SQLite 目录和可清理 Redis 容器，禁用真实模型/embedding/Tavily/OTLP；也可以用 `--redis-url` 指向自己负责的隔离 Redis。保存 API、Worker、MCP、loadtest 日志、过程结果和逐请求 JSON/CSV。Worker kill/restart、审批参数替换和回执重放均有真实进程或 HTTP 证据。

离线模型为 `synthetic-catalog-v1`。记录中的商品、订单、地址和延迟均是工程验收夹具，不是真实用户购买或真实模型消费。三次 1/2 并发对照不能证明生产吞吐、尾延迟、模型优劣或成本收益。

## 失败与修复记录

- 初次整合 Python 全套：391 passed、1 failed，退出码 1。失败为新增 FakeSynthetic 构造签名未适配 mock delay；修正模拟接口，保留“无真实凭证读取”的断言。
- 真实并发验收出现 9 请求中 1 个 502，退出码 1：短至 1.5 秒的测试租约在负载下失效，且 Worker 在可重投时先发布 failed，使同步客户端提前认定终态。修复 retrying/死信终态区分、原子 owner 检查、同 Worker active pending 去重；验收租约改为 5 秒。失败原始记录保存在 [失败运行](../artifacts/failed-runtime-acceptance-1/)。未删去失败样本来改善成功率。
- API/Worker 同时启动曾触发 SQLite 建表竞争；已增加本地共享目录启动锁并验证并发建表。
- 验收脚本曾因 native context 字段、SQL 时间序列化差异、导入路径、过短启动等待和清理等待失败；分别修正脚本的实际 SDK/接口契约、同接口前后状态比较、模块路径和有界等待。只有最终完整命令退出 0 才计作通过。
- 独立审查指出前端错误完成判定、刷新后审批不可恢复、MCP同步超时无任务句柄、幂等 marker 过期后的冲突处理；均按具体调用链修复并增加验收。

## UNVERIFIED 与未实现

| 项目 | 状态与原因 |
|---|---|
| 真实 LLM、embedding、reranker、Qdrant服务、judge回归 | UNVERIFIED：本轮明确未调用真实 provider |
| 真实 Token、账单、大小模型质量/节费 | UNVERIFIED：离线夹具不能提供此类证据 |
| Langfuse/OTLP collector线上接收与查询 | UNVERIFIED：仅验证默认出口隐私过滤 |
| GitHub Actions远端运行与下载artifact | UNVERIFIED：新增workflow尚未推送/触发 |
| Docker Compose全服务联合启动 | UNVERIFIED：配置校验与API镜像冒烟不等于全栈Compose验收 |
| PostgreSQL、多主机控制面、OIDC | 未实现：当前身份静态适配器、控制面要求共享本地持久目录 |
| 商家库存、支付、外部写Exactly Once | 未实现：库存为内存夹具，外部副作用需幂等与核对 |
| 浏览器自动化点击、locust线上负载、真实多Agent加速 | UNVERIFIED：构建和HTTP页面检查不等于这些运行证据 |
| 自动调权/候选版本晋级飞轮、股票历史回测 | 未实现：设计已明确数据集隔离、门槛、灰度和回滚，本轮没有虚构自进化效果 |

本次已发现第三方库的 Starlette/httpx 弃用警告和 MCP settings forward reference 警告；未修改第三方库或把 warning 当作测试失败隐去。
