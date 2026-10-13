# Globex 运行时治理与可靠性设计

## 目标与已验证基线
用户授权在 Globex 主项目直接设计、实现此前审计发现的缺口，用实际工程成果支撑 Agent 岗位面试。保留 Python / AgentScope 及电商领域，不移植 FinAgent 的金融业务。审计 main 为 baa675bd418f3ea751aaa2f6997d72005ef434a2；Python 3.11.16、锁定 AgentScope 2.0.6；原有 291 项测试通过，Node 22.22.2 前端类型检查及生产构建通过。关键词召回 67 样本实跑通过；真实在线调用和历史压测数字没有复核。

## 决策
选择沿现有洋葱架构补强边界。全面重写会丢失现有工具、记忆和检索测试；仅增加提示词无法解决授权、持久恢复和错误计量。当前交付依赖 SQLite 与可选 Redis，不为架构观感新增 Kafka/Kubernetes。生产 OIDC、跨地域容灾、真实支付和模型训练不属于本轮成果。

## 1. 身份与隔离
新增服务端 Principal(tenant_id, subject, permissions)。GLOBEX_AUTH_TOKENS 环境变量提供不透明 Bearer token 到身份的映射；服务端只保存 token 哈希，不打印配置。未配置时受保护 API 拒绝访问，不生成默认管理员。该适配器可替换为 IdP，但不能宣称已接 OIDC。

身份衍生 buyer_key = hash(tenant_id, subject)，作为订单、偏好、缓存的存储键。会话存储键同时含 owner_scope；相同 userId 或会话外部 ID 在不同租户映射到不同空间。客户端 buyer_id 仅允许与已认证 subject 一致，不能覆盖身份。任务查询、订单查询/取消、审批读取和 WebSocket 均校验所有者。WebSocket 在首帧传 token，避免凭证进入 URL 日志。前端提供会话级 token 输入，并从 /commerce/me 读取身份。

## 2. 写操作审批
新增持久化 SQLite 审批账本。状态 pending → approved → executing → completed，执行结果不明确进入 uncertain；过期、身份、会话、工具、规范 JSON 参数摘要任一不匹配均拒绝。模型调用写工具时生成审批提案；用户查看提案并通过受认证 API 批准，然后下一次请求携带 approval_id。approval_id 放在可信运行上下文，不交给模型生成。

执行前用数据库原子 CAS 领取批准；重复使用同一已完成审批返回保存的回执。executing/uncertain 不自动重放副作用，需要核对业务状态。此行为不等于所有外部操作 Exactly-Once。直接取消订单接口也走同一审批门。原业务工具白名单仅解决框架交互，不能绕过应用审批。

## 3. 任务可靠性
Redis Streams 消费循环实际接入两个 stream 的 stale pending 回收，保留 message_id 与所属 stream 以便 ack。以并发槽控制正常消费和回收总执行量；长任务续租避免仍在工作的消息被重新领取。失败或 [error] 结果进入 failed，不能标为 done；死信同步更新任务状态。请求幂等指纹含所有者、会话和请求输入，避免身份碰撞。等待同步结果必须按 task_id 查询，不能把同会话另一任务的 final.result 当成自己的答案。

同会话执行串行，Agent 原生状态在安全工具完成边界落盘。恢复区分 native AgentState 与外部工具回执：能够重用已完成结果；中断的模型流不能恢复 Token 级进度。先验证具体事件边界，再声明恢复粒度；任何没有实跑的跨进程恢复标 UNVERIFIED。

## 4. 模型治理
保留 OpenAI 兼容模型适配，明确调用及总预算超时、有限重试、备用路由。只对瞬时错误重试；流已发出后不能静默拼接不同模型答案。Token 使用 Provider Usage，缺失标 UNKNOWN；价格估算与账单费用分开，价格配置带版本。预算路由使用明确配置和稳定分桶，默认关闭灰度。进程内并发限制不宣称是集群全局配额。

离线模式用确定性 AgentScope Model，明确 SYNTHETIC，不能使用假密钥伪装在线调用；同一工具与治理路径实际接通。

## 5. 评测、CI 与观测
loadtest 成功须满足 HTTP 2xx、有效 final_text 且无 [error]，统计失败类别，失败必须非零退出。保留每个请求与多次重复原始 JSON/CSV，记录 SHA、环境、参数、输出质量判据；小样本 P95/P99不宣称代表生产 SLO。原 291 项测试保持，并增加认证、越权、审批重放、参数替换、恢复、模型故障和压测误判回归。

CI 固定 Python 3.11、Node 22.22.2，uv frozen、npm ci；单元测试、前端构建、离线 E2E、Redis 集成分开报告。未触发远程 CI 不称 GitHub Actions 已通过。审计保留受控 owner/run/task 关联，避免明文 token、Prompt、地址和私有文档进入运行日志。

## 接口与模块所有权
auth_identity.py：AuthDirectory.from_json / authenticate(token) / Principal.buyer_key / scope_session(raw)。
approval_store.py：async propose(owner, session, tool, arguments)、approve(owner, id)、begin(owner, session, tool, arguments, approval_id)、complete(id,result)、mark_uncertain(id)。begin 返回 decision、approval_id、result。
队列：IntentTask 增加 approval_id；TaskStatus 增加 buyer_id；消费器与 worker 保留原接口并增加恢复治理。
装配、DTO、HTTP/WS、工具接线、前端、离线 Model、文档与 CI 由主 Agent 统一修改。身份/审批、队列/worker、网关/评测分配独立文件集，不并发修改 composition.py、settings.py、server.py 或 orchestrator.py。

## 验收与剩余边界
必须实跑：全量 pytest；前端构建；跨租户查询/订阅拒绝；审批未授权、过期、替参、重放；任务失败正确分类；实际 pending 恢复；完整离线 Agent 工具链；Benchmark 原始记录。既有隐式行为变化须保留业务断言并更新公开接口契约，不删除或关闭旧测试。
数据飞轮自动调参、Prompt A/B、生产 IdP、分布式模型配额与真实 LLM 费用另列未完成项；不能把本轮修复包装为这些能力已实现。
