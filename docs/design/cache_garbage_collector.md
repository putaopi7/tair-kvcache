# Cache Garbage Collector 后台扫描 GC 设计

| 项目 | 内容 |
|---|---|
| 状态 | V1 已实现并完成相关单测/E2E；基线已包含 CacheReclaimer 异步删除与过度逐出优化（#234） |
| 更新时间 | 2026-07-22 |
| 涉及模块 | `manager`、`meta`、`config`、`metrics`、`service` |
| 历史参考 | [PR #184](https://github.com/alibaba/tair-kvcache/pull/184) |

本文档定义 Cache Garbage Collector（以下简称 GC）V1 的行为契约和实现边界。PR #184 仅作为历史实现参考；最终行为以本文档和实现为准。

## 1. 背景

### 1.1 为什么需要后台 GC

KVCM 中 Cache Location 的正常生命周期为：

```text
CLS_WRITING -> CLS_SERVING -> CLS_DELETING -> metadata removed
```

当前异常 metadata 主要依赖两类被动触发器清理：

1. 容量达到水位后，由 Reclaimer 采样逐出；
2. key 再次被访问时，通过 `MightExist()` 等检查机会式清理。

如果容量没有达到水位，异常 key 此后也不再被访问，metadata 就可能长期残留。本需求建立一个只在 leader 运行的后台 GC，使确定无效的对象能够主动收敛，并为后续 TTL、存储节点丢失等场景提供统一的后台执行入口。

当前 metadata backend 没有按 Location 状态或创建时间查询的索引。V1 不在写入热路径维护新索引，而是按 cursor 分批扫描 authoritative metadata。cursor 只把一次全量扫描摊到多个 tick；一个完整 round 仍会读取全部 Block 和 Location metadata，成本约为 `O(keyspace + location metadata)`。

V1 采用“以固定间隔扫完一轮，再长时间休眠”的简单策略。默认 WRITING grace 和 round cooldown 都是 24 小时，避免小 keyspace 被持续重复扫描。若 active round 的开销仍不可接受，再根据性能数据引入动态 pacing 或到期索引。

### 1.2 当前已知的异常状态

#### 长期 CLS_WRITING

`StartWriteCache` 会把 `CLS_WRITING` Location 写入 metadata，并把 `write_session_id` 保存在当前 leader 的内存中。服务端把 write session timeout 限制为最多 1800 秒；健康 leader 会在 client `FinishWriteCache` 或 timeout callback 中完成状态流转或清理。

如果持有 session 和 timer 的进程在完成前退出，例如 `StartWriteCache` 后发生主备切换，新 leader 无法恢复旧进程内存中的 session：

1. 旧 `write_session_id` 无法继续 Finish；
2. metadata 可能长期停留在 `CLS_WRITING`；
3. 后续写入可能因已有 WRITING Location 而被阻塞；
4. 若没有水位逐出和业务访问，该对象无法自行收敛。

#### 长期 CLS_DELETING

正常删除需要完成状态 CAS、Sync、物理数据删除和最终 metadata CAD。链路中断可能留下长期 `CLS_DELETING`，但仅凭状态和持续时间无法区分：

- 数据尚未释放，可以重试；
- 数据已经释放，只缺最终 CAD；
- URI 空间已经被其他数据复用。

后两种情况下盲目重试物理删除可能造成 UAF。安全恢复依赖 storage URI version/epoch 和后端原子校验，因此长期 DELETING 是已知场景，但不进入 V1。

### 1.3 V1 取舍

V1 只处理“创建时间超过 grace 的 `CLS_WRITING` Location”，并只建设完成该场景所需的最小闭环：

```text
leader LoopThread
  -> authoritative batch scan
  -> 固定 WRITING 判定
  -> WRITING 条件删除
  -> 复用 SchedulePlanExecutor::SubmitAsync
  -> 记录结果
```

V1 不提前抽象通用 Rule Engine、Candidate Source、Action Dispatcher 或 Admission Controller。扩展点只保留在组件边界上：GC 负责调度，MetaIndexer 提供无副作用维护扫描，Executor 提供带状态前置条件的删除。接入第二种真实场景时，再提取共同接口。

现有 `WriteLocationManager` 的 location ID 隔离问题，以及 Reclaimer 与 `FinishWriteCache` 的结算窗口，是独立的既存正确性问题。它们应单独修复和回归，不作为 GC V1 的依赖，也不在本 PR 顺手改造。

## 2. 设计范围

### 2.1 V1 目标

V1 实现以下五项能力：

1. **低频全量巡检**：按 Instance 和 cursor 分批读取 authoritative metadata；每个 tick 最多推进一个 batch，完成一轮后进入 cooldown。
2. **无副作用读取**：GC 扫描不能更新 LRU/access time、hit count、revisit histogram，也不能改变 online hot cache。
3. **保守识别长期 WRITING**：只处理状态、创建时间和标识均明确有效，且年龄达到 grace 的 Location；异常时间或读取错误一律跳过。
4. **异步条件删除**：Executor 接受请求后在 worker 中重新读取 metadata，并且只允许 `CLS_WRITING -> CLS_DELETING`；并发 Finish 或其他删除者由 CAS 仲裁。
5. **有界异步反压和 leader 生命周期**：使用小型在途窗口并行消费 Executor 能力，每个 tick 最多提交一个请求，达到窗口上限后停止扫描；GC 只在 leader recovery 完成后运行，降级时在 metadata cleanup 前停止并 join。

所有扫描、判定和提交均保持严格的 `instance_id` 隔离。

### 2.2 V1 非目标

以下能力不进入 V1：

1. 通用 Rule/Candidate/Action 框架和动态规则注册。
2. WRITING deadline ZSET、TTL expiry index、CDC 或其他增量候选源。
3. 动态 pacing、adaptive backoff、每轮预算、跨 Instance 公平调度。
4. 动态或大规模并发窗口、多维 bytes/Group 配额、Future deadline 和持久化任务。
5. `WriteLocationManager` 三元组索引、settling guard，以及 Reclaimer/Finish 竞态修复。
6. `CLS_DELETING` 自动恢复和 storage version/epoch。
7. TairMempool node-missing、Block TTL 和闲置 Instance 清理。
8. 查询链路 fast-submit 重构。
9. GC lease/generation、跨进程 cursor 或 pending 恢复。
10. data storage I/O timeout/cancel 和 CAS 后补偿状态机。
11. dry-run、动态配置热更新和按 Group/Instance 单独启停。
12. 不改变 #234 的异步删除结果契约：继续复用 accepted、端到端 Future、`PlanExecuteResult`、`EC_PARTIAL_OK` 和 promise exactly-once 完成语义；GC V1 仅增加删除前置状态条件并消费结果。

### 2.3 依赖与交付边界

GC 的 `LoopThread` 回调会同步调用 Registry 和 maintenance scan，并复用现有 metadata Redis 客户端的 timeout 和 retry 语义。公共 Redis command timeout 不是 GC V1 合入或生产启用的硬前置；CacheReclaimer 和其他 metadata 调用方也依赖同一套基础设施。

GC 新增的风险暴露点是：demotion 必须在 cleanup 前 `Join()`，因此慢或失联的 Redis 调用可能延长降级时间。GC 默认关闭，生产上线时应灰度开启并监控扫描错误与降级时延；若确认底层命令缺少有限返回上界，应作为公共基础设施问题独立治理。GC V1 不自行重构 Redis 建连、认证、重连、timeout/cancel 或连接池语义。

当前基线已包含 #234 的异步删除能力。GC 已通过 `SubmitAsync()` 接入该链路，并在共享 `PrepareDeleteTaskImpl` 中使用可选 `expected_status` 完成 `WRITING -> DELETING` 条件筛选与 CAS；其他调用方不设置该字段时行为不变。首次入队 accepted、端到端 Future、Get/CAS/Sync、delay、二次入队、物理删除和 promise 收敛均沿用 #234 的实现，现有同步 `Submit()` 也保持兼容。

## 3. 总体设计

### 3.1 组件与调用链

```mermaid
flowchart LR
    L[Leader lifecycle] --> G[CacheGarbageCollector]
    G --> R[Registry snapshot]
    R --> M[ScanLocationsForMaintenance]
    M --> P[Old WRITING predicate]
    P --> E[SchedulePlanExecutor conditional SubmitAsync]
    E --> F[Bounded Future window]
    F --> G
```

| 组件 | V1 职责 |
|---|---|
| `CacheGarbageCollector` | 复用公共 `LoopThread` 做单线程调度，维护 round/cursor、固定判定、有界 Future 轮询和 pending target 去重 |
| `RegistryManager` | 在每轮开始时提供 Instance Group/Instance 快照 |
| `MetaIndexer` | 从 authoritative backend 返回一个无副作用的 metadata batch |
| `SchedulePlanExecutor` | accepted 准入、worker 内重新读取、条件 CAS、Sync、物理删除、最终 CAD 和端到端 Future |
| `Server/CacheManager` | 在 leader recovery/demotion 和进程析构时管理 GC 生命周期 |

GC 不直接访问 data storage，也不自行实现 Location 删除状态机。

GC 的扫描协调不放入 `SchedulePlanExecutor`。Executor 是 Reclaimer、GC 等调用方共享的删除执行资源；把可能受 Redis SCAN/Get 延迟影响的完整扫描循环放入其中，会占用删除 worker，并把巡检速度与删除吞吐耦合。若扫描任务在 Executor worker 内提交删除后等待 Future，单 worker 配置下还会形成队内自依赖。

因此 V1 只为扫描协调保留一个轻量串行循环，并复用 `common::LoopThread`，不自行实现线程、定时等待和条件变量。GC 找到候选后立即调用 `SchedulePlanExecutor::SubmitAsync()`；Get/CAS/Sync、delay、物理删除和最终 CAD 仍全部由 Executor 执行。

### 3.2 最小运行状态

GC 调度循环只保存：

```cpp
struct ScanState {
    std::vector<InstanceScanEntry> instances;
    size_t instance_index = 0;
    std::string cursor = SCAN_BASE_CURSOR;
    uint64_t round_id = 0;
    std::optional<std::chrono::steady_clock::time_point> next_round_at;
};

struct InflightDelete {
    uint64_t round_id = 0;
    std::string instance_id;
    size_t target_count = 0;
    std::chrono::steady_clock::time_point submitted_at;
    std::vector<PendingLocationKey> pending_locations;
    std::future<PlanExecuteResult> future;
};

std::vector<InflightDelete> inflight_deletes;
std::set<PendingLocationKey> pending_locations;
```

`PendingLocationKey` 至少包含 `(instance_id, block_key, location_id)`。在途窗口和 pending 集合只由 `LoopThread` 回调访问，其规模分别受 `max_inflight_delete_requests` 和 `max_inflight_delete_requests * scan_batch_size` 约束。跨线程只保留 GC stop 标志和一个 `LoopThread` handle，不维护自建 condition variable 或通用 task table。

每次 leader recovery 后重新开始一个 round；降级、重启或重新成为 leader 时不恢复旧 cursor。重复覆盖由条件 CAS 保证安全。

### 3.3 LoopThread 回调

`LoopThread` 使用 strict interval，在上一次回调结束后至少等待 `scan_interval_ms`，因此慢调用返回后不会追赶错过的 tick。每个回调按以下顺序执行：

1. 若收到 stop 请求，立即退出；若持有未完成 Future，只丢弃本地 handle 和 pending，不取消或等待底层 Executor 任务。已接受任务在 leader cleanup 期间按 best-effort 语义继续执行。
2. 非阻塞轮询全部在途 Future：ready/exception/invalid 均记录结果、释放其 pending target 并移出窗口；未 ready 的任务继续占用槽位。
3. 若在途请求数已达到 `max_inflight_delete_requests`，本 tick 结束，不继续扫描。
4. 若仍处于 round cooldown，本 tick 结束。
5. 若没有本轮快照，从 Registry 获取 Group/Instance 列表，按 `(instance_group, instance_id)` 排序；失败时按普通 tick 间隔重试。
6. 对当前 Instance 调用一次 `ScanLocationsForMaintenance(cursor, scan_batch_size)`。
7. 保存 next cursor，遍历返回的 Location，筛选长期 WRITING，并过滤已经 pending 的 `(instance_id, block_key, location_id)`。
8. 删除 target 按 `(block_key, location_id)` 去重，最多保留 `scan_batch_size` 个。请求为空时不调用 Executor。
9. 非空请求设置 `expected_status=CLS_WRITING`，调用 `SubmitAsync()`：
   - `accepted=true` 且 Future valid：保存 Future，并为最终请求建立 pending target；
   - `accepted=false`：不建立 Future 或 pending，记录 `submit_rejected`；
   - accepted/Future 契约不一致：记录 `submit_contract`，不建立本地状态；
   - 调用抛异常：记录 `submit_exception`，不建立本地状态。
10. 当前 Instance cursor 回到 base 后推进到下一个 Instance；全部 Instance 完成后结束 round，并设置 `next_round_at = now + round_pause_ms`。
11. tick 结束后至少等待 `scan_interval_ms`。慢调用返回后不追赶错过的 tick。

窗口默认包含 2 个请求。一个慢或卡住的物理删除只占用一个槽位，其他槽位仍可继续扫描和提交；只有全部槽位被占用时才暂停扫描。这为当前无容量上限的 Executor 队列提供了 GC 调用方侧的硬反压，同时利用 #234 已提供的 worker 并发。每个 tick 仍最多提交一个请求，不会形成突发灌入。`inflight_delete_count` 和 `inflight_delete_age_ms` 分别表示当前在途请求数和最老任务年龄。

cursor 在 SubmitAsync 前已经推进。rejected、抛异常或 accepted/Future 契约错误时不回滚 cursor，也不保存该批候选；对象仍保留在 authoritative metadata，等待后续 full round 重新发现。这样避免为 V1 引入额外 retry queue。

同一 batch 中超过 target 上限的候选不会保存在额外 pending 表中；它们仍留在 authoritative metadata，等待后续 round 再次发现。这会牺牲极端场景的收敛速度，但保持 V1 状态简单。

### 3.4 全量扫描语义和成本

一个 round 只在开始时获取一次 Registry 快照，然后依次把每个 Instance 的 cursor 推进到 base。快照之后新增或删除的 Instance 允许本轮不可见或返回 Indexer 不存在，下一轮重新获取快照后收敛。

Redis SCAN 和本地 backend cursor 不提供并发 exactly-once：

- 同一 key 在一轮中可能重复；
- 并发删除或索引移动可能使本轮漏过 key；
- 并发新增 key 可能到下一轮才被看到。

V1 只要求重复处理安全，并由后续 round 重新覆盖。扫描发现不是删除授权；最终是否删除由 Executor 的状态 CAS 决定。

设某 Instance 有 `N` 个 Block、平均每个 Block 有 `L` 个 Location，则一轮判定成本约为 `O(N + N*L)`。`scan_batch_size` 是 backend hint，不保证 Local backend 的单 shard 返回量严格受限。

设 active round 耗时为 `S`、cooldown 为 `P`，正常候选的最坏发现时间约为：

```text
orphan_writing_grace_period + S + P
```

默认 grace 和 `P` 都为 24 小时，因此该能力定位为最终收敛，不提供分钟级 SLA。SCAN 并发遗漏、target 裁剪、backend 错误和全部在途槽位卡住都可能继续延长时间。

## 4. 详细设计

### 4.1 无副作用 authoritative scan

GC 不能直接复用在线 `GetLocations()`：

- Local/Dummy backend 的读取会更新 LRU 或 revisit 统计；
- dual-backend Running 状态下，现有 `ListKeys()` 可能扫描可淘汰的 hot cache，而不是完整 persistent metadata。

V1 对 GC 暴露一个合并的维护接口，避免 GC 自己拼接 List 和 Get：

```cpp
struct MaintenanceScanBatch {
    std::string next_cursor;
    KeyVector keys;
    CacheLocationMapVector locations;
    std::vector<ErrorCode> location_results;
};

ErrorCode MetaIndexer::ScanLocationsForMaintenance(
    RequestContext *request_context,
    const std::string &cursor,
    size_t limit,
    MaintenanceScanBatch &out) noexcept;
```

接口契约：

1. key 和 Location 都读取 authoritative backend；dual-backend 时不能只看 hot cache。
2. 读取不更新 access/LRU/revisit，也不向 hot cache 回填。
3. `keys`、`locations` 和 `location_results` 必须按下标对齐；shape 不一致视为整批错误，不做删除。
4. Scan 级错误时不推进 cursor；单 key 已不存在或读取失败时跳过该 key，cursor 正常推进，由下一轮再覆盖。
5. `limit` 只作为 backend hint；调用方不能假设实际数量一定不超过该值。

实现上，该契约下沉到 backend 的合并扫描接口。Redis/AsyncRedis 组合 authoritative `SCAN` 和只读反序列化；Local/Dummy 在底层容器中直接复制 Location，不调用在线 `Lookup/Touch`。backend manager 在 dual-backend 模式下强制选择 persistent backend，在线读取接口保持不变。

### 4.2 长期 orphan WRITING 判定

Location 同时满足以下条件才进入请求：

1. `status == CLS_WRITING`；
2. `instance_id`、`block_key` 和 `location_id` 均有效；
3. `create_time` 可解析，且不晚于当前时间；
4. `now - create_time >= orphan_writing_grace_period`。

任一条件不确定都 fail-closed 跳过，不尝试删除。V1 不逐 Location 记录拒绝原因，避免全量扫描制造高基数指标和日志；只对 batch shape、backend 读取等操作异常计数。

`create_time` 使用现有微秒时间戳。实现先判断 `now_us >= create_time`，再用有符号或检查过的 duration 计算 age，不能用无符号减法掩盖时钟回拨。

V1 不查询 `WriteLocationManager`。理由是：

- 服务端 write session 硬上限为 30 分钟；
- grace 最小为 1 小时，在 write session 硬上限之外保留 30 分钟安全余量；生产默认仍为 24 小时；
- 在健康进程中，超过 session 上限的 WRITING 已不再是合法活跃写入；
- HA 后旧 session 本来也不在新 leader 的内存中；
- 扫描后的并发 Finish 由最终条件 CAS 保护。

1 小时下限用于覆盖 write-session timeout 轮询、正常调度抖动和 AsyncRedis 的正常异步刷写窗口；若 metadata 异步写入超过 30 分钟仍未收敛，视为公共 backend 一致性故障，GC V1 不增加专用 `Sync/fence`。24 小时默认值不是为了容纳合法慢写，而是第一版自动删除路径的保守爆炸半径，为监控和人工干预留出更充足时间。

`create_time` 和 GC 的 `now` 可能来自不同机器的 wall clock。V1 假设集群 NTP 正常，秒级偏差相对至少 1 小时的 grace 可接受；时间回拨或未来时间一律跳过。若后续需要消除该假设，应让创建和判断使用同一个 authoritative time source。

### 4.3 条件化 Location 删除

扫描结果只负责发现，不能直接授权删除。V1 在 `CacheLocationDelRequest` 末尾增加可选前置状态：

```cpp
struct CacheLocationDelRequest {
    std::string instance_id;
    std::vector<int64_t> block_keys;
    std::vector<std::vector<std::string>> location_ids;
    std::chrono::microseconds delay{std::chrono::seconds(0)};
    std::optional<CacheLocationStatus> expected_status;
};
```

GC 始终设置 `expected_status=CLS_WRITING`。Executor 对每个 target：

1. 重新读取当前 Location；
2. Location 不存在或状态不是 `CLS_WRITING` 时跳过；
3. 构造固定的 `CLS_WRITING -> CLS_DELETING` CAS；
4. 只把 CAS 成功的精确子集交给现有 Sync、物理删除和 metadata CAD。

如果并发 `FinishWriteCache` 已经把状态改为 `CLS_SERVING`，GC 在重新读取或 CAS 阶段自然失败；如果 Reclaimer 同时提交同一 target，也只有一个 CAS winner。

未设置 `expected_status` 的现有调用方保持当前行为。V1 不新增 GC 专用删除状态机，也不改变物理删除和最终 CAD 的顺序。

#234 的 `SubmitAsync()` 首次入队成功后立即返回；Get/CAS/Sync、delay 和物理删除都在 Executor 中完成。`expected_status` 的筛选和固定 CAS 落在共享 `PrepareDeleteTaskImpl`，同步与异步入口复用同一 admission 语义。

### 4.4 有界 Future 窗口、pending 去重和结果

GC 同一时刻最多持有 `max_inflight_delete_requests` 个已接受请求的 `std::future<PlanExecuteResult>`：

- `SubmitAsync()` 只有返回 `accepted=true` 且 valid Future 时才保存 Future，并建立最终请求对应的 pending target；
- `accepted=false`、invalid Future、accepted/Future 契约不一致或抛异常均不建立 Future、pending 或其他本地状态；
- 每个 tick 先非阻塞轮询全部 Future，终态或异常只释放该任务的槽位和 pending；
- 未 ready 的 Future 继续占用槽位，但只在窗口全部占满时停止后续 Scan 和 Submit；
- pending key 包含 `instance_id`，相同 block/location 在不同 Instance 中互不影响；
- 空请求不进入 Executor。

pending 集合不承担删除授权，只用于覆盖请求 accepted 到 Executor CAS 之间的窗口，以及 Redis SCAN 可能在同一 round 重复返回 key 的情况。最终并发安全仍由 Executor 的 `WRITING -> DELETING` 条件 CAS 保证。窗口和单请求 target 上限共同限定 pending 集合大小，不新增 bytes credit、deadline 或跨进程恢复。

V1 沿用现有 `PlanExecuteResult` 语义，不新增 PARTIAL 或 per-target result：

- `EC_OK`：任务按现有 Executor 语义完成，也可能表示 target 已不存在或条件不匹配而 no-op；
- `EC_PARTIAL_OK`：物理删除或 metadata CAD 仅部分成功，是明确终态；记录结果并释放对应槽位；
- 其他 ErrorCode：删除链路明确失败，记录 `delete_result_count{status}`，并输出包含 round、Instance、target 数和错误信息的 warning；
- Future exception/invalid：Executor contract error。

GC 不因单次失败停止后续 round，也不自动重试同一请求。仍为 WRITING 的对象由后续 full scan 再次发现；已经进入 DELETING 的失败对象留给未来 DELETING reconciliation 处理。

## 5. 生命周期与并发

### 5.1 启动和重复调用

- `enabled=false`：不创建 GC `LoopThread`。
- `enabled=true`：只构造 GC；首次 leader recovery 成功后调用 `Start()`。
- `Start()` 在 `LoopThread` 已运行时幂等；线程已 join 后再次调用会创建新的 `LoopThread`、清空旧 cursor，并从新 round 开始。
- `RequestStop()` 和 `Join()` 均幂等。
- `enabled=true` 时配置非法或构造依赖缺失，服务初始化失败，不静默降级为 disabled；`enabled=false` 时不校验未使用的 GC 运行参数。
- leader promotion 时线程创建失败，`Start()` 返回错误，本轮不 Resume Reclaimer、也不开放 leader-only 请求。
- 某个 Instance 的 Indexer 尚未 recover 时，本轮跳过并告警，由下一轮覆盖，不让整个服务退出。

### 5.2 Leader recovery 和 demotion

升主顺序：

```text
RegistryManager::DoRecover()
  -> CacheManager::DoRecover()
  -> CacheGarbageCollector::Start()
  -> Reclaimer.Resume()
  -> enable leader-only requests
```

降级采用“先发停止信号，后 join”的顺序：

```text
GC.RequestStop() + Reclaimer.Pause()
  -> DisableLeaderOnlyRequests()
  -> WaitForAllLeaderOnlyRequestsToComplete()
  -> GC.Join()
  -> CacheManager::DoCleanup()
  -> RegistryManager::DoCleanup()
```

`RequestStop()` 只设置 GC stop 标志，并调用 `LoopThread::RunOnce()` 唤醒可能处于 tick/cooldown 等待的循环；回调看到 stop 后立即返回。该操作不 join，不能在关闭 leader 请求之前阻塞。`Join()` 随后调用 `LoopThread::Stop()` 完成 join，且必须位于 CacheManager/Registry cleanup 之前，保证 GC 不再访问它们。V1 不修改公共 `LoopThread` 接口。

`LoopThread` 回调在每次 Registry、Scan 和 SubmitAsync 之前检查 stop；已经进入的 Registry/Scan 同步调用不能强制取消，`Join()` 会沿用底层现有 timeout/retry 语义，慢调用可能相应延长 demotion。Executor admission 已异步化，不阻塞 GC 的 `Join()`；GC 在 stop 时只丢弃本地 Future handle 和 pending 集合，不等待端到端删除 Future。`Join()` 只保证扫描回调不再访问 Registry、MetaIndexerManager 等依赖，不为物理删除建立 drain 屏障。

降级时若 GC 已提交删除，只丢弃本地 Future handle，不取消 Executor 中的任务。该任务与 Reclaimer 已提交任务使用相同的 best-effort detach 语义：cleanup 前完成则正常收敛；若已完成 `WRITING -> DELETING` CAS，但后续重新获取 MetaIndexer、访问 DataStorage 或执行 metadata CAD 时依赖已被 cleanup，则 Future 可能以错误终态结束并留下长期 `CLS_DELETING`。V1 优先保证物理删除不会阻塞 leader demotion，不引入统一 Executor drain、任务资源 lease、跨 leader generation 或 DELETING 补偿；这些能力作为独立生命周期治理工作处理。

### 5.3 进程停止和析构

`CacheManager` 析构时首先停止并 join GC，再拆除 Registry、MetaIndexer、Executor、metrics 等 GC 依赖。当前未引入 GC 时的 WLM/Reclaimer shared ownership 顺序不是既存 UAF；本设计不借 GC PR 重排无关组件。

Stop 通过 GC stop flag、`LoopThread::RunOnce()` 和最终 `LoopThread::Stop()` 打断 tick sleep 与 round cooldown。若回调正在 Registry/metadata 同步调用中，`Stop()` 需要等待该调用按底层现有 timeout/retry 语义返回；若底层缺少有限返回上界，进程停止或 demotion 可能被延长。

## 6. 异常处理

| 场景 | V1 行为 |
|---|---|
| Registry snapshot 失败 | 丢弃不完整快照，普通 tick 后重试 |
| Instance/Indexer 不存在 | 跳过当前 Instance，下一轮重新发现 |
| Scan 失败 | 不推进 cursor，普通 tick 后重试 |
| key 在 SCAN 后被并发删除（`EC_NOENT`） | 正常跳过，不计入 operation error |
| 单 key Location 读取失败 | 跳过该 key、记录 `scan_key` error，下一轮覆盖 |
| batch shape 不一致 | 整批拒绝，不提交 |
| Location 字段或时间非法 | fail-closed 跳过 |
| 空候选 | 不调用 Executor |
| SubmitAsync rejected | 记录 `submit_rejected` error，不占槽位、不建立 pending；cursor 不回滚，后续 round 重新发现 |
| SubmitAsync 直接抛异常 | 记录 `submit_exception` error，不占槽位、不建立 pending；cursor 不回滚 |
| accepted/Future 不一致 | 记录 `submit_contract` error，不占槽位、不建立 pending；cursor 不回滚 |
| 条件不匹配或 CAS loser | 合法 no-op，不告警为数据错误 |
| Future 返回错误 | 释放对应槽位和 pending、记录结果，后续 round 继续 |
| Future 长期未完成 | 保留其槽位和 pending、持续上报最老 age；其他槽位仍可推进，V1 不超时释放 |
| stop 期间存在 Future | 丢弃本地 handle 和 pending，不取消或等待底层任务；任务 best effort 继续，CAS 后失败可能留下 `CLS_DELETING` |

所有错误路径都必须经过 `scan_interval_ms` 或 cooldown，不能形成零间隔重试。

## 7. 可观测性

V1 只保留能回答“是否在扫描、发现了什么、删除是否卡住”的核心指标：

| 指标 | 类型 | 说明 |
|---|---|---|
| `cache_gc.scan_round_count` | Counter | 完整 round 完成次数 |
| `cache_gc.scan_key_count` | Counter | 扫描 Block 数 |
| `cache_gc.candidate_count{reason}` | Counter | 候选数；V1 reason 固定为 `orphan_writing` |
| `cache_gc.delete_target_count` | Counter | 实际提交的 Location 数 |
| `cache_gc.delete_result_count{status}` | Counter | Future 终态 |
| `cache_gc.operation_error_count{stage}` | Counter | Registry/scan/submit/future 等异常；并发 `EC_NOENT`、条件不匹配和 CAS loser 不计入 |
| `cache_gc.inflight_delete_count` | Gauge | 当前在途删除请求数 |
| `cache_gc.inflight_delete_age_ms` | Gauge | 最老在途 Future 的年龄，无任务时为 0 |
| `cache_gc.round_duration_ms` | Gauge | 最近一个 active round 耗时 |

round 和结果日志包含 `round_id`、`instance_id`、target 数、result；扫描错误日志额外包含 cursor 和 error stage。正常逐 key 扫描不打 INFO，block/location 明细只在诊断级日志中输出，避免 GC 自身制造日志压力。

## 8. 测试方案

### 8.1 单元测试

1. disabled 不创建 `LoopThread`；Start/RequestStop/Join 重复调用安全，重新 Start 从 base cursor 开始。
2. 每个 tick 最多调用一次 Scan；cursor、Instance 推进和 round cooldown 正确。
3. Registry/Scan 失败按普通间隔重试，不推进错误 cursor，也不零间隔空转。
4. maintenance scan 在 dual-backend 下读取 persistent metadata，不依赖 hot cache。
5. Local/Dummy maintenance scan 不改变 LRU/access/revisit，也不回填 hot cache。
6. 只有状态为 WRITING、字段有效且年龄达到 grace 的 Location 成为候选；未来时间、解析失败和边界值 fail-closed。
7. grace 配置不得小于 1 小时，默认值为 24 小时。
8. 请求保持 Instance 隔离，按 `(block_key, location_id)` 去重并受 batch target 上限约束；空请求不调用 Executor。
9. Executor 对 GC target 固定执行 `WRITING -> DELETING`；请求排队后 Finish 先把状态改为 SERVING，以及扫描后已变为 DELETING 或 NOENT 时，均不做物理删除。
10. GC 与另一个删除者同时提交同一 target 时只有一个 CAS winner。
11. 有界窗口未满时一个慢 Future 不阻止其他 batch 提交，窗口占满后停止 Scan；Future 终态只释放自己的槽位和 pending，随后恢复扫描。
12. RequestStop 能打断 sleep/cooldown；活跃 maintenance scan 返回前 Join 必须等待，未完成删除 Future 不阻塞 Join 并按 best-effort detach；CacheManager cleanup 会先停止 GC 扫描线程。
13. 未设置 `expected_status` 的现有 Executor 调用方行为不变。
14. 并发删除导致的 `EC_NOENT` 不增加 operation error；SubmitAsync 直接抛异常归入 `submit_exception` 而不是 `tick_exception`。
15. accepted 到 CAS 期间同一 Instance 的相同 target 被 pending 过滤；不同 Instance 的相同 block/location 不互相抑制；rejected、invalid 和空请求不留下 pending。

测试使用注入时钟或回填旧 `create_time`，不通过放宽生产 grace 下限制造并发窗口。

### 8.2 集成测试

集成测试覆盖以下组合和场景：

1. KVCM + Dummy metadata + NFS data storage：模拟进程重启后 session 丢失，回填超过 grace 的 WRITING，确认删除链路完成 metadata 清理，Block 可再次写入。
2. 同一 Group 多 Instance 中，未到 grace 的 WRITING 不删除，只有过期 Instance 被处理。
3. cached metadata 模式下通过 persistent backend 发现并清理 orphan；persistent-only/no-backfill 和 LRU 不变由 backend component test 单独验证。
4. GC 开启期间执行真实 leader demotion，确认进程健康进入 standby，cleanup 完成后 scan round 不再增长。

并发 Finish、真实 Executor backlog、慢 admission 和 Join 等需要精确控制时序的场景在 C++ component test 中验证，避免依赖不稳定的 wall-clock 或外部故障注入。

NFS 只用于验证物理删除控制链路；Dummy 持久化文件用于构造和验证 metadata 最终清理。

### 8.3 性能测试方法

采用 matched A/B 和分层负载验证：

1. 固定 binary、资源配额、keyspace、请求序列、目标 QPS、运行时长和随机种子，对比 GC disabled、GC enabled 但处于 cooldown，以及 GC enabled 且 active scan 三种场景；轮换执行顺序并重复运行，降低缓存预热和系统抖动影响。
2. 分别构造无垃圾、少量 orphan WRITING 和大量 orphan WRITING，区分纯扫描成本、候选判定成本和实际删除成本。
3. 逐级扩大 keyspace，并组合不同 `scan_batch_size`、`scan_interval_ms`，观察 active round 的扫描吞吐和在线请求退化趋势。
4. 让慢物理删除分别占用一个和全部在途槽位，验证部分槽位被占用时仍可推进、窗口满时停止扫描的反压行为。

比较在线 Get/StartWrite/FinishWrite P50/P95/P99、Redis QPS/CPU/网络流量、KVCM CPU/RSS、active round 时长、每秒扫描 key 数和垃圾收敛时间，同时确认业务请求无新增失败、active-scan 场景确实产生扫描量。测试结果用于判断默认 batch/interval 是否可接受，以及 V2 是否需要动态 pacing、减少 metadata 读取量或引入索引化候选源。

## 9. 验收标准

1. disabled、standby 和 leader recovery 完成前不扫描、不提交。
2. GC 能从 authoritative backend 完成全量 round，且扫描不改变业务访问统计。
3. grace 最小 1 小时、默认 24 小时，在 30 分钟 write session 上限之外保留安全余量；判定异常时 fail-closed。
4. 只有长期 WRITING 进入删除请求，其他状态不会被 GC 删除。
5. 并发 Finish、Reclaimer 或重复 SCAN 由 `WRITING -> DELETING` CAS 安全仲裁。
6. 在途删除请求不超过配置上限；单个慢任务不会暂停整个 GC，全部槽位占满时形成硬反压，空请求和错误不会形成提交风暴或 CPU 空转。
7. 每轮完成后至少等待 `round_pause_ms`，不会持续重复全扫。
8. demotion 时先停止新 leader 请求，再在 cleanup 前完成 GC 扫描线程 join；慢 maintenance metadata 调用可以延长降级，但已接受的物理删除不参与 drain，也不阻塞 demotion。
9. 未设置 expected status 的现有 Executor 调用方保持兼容。
10. 通用规则框架、索引化扫描、DELETING、StorageMissing、TTL、闲置 Instance 和 Reclaimer/Finish 修复均未被隐式实现。

## 10. 实现落点与配置

### 10.1 主要修改文件

| 文件 | 职责 |
|---|---|
| `kv_cache_manager/manager/cache_garbage_collector.h/.cc` | 复用 `common::LoopThread`，维护 round/cursor、固定 WRITING 判定、有界 Future/pending 和两阶段停止 |
| `kv_cache_manager/manager/schedule_plan_executor.h/.cc` | 复用 #234 的共享 admission helper，通过可选 `expected_status` 实现固定条件 CAS；同步/异步接口保持兼容 |
| `kv_cache_manager/manager/cache_manager.h/.cc` | GC 构造、leader 启停和析构 |
| `kv_cache_manager/meta/meta_indexer.h/.cc` | 合并的 maintenance scan 接口 |
| `kv_cache_manager/meta/meta_storage_backend*.h/.cc` | authoritative List 与 no-touch Location 读取 |
| `kv_cache_manager/service/server_config.h/.cc` | GC 配置解析和校验 |
| `kv_cache_manager/service/server.cc` | leader recovery/demotion 接线 |
| `package/etc/default_server_config.conf`、`docs/configuration.md`、相关 BUILD | 配置、文档和构建目标 |
| 对应 `manager/meta/service` 测试 | 第 8 章单元与集成测试 |

V1 不修改 `write_location_manager.*` 和 `cache_reclaimer.*`，也不新增 rule/source/admission/task-tracker 文件。

公共 Redis command timeout 不作为 GC V1 的前置 PR；若现网验证确认 Redis 命令缺少有限返回上界，再由独立基础设施 PR 修改 `common/redis_client.*` 及相关测试，不混入 GC 主体 PR。

### 10.2 V1 配置

| 配置 | 默认值 | 说明 |
|---|---:|---|
| `kvcm.cache_gc.enabled` | `false` | 默认关闭，灰度开启 |
| `kvcm.cache_gc.scan_interval_ms` | 1000 | 相邻 tick 的最小间隔 |
| `kvcm.cache_gc.round_pause_ms` | 86400000 | 完成一个 full round 后的 cooldown |
| `kvcm.cache_gc.scan_batch_size` | 256 | backend key 数 hint，同时作为单请求 target 上限 |
| `kvcm.cache_gc.orphan_writing_grace_period_ms` | 86400000 | WRITING 自动清理 grace，必须不小于 3600000 ms |
| `kvcm.cache_gc.max_inflight_delete_requests` | 2 | GC 在途删除请求硬上限，必须大于 0；每个请求最多包含 `scan_batch_size` 个 target |

配置通过一个内聚的 `CacheGarbageCollector::Config` 传入。时间、batch 和在途请求上限必须为正，毫秒到内部 duration 的转换需要检查溢出。1 小时 grace 下限基于当前 1800 秒 write session 上限；若后续修改该协议上限，必须同步重新评估 GC grace 下限。

## 11. 后续演进

以下能力均不属于 V1，只有在真实场景或性能数据出现后再设计。

### 11.1 扫描资源治理

若 active full round 对 Redis 或在线延迟影响明显，可以增加：

- 根据 keyspace 和目标 round 时长计算 inter-batch pacing；
- scan budget、adaptive backoff 和大 Instance 公平调度；
- 可中断的 Local shard cursor；
- 动态并发窗口、bytes/Group 配额、deadline 和长期卡住告警。

扩大并发窗口前必须结合共享 Executor worker/队列负载和线上指标评估；V1 的固定小窗口不承诺物理执行隔离。

### 11.2 Candidate Source 和 Rule

接入第二种垃圾类型时，再从固定实现中提取：

- `FullMetadataScanSource` 或到期索引等 Candidate Source；
- 只负责判定和前置条件的 Rule；
- Location/Block target；
- 复用同一生命周期和预算的 Dispatcher。

WRITING 可增加按 `create_time` 排序的 ZSET，只查询到期 Location；TTL 可增加 expiry index。索引只负责发现候选，删除前仍要读取 authoritative metadata 并条件复核。双写原子性、状态出口清理、历史 backfill 和多 backend 兼容必须先解决。

### 11.3 StorageMissing 和 TTL

TairMempool node-missing 需要三态 probe：`AVAILABLE/MISSING/UNKNOWN`。只有明确 MISSING 才能删除；超时、探测未就绪和解析失败都必须视为 UNKNOWN。一个 Location 任一 spec 明确 MISSING 时，产品语义上整个 Location 失效。

Block TTL 需要先定义 TTL 写入、刷新、authoritative `expire_at`、并发写入竞争和条件 Block 删除。扫描不能刷新 TTL。

### 11.4 长期 DELETING

长期 DELETING 的安全恢复依赖 storage backend 校验 URI version/epoch，确认当前对象仍是 metadata 记录的同一代后才能重试释放。KVCM 单侧记录 epoch 不足以防止 URI 被复用。

### 11.5 独立需求

以下问题单独设计和交付：

1. `WriteLocationManager` 使用 `(instance_id, block_key, location_id)` 索引，并关闭 Reclaimer/Finish settling race；
2. 查询链路发现无效 Location 后的快速删除提交：复用 accepted 和端到端 Future，且不在查询线程同步执行 admission；
3. 连续 N 周无流量的闲置 Instance 回收；
4. GC task 持久化、lease/generation 和跨进程恢复；
5. data storage I/O timeout/cancel；
6. authoritative time source。

### 11.6 建议交付顺序

1. **GC V1 PR**：本文定义的 full scan、固定 WRITING 判定、条件删除、有界 Future/pending、leader 生命周期、指标和测试。
2. **公共基础设施跟进（按需）**：若现网验证确认 Redis 命令缺少有限返回上界，再独立补齐 connect/auth/command/reconnect timeout 和坏连接淘汰。
3. **独立正确性 PR**：WriteLocationManager Instance 隔离和 Reclaimer/Finish settling race。
4. **后续 PR**：根据性能和业务优先级分别接入 pacing/index、StorageMissing、TTL 和 DELETING reconciliation。
