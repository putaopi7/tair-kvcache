# ReportEvent 增量上报与权威快照对账设计

> 状态：待设计评审。
> 本文描述目标语义；设计确认后再继续修改代码和测试。

## 1. 一句话说明

ReportEvent 使用两条互补链路同步 KV Cache 元数据：

- 日常变化走 `EVENT_BLOCK_ADD` / `EVENT_BLOCK_DELETE`；
- 启动、周期校准、事件断档等场景走 `EVENT_BLOCK_SNAPSHOT`。

Snapshot 是一个 reporter 在某一时刻、跨全部介质的完整 cache 事实，不是增量补丁，也不是历史版本存档。

本方案的核心结论：

1. snapshot scope 只有 `instance_id + reporter host_ip_port`；
2. `medium` 是 block 属性，一台机器的 GPU/CPU/Disk 在一个 snapshot 中一起上报；
3. location id 保持稳定，不使用 copy-on-write location；
4. version 只写入 URI，查询只接受当前 committed version；
5. committed version 持久化到 instance metadata，并在响应中返回；
6. commit 后复用现有 reclaimer 任务机制删除旧数据；
7. 接受“写到一半失败，部分旧数据暂时不可见，等待下次完整 snapshot 修复”的语义。

## 2. 要解决的问题

只发增量的成本最低，但进程重启、网络中断、事件丢失或 Subscriber 基线损坏后，KVCM 可能长期保留已经不存在的 block。

只发全量容易收敛，但高频执行会反复重写所有 block，并放大 Redis 和后台清理负载。

因此目标是：

- 稳态成本与实际变化量成正比；
- 低频 snapshot 能权威修复增量链路漂移；
- 未提交版本不能被查询当成当前事实；
- Subscriber 能明确知道 KVCM 当前 committed version；
- KVCM 重启后从 instance metadata 恢复版本，不扫描全量 block 猜版本；
- 旧数据复用既有 reclaimer 删除，不再维护一套 snapshot 专用任务框架。

## 3. 在整体架构中的位置

KVCM 是 KV Cache metadata control plane，不传输真实 KV tensor，也不替代 Master/FlexLB 做最终调度。

```mermaid
flowchart LR
    Engine["RTP-LLM / vLLM / V6D"] --> Adapter["Reporter / Subscriber"]
    Adapter -->|"ADD / DELETE（高频）"| ReportEvent["KVCM ReportEvent"]
    Adapter -->|"SNAPSHOT（启动、周期、异常）"| ReportEvent
    ReportEvent --> Meta["block -> locations -> specs"]
    ReportEvent --> Version["instance metadata<br/>host committed version"]
    Meta --> Query["cache-aware query"]
    Version --> Query
    ReportEvent --> Reclaimer["既有 Reclaimer Supervisor"]
    Reclaimer --> Meta
```

职责边界：

- Engine Adapter 负责获得可信输入；
- Subscriber 负责事件顺序、源端 watermark、重试、snapshot barrier 和 ACK 后推进本地基线；
- KVCM 负责版本生成、元数据写入、持久化提交点、查询过滤、节点生命周期和旧 metadata 回收；
- Master/FlexLB 根据 KVCM 的 cache-aware candidates，再结合健康状态和实时负载做最终决策。

Engine 的 cache version、event sequence 或 epoch 与本文的 KVCM snapshot version 是不同概念：

- source watermark 证明 Engine 快照与后续事件的先后关系；
- KVCM snapshot version 标识哪一轮 metadata 对账已经提交。

## 4. Scope：只有 instance + reporter

```text
snapshot scope = instance_id + reporter host_ip_port
```

`medium` 不属于 scope，不参与：

- metadata marker key；
- committed version 状态；
- snapshot 频率限制；
- snapshot/delta 并发栅栏；
- scope 隔离和恢复。

同一 reporter 管理的 GPU、CPU、Disk 等缓存必须在一次 snapshot 请求中一起上报，并共享同一个 committed version。

不同 instance 或不同 reporter 相互隔离，可以并行处理。

`host_ip_port` 必须取自 `ReportEventRequest.host_ip_port`。数据 URI 的 host 可能是物理存储服务地址，不能代替 reporter 身份。

## 5. Snapshot 协议

### 5.1 Medium 下沉为 block 属性

一个 host snapshot 使用一个 `EVENT_BLOCK_SNAPSHOT` item。外层不再携带 `medium`，每个 block 自己声明 medium：

```protobuf
message BlockSnapshotItem {
    string block_key = 1;
    repeated LocationSpec specs = 2;
    string medium = 3;
}

message BlockSnapshotEventParams {
    reserved 1; // old medium field
    repeated BlockSnapshotItem blocks = 2;
}
```

保留原字段号可以避免 protobuf wire number 被误复用。Java、C++、HTTP JSON 文档和客户端必须同步更新。

示例：

```json
{
  "instance_id": "model-a",
  "host_ip_port": "10.0.0.8:9000",
  "storage_type": "ST_EVENT_REPORT",
  "events": [
    {
      "event_type": "EVENT_BLOCK_SNAPSHOT",
      "block_snapshot": {
        "blocks": [
          {
            "block_key": "101",
            "medium": "hbm",
            "specs": [
              {
                "name": "full_attention",
                "uri": "rtp-llm://10.0.0.8:9600/hbm/101"
              }
            ]
          },
          {
            "block_key": "102",
            "medium": "mem",
            "specs": [
              {
                "name": "full_attention",
                "uri": "rtp-llm://10.0.0.8:9600/mem/102"
              }
            ]
          }
        ]
      }
    }
  ]
}
```

### 5.2 完整性语义

一次 `EVENT_BLOCK_SNAPSHOT` 是该 reporter 全部 medium 的完整集合：

- 空 `blocks` 表示该 reporter 当前没有任何 cache；
- 未出现的 medium 表示该 medium 为空；
- 唯一键是 `(medium, block_key)`，同一组合不能重复；
- 每个 block 的 `specs` 是该 location 当前完整的组件集合；
- medium、spec name 不能为空；
- 同一 block 内 spec name 不能重复；
- URI 必须合法，且不能预带 KVCM 保留参数；
- 一个请求最多有一个 snapshot item；
- snapshot 不能与 ADD、DELETE、HOST_DOWN 混在同一请求；
- 同一 snapshot 不能拆成多个普通请求分页提交。

如果单请求无法承载大快照，后续应设计显式 Begin/Page/Commit staging 协议；不能把多个独立 snapshot 当分页。

## 6. Location 与 URI

### 6.1 Stable location id

location 仍按 block 的 medium 区分：

```text
kvs#event_report#<medium>#<reporter host_ip_port>
```

例如：

```text
kvs#event_report#hbm#10.0.0.8:9000
kvs#event_report#mem#10.0.0.8:9000
```

v1 更新到 v2 时，直接覆盖同一个 block 下该 stable location 的完整 specs。

明确删除下面的 copy-on-write 形式：

```text
kvs#event_report#<medium>#snapshot_v=<version>#<reporter host_ip_port>
```

实现中不再需要：

- versioned location id 的构造和解析；
- 新旧两代 location 并存；
- `BatchReplaceLocationSpecs` 的 create/replace 两阶段；
- 以 location generation 实现可见性切换。

Snapshot 写入使用单阶段的“按 stable location 覆盖完整 specs”操作。底层跨 key 仍不是事务，失败语义见第 10 节。

### 6.2 Version 只写 URI

KVCM 在 reporter URI 上追加：

```text
kvcm_instance_id=<instance_id>
kvcm_host_ip_port=<reporter host_ip_port>
kvcm_medium=<block medium>
kvcm_snapshot_version=<opaque version>
```

例如：

```text
rtp-llm://10.0.0.8:9600/hbm/101
  ?kvcm_instance_id=model-a
  &kvcm_host_ip_port=10.0.0.8%3A9000
  &kvcm_medium=hbm
  &kvcm_snapshot_version=018f4e3c-7d91-7b12-a3c4-5d6e7f809abc
```

要求：

- 所有 `kvcm_*` 参数属于 KVCM 保留命名空间；
- reporter 输入只要携带该前缀就拒绝；
- URI 中的 instance、reporter、medium 必须与请求、block 和 location id 一致；
- 同一个 location 内的 specs 必须具有相同 scope、medium 和 version；
- 参数缺失、重复、非法或不匹配时 fail closed。

## 7. Version 与 instance metadata

### 7.1 只保留 committed marker

每个 instance 的 metadata hash 中，为每个 reporter 保存当前 committed version：

```text
__event_snapshot_version__#<encoded host_ip_port> = <opaque committed version>
```

它与 `key_count`、`storage_usage` 一起持久化，但更新必须是 field-level merge/upsert。普通 `PersistMetaData` 不能用全量替换把 version marker 清掉。

删除：

```text
__event_snapshot_allocated_version__#...
```

重启时只读取 instance metadata 中的 committed marker，不扫描全量 block 推断最大版本。

### 7.2 为什么 version 必须是不可复用 token

删除 allocated high-water 后，不能再简单使用 `committed + 1`。

反例：

1. committed=7；
2. snapshot 8 覆盖一半后 KVCM 崩溃；
3. 重启只恢复 committed=7；
4. 若再次分配 8，第一次失败残留的 URI 也会在第二次 8 commit 后被误激活。

因此本设计把 snapshot version 定义为不可复用的 opaque generation，推荐 UUIDv7/128-bit token：

- 查询只做“是否等于 committed”的判断；
- Reclaimer 只做“是否不等于 committed”的判断；
- Subscriber 不依赖 version 大小排序；
- 日志和监控用 token 对齐同一轮提交。

若实现必须坚持单调 `uint64`，则必须二选一：

1. 保留持久化 high-water；或
2. 每次写前同步清理所有未提交残留，再允许复用数字。

“单调 uint64 + 只存 committed + 允许原地部分失败”不能同时保证不误激活。本文选择 opaque token，以满足删除 high-water 的简化目标。

## 8. Response：明确返回 committed version

`ReportEventResponse` 增加：

```protobuf
string committed_snapshot_version = 4;
uint64 retry_after_ms = 5;
```

语义：

- snapshot 成功：返回本次新 token；
- snapshot 失败或 partial：返回失败前的 committed token；
- ADD、DELETE、REGISTER、HEARTBEAT：返回该 scope 当前 committed token；
- 从未成功提交过 snapshot：返回空字符串；
- 被频率限制时：同时返回当前 committed token 和建议等待时间。

Subscriber 以响应字段为准，不从 URI、时间戳或本地计数猜测 KVCM 当前版本。

如果 mutation 已提交但 ACK 丢失，单写 Subscriber 重试时可以通过返回的 committed token 与自己的上一次已知 token 对比。即使无法确认，也可以稍后重发完整 snapshot，最终状态仍会收敛。

若运维需要不发 mutation 就读取 version，应在现有 host-state 查询接口中显式返回，而不是扫描 URI。

## 9. 写入与提交流程

```mermaid
sequenceDiagram
    participant S as Subscriber
    participant C as CacheManager
    participant E as EventReportBackend
    participant M as Meta Index
    participant R as Existing Reclaimer
    participant Q as Query

    S->>C: EVENT_BLOCK_SNAPSHOT(all media)
    C->>C: 校验完整 host snapshot
    C->>E: 检查频率并获取 host-level snapshot fence
    E-->>C: 生成 opaque version N
    C->>M: stable location 原地覆盖，URI version=N
    C->>M: Sync 本次全部 keys
    C->>M: metadata HSET committed=N
    C->>E: 发布内存 committed=N
    C-->>S: committed_snapshot_version=N
    C->>R: 提交旧 location 删除任务
    Q->>M: 只返回 URI version=N
```

严格顺序：

1. 完整校验 snapshot；
2. 检查 per-scope 最小间隔；
3. 获取 `(instance_id, reporter)` 的 snapshot fence；
4. 生成不可复用 version N；
5. 为全部 block 构造带 N 的 URI；
6. 按 stable location 单阶段覆盖完整 specs；
7. `Sync` 本次涉及的全部 keys；
8. 将 committed=N merge 到 instance metadata；
9. 更新内存 committed=N；
10. 返回 N；
11. 扫描旧 location，并把删除请求提交给既有 reclaimer supervisor。

步骤 8 是发布点：

- 之前查询只认可旧 committed；
- 之后查询只认可 N；
- 清理只能在 committed=N 成功后启动。

## 10. 原地覆盖与失败语义

本设计不再承诺 copy-on-write 的“失败时旧快照仍完整可读”。

假设当前 committed=C，新 snapshot=N：

- 已覆盖成 N 的 block 在 commit 前会被查询过滤；
- 尚未覆盖的 block 仍可能以 C 可见；
- 全部写入和 Sync 成功后才发布 N；
- N 发布后，本次上报的 N 数据可见；
- 未上报或失败残留的非 N 数据被过滤，随后由 reclaimer 删除。

失败矩阵：

| 失败位置 | committed 是否变化 | 查询表现 | 后续动作 |
| --- | --- | --- | --- |
| 校验失败 | 否 | 旧版本不变 | 修正请求 |
| 频率限制 | 否 | 旧版本不变 | 按 `retry_after_ms` 重试 |
| version/URI 准备失败 | 否 | 尚未写 metadata | 修正或重试 |
| 原地覆盖部分失败 | 否 | 已覆盖的 N 不可见，其余 C 可能可见 | 重试完整 snapshot |
| Sync 失败 | 否 | 同上 | 重试完整 snapshot |
| committed metadata 写失败 | 否 | N 不发布 | 重试完整 snapshot |
| committed 已持久化、内存发布前崩溃 | 是 | 重启从 metadata 恢复 N | 不回滚 |
| ACK 丢失 | 是 | N 已可见 | 查询响应 version 或重发完整 snapshot |
| 清理中崩溃 | 是 | N 可见，旧数据不可见但占空间 | 下次 snapshot 再触发清理 |

安全属性是“未提交 token 不会被当成当前 token”，不是“失败后旧快照始终完整”。

Cache 不是唯一副本，短暂的 cache miss 只会退化为重新计算或远端加载，因此该取舍可接受。

## 11. 查询规则

查询 event-report location 时检查：

1. location id 能否解析出 medium 和 reporter；
2. URI 的 instance、reporter、medium 是否匹配；
3. URI version 是否等于该 host scope 的 committed token；
4. reporter 节点是否可用。

任一条件不满足，该 location 不可见。

兼容规则：

- committed 为空时，允许读取没有 snapshot version 的历史增量数据；
- host 首次成功提交 snapshot 后，只接受当前 committed token；
- snapshot 后的 ADD/DELETE 必须继承当前 committed token；
- URI 使用未知 token，即使格式合法也不可见。

查询过滤负责即时正确性；Reclaimer 只负责空间回收。

## 12. 跨请求栅栏与专用错误码

### 12.1 Host-level 栅栏

同一 `(instance_id, reporter)` scope 维护：

- 最多一个 `in_flight snapshot`；
- `active_delta_mutations` 引用计数；
- 当前 committed token。

规则：

- 有 active delta 时 snapshot 不能开始；
- snapshot in flight 时 ADD/DELETE 不能开始；
- delta lease 固定当前 committed token；
- delta URI 使用固定 token；
- 同一 reporter 的不同 medium 共用一把栅栏；
- 不同 reporter 或 instance 可以并行。

### 12.2 专用可重试错误码

新增协议错误码：

```protobuf
SNAPSHOT_IN_PROGRESS = 11;
DELTA_IN_PROGRESS = 12;
SNAPSHOT_RATE_LIMITED = 13;
```

建议内部错误码一一对应：

```text
EC_SNAPSHOT_IN_PROGRESS
EC_DELTA_IN_PROGRESS
EC_SNAPSHOT_RATE_LIMITED
```

返回规则：

- snapshot 已在途，delta 请求返回 `SNAPSHOT_IN_PROGRESS`；
- delta lease 活跃，snapshot 请求返回 `DELTA_IN_PROGRESS`；
- 距离上次成功 snapshot 太近，返回 `SNAPSHOT_RATE_LIMITED` 和 `retry_after_ms`；
- 字段非法仍返回 `INVALID_ARGUMENT`；
- 存储故障返回 `INTERNAL_ERROR` 或对应 IO 错误。

Subscriber 只对前三类做精准退避：

- busy 错误使用短抖动退避；
- rate limited 等待 `retry_after_ms`；
- invalid argument 不自动无限重试；
- snapshot 重试必须重发完整 host snapshot。

## 13. Snapshot 频率限制

`EventReportBackend` 为每个 host-level scope 维护最小 snapshot 间隔：

```text
snapshot_min_interval = 30s  // 默认值，可配置
```

语义：

- 首次 snapshot 不受限；
- 间隔从上一次成功 commit 计算，不从失败尝试计算；
- 写入或 Sync 失败后允许立即重试；
- REGISTER/HEARTBEAT/ADD/DELETE 不受 snapshot interval 限制；
- KVCM 返回剩余 `retry_after_ms`；
- Subscriber 仍应增加随机抖动，避免多个 host 整点同时上报。

频率限制主要防止 Subscriber bug 或错误配置把 full snapshot 当成高频轮询写接口。它不是 Subscriber 正常调度的替代品。

如果需要跨 KVCM 重启保持严格限流，可以在 committed metadata 同时保存 `last_snapshot_commit_time`；第一期只在 EventReportBackend 内存限流时，重启后的短窗口放宽是可接受的。

## 14. 清理复用现有 Reclaimer

### 14.1 不新增 snapshot 专用调度器

删除独立的 `ScheduleStaleSnapshotCleanup`、latest-version 合并表和专用退避重试状态。

snapshot commit 成功后：

1. 使用现有 MetaIndexer 分页扫描 instance metadata；
2. 找出 reporter 属于当前 scope、URI token 不等于 committed=N 的 location；
3. 按现有 `CacheLocationDelRequest` 或等价删除请求分批；
4. 提交给现有 `reclaimer_task_supervisor_->Submit(...)`；
5. 删除、`EC_NOENT`、容量扣减和实际 metadata mutation 复用现有 reclaimer 路径。

Reclaimer 不解析 token 大小，只比较是否等于当前 committed token。

### 14.2 崩溃与重试

- 清理失败不影响查询正确性；
- 清理过程中 KVCM 崩溃，残留数据继续被 version filter 隐藏；
- 下一次成功 snapshot 再扫描并提交删除；
- HOST_DOWN 继续复用现有 host cleanup 与 node generation fencing；
- 不为 snapshot 另建内存重试队列；
- 现有 reclaimer 自身已有的任务保障可以复用，但本文不叠加第二套退避。

### 14.3 当前不引入反向索引

第一期不新增持久化 `location_id -> block_key set`，避免引入 Redis 双写一致性和恢复复杂度。

代价是每次 snapshot commit 后清理需要扫描 instance 的 `N` 条 block metadata。扫描是低频后台任务，查询正确性不依赖它。

必须监控扫描耗时、读取量和 backlog；当它持续超过阈值时，再以数据决定是否引入可 rebuild 的反向索引。

### 14.4 只删除 metadata

本 Reclaimer 删除 KVCM 中陈旧的 location/meta 引用。

物理 Cache Store 中 KV tensor 的释放仍由 Engine/Cache Store 生命周期负责。若未来需要 KVCM 主动删除远端物理 cache，应设计独立的鉴权、确认、幂等和重试协议。

## 15. 节点生命周期与恢复

- REGISTER：建立 reporter 和 medium 能力，不改变 committed token；
- HEARTBEAT：更新可用性；
- HOST_DOWN/UNREGISTER：先让 reporter 对查询不可用，再复用现有 reclaimer 删除全部 location；
- 快速重新注册使用 node generation 防止旧 HOST_DOWN 任务误删新数据。

KVCM 启动恢复：

1. 从 instance metadata 读取每个 host 的 committed token；
2. 恢复 EventReportBackend 内存版本状态；
3. 不扫描 block 来猜 version；
4. 不恢复 allocated high-water，因为该状态已删除；
5. malformed marker 对该 scope fail closed，并暴露健康告警；
6. 旧 token 数据仍由查询过滤，下次 snapshot commit 再触发清理。

## 16. 性能与容量估算

### 16.1 典型场景

假设：

- 1 个 instance；
- 10 个 reporter host；
- 每台 5000 个 block；
- 每个 block 一个 event-report location、一个 spec；
- snapshot 最快每 30 秒一次；
- 每轮 5% block 发生淘汰；
- metadata scan 每页 256 个 block。

一次“10 台 host 都完成 snapshot”的逻辑工作量：

| 项目 | 数量 |
| --- | ---: |
| stable location 覆盖 | 10 × 5000 = 50,000 block writes |
| committed metadata 更新 | 10 次 |
| cleanup 扫描 | 每 host 扫 50,000，合计 500,000 block inspections |
| scan page 请求 | ceil(50,000 / 256) × 10 ≈ 1,960 次 |
| 旧 location 删除（按 5%） | 10 × 250 = 2,500 次 |

摊到 30 秒窗口的逻辑平均值：

| 项目 | 平均值 |
| --- | ---: |
| block 覆盖 | ≈ 1,667/s |
| scan page | ≈ 65/s |
| stale delete | ≈ 83/s |

这些是“逻辑 metadata 操作”，不等于实际 Redis 网络 round trip。批量 HSET、pipeline 和 reclaimer batch 会减少 round trip，但不会减少序列化字节数和 block inspection 数。

如果每个 host snapshot 串行写入 5000 blocks 的实测吞吐是 10,000 logical writes/s，则纯写入约 0.5 秒；若后台扫描吞吐是 20,000 blocks/s，则扫描 50,000 blocks 约 2.5 秒。它们只是容量估算基线，最终以压测的 P50/P95/P99 为准。

所有 host 必须加随机抖动。否则 30 秒整点同时到达会形成 50,000 写入和 10 个全量扫描的瞬时尖峰。

### 16.2 监控阈值与反向索引信号

至少监控：

- snapshot block 数、写入耗时、Sync 耗时、commit 耗时；
- cleanup 扫描 keys、pages、bytes 和耗时；
- cleanup 删除数量；
- reclaimer queue wait、执行耗时和 backlog；
- snapshot rate-limit 次数；
- Redis QPS、带宽、pipeline 大小和 latency。

以下任一情况持续多个窗口时，应评估引入可 rebuild 的 `location_id -> block_key set`：

- `snapshot_cleanup_scan_latency_p99 > 10s`；
- 同 scope 的上一轮 cleanup 尚未结束，下一轮 snapshot 已到达；
- cleanup backlog 持续超过一个 snapshot 周期；
- cleanup scan 占 Redis 读请求或带宽超过 20%；
- instance 增长导致 `N / host_blocks` 很大，扫描绝大部分数据都与目标 host 无关。

反向索引是性能优化，不是查询正确性的前提。

## 17. 兼容性与灰度

- stable location id 与历史 ADD/DELETE 一致；
- URI 内部参数由 KVCM 生成；
- response 新字段对旧 protobuf 客户端向后兼容；
- Java 与 C++ proto 同步；
- 删除外层 snapshot medium 时保留原 field number；
- `EVENT_BLOCK_DELETE.spec_names` 兼容策略独立评审；
- committed 为空时保留旧增量读取路径，首次 snapshot 后进入严格 token 模式。

灰度顺序：

1. 先部署理解 URI token、instance marker、专用错误码和 response version 的 KVCM；
2. 再部署能产生单个跨 medium host snapshot 的 Subscriber；
3. 小流量开启启动 snapshot；
4. 再开启带随机抖动的低频周期对账；
5. 观察 partial、rate-limit、cleanup scan 和 reclaimer backlog。

## 18. 测试计划

### 18.1 单元测试

Scope 与协议：

- scope 只含 instance/host；
- 一个 snapshot 同时包含 HBM/Memory/Disk；
- medium 从 block 读取；
- `(medium, block_key)` 重复被拒绝；
- 不同 host、instance 隔离；
- marker key 不含 medium。

Location、URI 与 token：

- 两轮 snapshot 的 location id 完全不变；
- versioned location 构造/解析接口不存在；
- token 只存在 URI；
- UUID/token 不复用；
- URI 参数重复、非法、scope/medium 不匹配时 fail closed；
- committed 为空的兼容读取与非空后的严格过滤。

提交与恢复：

- URI prepare、overwrite、Sync、committed metadata 各阶段故障注入；
- 原地覆盖部分失败不推进 committed；
- 失败后用新 token 完整重试收敛；
- committed marker 不被 `PersistMetaData` 覆盖；
- 响应成功返回新 token，失败返回旧 token；
- 重启只从 instance metadata 恢复 token；
- malformed marker 只隔离对应 scope。

并发、错误码和限流：

- snapshot in flight 时 delta 返回 `SNAPSHOT_IN_PROGRESS`；
- active delta 时 snapshot 返回 `DELTA_IN_PROGRESS`；
- 成功 snapshot 后 30 秒内返回 `SNAPSHOT_RATE_LIMITED`；
- 失败 snapshot 可立即重试；
- 不同 scope 并行；
- `retry_after_ms` 递减且边界正确。

Reclaimer：

- commit 后通过现有 supervisor 收到删除任务；
- 删除旧 token、遗漏 block 和遗漏 medium；
- 不删除当前 token、其他 host 或 instance；
- `EC_NOENT` 幂等；
- 清理中断后查询仍正确；
- 下一次 snapshot 再触发残留清理；
- 不存在 snapshot 专用调度/退避状态。

### 18.2 集成测试

- reporter 一次上报 HBM+Memory，响应返回 committed token；
- 查询只看到本次完整 host snapshot；
- 下一轮遗漏 block/medium 后立即不可见，随后由现有 reclaimer 删除；
- snapshot 后 ADD/DELETE 继承 token；
- busy、rate-limit 与 invalid argument 错误可区分；
- Sync/marker 故障后 token 不变化，完整重试恢复；
- KVCM 重启后恢复 token 和查询过滤；
- 清理中 kill KVCM，重启后旧数据仍不可见，下次 snapshot 可清理；
- 10 host × 5000 blocks 压测记录 Redis ops、commit latency、scan latency 和 backlog；
- ASAN/TSAN 或等价并发检测覆盖 snapshot/delta/reclaimer。

## 19. 可观测性

建议提供：

- `snapshot_request_total{result}`；
- `snapshot_block_count{medium}`；
- `snapshot_write_latency_ms`；
- `snapshot_sync_latency_ms`；
- `snapshot_commit_latency_ms`；
- `snapshot_fence_reject_total{operation,reason}`；
- `snapshot_rate_limited_total`；
- `snapshot_stale_location_filtered_total{reason}`；
- `snapshot_cleanup_scan_keys`；
- `snapshot_cleanup_scan_pages`；
- `snapshot_cleanup_scan_bytes`；
- `snapshot_cleanup_scan_latency_ms`；
- `snapshot_cleanup_delete_total`；
- `snapshot_reclaimer_queue_wait_ms`；
- `snapshot_reclaimer_backlog`。

结构化日志至少包含 instance、reporter、committed token、candidate token、阶段和错误码。不要把 block key、reporter、token 等高基数字段放入常驻 metrics label。

## 20. 正确性约束

实现必须满足：

1. scope 只有 instance_id + reporter host_ip_port；
2. 一个 snapshot 覆盖 reporter 的全部 medium；
3. medium 是 block 属性，不参与 version 和 fence；
4. location id 不含 version；
5. token 只存 URI 和 instance committed metadata；
6. token 不复用，未提交 token 永远不会被误激活；
7. committed 只有在全部写入 Sync 成功后更新；
8. 响应明确返回当前 committed token；
9. snapshot 与同 scope delta 不并发，并返回专用可重试错误码；
10. snapshot 受 per-scope 最小间隔保护；
11. 旧数据删除复用现有 reclaimer；
12. cleanup 失败不影响查询正确性；
13. 普通 metadata 持久化不能覆盖 version marker；
14. 重启从 instance metadata 恢复 committed token；
15. Subscriber 未拿到完整多介质事实时不能发送权威 snapshot。

## 21. 非目标

本文不提供：

- 历史 snapshot 查询或回滚；
- copy-on-write location；
- allocated high-water marker；
- snapshot 专用清理调度器和退避队列；
- 第一阶段的持久化 location 反向索引；
- 跨 block 线性一致读；
- 多写 leader 分布式共识；
- KVCM 主动删除远端物理 KV tensor。

本设计以“host scope + stable location + opaque URI token + instance committed metadata + existing reclaimer”保证最终收敛，并明确接受原地覆盖失败时的短暂 cache miss。
