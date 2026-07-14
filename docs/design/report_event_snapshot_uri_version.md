# ReportEvent Snapshot URI 版本方案

## 1. 定位

Subscriber 采用“增量为主、全量校准”的上报模型：

- 稳态下发送有序的 `EVENT_BLOCK_ADD` / `EVENT_BLOCK_DELETE`。
- 周期性、启动恢复、事件断档或本地状态异常时，发送 `EVENT_BLOCK_SNAPSHOT` 作为权威校准屏障。
- RTP-LLM 在缺少原生增量事件时，可以轮询全量状态并在本地计算增量；仍应低频发送完整 snapshot 做最终一致性校准。
- vLLM 等具备原生 KV events 的引擎直接发送增量，并在 full-clear、重连或序列缺口时补 snapshot。

Snapshot 不适合每几十毫秒上报。每次 snapshot 都要重写本次完整集合中的 metadata，并触发一次异步扫描；高频状态变化应走增量事件。

## 2. 语义

`EVENT_BLOCK_SNAPSHOT` 对下面的 scope 具有完整、权威语义：

```text
instance_id + host_ip_port + medium
```

一个 snapshot 必须满足：

- `blocks` 是该 scope 当前完整的 block 集合，空集合表示清空该 scope。
- 每个 block 的 `specs` 是该 location 当前完整的 spec 集合，包括 full-attention、mamba-state 等所有组件。
- 一个 snapshot 不能分页或拆成多个同 scope 请求；同一 scope 同时只允许一个在途 snapshot。
- Snapshot 和 `EVENT_BLOCK_ADD` / `EVENT_BLOCK_DELETE` 必须使用不同的 `ReportEvent` 请求。Subscriber 应等待 snapshot ACK 后再继续发送后续增量，形成明确的顺序屏障。

`EVENT_NODE_REGISTER`、`EVENT_HEARTBEAT` 等非 block mutation 可以和 snapshot 同请求发送。

## 3. URI 版本

方案保留 PR230 的 proto 形状，但不增加 `location_id -> block_key` 反向索引。Event report backend 为每个 scope 维护单调递增版本，并把下面参数写入 `LocationSpec.uri`：

```text
kvcm_instance_id=<instance_id>
kvcm_host_ip_port=<reporter host_ip_port>
kvcm_medium=<medium>
kvcm_snapshot_version=<version>
```

示例：

```text
event_report://physical-storage:9600/cache/123?kvcm_instance_id=test_instance&kvcm_host_ip_port=192.168.1.1%3A8080&kvcm_medium=gpu&kvcm_snapshot_version=2&size=4096
```

`kvcm_host_ip_port` 必须使用 `ReportEventRequest.host_ip_port`。数据 URI 的 host 可能是物理存储地址，不能代替 reporter 身份。解析旧版本 URI 时可以回退到 URI host，以兼容早期原型数据。

## 4. 写入与可见性

Snapshot 流程如下：

1. 完整校验 `medium`、重复 block、重复/空 spec name 和所有 URI。
2. 为 scope 分配版本；另一个 snapshot 在途时拒绝新请求。
3. 给本次所有 URI 写入版本，并按 block 批量替换该 location 的完整 specs。
4. 只有所有 block 写入成功时才 commit 版本。
5. Commit 后立即调度后台清理。

查询只接受等于 committed version 的 URI。这样：

- 写入中或失败后未 commit 的更高版本不可见。
- Commit 瞬间新版本可见，旧版本立即失效，不需要等待后台扫描。
- 同一 scope 已有 committed snapshot 时，普通 `EVENT_BLOCK_ADD` 沿用当前版本。合并前会丢弃该 location 遗留的旧版本 specs，避免异步清理误删新增组件。

`EVENT_BLOCK_DELETE.spec_names` 继续按组件名删除；snapshot 则替换 reported block 的全部 specs。这两条规则允许 full-attention 和 mamba-state 独立增删，同时保证全量校准能纠正漂移。

## 5. 后台清理与成本

Commit 后，`CleanupStaleSnapshotLocations` 扫描该 instance 的 CacheMeta，只处理目标 `storage_type + host_ip_port + medium`，并删除不属于当前 committed version 的 location。查询路径在清理完成前已经通过版本规则屏蔽旧数据。

清理扫描的复杂度是该 instance 的 metadata 总量，不在 snapshot 请求同步路径执行。同一 scope 连续提交多个版本时，后台任务合并到最新版本，避免重复堆积全量扫描。这个折中省去了额外 Redis set/反向索引，但要求 snapshot 保持低频；如果未来需要高频全量上报，应重新评估持久化反向索引或原生增量源。

## 6. 恢复与失败处理

Event report backend 的版本表是内存状态。KVCM 创建或恢复 MetaSearcher 后，会完整扫描已有 location：

1. 从 location id 解析 reporter 的 `host_ip_port + medium`。
2. 从 URI 解析 instance、reporter host、medium 和 snapshot version。
3. 仅在 URI scope 与 location scope 完全一致时观察版本。
4. 用每个 scope 的最大版本恢复 committed version。

只有扫描完整返回 `EC_OK` 才标记恢复完成。`EC_PARTIAL_OK` 或扫描错误会阻止后续 `ReportEvent` mutation，并要求调用方重试，避免从不完整基线分配错误版本。后台对象和清理任务通过 `shared_ptr` 保持生命周期，关闭 backend 时清空版本状态。

## 7. 与反向索引方案的取舍

本方案不维护 `location_id -> block_key` 持久反向索引，也不在 snapshot 请求路径计算 `reported - existing` / `existing - reported`。新版本负责即时可见性切换，异步扫描负责最终回收 metadata。

优点是改动集中、无需扩展 meta backend；代价是每次低频 snapshot 会重写完整 reported set，并最终扫描 instance metadata。增量事件承担稳态流量，snapshot 只承担校准，是该方案成立的前提。
