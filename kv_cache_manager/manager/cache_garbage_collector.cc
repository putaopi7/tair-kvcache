#include "kv_cache_manager/manager/cache_garbage_collector.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <utility>

#include "kv_cache_manager/common/logger.h"
#include "kv_cache_manager/common/request_context.h"
#include "kv_cache_manager/common/timestamp_util.h"
#include "kv_cache_manager/config/instance_group.h"
#include "kv_cache_manager/config/instance_info.h"
#include "kv_cache_manager/config/registry_manager.h"
#include "kv_cache_manager/meta/cache_location.h"
#include "kv_cache_manager/meta/common.h"
#include "kv_cache_manager/meta/meta_indexer.h"
#include "kv_cache_manager/meta/meta_indexer_manager.h"
#include "kv_cache_manager/meta/types.h"

namespace kv_cache_manager {

#define DEFINE_METRICS_NAME_FOR_CACHE_GC(name) DEFINE_METRICS_NAME_(CacheGarbageCollector, cache_gc, name)
#define REGISTER_COUNTER_METRICS_FOR_CACHE_GC(name) REGISTER_METRICS_COUNTER_(metrics_registry_, cache_gc, name)
#define REGISTER_GAUGE_METRICS_FOR_CACHE_GC(name) REGISTER_METRICS_GAUGE_(metrics_registry_, cache_gc, name)

DEFINE_METRICS_NAME_FOR_CACHE_GC(scan_round_count);
DEFINE_METRICS_NAME_FOR_CACHE_GC(scan_key_count);
DEFINE_METRICS_NAME_FOR_CACHE_GC(candidate_count);
DEFINE_METRICS_NAME_FOR_CACHE_GC(delete_target_count);
DEFINE_METRICS_NAME_FOR_CACHE_GC(inflight_delete_count);
DEFINE_METRICS_NAME_FOR_CACHE_GC(inflight_delete_age_ms);
DEFINE_METRICS_NAME_FOR_CACHE_GC(round_duration_ms);

namespace {

constexpr const char *kDeleteResultMetricName = "cache_gc.delete_result_count";
constexpr const char *kOperationErrorMetricName = "cache_gc.operation_error_count";

} // namespace

CacheGarbageCollector::CacheGarbageCollector(Config config,
                                             std::shared_ptr<RegistryManager> registry_manager,
                                             std::shared_ptr<MetaIndexerManager> meta_indexer_manager,
                                             std::shared_ptr<SchedulePlanExecutor> schedule_plan_executor,
                                             std::shared_ptr<MetricsRegistry> metrics_registry)
    : config_(std::move(config))
    , registry_manager_(std::move(registry_manager))
    , meta_indexer_manager_(std::move(meta_indexer_manager))
    , schedule_plan_executor_(std::move(schedule_plan_executor))
    , metrics_registry_(std::move(metrics_registry))
    , cursor_(SCAN_BASE_CURSOR) {}

CacheGarbageCollector::~CacheGarbageCollector() { Stop(); }

ErrorCode CacheGarbageCollector::Validate() const noexcept {
    if (!config_.enabled) {
        return EC_OK;
    }
    if (!registry_manager_ || !meta_indexer_manager_ || !schedule_plan_executor_ || !metrics_registry_) {
        KVCM_LOG_ERROR("cache gc enabled with missing dependency");
        return EC_CONFIG_ERROR;
    }
    if (config_.scan_interval_ms <= 0 || config_.round_pause_ms <= 0 || config_.scan_batch_size == 0 ||
        config_.max_inflight_delete_requests == 0 ||
        config_.orphan_writing_grace_period_ms < kMinCacheGcOrphanWritingGracePeriodMs) {
        KVCM_LOG_ERROR("invalid cache gc config, scan_interval_ms[%ld], round_pause_ms[%ld], scan_batch_size[%zu], "
                       "max_inflight_delete_requests[%zu], orphan_writing_grace_period_ms[%ld], grace must be at "
                       "least[%ld]",
                       config_.scan_interval_ms,
                       config_.round_pause_ms,
                       config_.scan_batch_size,
                       config_.max_inflight_delete_requests,
                       config_.orphan_writing_grace_period_ms,
                       kMinCacheGcOrphanWritingGracePeriodMs);
        return EC_CONFIG_ERROR;
    }
    if (config_.scan_batch_size > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        KVCM_LOG_ERROR("invalid cache gc scan_batch_size[%zu]", config_.scan_batch_size);
        return EC_CONFIG_ERROR;
    }
    const auto max_steady_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(Clock::duration::max()).count() / 2;
    if (config_.scan_interval_ms > max_steady_ms || config_.round_pause_ms > max_steady_ms) {
        KVCM_LOG_ERROR("cache gc duration config overflows steady clock");
        return EC_CONFIG_ERROR;
    }
    return EC_OK;
}

void CacheGarbageCollector::RegisterMetrics() {
    REGISTER_COUNTER_METRICS_FOR_CACHE_GC(scan_round_count);
    REGISTER_COUNTER_METRICS_FOR_CACHE_GC(scan_key_count);
    METRICS_(cache_gc, candidate_count) = metrics_registry_->GetCounter(METRICS_NAME_(cache_gc, candidate_count),
                                                                        MetricsTags{{"reason", "orphan_writing"}});
    REGISTER_COUNTER_METRICS_FOR_CACHE_GC(delete_target_count);
    REGISTER_GAUGE_METRICS_FOR_CACHE_GC(inflight_delete_count);
    REGISTER_GAUGE_METRICS_FOR_CACHE_GC(inflight_delete_age_ms);
    REGISTER_GAUGE_METRICS_FOR_CACHE_GC(round_duration_ms);
}

ErrorCode CacheGarbageCollector::Start() noexcept {
    if (!config_.enabled) {
        return EC_OK;
    }
    if (Validate() != EC_OK) {
        return EC_CONFIG_ERROR;
    }

    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (running_) {
        return EC_OK;
    }

    try {
        RegisterMetrics();
        ResetWorkerState();
        stop_requested_.store(false, std::memory_order_release);
        worker_ = LoopThread::CreateLoopThread(
            [this]() { RunOneTick(); }, config_.scan_interval_ms * 1000, "CacheGarbageCollector", true);
        if (!worker_) {
            stop_requested_.store(true, std::memory_order_release);
            KVCM_LOG_ERROR("start cache gc worker failed: LoopThread creation returned null");
            return EC_ERROR;
        }
        running_ = true;
    } catch (const std::exception &e) {
        worker_.reset();
        running_ = false;
        stop_requested_.store(true, std::memory_order_release);
        KVCM_LOG_ERROR("start cache gc worker failed: %s", e.what());
        return EC_ERROR;
    } catch (...) {
        worker_.reset();
        running_ = false;
        stop_requested_.store(true, std::memory_order_release);
        KVCM_LOG_ERROR("start cache gc worker failed with unknown exception");
        return EC_ERROR;
    }
    KVCM_LOG_INFO("cache gc worker started");
    return EC_OK;
}

void CacheGarbageCollector::RequestStop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (worker_) {
        // Wake a long tick/cooldown wait without joining here. RunOneTick observes
        // stop_requested_ and returns without touching GC dependencies.
        worker_->RunOnce();
    }
}

void CacheGarbageCollector::Join() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (worker_) {
        worker_->Stop();
        worker_.reset();
    }
    // Match CacheReclaimer's demotion policy: accepted deletes continue best effort in the Executor, while GC never
    // waits for their end-to-end Futures. Clearing these observer handles neither cancels the tasks nor keeps their
    // MetaIndexer/DataStorage dependencies alive across leader cleanup; a post-CAS failure may remain CLS_DELETING.
    inflight_deletes_.clear();
    pending_locations_.clear();
    METRICS_(cache_gc, inflight_delete_count) = 0;
    METRICS_(cache_gc, inflight_delete_age_ms) = 0;
    running_ = false;
}

void CacheGarbageCollector::Stop() noexcept {
    RequestStop();
    Join();
}

bool CacheGarbageCollector::IsRunning() const noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return running_;
}

void CacheGarbageCollector::ResetWorkerState() noexcept {
    instances_.clear();
    instance_index_ = 0;
    cursor_ = SCAN_BASE_CURSOR;
    round_active_ = false;
    next_round_at_.reset();
    round_context_.reset();
    inflight_deletes_.clear();
    pending_locations_.clear();
    METRICS_(cache_gc, inflight_delete_count) = 0;
    METRICS_(cache_gc, inflight_delete_age_ms) = 0;
    METRICS_(cache_gc, round_duration_ms) = 0;
}

void CacheGarbageCollector::RunOneTick() noexcept {
    try {
        if (stop_requested_.load(std::memory_order_acquire)) {
            return;
        }
        PollInflightDeletes();
        if (inflight_deletes_.size() >= config_.max_inflight_delete_requests) {
            return;
        }
        if (stop_requested_.load(std::memory_order_acquire)) {
            return;
        }

        const auto now = Clock::now();
        if (next_round_at_.has_value() && now < next_round_at_.value()) {
            return;
        }
        if (!round_active_ && !BeginRound()) {
            return;
        }
        if (!round_active_) {
            return;
        }
        if (instance_index_ >= instances_.size()) {
            CompleteRound();
            return;
        }

        const InstanceScanEntry entry = instances_[instance_index_];
        if (stop_requested_.load(std::memory_order_acquire)) {
            return;
        }
        auto indexer = meta_indexer_manager_->GetMetaIndexer(entry.instance_id);
        if (!indexer) {
            RecordOperationError("indexer_missing");
            KVCM_LOG_WARN("cache gc round[%lu] skip missing indexer, group[%s] instance[%s]",
                          round_id_,
                          entry.instance_group.c_str(),
                          entry.instance_id.c_str());
            AdvanceInstance();
            return;
        }

        MaintenanceScanBatch batch;
        const std::string scan_cursor = cursor_;
        ErrorCode ec =
            indexer->ScanLocationsForMaintenance(round_context_.get(), scan_cursor, config_.scan_batch_size, batch);
        if (ec != EC_OK) {
            RecordOperationError("scan");
            KVCM_LOG_WARN("cache gc round[%lu] scan failed, group[%s] instance[%s] cursor[%s] ec[%d]",
                          round_id_,
                          entry.instance_group.c_str(),
                          entry.instance_id.c_str(),
                          scan_cursor.c_str(),
                          static_cast<int>(ec));
            return;
        }

        METRICS_(cache_gc, scan_key_count) += batch.keys.size();
        cursor_ = batch.next_cursor;
        CacheLocationDelRequest request =
            BuildDeleteRequest(entry.instance_id, batch, TimestampUtil::GetCurrentTimeUs());

        if (cursor_ == SCAN_BASE_CURSOR) {
            AdvanceInstance();
        }

        if (request.block_keys.empty() || stop_requested_.load(std::memory_order_acquire)) {
            return;
        }

        const size_t target_count = [&request]() {
            size_t count = 0;
            for (const auto &ids : request.location_ids) {
                count += ids.size();
            }
            return count;
        }();

        AsyncDeleteSubmitResult submit_result;
        try {
            submit_result = schedule_plan_executor_->SubmitAsync(request);
        } catch (const std::exception &e) {
            RecordOperationError("submit_exception");
            KVCM_LOG_ERROR("cache gc SubmitAsync threw, instance[%s] error[%s]", entry.instance_id.c_str(), e.what());
            return;
        } catch (...) {
            RecordOperationError("submit_exception");
            KVCM_LOG_ERROR("cache gc SubmitAsync threw unknown exception, instance[%s]", entry.instance_id.c_str());
            return;
        }
        if (!submit_result.accepted) {
            if (submit_result.future.valid()) {
                RecordOperationError("submit_contract");
                KVCM_LOG_ERROR("cache gc SubmitAsync rejected with a valid Future, instance[%s]",
                               entry.instance_id.c_str());
            } else {
                RecordOperationError("submit_rejected");
            }
            return;
        }
        if (!submit_result.future.valid()) {
            RecordOperationError("submit_contract");
            KVCM_LOG_ERROR("cache gc SubmitAsync accepted with an invalid Future, instance[%s]",
                           entry.instance_id.c_str());
            return;
        }

        std::vector<PendingLocationKey> request_pending_locations;
        try {
            request_pending_locations.reserve(target_count);
            for (size_t block_index = 0; block_index < request.block_keys.size(); ++block_index) {
                for (const auto &location_id : request.location_ids[block_index]) {
                    PendingLocationKey key{entry.instance_id, request.block_keys[block_index], location_id};
                    request_pending_locations.emplace_back(key);
                    if (pending_locations_.insert(key).second) {
                        continue;
                    }
                    // BuildDeleteRequest de-duplicates and filters pending targets, and this callback is
                    // single-threaded. Reaching here is an invariant violation; keep the existing owner while the
                    // Executor's conditional CAS remains the final deletion-safety barrier.
                    request_pending_locations.pop_back();
                    RecordOperationError("pending_contract");
                    KVCM_LOG_ERROR("cache gc accepted duplicate pending target, instance[%s] block[%ld] location[%s]",
                                   entry.instance_id.c_str(),
                                   request.block_keys[block_index],
                                   location_id.c_str());
                }
            }
            inflight_deletes_.emplace_back(InflightDelete{
                .round_id = round_id_,
                .instance_id = entry.instance_id,
                .target_count = target_count,
                .submitted_at = Clock::now(),
                .pending_locations = request_pending_locations,
                .future = std::move(submit_result.future),
            });
        } catch (...) {
            for (const auto &pending_location : request_pending_locations) {
                pending_locations_.erase(pending_location);
            }
            throw;
        }
        METRICS_(cache_gc, delete_target_count) += target_count;
        UpdateInflightDeleteMetrics();
    } catch (const std::exception &e) {
        RecordOperationError("tick_exception");
        KVCM_LOG_ERROR("cache gc tick threw exception: %s", e.what());
    } catch (...) {
        RecordOperationError("tick_exception");
        KVCM_LOG_ERROR("cache gc tick threw unknown exception");
    }
}

void CacheGarbageCollector::PollInflightDeletes() noexcept {
    auto it = inflight_deletes_.begin();
    while (it != inflight_deletes_.end()) {
        if (!it->future.valid()) {
            RecordOperationError("future_invalid");
            RecordDeleteResult("invalid");
            KVCM_LOG_ERROR("cache gc stored invalid Future, round[%lu] instance[%s] targets[%zu]",
                           it->round_id,
                           it->instance_id.c_str(),
                           it->target_count);
            ReleasePendingLocations(*it);
            it = inflight_deletes_.erase(it);
            continue;
        }

        const uint64_t round_id = it->round_id;
        const std::string instance_id = it->instance_id;
        const size_t target_count = it->target_count;
        try {
            if (it->future.wait_for(std::chrono::microseconds::zero()) != std::future_status::ready) {
                ++it;
                continue;
            }
            const PlanExecuteResult result = it->future.get();
            RecordDeleteResult(std::to_string(static_cast<int>(result.status)));
            if (result.status == EC_OK) {
                KVCM_LOG_DEBUG("cache gc delete completed, round[%lu] instance[%s] targets[%zu]",
                               round_id,
                               instance_id.c_str(),
                               target_count);
            } else {
                KVCM_LOG_WARN(
                    "cache gc delete completed with status[%d], round[%lu] instance[%s] targets[%zu] error[%s]",
                    static_cast<int>(result.status),
                    round_id,
                    instance_id.c_str(),
                    target_count,
                    result.error_message.c_str());
            }
        } catch (const std::exception &e) {
            RecordOperationError("future_get");
            RecordDeleteResult("exception");
            KVCM_LOG_ERROR("cache gc Future get failed, round[%lu] instance[%s] targets[%zu] error[%s]",
                           round_id,
                           instance_id.c_str(),
                           target_count,
                           e.what());
        } catch (...) {
            RecordOperationError("future_get");
            RecordDeleteResult("exception");
            KVCM_LOG_ERROR("cache gc Future get failed with unknown exception, round[%lu] instance[%s] targets[%zu]",
                           round_id,
                           instance_id.c_str(),
                           target_count);
        }
        ReleasePendingLocations(*it);
        it = inflight_deletes_.erase(it);
    }
    UpdateInflightDeleteMetrics();
}

void CacheGarbageCollector::UpdateInflightDeleteMetrics() noexcept {
    METRICS_(cache_gc, inflight_delete_count) = static_cast<double>(inflight_deletes_.size());
    int64_t oldest_age_ms = 0;
    const auto now = Clock::now();
    for (const auto &inflight : inflight_deletes_) {
        const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - inflight.submitted_at).count();
        oldest_age_ms = std::max<int64_t>(oldest_age_ms, age_ms);
    }
    METRICS_(cache_gc, inflight_delete_age_ms) = static_cast<double>(oldest_age_ms);
}

void CacheGarbageCollector::ReleasePendingLocations(const InflightDelete &inflight) noexcept {
    for (const auto &pending_location : inflight.pending_locations) {
        pending_locations_.erase(pending_location);
    }
}

bool CacheGarbageCollector::BeginRound() {
    const uint64_t next_round_id = round_id_ + 1;
    auto context = std::make_shared<RequestContext>("cache_gc_round_" + std::to_string(next_round_id));
    auto [group_ec, groups] = registry_manager_->ListInstanceGroup(context.get());
    if (group_ec != EC_OK) {
        RecordOperationError("list_groups");
        KVCM_LOG_WARN("cache gc list instance groups failed, ec[%d]", static_cast<int>(group_ec));
        return false;
    }

    std::vector<InstanceScanEntry> snapshot;
    for (const auto &group : groups) {
        if (stop_requested_.load(std::memory_order_acquire)) {
            return false;
        }
        if (!group || group->name().empty()) {
            RecordOperationError("registry_snapshot");
            KVCM_LOG_WARN("cache gc encountered invalid instance group in snapshot");
            return false;
        }
        auto [instance_ec, instances] = registry_manager_->ListInstanceInfo(context.get(), group->name());
        if (instance_ec != EC_OK) {
            RecordOperationError("list_instances");
            KVCM_LOG_WARN("cache gc list instances failed, group[%s] ec[%d]",
                          group->name().c_str(),
                          static_cast<int>(instance_ec));
            return false;
        }
        for (const auto &instance : instances) {
            if (!instance || instance->instance_id().empty()) {
                RecordOperationError("registry_snapshot");
                KVCM_LOG_WARN("cache gc encountered invalid instance in group[%s]", group->name().c_str());
                return false;
            }
            snapshot.push_back({group->name(), instance->instance_id()});
        }
    }

    std::sort(snapshot.begin(), snapshot.end());
    snapshot.erase(std::unique(snapshot.begin(), snapshot.end()), snapshot.end());
    instances_ = std::move(snapshot);
    instance_index_ = 0;
    cursor_ = SCAN_BASE_CURSOR;
    round_id_ = next_round_id;
    round_active_ = true;
    round_started_at_ = Clock::now();
    next_round_at_.reset();
    round_context_ = std::move(context);
    KVCM_LOG_INFO("cache gc round[%lu] started with[%zu] instances", round_id_, instances_.size());
    return true;
}

void CacheGarbageCollector::CompleteRound() noexcept {
    const auto now = Clock::now();
    const auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - round_started_at_).count();
    METRICS_(cache_gc, round_duration_ms) = static_cast<double>(std::max<int64_t>(0, duration_ms));
    ++METRICS_(cache_gc, scan_round_count);
    KVCM_LOG_INFO(
        "cache gc round[%lu] completed, instances[%zu] duration_ms[%ld]", round_id_, instances_.size(), duration_ms);

    instances_.clear();
    instance_index_ = 0;
    cursor_ = SCAN_BASE_CURSOR;
    round_active_ = false;
    round_context_.reset();
    next_round_at_ = now + std::chrono::milliseconds(config_.round_pause_ms);
}

void CacheGarbageCollector::AdvanceInstance() noexcept {
    ++instance_index_;
    cursor_ = SCAN_BASE_CURSOR;
    if (instance_index_ >= instances_.size()) {
        CompleteRound();
    }
}

CacheLocationDelRequest CacheGarbageCollector::BuildDeleteRequest(const std::string &instance_id,
                                                                  const MaintenanceScanBatch &batch,
                                                                  const int64_t now_us) {
    CacheLocationDelRequest request;
    request.instance_id = instance_id;
    request.expected_status = CLS_WRITING;

    if (instance_id.empty() || batch.keys.size() != batch.locations.size() ||
        batch.keys.size() != batch.location_results.size()) {
        RecordOperationError("scan_shape");
        return request;
    }

    std::set<std::pair<KeyType, std::string>> candidates;
    for (size_t i = 0; i < batch.keys.size(); ++i) {
        if (batch.location_results[i] != EC_OK) {
            if (batch.location_results[i] != EC_NOENT) {
                RecordOperationError("scan_key");
            }
            continue;
        }
        for (const auto &[location_id, location] : batch.locations[i]) {
            if (location && IsOrphanWriting(location_id, *location, now_us)) {
                candidates.emplace(batch.keys[i], location_id);
            }
        }
    }
    METRICS_(cache_gc, candidate_count) += candidates.size();

    std::map<KeyType, std::vector<std::string>> targets;
    size_t selected = 0;
    for (const auto &[block_key, location_id] : candidates) {
        if (pending_locations_.find(PendingLocationKey{instance_id, block_key, location_id}) !=
            pending_locations_.end()) {
            continue;
        }
        if (selected >= config_.scan_batch_size) {
            break;
        }
        targets[block_key].push_back(location_id);
        ++selected;
    }
    request.block_keys.reserve(targets.size());
    request.location_ids.reserve(targets.size());
    for (auto &[block_key, location_ids] : targets) {
        request.block_keys.push_back(block_key);
        request.location_ids.emplace_back(std::move(location_ids));
    }
    return request;
}

bool CacheGarbageCollector::IsOrphanWriting(const std::string &map_location_id,
                                            const CacheLocation &location,
                                            const int64_t now_us) const noexcept {
    if (map_location_id.empty() || location.id().empty() || map_location_id != location.id() ||
        location.status() != CLS_WRITING || location.create_time() <= 0 || now_us < location.create_time()) {
        return false;
    }
    const int64_t age_us = now_us - location.create_time();
    return age_us / 1000 >= config_.orphan_writing_grace_period_ms;
}

void CacheGarbageCollector::RecordOperationError(const std::string &stage) noexcept {
    try {
        if (metrics_registry_) {
            auto counter = metrics_registry_->GetCounter(kOperationErrorMetricName, MetricsTags{{"stage", stage}});
            ++counter;
        }
    } catch (...) { KVCM_LOG_ERROR("cache gc failed to record operation error metric"); }
}

void CacheGarbageCollector::RecordDeleteResult(const std::string &status) noexcept {
    try {
        if (metrics_registry_) {
            auto counter = metrics_registry_->GetCounter(kDeleteResultMetricName, MetricsTags{{"status", status}});
            ++counter;
        }
    } catch (...) { KVCM_LOG_ERROR("cache gc failed to record delete result metric"); }
}

} // namespace kv_cache_manager
