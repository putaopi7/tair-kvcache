#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kv_cache_manager/data_storage/data_storage_backend.h"
#include "kv_cache_manager/data_storage/snapshot_uri_utils.h"
#include "kv_cache_manager/data_storage/storage_config.h"
#include "kv_cache_manager/metrics/metrics_registry.h"

namespace kv_cache_manager {

// Unified backend for event-reporting storage types.
// Location_id prefix, metrics prefix, and protocol string are constants.
class EventReportBackend : public DataStorageBackend {
public:
    using CleanupCallback =
        std::function<void(const std::string &instance_id, const std::string &host_ip_port, uint64_t generation)>;

    EventReportBackend() = delete;
    explicit EventReportBackend(std::shared_ptr<MetricsRegistry> metrics_registry);
    ~EventReportBackend() override;

    // --- DataStorageBackend interface ---
    DataStorageType GetType() override;
    bool Available() override;
    double GetStorageUsageRatio(const std::string &trace_id) const override;
    ErrorCode DoOpen(const StorageConfig &config, const std::string &trace_id) override;
    ErrorCode Close() override;

    std::vector<std::pair<ErrorCode, DataStorageUri>> Create(const std::vector<std::string> &keys,
                                                             size_t size_per_key,
                                                             const std::string &trace_id,
                                                             std::function<void()> cb) override;
    std::vector<ErrorCode> Delete(const std::vector<DataStorageUri> &storage_uris,
                                  const std::string &trace_id,
                                  std::function<void()> cb) override;
    std::vector<bool> Exist(const std::vector<DataStorageUri> &storage_uris) override;
    std::vector<bool> MightExist(const std::vector<DataStorageUri> &storage_uris) override;
    std::vector<ErrorCode> Lock(const std::vector<DataStorageUri> &storage_uris) override;
    std::vector<ErrorCode> UnLock(const std::vector<DataStorageUri> &storage_uris) override;

    // --- Event reporting methods ---
    void SetCleanupCallback(CleanupCallback cb);
    bool IsCleanupCallbackSet() const { return cleanup_cb_set_.load(std::memory_order_acquire); }

    ErrorCode RegisterNode(const std::string &instance_id,
                           const std::string &host_ip_port,
                           const std::vector<std::string> &mediums);
    ErrorCode UnregisterNode(const std::string &instance_id, const std::string &host_ip_port);
    ErrorCode OnHeartbeat(const std::string &instance_id,
                          const std::string &host_ip_port,
                          const std::map<std::string, std::string> &system_status);
    void SetNodeUnavailable(const std::string &instance_id, const std::string &host_ip_port);
    bool IsNodeAvailable(const std::string &instance_id, const std::string &host_ip_port) const;
    uint64_t GetNodeGeneration(const std::string &instance_id, const std::string &host_ip_port) const;

    std::string BuildLocationId(const std::string &medium, const std::string &host_ip_port) const;
    std::string
    BuildSnapshotLocationId(const std::string &medium, const std::string &host_ip_port, uint64_t version) const;
    bool ParseLocationId(const std::string &location_id, std::string &out_medium, std::string &out_host_ip_port) const;
    std::string HostSuffix(const std::string &host_ip_port) const;
    // A delta lease pins the committed version until every metadata mutation in
    // that ReportEvent request has completed.
    bool BeginDeltaMutation(const SnapshotScopeKey &scope, uint64_t &out_committed_version);
    void EndDeltaMutation(const SnapshotScopeKey &scope);
    // Returns 0 while another snapshot or any delta mutation is in flight.
    uint64_t AllocateSnapshotVersion(const SnapshotScopeKey &scope);
    bool CommitSnapshotVersion(const SnapshotScopeKey &scope, uint64_t version);
    void AbortSnapshotVersion(const SnapshotScopeKey &scope, uint64_t version);
    void ObserveAllocatedSnapshotVersion(const SnapshotScopeKey &scope, uint64_t version);
    void ObserveSnapshotVersion(const SnapshotScopeKey &scope, uint64_t version);
    uint64_t GetSnapshotVersion(const SnapshotScopeKey &scope) const;
    DataStorageType GetStorageType() const;

private:
    struct NodeInfo {
        std::string instance_id;
        std::atomic<int64_t> last_heartbeat_ms{0};
        std::atomic<bool> available{true};
        std::atomic<int64_t> unavailable_since_ms{0};

        std::vector<std::string> mediums;
        mutable std::mutex status_mutex;
        std::map<std::string, std::string> last_system_status;
        MetricsTags metrics_tags;
    };

    void LivenessCheckerLoop();
    void ClearNodeGauges(const NodeInfo &info);
    static int64_t NowMillis() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    EventReportStorageSpec spec_;

    mutable std::shared_mutex nodes_mutex_;
    // instance_id -> (host_ip_port -> NodeInfo)
    std::unordered_map<std::string, std::unordered_map<std::string, std::unique_ptr<NodeInfo>>> instance_nodes_;
    // Persists across unregister/register to fence stale cleanup.
    // instance_id -> (host_ip_port -> generation)
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> node_generation_;
    struct SnapshotVersionState {
        // Process-local scope state, not a distributed lock. HA correctness also
        // relies on leader-only request fencing during ownership changes.
        uint64_t allocated = 0;
        uint64_t committed = 0;
        uint64_t in_flight = 0;
        uint64_t active_delta_mutations = 0;
    };
    // Snapshot generations are durable scope state, not node-liveness state.
    // They intentionally survive UnregisterNode so a re-registered reporter
    // continues from the persisted high-water mark.
    std::unordered_map<SnapshotScopeKey, SnapshotVersionState, SnapshotScopeKeyHash> snapshot_versions_;

    std::thread liveness_checker_thread_;
    std::atomic<bool> liveness_checker_running_{false};

    int64_t heartbeat_timeout_ms_ = EventReportStorageSpec::kDefaultHeartbeatTimeoutMs;
    int64_t cleanup_grace_ms_ = EventReportStorageSpec::kDefaultCleanupGraceMs;
    int64_t liveness_check_interval_ms_ = EventReportStorageSpec::kDefaultLivenessCheckIntervalMs;

    mutable std::mutex cleanup_cb_mutex_;
    CleanupCallback cleanup_callback_;
    std::atomic<bool> cleanup_cb_set_{false};
};

} // namespace kv_cache_manager
