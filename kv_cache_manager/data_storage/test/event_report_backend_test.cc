#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/data_storage/data_storage_uri.h"
#include "kv_cache_manager/data_storage/event_report_backend.h"
#include "kv_cache_manager/data_storage/storage_config.h"
#include "kv_cache_manager/metrics/metrics_registry.h"

using namespace kv_cache_manager;
using namespace std::chrono_literals;

class EventReportBackendTest : public TESTBASE {
public:
    void SetUp() override { metrics_registry_ = std::make_shared<MetricsRegistry>(); }

    static StorageConfig MakeConfig(int64_t hb_timeout_ms = 200,
                                    int64_t cleanup_grace_ms = 400,
                                    int64_t check_interval_ms = 50,
                                    DataStorageType type = DataStorageType::DATA_STORAGE_TYPE_EVENT_REPORT_L1P5) {
        auto spec = std::make_shared<EventReportStorageSpec>();
        spec->set_heartbeat_timeout_ms(hb_timeout_ms);
        spec->set_cleanup_grace_ms(cleanup_grace_ms);
        spec->set_liveness_check_interval_ms(check_interval_ms);
        return StorageConfig(type, "event_report_test_group", spec);
    }

    std::shared_ptr<MetricsRegistry> metrics_registry_;
};

// (1) GetType / Available / Create-Delete EC_UNIMPLEMENTED / GetStorageUsageRatio=1.0
TEST_F(EventReportBackendTest, BasicAccessors) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_FALSE(backend.Available());

    ASSERT_DOUBLE_EQ(1.0, backend.GetStorageUsageRatio("trace"));

    auto create_res = backend.Create({"k1", "k2"}, 64, "trace", []() {});
    ASSERT_EQ(create_res.size(), 2u);
    for (const auto &[ec, uri] : create_res) {
        ASSERT_EQ(ec, ErrorCode::EC_UNIMPLEMENTED);
    }
    DataStorageUri u;
    auto del_res = backend.Delete({u, u}, "trace", []() {});
    ASSERT_EQ(del_res.size(), 2u);
    for (auto ec : del_res) {
        ASSERT_EQ(ec, ErrorCode::EC_UNIMPLEMENTED);
    }

    // After Open(), GetType() returns the configured type
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(), "trace"));
    ASSERT_EQ(backend.GetType(), DataStorageType::DATA_STORAGE_TYPE_EVENT_REPORT_L1P5);
    ASSERT_TRUE(backend.Available());
    ASSERT_EQ(EC_OK, backend.Close());
}

TEST_F(EventReportBackendTest, BuildLocationIdIncludesEventReportType) {
    EventReportBackend l1p5_backend(metrics_registry_);
    ASSERT_EQ(EC_OK, l1p5_backend.Open(MakeConfig(), "trace"));
    EXPECT_EQ("kvs#event_report_l1p5#mem#10.0.0.1:8080", l1p5_backend.BuildLocationId("mem", "10.0.0.1:8080"));

    EventReportBackend l2_backend(metrics_registry_);
    ASSERT_EQ(EC_OK,
              l2_backend.Open(MakeConfig(200, 400, 50, DataStorageType::DATA_STORAGE_TYPE_EVENT_REPORT_L2), "trace"));
    EXPECT_EQ("kvs#event_report_l2#mem#10.0.0.1:8080", l2_backend.BuildLocationId("mem", "10.0.0.1:8080"));
}

TEST_F(EventReportBackendTest, OpenWithWrongSpecTypeFails) {
    EventReportBackend backend(metrics_registry_);
    auto spec = std::make_shared<NfsStorageSpec>();
    spec->set_root_path("/tmp");
    StorageConfig cfg(DataStorageType::DATA_STORAGE_TYPE_EVENT_REPORT_L1P5, "event_report_test", spec);
    ASSERT_NE(EC_OK, backend.Open(cfg, "trace"));
    ASSERT_FALSE(backend.Available());
}

TEST_F(EventReportBackendTest, OpenStartsLivenessLoopAndCloseStops) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(), "trace"));
    ASSERT_TRUE(backend.Available());
    ASSERT_TRUE(backend.liveness_checker_running_.load());
    ASSERT_TRUE(backend.liveness_checker_thread_.joinable());

    ASSERT_EQ(EC_OK, backend.Close());
    ASSERT_FALSE(backend.Available());
    ASSERT_FALSE(backend.liveness_checker_running_.load());
    ASSERT_EQ(EC_OK, backend.Close());
}

// (2) RegisterNode / UnregisterNode
TEST_F(EventReportBackendTest, RegisterNodeWithMediums) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(), "trace"));

    ASSERT_EQ(EC_BADARGS, backend.RegisterNode("test_inst", "", {"mem"}));
    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.1:8080", {"mem", "disk"}));
    ASSERT_TRUE(backend.IsNodeAvailable("test_inst", "10.0.0.1:8080"));

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.1:8080", {"disk", "ssd"}));
    {
        auto &host_map = backend.instance_nodes_["test_inst"];
        auto it = host_map.find("10.0.0.1:8080");
        ASSERT_NE(it, host_map.end());
        ASSERT_EQ(it->second->mediums.size(), 3u); // mem + disk + ssd
    }

    backend.SetNodeUnavailable("test_inst", "10.0.0.1:8080");
    ASSERT_FALSE(backend.IsNodeAvailable("test_inst", "10.0.0.1:8080"));
    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.1:8080", {"mem"}));
    ASSERT_TRUE(backend.IsNodeAvailable("test_inst", "10.0.0.1:8080"));

    ASSERT_EQ(EC_OK, backend.UnregisterNode("test_inst", "10.0.0.1:8080"));
    ASSERT_FALSE(backend.IsNodeAvailable("test_inst", "10.0.0.1:8080"));
    ASSERT_EQ(EC_NOENT, backend.UnregisterNode("test_inst", "10.0.0.1:8080"));
    ASSERT_EQ(EC_OK, backend.Close());
}

// (3) OnHeartbeat
TEST_F(EventReportBackendTest, OnHeartbeatRefreshesAndRevivesNode) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 200, /*grace*/ 5000, /*tick*/ 50), "trace"));
    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.3:8080", {"mem"}));

    int64_t initial_hb = 0;
    {
        auto &host_map = backend.instance_nodes_["test_inst"];
        auto it = host_map.find("10.0.0.3:8080");
        ASSERT_NE(it, host_map.end());
        initial_hb = it->second->last_heartbeat_ms.load();
        ASSERT_GT(initial_hb, 0);
    }

    std::this_thread::sleep_for(20ms);
    ASSERT_EQ(EC_OK, backend.OnHeartbeat("test_inst", "10.0.0.3:8080", {{"version", "er-0.18"}}));
    {
        auto &host_map = backend.instance_nodes_["test_inst"];
        auto it = host_map.find("10.0.0.3:8080");
        ASSERT_GT(it->second->last_heartbeat_ms.load(), initial_hb);
        ASSERT_EQ(it->second->last_system_status.at("version"), "er-0.18");
    }

    backend.SetNodeUnavailable("test_inst", "10.0.0.3:8080");
    ASSERT_FALSE(backend.IsNodeAvailable("test_inst", "10.0.0.3:8080"));
    ASSERT_EQ(EC_OK, backend.OnHeartbeat("test_inst", "10.0.0.3:8080", {}));
    {
        auto &host_map = backend.instance_nodes_["test_inst"];
        auto it = host_map.find("10.0.0.3:8080");
        ASSERT_TRUE(it->second->available.load());
        ASSERT_EQ(it->second->unavailable_since_ms.load(), 0);
    }

    ASSERT_EQ(EC_NODE_NOT_REGISTERED, backend.OnHeartbeat("test_inst", "99.99.99.99:8080", {{"x", "y"}}));
    ASSERT_EQ(backend.instance_nodes_["test_inst"].count("99.99.99.99:8080"), 0u);

    ASSERT_EQ(EC_OK, backend.Close());
}

// (5) LivenessCheckerLoop: healthy -> unavailable -> dead
TEST_F(EventReportBackendTest, LivenessLoopHealthyToUnavailableToCleanup) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 100, /*grace*/ 200, /*tick*/ 20), "trace"));

    std::atomic<int> cleanup_calls{0};
    std::string cleanup_host;
    backend.SetCleanupCallback([&](const std::string & /*instance_id*/, const std::string &host, uint64_t /*gen*/) {
        ++cleanup_calls;
        cleanup_host = host;
    });

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.4:8080", {"mem"}));
    ASSERT_TRUE(backend.IsNodeAvailable("test_inst", "10.0.0.4:8080"));

    std::this_thread::sleep_for(160ms);
    ASSERT_FALSE(backend.IsNodeAvailable("test_inst", "10.0.0.4:8080"));
    EXPECT_EQ(cleanup_calls.load(), 0);

    for (int i = 0; i < 50 && cleanup_calls.load() == 0; ++i) {
        std::this_thread::sleep_for(20ms);
    }
    EXPECT_GE(cleanup_calls.load(), 1);
    EXPECT_EQ(cleanup_host, "10.0.0.4:8080");

    EXPECT_EQ(backend.instance_nodes_["test_inst"].count("10.0.0.4:8080"), 0u);

    ASSERT_EQ(EC_OK, backend.Close());
}

// (6) Grace-period recovery
TEST_F(EventReportBackendTest, HeartbeatWithinGraceWindowRecovers) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 80, /*grace*/ 5000, /*tick*/ 20), "trace"));

    std::atomic<int> cleanup_calls{0};
    backend.SetCleanupCallback([&](const std::string &, const std::string &, uint64_t /*gen*/) { ++cleanup_calls; });

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.5:8080", {"mem"}));
    std::this_thread::sleep_for(140ms);
    ASSERT_FALSE(backend.IsNodeAvailable("test_inst", "10.0.0.5:8080"));

    ASSERT_EQ(EC_OK, backend.OnHeartbeat("test_inst", "10.0.0.5:8080", {}));
    ASSERT_TRUE(backend.IsNodeAvailable("test_inst", "10.0.0.5:8080"));

    std::this_thread::sleep_for(60ms);
    EXPECT_EQ(cleanup_calls.load(), 0);

    ASSERT_EQ(EC_OK, backend.Close());
}

// (7) Re-registration after cleanup
TEST_F(EventReportBackendTest, RegisterAfterCleanupCreatesNewEntry) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 80, /*grace*/ 120, /*tick*/ 20), "trace"));

    std::atomic<int> cleanup_calls{0};
    backend.SetCleanupCallback([&](const std::string &, const std::string &, uint64_t /*gen*/) { ++cleanup_calls; });

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.6:8080", {"mem"}));
    for (int i = 0; i < 80 && cleanup_calls.load() == 0; ++i) {
        std::this_thread::sleep_for(20ms);
    }
    ASSERT_GE(cleanup_calls.load(), 1);

    EXPECT_EQ(backend.instance_nodes_["test_inst"].count("10.0.0.6:8080"), 0u);

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.6:8080", {"mem", "disk"}));
    ASSERT_TRUE(backend.IsNodeAvailable("test_inst", "10.0.0.6:8080"));
    {
        auto &host_map = backend.instance_nodes_["test_inst"];
        auto it = host_map.find("10.0.0.6:8080");
        ASSERT_NE(it, host_map.end());
        EXPECT_EQ(it->second->mediums.size(), 2u);
    }

    ASSERT_EQ(EC_OK, backend.Close());
}

// (8) EVENT_HOST_DOWN: immediate removal, no cleanup callback
TEST_F(EventReportBackendTest, HostDownRemovesNodeFromTable) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 200, /*grace*/ 400, /*tick*/ 50), "trace"));

    std::atomic<int> cleanup_calls{0};
    backend.SetCleanupCallback([&](const std::string &, const std::string &, uint64_t /*gen*/) { ++cleanup_calls; });

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.7:8080", {"mem"}));
    ASSERT_TRUE(backend.IsNodeAvailable("test_inst", "10.0.0.7:8080"));

    backend.SetNodeUnavailable("test_inst", "10.0.0.7:8080");
    ASSERT_FALSE(backend.IsNodeAvailable("test_inst", "10.0.0.7:8080"));
    ASSERT_EQ(EC_OK, backend.UnregisterNode("test_inst", "10.0.0.7:8080"));

    EXPECT_EQ(backend.instance_nodes_["test_inst"].count("10.0.0.7:8080"), 0u);

    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(cleanup_calls.load(), 0);

    ASSERT_EQ(EC_OK, backend.Close());
}

// (9) Generation counter fences stale cleanup
TEST_F(EventReportBackendTest, GenerationBumpsOnReRegistration) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 200, /*grace*/ 5000, /*tick*/ 50), "trace"));

    const std::string host = "10.0.0.8:8080";
    ASSERT_EQ(0u, backend.GetNodeGeneration("test_inst", host));

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", host, {"mem"}));
    ASSERT_EQ(1u, backend.GetNodeGeneration("test_inst", host));

    backend.SetNodeUnavailable("test_inst", host);
    ASSERT_EQ(EC_OK, backend.UnregisterNode("test_inst", host));
    ASSERT_EQ(1u, backend.GetNodeGeneration("test_inst", host));

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", host, {"mem", "disk"}));
    ASSERT_EQ(2u, backend.GetNodeGeneration("test_inst", host));

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", host, {"ssd"}));
    ASSERT_EQ(3u, backend.GetNodeGeneration("test_inst", host));

    ASSERT_EQ(EC_OK, backend.Close());
}

// (10) Cleanup callback receives correct generation
TEST_F(EventReportBackendTest, LivenessLoopPassesGenerationToCallback) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 80, /*grace*/ 120, /*tick*/ 20), "trace"));

    std::atomic<uint64_t> received_gen{0};
    backend.SetCleanupCallback(
        [&](const std::string &, const std::string &, uint64_t gen) { received_gen.store(gen); });

    const std::string host = "10.0.0.9:8080";
    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", host, {"mem"}));
    uint64_t expected_gen = backend.GetNodeGeneration("test_inst", host);

    for (int i = 0; i < 80 && received_gen.load() == 0; ++i) {
        std::this_thread::sleep_for(20ms);
    }
    EXPECT_EQ(received_gen.load(), expected_gen);

    ASSERT_EQ(EC_OK, backend.Close());
}

TEST_F(EventReportBackendTest, OnHeartbeatPublishesMetricsGauges) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 5000, /*grace*/ 10000, /*tick*/ 50), "trace"));
    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.10:9600", {"mem"}));

    backend.OnHeartbeat("test_inst",
                        "10.0.0.10:9600",
                        {
                            {"hit_rate", "0.85"},
                            {"active_leases", "5"},
                            {"non_numeric_field", "BOTH_OK"},
                        });

    auto hit_rate_data = metrics_registry_->GetMetricsData("event_report.hit_rate");
    ASSERT_NE(hit_rate_data, nullptr);
    MetricsTags expected_tags = {{"instance_id", "test_inst"}, {"host", "10.0.0.10:9600"}};
    auto gauge = hit_rate_data->GetOrCreateGauge(expected_tags);
    ASSERT_DOUBLE_EQ(0.85, gauge.Get());

    auto leases_data = metrics_registry_->GetMetricsData("event_report.active_leases");
    ASSERT_NE(leases_data, nullptr);
    auto leases_gauge = leases_data->GetOrCreateGauge(expected_tags);
    ASSERT_DOUBLE_EQ(5.0, leases_gauge.Get());

    auto non_numeric = metrics_registry_->GetMetricsData("event_report.non_numeric_field");
    ASSERT_EQ(non_numeric, nullptr);

    backend.OnHeartbeat("test_inst",
                        "10.0.0.10:9600",
                        {
                            {"hit_rate", "0.90"},
                            {"brand_new_metric", "42"},
                        });

    auto new_data = metrics_registry_->GetMetricsData("event_report.brand_new_metric");
    ASSERT_NE(new_data, nullptr);
    auto new_gauge = new_data->GetOrCreateGauge(expected_tags);
    ASSERT_DOUBLE_EQ(42.0, new_gauge.Get());
    ASSERT_DOUBLE_EQ(0.90, gauge.Get());

    ASSERT_EQ(EC_OK, backend.Close());
}

TEST_F(EventReportBackendTest, SetNodeUnavailableZerosGauges) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 5000, /*grace*/ 10000, /*tick*/ 50), "trace"));

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.30:9600", {"mem"}));
    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.31:9600", {"mem"}));

    backend.OnHeartbeat("test_inst", "10.0.0.30:9600", {{"hit_rate", "0.90"}, {"mem_used", "8192"}});
    backend.OnHeartbeat("test_inst", "10.0.0.31:9600", {{"hit_rate", "0.80"}, {"mem_used", "4096"}});

    MetricsTags tags_30 = {{"instance_id", "test_inst"}, {"host", "10.0.0.30:9600"}};
    MetricsTags tags_31 = {{"instance_id", "test_inst"}, {"host", "10.0.0.31:9600"}};

    auto hr_data = metrics_registry_->GetMetricsData("event_report.hit_rate");
    ASSERT_NE(hr_data, nullptr);
    ASSERT_DOUBLE_EQ(0.90, hr_data->GetOrCreateGauge(tags_30).Get());
    ASSERT_DOUBLE_EQ(0.80, hr_data->GetOrCreateGauge(tags_31).Get());

    backend.SetNodeUnavailable("test_inst", "10.0.0.30:9600");

    ASSERT_DOUBLE_EQ(0.0, hr_data->GetOrCreateGauge(tags_30).Get());
    auto mu_data = metrics_registry_->GetMetricsData("event_report.mem_used");
    ASSERT_DOUBLE_EQ(0.0, mu_data->GetOrCreateGauge(tags_30).Get());

    ASSERT_DOUBLE_EQ(0.80, hr_data->GetOrCreateGauge(tags_31).Get());
    ASSERT_DOUBLE_EQ(4096, mu_data->GetOrCreateGauge(tags_31).Get());

    backend.SetNodeUnavailable("test_inst", "10.0.0.30:9600");
    ASSERT_DOUBLE_EQ(0.0, hr_data->GetOrCreateGauge(tags_30).Get());

    ASSERT_EQ(EC_OK, backend.Close());
}

TEST_F(EventReportBackendTest, UnregisterNodeCleansUpGauges) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 5000, /*grace*/ 10000, /*tick*/ 50), "trace"));

    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.20:9600", {"mem"}));
    ASSERT_EQ(EC_OK, backend.RegisterNode("test_inst", "10.0.0.21:9600", {"mem"}));

    backend.OnHeartbeat("test_inst", "10.0.0.20:9600", {{"hit_rate", "0.75"}, {"mem_used", "4096"}});
    backend.OnHeartbeat("test_inst", "10.0.0.21:9600", {{"hit_rate", "0.60"}, {"mem_used", "2048"}});

    MetricsTags tags_20 = {{"instance_id", "test_inst"}, {"host", "10.0.0.20:9600"}};
    MetricsTags tags_21 = {{"instance_id", "test_inst"}, {"host", "10.0.0.21:9600"}};

    auto hr_data = metrics_registry_->GetMetricsData("event_report.hit_rate");
    ASSERT_NE(hr_data, nullptr);
    ASSERT_DOUBLE_EQ(0.75, hr_data->GetOrCreateGauge(tags_20).Get());
    ASSERT_DOUBLE_EQ(0.60, hr_data->GetOrCreateGauge(tags_21).Get());

    ASSERT_EQ(EC_OK, backend.UnregisterNode("test_inst", "10.0.0.20:9600"));

    auto hr_values = hr_data->GetMetricsValues();
    for (const auto &[tags, val] : hr_values) {
        ASSERT_NE(tags, tags_20) << "node 20 gauge should have been removed";
    }
    auto mu_data = metrics_registry_->GetMetricsData("event_report.mem_used");
    auto mu_values = mu_data->GetMetricsValues();
    for (const auto &[tags, val] : mu_values) {
        ASSERT_NE(tags, tags_20) << "node 20 gauge should have been removed";
    }

    ASSERT_DOUBLE_EQ(0.60, hr_data->GetOrCreateGauge(tags_21).Get());
    ASSERT_DOUBLE_EQ(2048, mu_data->GetOrCreateGauge(tags_21).Get());

    ASSERT_EQ(EC_OK, backend.Close());
}

TEST_F(EventReportBackendTest, TwoInstancesSameHostIsolated) {
    EventReportBackend backend(metrics_registry_);
    ASSERT_EQ(EC_OK, backend.Open(MakeConfig(/*hb*/ 200, /*grace*/ 5000, /*tick*/ 50), "trace"));

    const std::string host = "10.0.0.50:8080";
    const std::string inst_a = "instance_a";
    const std::string inst_b = "instance_b";

    ASSERT_EQ(EC_OK, backend.RegisterNode(inst_a, host, {"mem"}));
    ASSERT_EQ(EC_OK, backend.RegisterNode(inst_b, host, {"mem", "disk"}));

    ASSERT_TRUE(backend.IsNodeAvailable(inst_a, host));
    ASSERT_TRUE(backend.IsNodeAvailable(inst_b, host));

    ASSERT_EQ(1u, backend.GetNodeGeneration(inst_a, host));
    ASSERT_EQ(1u, backend.GetNodeGeneration(inst_b, host));

    backend.SetNodeUnavailable(inst_a, host);
    ASSERT_FALSE(backend.IsNodeAvailable(inst_a, host));
    ASSERT_TRUE(backend.IsNodeAvailable(inst_b, host));

    ASSERT_EQ(EC_OK, backend.OnHeartbeat(inst_b, host, {{"metric", "42"}}));
    ASSERT_TRUE(backend.IsNodeAvailable(inst_b, host));

    ASSERT_EQ(EC_OK, backend.UnregisterNode(inst_a, host));
    ASSERT_EQ(EC_NOENT, backend.UnregisterNode(inst_a, host));
    ASSERT_TRUE(backend.IsNodeAvailable(inst_b, host));
    ASSERT_EQ(1u, backend.GetNodeGeneration(inst_a, host));

    ASSERT_EQ(EC_OK, backend.RegisterNode(inst_a, host, {"ssd"}));
    ASSERT_EQ(2u, backend.GetNodeGeneration(inst_a, host));
    ASSERT_EQ(1u, backend.GetNodeGeneration(inst_b, host));

    ASSERT_EQ(EC_OK, backend.Close());
}

TEST(EventReportBackendSnapshotTest, RequiresSnapshotBeforeAnyDelta) {
    EventReportBackend backend(nullptr);
    const ReporterSnapshotKey scope{"instance-a", "10.0.0.1:8080"};

    std::string committed;
    EXPECT_EQ(EC_SNAPSHOT_REQUIRED, backend.BeginDeltaMutation(scope, committed));
    EXPECT_TRUE(committed.empty());
    EXPECT_TRUE(backend.GetSnapshotVersion(scope).empty());
}

TEST(EventReportBackendSnapshotTest, SnapshotCommitPublishesOpaqueToken) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey scope{"instance-a", "10.0.0.1:8080"};

    std::string candidate;
    uint64_t retry_after_ms = 123;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(scope, candidate, retry_after_ms));
    EXPECT_EQ(0u, retry_after_ms);
    EXPECT_TRUE(IsValidSnapshotVersionToken(candidate));
    EXPECT_TRUE(backend.GetSnapshotVersion(scope).empty());

    EXPECT_TRUE(backend.CommitSnapshotVersion(scope, candidate));
    EXPECT_EQ(candidate, backend.GetSnapshotVersion(scope));

    std::string committed;
    ASSERT_EQ(EC_OK, backend.BeginDeltaMutation(scope, committed));
    EXPECT_EQ(candidate, committed);
    backend.EndDeltaMutation(scope);
}

TEST(EventReportBackendSnapshotTest, SnapshotAndDeltaWaitForEachOther) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey reporter_key{"instance-a", "10.0.0.1:8080"};

    std::string first;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, first, retry_after_ms));

    std::string concurrent_snapshot;
    EXPECT_EQ(EC_SNAPSHOT_IN_PROGRESS, backend.BeginSnapshot(reporter_key, concurrent_snapshot, retry_after_ms));

    std::promise<void> delta_started;
    std::promise<std::pair<ErrorCode, std::string>> delta_result;
    auto delta_result_future = delta_result.get_future();
    std::thread delta_thread([&] {
        delta_started.set_value();
        std::string committed;
        const ErrorCode ec = backend.BeginDeltaMutation(reporter_key, committed);
        delta_result.set_value({ec, committed});
    });
    delta_started.get_future().wait();
    EXPECT_EQ(std::future_status::timeout, delta_result_future.wait_for(20ms));
    EXPECT_TRUE(backend.CommitSnapshotVersion(reporter_key, first));
    const auto delta_status = delta_result_future.wait_for(1s);
    EXPECT_EQ(std::future_status::ready, delta_status);
    if (delta_status != std::future_status::ready) {
        backend.Close();
    }
    const auto [delta_ec, delta_version] = delta_result_future.get();
    EXPECT_EQ(EC_OK, delta_ec);
    EXPECT_EQ(first, delta_version);
    delta_thread.join();

    // Release the delta lease acquired by delta_thread only after a new
    // snapshot has closed the gate and started waiting for it.
    std::promise<void> snapshot_started;
    std::promise<std::tuple<ErrorCode, std::string, uint64_t>> snapshot_result;
    auto snapshot_result_future = snapshot_result.get_future();
    std::thread snapshot_thread([&] {
        snapshot_started.set_value();
        std::string candidate;
        uint64_t retry_ms = 0;
        const ErrorCode ec = backend.BeginSnapshot(reporter_key, candidate, retry_ms);
        snapshot_result.set_value({ec, candidate, retry_ms});
    });
    snapshot_started.get_future().wait();
    EXPECT_EQ(std::future_status::timeout, snapshot_result_future.wait_for(20ms));
    backend.EndDeltaMutation(reporter_key);
    const auto snapshot_status = snapshot_result_future.wait_for(1s);
    EXPECT_EQ(std::future_status::ready, snapshot_status);
    if (snapshot_status != std::future_status::ready) {
        backend.Close();
    }
    const auto [snapshot_ec, candidate, retry_ms] = snapshot_result_future.get();
    EXPECT_EQ(EC_OK, snapshot_ec);
    EXPECT_TRUE(IsValidSnapshotVersionToken(candidate));
    EXPECT_EQ(0u, retry_ms);
    snapshot_thread.join();
    backend.AbortSnapshotVersion(reporter_key, candidate);
}

TEST(EventReportBackendSnapshotTest, AbortUnblocksWaitingDeltaWithoutPublishingCandidate) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey reporter_key{"instance-a", "10.0.0.1:8080"};

    std::string first;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, first, retry_after_ms));
    ASSERT_TRUE(backend.CommitSnapshotVersion(reporter_key, first));

    std::string candidate;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, candidate, retry_after_ms));
    std::string observed_committed;
    std::string observed_in_flight;
    backend.GetSnapshotVersionTokens(reporter_key, observed_committed, observed_in_flight);
    EXPECT_EQ(first, observed_committed);
    EXPECT_EQ(candidate, observed_in_flight);
    std::promise<std::pair<ErrorCode, std::string>> delta_result;
    auto delta_result_future = delta_result.get_future();
    std::thread delta_thread([&] {
        std::string committed;
        const ErrorCode ec = backend.BeginDeltaMutation(reporter_key, committed);
        delta_result.set_value({ec, committed});
    });
    EXPECT_EQ(std::future_status::timeout, delta_result_future.wait_for(20ms));
    backend.AbortSnapshotVersion(reporter_key, candidate);
    backend.GetSnapshotVersionTokens(reporter_key, observed_committed, observed_in_flight);
    EXPECT_EQ(first, observed_committed);
    EXPECT_TRUE(observed_in_flight.empty());
    const auto delta_status = delta_result_future.wait_for(1s);
    EXPECT_EQ(std::future_status::ready, delta_status);
    if (delta_status != std::future_status::ready) {
        backend.Close();
    }
    const auto [delta_ec, committed] = delta_result_future.get();
    EXPECT_EQ(EC_OK, delta_ec);
    EXPECT_EQ(first, committed);
    delta_thread.join();
    backend.EndDeltaMutation(reporter_key);
}

TEST(EventReportBackendSnapshotTest, CommitUnblocksAllWaitingDeltasWithNewToken) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey reporter_key{"instance-a", "10.0.0.1:8080"};

    std::string candidate;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, candidate, retry_after_ms));

    constexpr size_t kWaiterCount = 4;
    std::vector<std::future<std::pair<ErrorCode, std::string>>> futures;
    futures.reserve(kWaiterCount);
    for (size_t i = 0; i < kWaiterCount; ++i) {
        futures.push_back(std::async(std::launch::async, [&backend, &reporter_key] {
            std::string committed;
            const ErrorCode ec = backend.BeginDeltaMutation(reporter_key, committed);
            if (ec == EC_OK) {
                backend.EndDeltaMutation(reporter_key);
            }
            return std::make_pair(ec, committed);
        }));
    }
    for (auto &future : futures) {
        EXPECT_EQ(std::future_status::timeout, future.wait_for(20ms));
    }

    ASSERT_TRUE(backend.CommitSnapshotVersion(reporter_key, candidate));
    for (auto &future : futures) {
        ASSERT_EQ(std::future_status::ready, future.wait_for(1s));
        const auto [ec, committed] = future.get();
        EXPECT_EQ(EC_OK, ec);
        EXPECT_EQ(candidate, committed);
    }
}

TEST(EventReportBackendSnapshotTest, SnapshotWaitsUntilEveryAdmittedDeltaDrains) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey reporter_key{"instance-a", "10.0.0.1:8080"};

    std::string first;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, first, retry_after_ms));
    ASSERT_TRUE(backend.CommitSnapshotVersion(reporter_key, first));

    constexpr size_t kActiveDeltaCount = 3;
    for (size_t i = 0; i < kActiveDeltaCount; ++i) {
        std::string committed;
        ASSERT_EQ(EC_OK, backend.BeginDeltaMutation(reporter_key, committed));
        ASSERT_EQ(first, committed);
    }

    auto snapshot = std::async(std::launch::async, [&] {
        std::string candidate;
        uint64_t retry_ms = 0;
        const ErrorCode ec = backend.BeginSnapshot(reporter_key, candidate, retry_ms);
        return std::make_tuple(ec, candidate, retry_ms);
    });
    EXPECT_EQ(std::future_status::timeout, snapshot.wait_for(20ms));

    backend.EndDeltaMutation(reporter_key);
    EXPECT_EQ(std::future_status::timeout, snapshot.wait_for(20ms));
    backend.EndDeltaMutation(reporter_key);
    EXPECT_EQ(std::future_status::timeout, snapshot.wait_for(20ms));
    backend.EndDeltaMutation(reporter_key);

    ASSERT_EQ(std::future_status::ready, snapshot.wait_for(1s));
    const auto [ec, candidate, retry_ms] = snapshot.get();
    EXPECT_EQ(EC_OK, ec);
    EXPECT_TRUE(IsValidSnapshotVersionToken(candidate));
    EXPECT_EQ(0u, retry_ms);
    EXPECT_TRUE(backend.CommitSnapshotVersion(reporter_key, candidate));
}

TEST(EventReportBackendSnapshotTest, ConcurrentSnapshotsHaveExactlyOneWinner) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey reporter_key{"instance-a", "10.0.0.1:8080"};
    constexpr size_t kContenderCount = 12;

    std::promise<void> start;
    auto start_signal = start.get_future().share();
    std::vector<std::future<std::pair<ErrorCode, std::string>>> contenders;
    contenders.reserve(kContenderCount);
    for (size_t i = 0; i < kContenderCount; ++i) {
        contenders.push_back(std::async(std::launch::async, [&] {
            start_signal.wait();
            std::string candidate;
            uint64_t retry_after_ms = 0;
            const ErrorCode ec = backend.BeginSnapshot(reporter_key, candidate, retry_after_ms);
            return std::make_pair(ec, candidate);
        }));
    }
    start.set_value();

    size_t winner_count = 0;
    size_t busy_count = 0;
    std::string winning_token;
    for (auto &contender : contenders) {
        ASSERT_EQ(std::future_status::ready, contender.wait_for(1s));
        const auto [ec, candidate] = contender.get();
        if (ec == EC_OK) {
            ++winner_count;
            winning_token = candidate;
        } else {
            EXPECT_EQ(EC_SNAPSHOT_IN_PROGRESS, ec);
            EXPECT_TRUE(candidate.empty());
            ++busy_count;
        }
    }
    EXPECT_EQ(1u, winner_count);
    EXPECT_EQ(kContenderCount - 1, busy_count);
    ASSERT_TRUE(IsValidSnapshotVersionToken(winning_token));
    EXPECT_TRUE(backend.CommitSnapshotVersion(reporter_key, winning_token));
}

TEST(EventReportBackendSnapshotTest, UnregisterCancelsSnapshotWaitingForActiveDelta) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const std::string instance_id = "instance-a";
    const std::string host = "10.0.0.1:8080";
    const ReporterSnapshotKey reporter_key{instance_id, host};
    ASSERT_EQ(EC_OK, backend.RegisterNode(instance_id, host, {"hbm"}));

    std::string first;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, first, retry_after_ms));
    ASSERT_TRUE(backend.CommitSnapshotVersion(reporter_key, first));
    std::string committed;
    ASSERT_EQ(EC_OK, backend.BeginDeltaMutation(reporter_key, committed));

    auto snapshot = std::async(std::launch::async, [&] {
        std::string candidate = "stale";
        uint64_t retry_ms = 99;
        const ErrorCode ec = backend.BeginSnapshot(reporter_key, candidate, retry_ms);
        return std::make_tuple(ec, candidate, retry_ms);
    });
    EXPECT_EQ(std::future_status::timeout, snapshot.wait_for(20ms));
    ASSERT_EQ(EC_OK, backend.UnregisterNode(instance_id, host));

    ASSERT_EQ(std::future_status::ready, snapshot.wait_for(1s));
    const auto [ec, candidate, retry_ms] = snapshot.get();
    EXPECT_EQ(EC_SNAPSHOT_REQUIRED, ec);
    EXPECT_TRUE(candidate.empty());
    EXPECT_EQ(0u, retry_ms);
    backend.EndDeltaMutation(reporter_key);
}

TEST(EventReportBackendSnapshotTest, UnregisterUnblocksWaitingDeltaAndRequiresNewSnapshot) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const std::string instance_id = "instance-a";
    const std::string host = "10.0.0.1:8080";
    const ReporterSnapshotKey reporter_key{instance_id, host};
    ASSERT_EQ(EC_OK, backend.RegisterNode(instance_id, host, {"hbm"}));

    std::string first;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, first, retry_after_ms));
    ASSERT_TRUE(backend.CommitSnapshotVersion(reporter_key, first));
    std::string second;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, second, retry_after_ms));

    auto delta = std::async(std::launch::async, [&] {
        std::string committed = "stale";
        const ErrorCode ec = backend.BeginDeltaMutation(reporter_key, committed);
        return std::make_pair(ec, committed);
    });
    EXPECT_EQ(std::future_status::timeout, delta.wait_for(20ms));
    ASSERT_EQ(EC_OK, backend.UnregisterNode(instance_id, host));
    ASSERT_EQ(std::future_status::ready, delta.wait_for(1s));
    const auto [ec, committed] = delta.get();
    EXPECT_EQ(EC_SNAPSHOT_REQUIRED, ec);
    EXPECT_TRUE(committed.empty());
}

TEST(EventReportBackendSnapshotTest, OtherReporterIsNotBlockedBySnapshot) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey reporter_a{"instance-a", "10.0.0.1:8080"};
    const ReporterSnapshotKey reporter_b{"instance-a", "10.0.0.2:8080"};
    uint64_t retry_after_ms = 0;

    std::string token_b;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_b, token_b, retry_after_ms));
    ASSERT_TRUE(backend.CommitSnapshotVersion(reporter_b, token_b));

    std::string token_a;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_a, token_a, retry_after_ms));
    std::string committed_b;
    EXPECT_EQ(EC_OK, backend.BeginDeltaMutation(reporter_b, committed_b));
    EXPECT_EQ(token_b, committed_b);
    backend.EndDeltaMutation(reporter_b);
    backend.AbortSnapshotVersion(reporter_a, token_a);
}

TEST(EventReportBackendSnapshotTest, AbortNeverPublishesAndWrongTokenCannotCommit) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey scope{"instance-a", "10.0.0.1:8080"};

    std::string candidate;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(scope, candidate, retry_after_ms));
    EXPECT_FALSE(backend.CommitSnapshotVersion(scope, std::string(32, 'f')));
    EXPECT_TRUE(backend.GetSnapshotVersion(scope).empty());

    backend.AbortSnapshotVersion(scope, std::string(32, 'e'));
    std::string still_blocked;
    EXPECT_EQ(EC_SNAPSHOT_IN_PROGRESS, backend.BeginSnapshot(scope, still_blocked, retry_after_ms));

    backend.AbortSnapshotVersion(scope, candidate);
    EXPECT_TRUE(backend.GetSnapshotVersion(scope).empty());
    EXPECT_EQ(EC_OK, backend.BeginSnapshot(scope, still_blocked, retry_after_ms));
    backend.AbortSnapshotVersion(scope, still_blocked);
}

TEST(EventReportBackendSnapshotTest, SnapshotRateLimitReturnsRetryDelay) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(30'000);
    const ReporterSnapshotKey scope{"instance-a", "10.0.0.1:8080"};

    std::string first;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(scope, first, retry_after_ms));
    ASSERT_TRUE(backend.CommitSnapshotVersion(scope, first));

    std::string second;
    EXPECT_EQ(EC_SNAPSHOT_RATE_LIMITED, backend.BeginSnapshot(scope, second, retry_after_ms));
    EXPECT_GT(retry_after_ms, 0u);
    EXPECT_LE(retry_after_ms, 30'000u);
    EXPECT_TRUE(second.empty());

    const uint64_t first_retry_after_ms = retry_after_ms;
    std::this_thread::sleep_for(2ms);
    second = "stale";
    retry_after_ms = 0;
    EXPECT_EQ(EC_SNAPSHOT_RATE_LIMITED, backend.BeginSnapshot(scope, second, retry_after_ms));
    EXPECT_TRUE(second.empty());
    EXPECT_GT(retry_after_ms, 0u);
    EXPECT_LE(retry_after_ms, first_retry_after_ms);

    backend.SetSnapshotMinIntervalMsForTest(0);
    EXPECT_EQ(EC_OK, backend.BeginSnapshot(scope, second, retry_after_ms));
    backend.AbortSnapshotVersion(scope, second);
}

TEST(EventReportBackendSnapshotTest, ScopesAreIsolatedByInstanceAndReporterHost) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey scope_a{"instance-a", "10.0.0.1:8080"};
    const ReporterSnapshotKey scope_b{"instance-a", "10.0.0.2:8080"};
    const ReporterSnapshotKey scope_c{"instance-b", "10.0.0.1:8080"};

    std::string token_a;
    std::string token_b;
    std::string token_c;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(scope_a, token_a, retry_after_ms));
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(scope_b, token_b, retry_after_ms));
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(scope_c, token_c, retry_after_ms));
    EXPECT_NE(token_a, token_b);
    EXPECT_NE(token_a, token_c);
    EXPECT_NE(token_b, token_c);

    EXPECT_TRUE(backend.CommitSnapshotVersion(scope_a, token_a));
    EXPECT_TRUE(backend.CommitSnapshotVersion(scope_b, token_b));
    EXPECT_TRUE(backend.CommitSnapshotVersion(scope_c, token_c));
    EXPECT_EQ(token_a, backend.GetSnapshotVersion(scope_a));
    EXPECT_EQ(token_b, backend.GetSnapshotVersion(scope_b));
    EXPECT_EQ(token_c, backend.GetSnapshotVersion(scope_c));
}

TEST(EventReportBackendSnapshotTest, UnregisterForcesFullSnapshotAgain) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const std::string instance_id = "instance-a";
    const std::string host = "10.0.0.1:8080";
    const ReporterSnapshotKey scope{instance_id, host};

    ASSERT_EQ(EC_OK, backend.RegisterNode(instance_id, host, {"hbm", "dram"}));
    std::string token;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(scope, token, retry_after_ms));
    ASSERT_TRUE(backend.CommitSnapshotVersion(scope, token));
    ASSERT_EQ(token, backend.GetSnapshotVersion(scope));

    ASSERT_EQ(EC_OK, backend.UnregisterNode(instance_id, host));
    EXPECT_TRUE(backend.GetSnapshotVersion(scope).empty());
    std::string committed;
    EXPECT_EQ(EC_SNAPSHOT_REQUIRED, backend.BeginDeltaMutation(scope, committed));
}

TEST(EventReportBackendSnapshotTest, FailedDeltaClearsOutputAndDoesNotCreateACommit) {
    EventReportBackend backend(nullptr);
    std::string committed = "stale-token";

    EXPECT_EQ(EC_BADARGS, backend.BeginDeltaMutation({"", "10.0.0.1:8080"}, committed));
    EXPECT_TRUE(committed.empty());

    committed = "stale-token";
    const ReporterSnapshotKey scope{"instance-a", "10.0.0.1:8080"};
    EXPECT_EQ(EC_SNAPSHOT_REQUIRED, backend.BeginDeltaMutation(scope, committed));
    EXPECT_TRUE(committed.empty());
    EXPECT_TRUE(backend.GetSnapshotVersion(scope).empty());
}

TEST(EventReportBackendSnapshotTest, UnregisterThenReregisterRequiresNewSnapshot) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const std::string instance_id = "instance-a";
    const std::string host = "10.0.0.1:8080";
    const ReporterSnapshotKey scope{instance_id, host};

    ASSERT_EQ(EC_OK, backend.RegisterNode(instance_id, host, {"hbm", "dram"}));
    std::string first_token;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(scope, first_token, retry_after_ms));
    ASSERT_TRUE(backend.CommitSnapshotVersion(scope, first_token));

    ASSERT_EQ(EC_OK, backend.UnregisterNode(instance_id, host));
    ASSERT_EQ(EC_OK, backend.RegisterNode(instance_id, host, {"hbm", "dram"}));
    EXPECT_TRUE(backend.GetSnapshotVersion(scope).empty());
    std::string committed = "stale-token";
    EXPECT_EQ(EC_SNAPSHOT_REQUIRED, backend.BeginDeltaMutation(scope, committed));
    EXPECT_TRUE(committed.empty());

    std::string second_token;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(scope, second_token, retry_after_ms));
    EXPECT_NE(first_token, second_token);
    EXPECT_TRUE(backend.CommitSnapshotVersion(scope, second_token));
}

TEST(EventReportBackendSnapshotTest, StableLocationIdHasNoSnapshotGeneration) {
    EventReportBackend backend(nullptr);
    const std::string location_id = backend.BuildLocationId("hbm", "10.0.0.1:8080");
    EXPECT_EQ("kvs#event_report#hbm#10.0.0.1:8080", location_id);

    std::string medium;
    std::string host;
    EXPECT_TRUE(backend.ParseLocationId(location_id, medium, host));
    EXPECT_EQ("hbm", medium);
    EXPECT_EQ("10.0.0.1:8080", host);
    EXPECT_FALSE(backend.ParseLocationId("kvs#event_report#hbm#snapshot_v=7#10.0.0.1:8080", medium, host));
}

TEST(EventReportBackendSnapshotTest, RegisterAndHeartbeatPreserveCommittedToken) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const std::string instance_id = "instance-a";
    const std::string host = "10.0.0.1:8080";
    const ReporterSnapshotKey reporter_key{instance_id, host};

    ASSERT_EQ(EC_OK, backend.RegisterNode(instance_id, host, {"hbm"}));
    std::string token;
    uint64_t retry_after_ms = 0;
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, token, retry_after_ms));
    ASSERT_TRUE(backend.CommitSnapshotVersion(reporter_key, token));

    EXPECT_EQ(EC_OK, backend.RegisterNode(instance_id, host, {"memory"}));
    EXPECT_EQ(EC_OK, backend.OnHeartbeat(instance_id, host, {{"load", "1"}}));
    EXPECT_EQ(token, backend.GetSnapshotVersion(reporter_key));

    std::string committed;
    ASSERT_EQ(EC_OK, backend.BeginDeltaMutation(reporter_key, committed));
    EXPECT_EQ(token, committed);
    backend.EndDeltaMutation(reporter_key);
}

TEST(EventReportBackendSnapshotTest, InvalidInputsDoNotMutateOrReleaseSnapshotState) {
    EventReportBackend backend(nullptr);
    backend.SetSnapshotMinIntervalMsForTest(0);
    const ReporterSnapshotKey reporter_key{"instance-a", "10.0.0.1:8080"};

    std::string candidate = "stale";
    uint64_t retry_after_ms = 99;
    EXPECT_EQ(EC_BADARGS, backend.BeginSnapshot({"", reporter_key.host_ip_port}, candidate, retry_after_ms));
    EXPECT_TRUE(candidate.empty());
    EXPECT_EQ(0u, retry_after_ms);

    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, candidate, retry_after_ms));
    ASSERT_TRUE(IsValidSnapshotVersionToken(candidate));
    EXPECT_FALSE(backend.CommitSnapshotVersion(reporter_key, ""));
    EXPECT_FALSE(backend.CommitSnapshotVersion(reporter_key, std::string(31, 'a')));
    EXPECT_FALSE(backend.CommitSnapshotVersion(reporter_key, std::string(32, 'g')));

    backend.AbortSnapshotVersion(reporter_key, std::string(32, 'f'));
    std::string blocked = "stale";
    retry_after_ms = 99;
    EXPECT_EQ(EC_SNAPSHOT_IN_PROGRESS, backend.BeginSnapshot(reporter_key, blocked, retry_after_ms));
    EXPECT_TRUE(blocked.empty());
    EXPECT_EQ(0u, retry_after_ms);

    backend.AbortSnapshotVersion(reporter_key, candidate);
    ASSERT_EQ(EC_OK, backend.BeginSnapshot(reporter_key, candidate, retry_after_ms));
    backend.AbortSnapshotVersion(reporter_key, candidate);
}

TEST(EventReportBackendSnapshotTest, LocationIdParserRejectsMalformedAndLegacyIds) {
    EventReportBackend backend(nullptr);
    std::string medium;
    std::string host;

    const std::string valid = backend.BuildLocationId("hbm-cache", "host.example:8080");
    ASSERT_TRUE(backend.ParseLocationId(valid, medium, host));
    EXPECT_EQ("hbm-cache", medium);
    EXPECT_EQ("host.example:8080", host);

    for (const std::string &invalid : {
             std::string{},
             std::string{"kvs#event_report#"},
             std::string{"kvs#event_report##host:8080"},
             std::string{"kvs#event_report#hbm#"},
             std::string{"kvs#event_report#hbm#host:8080#extra"},
             std::string{"kvs#event_report#hbm#snapshot_v=7#host:8080"},
             std::string{"other#event_report#hbm#host:8080"},
         }) {
        EXPECT_FALSE(backend.ParseLocationId(invalid, medium, host)) << invalid;
    }
}

TEST(EventReportBackendSnapshotTest, UriCarriesOnlyOpaqueSnapshotToken) {
    const std::string token = "00112233445566778899aabbccddeeff";
    const std::string reporter_uri = "vineyard://127.0.0.1:9600/object?size=1024";
    std::string versioned_uri;
    ASSERT_TRUE(AddSnapshotVersionToUri(reporter_uri, token, versioned_uri));
    EXPECT_NE(std::string::npos, versioned_uri.find("s_version=" + token));
    EXPECT_EQ(std::string::npos, versioned_uri.find("kvcm_"));

    SnapshotUriInfo info;
    ASSERT_TRUE(ParseSnapshotUriInfo(versioned_uri, info));
    EXPECT_EQ(token, info.version);
    EXPECT_TRUE(HasEventReportInternalUriMetadata(DataStorageUri(versioned_uri)));

    EXPECT_FALSE(ParseSnapshotUriInfo(versioned_uri + "&s_version=" + token, info));
    EXPECT_FALSE(AddSnapshotVersionToUri(versioned_uri, token, versioned_uri));
    EXPECT_FALSE(IsValidSnapshotVersionToken(""));
    EXPECT_FALSE(IsValidSnapshotVersionToken("7"));
    EXPECT_FALSE(IsValidSnapshotVersionToken(std::string(32, 'g')));
}

TEST(EventReportBackendSnapshotTest, SnapshotUriUtilitiesHandleExactParameterBoundaries) {
    const std::string token = "00112233445566778899aabbccddeeff";
    const std::string raw_uri =
        "event_report://10.0.0.1:8080/mem?size=7&user_s_version=kept&s_version_hint=kept";
    EXPECT_EQ(0u, CountUriParam(raw_uri, KVCM_SNAPSHOT_VERSION_PARAM));

    std::string versioned_uri;
    ASSERT_TRUE(AddSnapshotVersionToUri(raw_uri, token, versioned_uri));
    EXPECT_EQ(1u, CountUriParam(versioned_uri, KVCM_SNAPSHOT_VERSION_PARAM));
    const DataStorageUri parsed(versioned_uri);
    ASSERT_TRUE(parsed.Valid());
    EXPECT_EQ("7", parsed.GetParam("size"));
    EXPECT_EQ("kept", parsed.GetParam("user_s_version"));
    EXPECT_EQ("kept", parsed.GetParam("s_version_hint"));
    EXPECT_EQ(token, parsed.GetParam(KVCM_SNAPSHOT_VERSION_PARAM));

    SnapshotUriInfo info;
    EXPECT_FALSE(ParseSnapshotUriInfo(raw_uri, info));
    EXPECT_FALSE(ParseSnapshotUriInfo(
        "event_report://10.0.0.1:8080/mem?s_version=" + std::string(31, 'a'), info));
    EXPECT_FALSE(ParseSnapshotUriInfo(
        "event_report://10.0.0.1:8080/mem?s_version=" + std::string(32, 'g'), info));
    EXPECT_FALSE(ParseSnapshotUriInfo(versioned_uri + "&s_version=" + token, info));
    EXPECT_FALSE(AddSnapshotVersionToUri("", token, versioned_uri));
    EXPECT_FALSE(AddSnapshotVersionToUri(raw_uri, std::string(31, 'a'), versioned_uri));
}
