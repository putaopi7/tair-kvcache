#pragma once

#include <charconv>
#include <cstdint>
#include <functional>
#include <string>
#include <system_error>

#include "kv_cache_manager/data_storage/data_storage_uri.h"

namespace kv_cache_manager {

inline constexpr const char *KVCM_SNAPSHOT_INSTANCE_PARAM = "kvcm_instance_id";
inline constexpr const char *KVCM_SNAPSHOT_HOST_PARAM = "kvcm_host_ip_port";
inline constexpr const char *KVCM_SNAPSHOT_MEDIUM_PARAM = "kvcm_medium";
inline constexpr const char *KVCM_SNAPSHOT_VERSION_PARAM = "kvcm_snapshot_version";

struct SnapshotScopeKey {
    std::string instance_id;
    std::string host_ip_port;
    std::string medium;

    bool operator==(const SnapshotScopeKey &other) const {
        return instance_id == other.instance_id && host_ip_port == other.host_ip_port && medium == other.medium;
    }
};

struct SnapshotScopeKeyHash {
    size_t operator()(const SnapshotScopeKey &key) const {
        size_t seed = std::hash<std::string>{}(key.instance_id);
        seed ^= std::hash<std::string>{}(key.host_ip_port) + 0x9e3779b9U + (seed << 6) + (seed >> 2);
        seed ^= std::hash<std::string>{}(key.medium) + 0x9e3779b9U + (seed << 6) + (seed >> 2);
        return seed;
    }
};

struct SnapshotUriInfo {
    SnapshotScopeKey scope;
    uint64_t version = 0;
};

inline bool ParseSnapshotUint64(const std::string &text, uint64_t &out) {
    if (text.empty()) {
        return false;
    }
    uint64_t value = 0;
    auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || ptr != text.data() + text.size()) {
        return false;
    }
    out = value;
    return true;
}

inline std::string HostIpPortFromUri(const DataStorageUri &uri) {
    if (!uri.Valid() || uri.GetHostName().empty()) {
        return {};
    }
    std::string host_ip_port = uri.GetHostName();
    if (uri.GetPort() > 0) {
        host_ip_port += ":" + std::to_string(uri.GetPort());
    }
    return host_ip_port;
}

inline bool ParseSnapshotUriInfo(const DataStorageUri &uri, SnapshotUriInfo &out) {
    if (!uri.Valid()) {
        return false;
    }
    const std::string version_text = uri.GetParam(KVCM_SNAPSHOT_VERSION_PARAM);
    if (!ParseSnapshotUint64(version_text, out.version) || out.version == 0) {
        return false;
    }
    out.scope.instance_id = uri.GetParam(KVCM_SNAPSHOT_INSTANCE_PARAM);
    out.scope.medium = uri.GetParam(KVCM_SNAPSHOT_MEDIUM_PARAM);
    out.scope.host_ip_port = uri.GetParam(KVCM_SNAPSHOT_HOST_PARAM);
    if (out.scope.host_ip_port.empty()) {
        // Compatibility with the initial URI-version prototype, which derived
        // the reporting host from the data URI itself.
        out.scope.host_ip_port = HostIpPortFromUri(uri);
    }
    return !out.scope.instance_id.empty() && !out.scope.medium.empty() && !out.scope.host_ip_port.empty();
}

inline bool ParseSnapshotUriInfo(const std::string &uri_text, SnapshotUriInfo &out) {
    return ParseSnapshotUriInfo(DataStorageUri(uri_text), out);
}

inline bool AddSnapshotVersionToUri(const std::string &raw_uri,
                                    const SnapshotScopeKey &scope,
                                    uint64_t version,
                                    std::string &out_uri) {
    DataStorageUri uri(raw_uri);
    if (!uri.Valid() || scope.instance_id.empty() || scope.host_ip_port.empty() || scope.medium.empty() ||
        version == 0) {
        return false;
    }
    uri.SetParam(KVCM_SNAPSHOT_INSTANCE_PARAM, scope.instance_id);
    uri.SetParam(KVCM_SNAPSHOT_HOST_PARAM, scope.host_ip_port);
    uri.SetParam(KVCM_SNAPSHOT_MEDIUM_PARAM, scope.medium);
    uri.SetParam(KVCM_SNAPSHOT_VERSION_PARAM, std::to_string(version));
    out_uri = uri.ToUriString();
    return !out_uri.empty();
}

} // namespace kv_cache_manager
