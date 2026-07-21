#pragma once

#include <charconv>
#include <cstdint>
#include <functional>
#include <string>
#include <system_error>

#include "kv_cache_manager/data_storage/data_storage_uri.h"

namespace kv_cache_manager {

inline constexpr const char *KVCM_INTERNAL_URI_PARAM_PREFIX = "kvcm_";
inline constexpr const char *KVCM_EVENT_REPORT_INSTANCE_PARAM = "kvcm_instance_id";
inline constexpr const char *KVCM_EVENT_REPORT_HOST_PARAM = "kvcm_host_ip_port";
inline constexpr const char *KVCM_EVENT_REPORT_MEDIUM_PARAM = "kvcm_medium";
inline constexpr const char *KVCM_SNAPSHOT_VERSION_PARAM = "kvcm_snapshot_version";
inline constexpr const char *KVCM_SNAPSHOT_VERSION_METADATA_PREFIX = "__event_snapshot_version__#";
inline constexpr const char *KVCM_SNAPSHOT_ALLOCATED_VERSION_METADATA_PREFIX = "__event_snapshot_allocated_version__#";
inline constexpr const char *KVCM_EVENT_REPORT_LOCATION_PREFIX = "kvs#event_report#";
inline constexpr const char *KVCM_SNAPSHOT_LOCATION_VERSION_PREFIX = "snapshot_v=";

struct SnapshotScopeKey {
    std::string instance_id;
    std::string host_ip_port;
    std::string medium;

    bool operator==(const SnapshotScopeKey &other) const noexcept {
        return instance_id == other.instance_id && host_ip_port == other.host_ip_port && medium == other.medium;
    }

    bool operator!=(const SnapshotScopeKey &other) const noexcept { return !(*this == other); }
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

inline bool HasEventReportInternalUriMetadata(const DataStorageUri &uri) {
    return uri.HasParamWithPrefix(KVCM_INTERNAL_URI_PARAM_PREFIX);
}

inline bool ParseEventReportScopeFromUri(const DataStorageUri &uri, SnapshotScopeKey &out) {
    if (!uri.Valid()) {
        return false;
    }
    out.instance_id = uri.GetParam(KVCM_EVENT_REPORT_INSTANCE_PARAM);
    out.medium = uri.GetParam(KVCM_EVENT_REPORT_MEDIUM_PARAM);
    out.host_ip_port = uri.GetParam(KVCM_EVENT_REPORT_HOST_PARAM);
    if (out.host_ip_port.empty()) {
        // Compatibility with the initial URI-version prototype, which derived
        // the reporting host from the data URI itself.
        out.host_ip_port = HostIpPortFromUri(uri);
    }
    return !out.instance_id.empty() && !out.medium.empty() && !out.host_ip_port.empty();
}

inline bool ParseSnapshotUriInfo(const DataStorageUri &uri, SnapshotUriInfo &out) {
    const std::string version_text = uri.GetParam(KVCM_SNAPSHOT_VERSION_PARAM);
    return ParseSnapshotUint64(version_text, out.version) && out.version > 0 &&
           ParseEventReportScopeFromUri(uri, out.scope);
}

inline bool ParseSnapshotUriInfo(const std::string &uri_text, SnapshotUriInfo &out) {
    return ParseSnapshotUriInfo(DataStorageUri(uri_text), out);
}

inline bool AddEventReportScopeToUri(const std::string &raw_uri, const SnapshotScopeKey &scope, std::string &out_uri) {
    DataStorageUri uri(raw_uri);
    if (!uri.Valid() || scope.instance_id.empty() || scope.host_ip_port.empty() || scope.medium.empty()) {
        return false;
    }
    uri.SetParam(KVCM_EVENT_REPORT_INSTANCE_PARAM, scope.instance_id);
    uri.SetParam(KVCM_EVENT_REPORT_HOST_PARAM, scope.host_ip_port);
    uri.SetParam(KVCM_EVENT_REPORT_MEDIUM_PARAM, scope.medium);
    out_uri = uri.ToUriString();
    return !out_uri.empty();
}

inline bool AddSnapshotVersionToUri(const std::string &raw_uri,
                                    const SnapshotScopeKey &scope,
                                    uint64_t version,
                                    std::string &out_uri) {
    if (version == 0 || !AddEventReportScopeToUri(raw_uri, scope, out_uri)) {
        return false;
    }
    DataStorageUri uri(out_uri);
    uri.SetParam(KVCM_SNAPSHOT_VERSION_PARAM, std::to_string(version));
    out_uri = uri.ToUriString();
    return !out_uri.empty();
}

inline std::string
BuildEventReportLocationId(const std::string &medium, const std::string &host_ip_port, uint64_t version = 0) {
    std::string result(KVCM_EVENT_REPORT_LOCATION_PREFIX);
    result.reserve(result.size() + medium.size() + host_ip_port.size() + 34);
    result.append(medium);
    result.push_back('#');
    if (version > 0) {
        result.append(KVCM_SNAPSHOT_LOCATION_VERSION_PREFIX);
        result.append(std::to_string(version));
        result.push_back('#');
    }
    result.append(host_ip_port);
    return result;
}

inline bool
ParseEventReportLocationId(const std::string &location_id, std::string &out_medium, std::string &out_host_ip_port) {
    const size_t prefix_size = std::char_traits<char>::length(KVCM_EVENT_REPORT_LOCATION_PREFIX);
    if (location_id.size() <= prefix_size ||
        location_id.compare(0, prefix_size, KVCM_EVENT_REPORT_LOCATION_PREFIX, prefix_size) != 0) {
        return false;
    }
    const size_t host_separator = location_id.find('#', prefix_size);
    if (host_separator == std::string::npos || host_separator == prefix_size ||
        host_separator + 1 >= location_id.size()) {
        return false;
    }
    out_medium = location_id.substr(prefix_size, host_separator - prefix_size);
    const size_t version_prefix_size = std::char_traits<char>::length(KVCM_SNAPSHOT_LOCATION_VERSION_PREFIX);
    const size_t host_begin = host_separator + 1;
    if (location_id.compare(
            host_begin, version_prefix_size, KVCM_SNAPSHOT_LOCATION_VERSION_PREFIX, version_prefix_size) != 0) {
        out_host_ip_port = location_id.substr(host_begin);
        return true;
    }

    const size_t version_begin = host_begin + version_prefix_size;
    const size_t version_end = location_id.find('#', version_begin);
    uint64_t version = 0;
    if (version_end == std::string::npos ||
        !ParseSnapshotUint64(location_id.substr(version_begin, version_end - version_begin), version) || version == 0 ||
        version_end + 1 >= location_id.size()) {
        return false;
    }
    out_host_ip_port = location_id.substr(version_end + 1);
    return true;
}

// Snapshot data and its commit point deliberately use separate records.  URI
// versions identify which snapshot wrote a location; this instance-level
// metadata field is the durable proof that the whole snapshot was committed.
// The reporter-host length makes the key unambiguous even when host or medium
// contain separator characters.
inline std::string SnapshotScopeMetadataKey(const char *prefix, const SnapshotScopeKey &scope) {
    return std::string(prefix) + std::to_string(scope.host_ip_port.size()) + ":" + scope.host_ip_port + scope.medium;
}

inline std::string SnapshotVersionMetadataKey(const SnapshotScopeKey &scope) {
    return SnapshotScopeMetadataKey(KVCM_SNAPSHOT_VERSION_METADATA_PREFIX, scope);
}

inline std::string SnapshotAllocatedVersionMetadataKey(const SnapshotScopeKey &scope) {
    return SnapshotScopeMetadataKey(KVCM_SNAPSHOT_ALLOCATED_VERSION_METADATA_PREFIX, scope);
}

inline bool IsSnapshotVersionMetadataKey(const std::string &key) {
    return key.rfind(KVCM_SNAPSHOT_VERSION_METADATA_PREFIX, 0) == 0;
}

inline bool IsSnapshotAllocatedVersionMetadataKey(const std::string &key) {
    return key.rfind(KVCM_SNAPSHOT_ALLOCATED_VERSION_METADATA_PREFIX, 0) == 0;
}

inline bool ParseSnapshotScopeMetadataKey(const std::string &instance_id,
                                          const std::string &key,
                                          const char *prefix,
                                          SnapshotScopeKey &out) {
    const size_t prefix_size = std::char_traits<char>::length(prefix);
    if (instance_id.empty() || key.rfind(prefix, 0) != 0) {
        return false;
    }
    const size_t length_separator = key.find(':', prefix_size);
    if (length_separator == std::string::npos) {
        return false;
    }
    uint64_t host_size = 0;
    if (!ParseSnapshotUint64(key.substr(prefix_size, length_separator - prefix_size), host_size)) {
        return false;
    }
    const size_t host_begin = length_separator + 1;
    const size_t remaining = key.size() - host_begin;
    if (host_size == 0 || host_size >= remaining) {
        return false;
    }
    out.instance_id = instance_id;
    out.host_ip_port = key.substr(host_begin, static_cast<size_t>(host_size));
    out.medium = key.substr(host_begin + static_cast<size_t>(host_size));
    return !out.medium.empty();
}

inline bool
ParseSnapshotVersionMetadataKey(const std::string &instance_id, const std::string &key, SnapshotScopeKey &out) {
    return ParseSnapshotScopeMetadataKey(instance_id, key, KVCM_SNAPSHOT_VERSION_METADATA_PREFIX, out);
}

inline bool ParseSnapshotAllocatedVersionMetadataKey(const std::string &instance_id,
                                                     const std::string &key,
                                                     SnapshotScopeKey &out) {
    return ParseSnapshotScopeMetadataKey(instance_id, key, KVCM_SNAPSHOT_ALLOCATED_VERSION_METADATA_PREFIX, out);
}

} // namespace kv_cache_manager
