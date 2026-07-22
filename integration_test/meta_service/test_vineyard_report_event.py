#!/usr/bin/env python3
"""
Integration tests for Event Report ReportEvent HTTP interface.

Usage:
    # 1. Start KVCM service locally:
    #    bazel-bin/kv_cache_manager/kv_cache_manager_bin \
    #        --env kvcm.service.rpc_port=56010 \
    #        --env kvcm.service.http_port=56020 \
    #        --env kvcm.service.admin_rpc_port=56030 \
    #        --env kvcm.service.admin_http_port=56040 \
    #        --env kvcm.service.enable_debug_service=false
    # 2. Run this script:
    python test_vineyard_report_event.py \
        --host localhost --http_port 56020 --admin_http_port 56040 \
        --instance_id event_report_cluster_0
"""

import argparse
import json
import sys
import time
import statistics
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import parse_qsl, urlsplit

import requests


BASE_URL = ""
ADMIN_URL = ""
INSTANCE_ID = "event_report_cluster_0"
SKIP_BENCH = False
ONLY_BENCH = False
# Requires small heartbeat_timeout_ms/cleanup_grace_ms in addStorage spec.
ENABLE_LIVENESS_TIMING_TESTS = False
HEARTBEAT_TIMEOUT_MS = 1000
CLEANUP_GRACE_MS = 2000


class KVCMClient:
    def __init__(self, base_url, admin_url=None):
        self.base_url = base_url
        self.admin_url = admin_url or base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def report_event(self, payload, check_ok=True):
        url = f"{self.base_url}/api/reportEvent"
        resp = self.session.post(url, json=payload)
        resp.raise_for_status()
        body = resp.json()
        if check_ok:
            code = body.get("header", {}).get("status", {}).get("code")
            assert code in ("OK", 1, "1", None), (
                f"ReportEvent failed: code={code}, body={json.dumps(body, ensure_ascii=False)}"
            )
        return body

    def register_instance(self, data):
        url = f"{self.base_url}/api/registerInstance"
        resp = self.session.post(url, json=data)
        resp.raise_for_status()
        body = resp.json()
        code = body.get("header", {}).get("status", {}).get("code")
        assert code == "OK", f"registerInstance failed: {json.dumps(body)}"
        return body

    def add_storage(self, data):
        url = f"{self.admin_url}/api/addStorage"
        resp = self.session.post(url, json=data)
        resp.raise_for_status()
        body = resp.json()
        code = body.get("header", {}).get("status", {}).get("code")
        if code not in ("OK", "DUPLICATE_ENTITY"):
            raise AssertionError(f"addStorage failed: {json.dumps(body)}")
        return body

    def create_instance_group(self, data):
        url = f"{self.admin_url}/api/createInstanceGroup"
        resp = self.session.post(url, json=data)
        resp.raise_for_status()
        body = resp.json()
        code = body.get("header", {}).get("status", {}).get("code")
        if code not in ("OK", "DUPLICATE_ENTITY"):
            raise AssertionError(f"createInstanceGroup failed: {json.dumps(body)}")
        return body

    def get_instance_group(self, data):
        url = f"{self.admin_url}/api/getInstanceGroup"
        resp = self.session.post(url, json=data)
        resp.raise_for_status()
        return resp.json()

    def get_cache_location(self, data):
        url = f"{self.base_url}/api/getCacheLocation"
        resp = self.session.post(url, json=data)
        resp.raise_for_status()
        return resp.json()

    def start_write_cache_with_min_replica(self, data, check_response=True):
        url = f"{self.base_url}/api/startWriteCache"
        resp = self.session.post(url, json=data)
        resp.raise_for_status()
        body = resp.json()
        if check_response:
            code = body.get("header", {}).get("status", {}).get("code")
            assert code == "OK", f"startWriteCache failed: {json.dumps(body)}"
        return body

    def finish_write_cache(self, data, check_response=True):
        url = f"{self.base_url}/api/finishWriteCache"
        resp = self.session.post(url, json=data)
        resp.raise_for_status()
        body = resp.json()
        if check_response:
            code = body.get("header", {}).get("status", {}).get("code")
            assert code == "OK", f"finishWriteCache failed: {json.dumps(body)}"
        return body

    def get_host_cache_state(self, data, check_response=True):
        url = f"{self.base_url}/api/getHostCacheState"
        resp = self.session.post(url, json=data)
        resp.raise_for_status()
        body = resp.json()
        if check_response:
            code = body.get("header", {}).get("status", {}).get("code")
            assert code == "OK", f"getHostCacheState failed: {json.dumps(body)}"
        return body

    def close(self):
        self.session.close()


# ---------------------------------------------------------------------------
# EventItem builders
# ---------------------------------------------------------------------------
def _ev_node_register(mediums):
    return {
        "event_type": "EVENT_NODE_REGISTER",
        "node_register": {"mediums": list(mediums)},
    }


def _ev_block_add(block_key, medium, specs):
    """Build a block_add event.

    Args:
        specs: list of {"name": ..., "uri": ...} dicts.
    """
    return {
        "event_type": "EVENT_BLOCK_ADD",
        "block_add": {
            "block_key": str(block_key),
            "medium": medium,
            "specs": specs,
        },
    }


def _make_single_spec(name, uri):
    """Convenience: build a one-element specs list."""
    return [{"name": name, "uri": uri}]


def _ev_block_delete(block_key, medium, spec_names):
    return {
        "event_type": "EVENT_BLOCK_DELETE",
        "block_delete": {
            "block_key": str(block_key),
            "medium": medium,
            "spec_names": list(spec_names),
        },
    }


def _ev_block_snapshot(blocks):
    """Build one all-medium authoritative snapshot."""
    snapshot_blocks = []
    for block in blocks:
        item = dict(block)
        item["block_key"] = str(item["block_key"])
        snapshot_blocks.append(item)
    return {
        "event_type": "EVENT_BLOCK_SNAPSHOT",
        "block_snapshot": {"blocks": snapshot_blocks},
    }


def _ev_host_down():
    return {"event_type": "EVENT_HOST_DOWN", "host_down": {}}


def _ev_heartbeat(system_status=None):
    return {
        "event_type": "EVENT_HEARTBEAT",
        "heartbeat": {"system_status": system_status or {}},
    }


def _make_request(instance_id, host_ip_port, events, trace_id="test", storage_type="ST_EVENT_REPORT_L2"):
    return {
        "trace_id": trace_id,
        "instance_id": instance_id,
        "host_ip_port": host_ip_port,
        "events": events,
        "storage_type": storage_type,
    }


def _build_event_report_uri(host_ip_port, medium, params=None):
    """Build event-report URI: event_report://{ip}:{port}/{medium}?k=v&..."""
    base = f"event_report://{host_ip_port}/{medium}"
    if not params:
        return base
    query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return f"{base}?{query}"


def _query_block_specs(client, instance_id, block_key, trace_id):
    """Return the flattened location specs visible for one block."""
    resp = client.get_cache_location({
        "trace_id": trace_id,
        "instance_id": instance_id,
        "query_type": "QT_BATCH_GET",
        "block_keys": [block_key],
        "block_mask": {"offset": 0},
    })
    code = resp.get("header", {}).get("status", {}).get("code")
    assert code in ("OK", 1, "1", None), (
        f"getCacheLocation failed: code={code}, body={json.dumps(resp, ensure_ascii=False)}"
    )
    return [
        spec
        for location in resp.get("locations", [])
        for spec in location.get("location_specs", [])
        if spec.get("uri")
    ]


def _assert_reporter_scope(
    test_case, actual_uri, reported_uri, instance_id, host, medium, version=None
):
    """Assert the reporter URI is preserved except for the opaque s_version."""
    actual = urlsplit(actual_uri)
    reported = urlsplit(reported_uri)
    test_case.assertEqual(actual.scheme, reported.scheme)
    test_case.assertEqual(actual.netloc, reported.netloc)
    test_case.assertEqual(actual.path, reported.path)
    actual_params = parse_qsl(actual.query, keep_blank_values=True)
    reported_params = parse_qsl(reported.query, keep_blank_values=True)
    visible_params = [(k, v) for k, v in actual_params if k != "s_version"]
    test_case.assertEqual(visible_params, reported_params)
    test_case.assertFalse(any(k.startswith("kvcm_") for k, _ in actual_params))
    versions = [v for k, v in actual_params if k == "s_version"]
    if version is None:
        test_case.assertEqual(len(versions), 1)
        test_case.assertEqual(len(versions[0]), 32)
        test_case.assertTrue(
            all(c in "0123456789abcdef" for c in versions[0])
        )
    else:
        test_case.assertEqual(versions, [version])


def _snapshot_version_from_uri(test_case, uri):
    values = [
        value
        for key, value in parse_qsl(
            urlsplit(uri).query, keep_blank_values=True
        )
        if key == "s_version"
    ]
    test_case.assertEqual(len(values), 1, "URI must contain one s_version")
    version = values[0]
    test_case.assertEqual(len(version), 32)
    test_case.assertTrue(all(c in "0123456789abcdef" for c in version))
    return version


def _wait_for_block_spec_names(client, instance_id, block_key, expected_names, trace_id, timeout_seconds=5):
    """Wait for async metadata cleanup/cache invalidation without fixed sleeps."""
    deadline = time.monotonic() + timeout_seconds
    last_specs = []
    while time.monotonic() < deadline:
        last_specs = _query_block_specs(client, instance_id, block_key, trace_id)
        if {spec.get("name") for spec in last_specs} == set(expected_names):
            return last_specs
        time.sleep(0.05)
    raise AssertionError(
        f"block {block_key}: expected specs={set(expected_names)}, actual={last_specs}"
    )


# ---------------------------------------------------------------------------
# Functional tests
# ---------------------------------------------------------------------------
class EventReportFunctionalTest(unittest.TestCase):
    HOST = "192.168.1.200:8080"
    EVENT_REPORT_STORAGE_NAME = "event_report_default"
    INSTANCE_GROUP_NAME = "event_report_test_group"

    @classmethod
    def setUpClass(cls):
        cls.client = KVCMClient(BASE_URL, ADMIN_URL)
        cls.instance_id = INSTANCE_ID
        cls._ensure_event_report_storage_registered()
        cls._ensure_instance_group_created()
        cls._ensure_instance_registered()
        # Register host so subsequent events have a NodeInfo entry.
        cls.client.report_event(
            _make_request(
                cls.instance_id,
                cls.HOST,
                [_ev_node_register(["mem", "disk"])],
                trace_id="setup_register_host",
            )
        )
        cls.client.report_event(
            _make_request(
                cls.instance_id,
                cls.HOST,
                [_ev_block_snapshot([])],
                "setup_initial_snapshot",
            )
        )

    @classmethod
    def _ensure_event_report_storage_registered(cls):
        try:
            cls.client.add_storage({
                "trace_id": "setup_storage",
                "storage": {
                    "global_unique_name": cls.EVENT_REPORT_STORAGE_NAME,
                    "storage_type": "ST_EVENT_REPORT_L2",
                    "event_report": {
                        "heartbeat_timeout_ms": (
                            HEARTBEAT_TIMEOUT_MS
                            if ENABLE_LIVENESS_TIMING_TESTS else 30000
                        ),
                        "cleanup_grace_ms": (
                            CLEANUP_GRACE_MS
                            if ENABLE_LIVENESS_TIMING_TESTS else 300000
                        ),
                        "liveness_check_interval_ms": (
                            100 if ENABLE_LIVENESS_TIMING_TESTS else 5000
                        ),
                    },
                    "check_storage_available_when_open": False,
                },
            })
            print(f"[SETUP] Event report storage '{cls.EVENT_REPORT_STORAGE_NAME}' registered")
        except Exception as e:
            print(f"[WARN] addStorage failed (may already exist): {e}")

    @classmethod
    def _ensure_instance_group_created(cls):
        existing = cls.client.get_instance_group({
            "trace_id": "setup_get_ig",
            "name": cls.INSTANCE_GROUP_NAME,
        })
        code = existing.get("header", {}).get("status", {}).get("code")
        if code == "OK":
            candidates = existing.get("instance_group", {}).get(
                "event_report_storage_candidates", []
            )
            if cls.EVENT_REPORT_STORAGE_NAME not in candidates:
                raise AssertionError(
                    f"InstanceGroup {cls.INSTANCE_GROUP_NAME!r} does not use "
                    f"EventReport storage {cls.EVENT_REPORT_STORAGE_NAME!r}"
                )
            print(f"[SETUP] InstanceGroup '{cls.INSTANCE_GROUP_NAME}' reused")
            return

        cls.client.create_instance_group({
            "trace_id": "setup_ig",
            "instance_group": {
                "name": cls.INSTANCE_GROUP_NAME,
                "storage_candidates": ["nfs_01"],
                "global_quota_group_name": "default_quota_group",
                "max_instance_count": 100,
                "quota": {
                    "capacity": 10737418240,
                    "quota_config": [{"storage_type": 4, "capacity": 10737418240}],
                },
                "cache_config": {
                    "reclaim_strategy": {
                        "storage_unique_name": "nfs_01",
                        "reclaim_policy": 1,
                        "trigger_strategy": {"used_size": 1073741824, "used_percentage": 0.8},
                        "trigger_period_seconds": 60,
                        "reclaim_step_size": 1073741824,
                        "reclaim_step_percentage": 10,
                    },
                    "data_storage_strategy": 2,
                    "meta_indexer_config": {
                        "max_key_count": 1000000,
                        "mutex_shard_num": 16,
                        "batch_key_size": 16,
                        "meta_storage_backend_config": {"storage_type": "local", "storage_uri": ""},
                        "meta_cache_policy_config": {"type": "LRU", "capacity": 10000},
                    },
                },
                "event_report_storage_candidates": [cls.EVENT_REPORT_STORAGE_NAME],
                "version": 1,
            },
        })
        print(f"[SETUP] InstanceGroup '{cls.INSTANCE_GROUP_NAME}' created")

    @classmethod
    def _ensure_instance_registered(cls):
        try:
            cls.client.register_instance({
                "trace_id": "setup",
                "instance_group": cls.INSTANCE_GROUP_NAME,
                "instance_id": cls.instance_id,
                "block_size": 128,
                "model_deployment": {
                    "model_name": "test_er_model",
                    "dtype": "FP8",
                    "use_mla": False,
                    "tp_size": 1,
                    "dp_size": 1,
                    "pp_size": 1,
                },
                "location_spec_infos": [
                    {"name": "tp0", "size": 1024},
                ],
            })
        except Exception as e:
            print(f"[WARN] register_instance failed (may already exist): {e}")

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    # 1. NODE_REGISTER (with mediums)
    def test_01_node_register(self):
        body = self.client.report_event(
            _make_request(
                self.instance_id, self.HOST,
                [_ev_node_register(["mem", "disk", "ssd"])],
                trace_id="t01",
            )
        )
        self.assertIn("header", body)

    # 2. NODE_REGISTER is idempotent and merges mediums
    def test_02_node_register_idempotent(self):
        host = "192.168.1.201:8080"
        self.client.report_event(
            _make_request(self.instance_id, host, [_ev_node_register(["mem"])], trace_id="t02a")
        )
        body = self.client.report_event(
            _make_request(self.instance_id, host, [_ev_node_register(["mem", "disk"])], trace_id="t02b")
        )
        self.assertIn("header", body)

    # 3. BLOCK_ADD with single spec
    def test_03_block_add(self):
        uri = _build_event_report_uri(self.HOST, "mem", {"gpu": "A100"})
        body = self.client.report_event(
            _make_request(
                self.instance_id, self.HOST,
                [_ev_block_add(9001, "mem", _make_single_spec("default", uri))],
                trace_id="t03",
            )
        )
        self.assertIn("header", body)

    # 4. BLOCK_ADD then query: spec name/uri should match what was sent
    def test_04_block_add_then_query(self):
        block_key = 9002
        uri = _build_event_report_uri(self.HOST, "mem", {"flavor": "test_query"})
        spec_name = "tp0"
        self.client.report_event(
            _make_request(
                self.instance_id, self.HOST,
                [_ev_block_add(block_key, "mem", _make_single_spec(spec_name, uri))],
                trace_id="t04",
            )
        )

        resp = self.client.get_cache_location({
            "trace_id": "t04_query",
            "instance_id": self.instance_id,
            "query_type": "QT_BATCH_GET",
            "block_keys": [block_key],
            "block_mask": {"offset": 0},
        })

        locations = resp.get("locations", [])
        self.assertGreater(len(locations), 0, "Expected at least one location after BLOCK_ADD")
        specs = locations[0].get("location_specs", [])
        self.assertGreater(len(specs), 0)
        _assert_reporter_scope(
            self,
            specs[0]["uri"],
            uri,
            self.instance_id,
            self.HOST,
            "mem",
        )
        self.assertEqual(specs[0]["name"], spec_name,
                         f"spec.name should be {spec_name}")

    # 5. Two mediums on same host: each becomes its own location_id
    def test_05_block_add_multi_medium(self):
        block_key = 9020
        host = "192.168.1.220:8080"
        # NODE_REGISTER first so the host is known.
        self.client.report_event(
            _make_request(self.instance_id, host, [_ev_node_register(["mem", "disk"])], trace_id="t05a")
        )
        self.client.report_event(
            _make_request(
                self.instance_id, host,
                [_ev_block_snapshot([])],
                trace_id="t05_initial_snapshot",
            )
        )
        uri_mem = _build_event_report_uri(host, "mem")
        uri_disk = _build_event_report_uri(host, "disk")
        body = self.client.report_event(
            _make_request(
                self.instance_id, host,
                [
                    _ev_block_add(block_key, "mem", _make_single_spec("mem_spec", uri_mem)),
                    _ev_block_add(block_key, "disk", _make_single_spec("disk_spec", uri_disk)),
                ],
                trace_id="t05b",
            )
        )
        self.assertIn("header", body)

        resp = self.client.get_cache_location({
            "trace_id": "t05_query",
            "instance_id": self.instance_id,
            "query_type": "QT_BATCH_GET",
            "block_keys": [block_key],
            "block_mask": {"offset": 0},
        })
        code = resp.get("header", {}).get("status", {}).get("code")
        self.assertEqual(code, "OK", f"getCacheLocation failed: {json.dumps(resp, ensure_ascii=False)}")

        locations = resp.get("locations", [])
        self.assertEqual(len(locations), 1, "QT_BATCH_GET with one key should return one row")
        specs = locations[0].get("location_specs", [])
        by_name = {s["name"]: s for s in specs if s.get("name")}
        self.assertIn("mem_spec", by_name, f"Expected mem_spec, specs={list(by_name)}")
        self.assertIn("disk_spec", by_name, f"Expected disk_spec, specs={list(by_name)}")
        _assert_reporter_scope(
            self, by_name["mem_spec"]["uri"], uri_mem, self.instance_id, host, "mem"
        )
        _assert_reporter_scope(
            self, by_name["disk_spec"]["uri"], uri_disk, self.instance_id, host, "disk"
        )

    # 5b. BLOCK_ADD with multiple specs in one CacheLocation
    def test_05b_block_add_multi_spec(self):
        block_key = 9025
        host = "192.168.1.221:8080"
        self.client.report_event(
            _make_request(self.instance_id, host, [_ev_node_register(["mem"])], trace_id="t05b_reg")
        )
        self.client.report_event(
            _make_request(
                self.instance_id, host,
                [_ev_block_snapshot([])],
                trace_id="t05b_initial_snapshot",
            )
        )
        uri_spec0 = _build_event_report_uri(host, "mem", {"obj_id": "o1", "size": "512"})
        uri_spec1 = _build_event_report_uri(host, "mem", {"obj_id": "o2", "size": "512"})
        body = self.client.report_event(
            _make_request(
                self.instance_id, host,
                [_ev_block_add(block_key, "mem", [
                    {"name": "spec_4096", "uri": uri_spec0},
                    {"name": "spec_8192", "uri": uri_spec1},
                ])],
                trace_id="t05b_add",
            )
        )
        self.assertIn("header", body)

        resp = self.client.get_cache_location({
            "trace_id": "t05b_query",
            "instance_id": self.instance_id,
            "query_type": "QT_BATCH_GET",
            "block_keys": [block_key],
            "block_mask": {"offset": 0},
        })
        code = resp.get("header", {}).get("status", {}).get("code")
        self.assertEqual(code, "OK", f"getCacheLocation failed: {json.dumps(resp, ensure_ascii=False)}")

        locations = resp.get("locations", [])
        self.assertEqual(len(locations), 1)
        specs = locations[0].get("location_specs", [])
        by_name = {s["name"]: s for s in specs}
        self.assertIn("spec_4096", by_name, f"Expected spec_4096 in specs={list(by_name)}")
        self.assertIn("spec_8192", by_name, f"Expected spec_8192 in specs={list(by_name)}")
        _assert_reporter_scope(
            self, by_name["spec_4096"]["uri"], uri_spec0, self.instance_id, host, "mem"
        )
        _assert_reporter_scope(
            self, by_name["spec_8192"]["uri"], uri_spec1, self.instance_id, host, "mem"
        )

    # 6. BLOCK_DELETE removes the specific (block_key, medium) entry
    def test_06_block_delete(self):
        block_key = 9003
        uri = _build_event_report_uri(self.HOST, "mem")
        self.client.report_event(
            _make_request(
                self.instance_id, self.HOST,
                [_ev_block_add(block_key, "mem", _make_single_spec("spec_4096", uri))],
                trace_id="t06a",
            )
        )
        body = self.client.report_event(
            _make_request(
                self.instance_id, self.HOST,
                [_ev_block_delete(block_key, "mem", ["spec_4096"])],
                trace_id="t06b",
            )
        )
        self.assertIn("header", body)
        # Query-after-delete skipped: MetaSearchCache may still serve stale entry.

    # 7. BLOCK_DELETE on missing key/medium is a no-op (idempotent)
    def test_07_block_delete_nonexistent(self):
        body = self.client.report_event(
            _make_request(
                self.instance_id, self.HOST,
                [_ev_block_delete(99999, "mem", ["spec_4096"])],
                trace_id="t07",
            ),
            check_ok=False,
        )
        self.assertIn("header", body)

    # 8. HOST_DOWN cleans up all mediums under the host
    def test_08_host_down(self):
        down_host = "192.168.1.202:8080"
        block_keys = [9010, 9011, 9012]
        # Register + add mem/disk replicas in a single batch.
        events = [_ev_node_register(["mem", "disk"])]
        for bk in block_keys:
            events.append(
                _ev_block_add(
                    bk,
                    "mem",
                    _make_single_spec(
                        "spec_4096",
                        _build_event_report_uri(
                            down_host,
                            "mem"))))
            events.append(
                _ev_block_add(
                    bk,
                    "disk",
                    _make_single_spec(
                        "spec_4096",
                        _build_event_report_uri(
                            down_host,
                            "disk"))))
        self.client.report_event(
            _make_request(self.instance_id, down_host, events[:1], trace_id="t08_register")
        )
        self.client.report_event(
            _make_request(
                self.instance_id, down_host,
                [_ev_block_snapshot([])],
                trace_id="t08_initial_snapshot",
            )
        )
        self.client.report_event(
            _make_request(self.instance_id, down_host, events[1:], trace_id="t08a")
        )

        body = self.client.report_event(
            _make_request(self.instance_id, down_host, [_ev_host_down()], trace_id="t08b")
        )
        self.assertIn("header", body)

    # 9. HOST_DOWN is idempotent
    def test_09_host_down_idempotent(self):
        down_host = "192.168.1.203:8080"
        self.client.report_event(
            _make_request(self.instance_id, down_host, [_ev_node_register(["mem"])], trace_id="t09a")
        )
        body1 = self.client.report_event(
            _make_request(self.instance_id, down_host, [_ev_host_down()], trace_id="t09b")
        )
        body2 = self.client.report_event(
            _make_request(self.instance_id, down_host, [_ev_host_down()], trace_id="t09c")
        )
        self.assertIn("header", body1)
        self.assertIn("header", body2)

    # 10. HEARTBEAT extends liveness; payload is opaque
    def test_10_heartbeat(self):
        body = self.client.report_event(
            _make_request(
                self.instance_id, self.HOST,
                [_ev_heartbeat({"version": "er-0.18", "cpu": "45%"})],
                trace_id="t10",
            )
        )
        self.assertIn("header", body)

    # 11. Mixed batch: register + add + heartbeat in a single RPC
    def test_11_mixed_batch(self):
        host = "192.168.1.230:8080"
        block_key = 9030
        events = [
            _ev_node_register(["mem"]),
            _ev_block_add(block_key, "mem", _make_single_spec("spec_4096", _build_event_report_uri(host, "mem"))),
            _ev_heartbeat({"phase": "boot"}),
        ]
        self.client.report_event(
            _make_request(self.instance_id, host, events[:1], trace_id="t11_register")
        )
        self.client.report_event(
            _make_request(
                self.instance_id, host,
                [_ev_block_snapshot([])],
                trace_id="t11_initial_snapshot",
            )
        )
        body = self.client.report_event(
            _make_request(self.instance_id, host, events[1:], trace_id="t11")
        )
        self.assertIn("header", body)

    # 12. Empty events array: should be a no-op success
    def test_12_empty_batch(self):
        body = self.client.report_event(
            _make_request(self.instance_id, self.HOST, [], trace_id="t12")
        )
        self.assertIn("header", body)

    # 13. Missing block_add params: server must surface a per-item failure
    def test_13_block_add_missing_params(self):
        body = self.client.report_event(
            _make_request(
                self.instance_id, self.HOST,
                [{"event_type": "EVENT_BLOCK_ADD"}],
                trace_id="t13",
            ),
            check_ok=False,
        )
        self.assertIn("header", body)
        self.assertIn("item_results", body)

    # 14. Empty top-level host_ip_port: request-level validation must reject
    def test_14_missing_host_ip_port(self):
        body = self.client.report_event(
            {
                "trace_id": "t14",
                "instance_id": self.instance_id,
                "host_ip_port": "",
                "events": [_ev_node_register(["mem"])],
                "storage_type": "ST_EVENT_REPORT_L2",
            },
            check_ok=False,
        )
        self.assertIn("header", body)

    # 15. StartWriteCacheWithMinReplica: event report eviction with min_replica_count=2
    def test_15_start_write_cache_with_min_replica(self):
        block_key = 8_000_000_000 + time.time_ns() % 1_000_000_000
        uri = _build_event_report_uri(self.HOST, "mem")

        # Step 1: 1 EVENT_REPORT replica only.
        self.client.report_event(
            _make_request(
                self.instance_id, self.HOST,
                [_ev_block_add(block_key, "mem", _make_single_spec("spec_4096", uri))],
                trace_id="t15_add",
            )
        )

        # Step 2: ask for evict; with only 1 replica we expect a remote write.
        resp = self.client.start_write_cache_with_min_replica({
            "trace_id": "t15_evict_1",
            "instance_id": self.instance_id,
            "block_keys": [block_key],
            "write_timeout_seconds": 30,
            "min_replica_count": 2,
        })
        self.assertIn("write_session_id", resp)
        write_session_id = resp["write_session_id"]
        self.assertTrue(write_session_id, "Expected non-empty write_session_id")
        locations = resp.get("locations", [])
        self.assertGreater(len(locations), 0,
                           "Expected remote write locations since only 1 replica exists")

        # Step 3: Finish the write to bring up replica count to 2.
        self.client.finish_write_cache({
            "trace_id": "t15_evict_finish",
            "instance_id": self.instance_id,
            "write_session_id": write_session_id,
            "success_blocks": {"bool_masks": {"values": [True]}},
        })

        # Step 4: Now n_total=2; evict should skip remote allocation.
        resp2 = self.client.start_write_cache_with_min_replica({
            "trace_id": "t15_evict_2",
            "instance_id": self.instance_id,
            "block_keys": [block_key],
            "write_timeout_seconds": 30,
            "min_replica_count": 2,
        })
        locations2 = resp2.get("locations", [])
        self.assertEqual(len(locations2), 0,
                         "Expected no write locations since 2 replicas already exist")

    # 16a. Heartbeat timeout -> location filtered out, then recovery on heartbeat resume.
    def test_16a_heartbeat_timeout_then_recovery(self):
        if not ENABLE_LIVENESS_TIMING_TESTS:
            self.skipTest("--enable-liveness-timing-tests not set; skipping")

        host = "192.168.1.250:8080"
        block_key = 9100
        # Step 1: register, then establish the reporter's initial snapshot.
        self.client.report_event(
            _make_request(
                self.instance_id, host, [_ev_node_register(["mem"])],
                trace_id="t16a_register",
            )
        )
        self.client.report_event(
            _make_request(self.instance_id, host, [_ev_block_snapshot([{
                "block_key": block_key,
                "medium": "mem",
                "specs": _make_single_spec(
                    "spec_4096", _build_event_report_uri(host, "mem")
                ),
            }])], trace_id="t16a_setup")
        )
        # Confirm the replica is queryable.
        resp = self.client.get_cache_location({
            "trace_id": "t16a_q1",
            "instance_id": self.instance_id,
            "query_type": "QT_BATCH_GET",
            "block_keys": [block_key],
            "block_mask": {"offset": 0},
        })
        self.assertGreater(len(resp.get("locations", [])), 0)

        # Step 2: skip heartbeat past heartbeat_timeout_ms (within cleanup_grace_ms).
        time.sleep((HEARTBEAT_TIMEOUT_MS + 500) / 1000.0)
        resp_after_timeout = self.client.get_cache_location({
            "trace_id": "t16a_q2",
            "instance_id": self.instance_id,
            "query_type": "QT_BATCH_GET",
            "block_keys": [block_key],
            "block_mask": {"offset": 0},
        })
        # MetaSearchCache may still serve stale entry; not a hard assertion.

        # Step 3: heartbeat resumes within grace -> node recovers.
        self.client.report_event(
            _make_request(self.instance_id, host, [_ev_heartbeat({})], trace_id="t16a_hb")
        )
        resp_recovered = self.client.get_cache_location({
            "trace_id": "t16a_q3",
            "instance_id": self.instance_id,
            "query_type": "QT_BATCH_GET",
            "block_keys": [block_key],
            "block_mask": {"offset": 0},
        })
        self.assertGreater(len(resp_recovered.get("locations", [])), 0,
                           "Replica must be queryable again after heartbeat resumed within grace")

    # 16b. Heartbeat timeout exceeds cleanup_grace_ms -> CleanupHostLocations triggered.
    def test_16b_heartbeat_exceeds_grace_triggers_cleanup(self):
        if not ENABLE_LIVENESS_TIMING_TESTS:
            self.skipTest("--enable-liveness-timing-tests not set; skipping")

        host = "192.168.1.251:8080"
        block_key = 9101
        self.client.report_event(
            _make_request(
                self.instance_id, host, [_ev_node_register(["mem"])],
                trace_id="t16b_register",
            )
        )
        self.client.report_event(
            _make_request(self.instance_id, host, [_ev_block_snapshot([{
                "block_key": block_key,
                "medium": "mem",
                "specs": _make_single_spec(
                    "spec_4096", _build_event_report_uri(host, "mem")
                ),
            }])], trace_id="t16b_setup")
        )

        # Wait past hb_timeout + cleanup_grace + scheduler slack.
        wait_ms = HEARTBEAT_TIMEOUT_MS + CLEANUP_GRACE_MS + 1500
        time.sleep(wait_ms / 1000.0)

        # Soft check: MetaSearchCache TTL may delay eviction.
        resp = self.client.get_cache_location({
            "trace_id": "t16b_q",
            "instance_id": self.instance_id,
            "query_type": "QT_BATCH_GET",
            "block_keys": [block_key],
            "block_mask": {"offset": 0},
        })
        # Every spec.uri returned must NOT belong to the cleaned-up host.
        for loc in resp.get("locations", []):
            for spec in loc.get("location_specs", []):
                self.assertNotIn(host, spec.get("uri", ""),
                                 f"Cleanup should have removed host [{host}] from results")

    # 16. GetHostCacheState — per-host prefix match verification
    def test_16_get_host_cache_state(self):
        instance_id = f"{self.instance_id}_host_cache_state"
        self.client.register_instance({
            "trace_id": "t16_register",
            "instance_group": self.INSTANCE_GROUP_NAME,
            "instance_id": instance_id,
            "block_size": 128,
            "query_type": "QT_PREFIX_MATCH",
            "model_deployment": {
                "model_name": "test_er_model",
                "dtype": "FP8",
                "use_mla": False,
                "tp_size": 1,
                "dp_size": 1,
                "pp_size": 1,
            },
            "location_spec_infos": [
                {"name": "tp0", "size": 1024},
            ],
        })

        # Data layout (3 hosts, different key subsets):
        #   key 10000: host_A, host_B, host_C
        #   key 10001: host_A, host_B
        #   key 10002: host_B only
        #   key 10003: host_A, host_B
        #   key 10004: no host
        #
        # Query keys = [10000, 10001, 10002, 10003, 10004]
        #   host_A: 10000, 10001 (miss 10002) -> prefix=2
        #   host_B: 10000, 10001, 10002, 10003 (miss 10004) -> prefix=4
        #   host_C: 10000 (miss 10001) -> prefix=1
        hosts = [
            ("10.0.0.1:8080", [10000, 10001, 10003]),
            ("10.0.0.2:8080", [10000, 10001, 10002, 10003]),
            ("10.0.0.3:8080", [10000]),
        ]

        # 1. Each host: NODE_REGISTER + BLOCK_ADD
        for host, keys in hosts:
            events = [_ev_node_register(["mem"])]
            for key in keys:
                uri = _build_event_report_uri(host, "mem")
                events.append(
                    _ev_block_add(key, "mem", _make_single_spec("tp0", uri))
                )
            self.client.report_event(
                _make_request(self.instance_id, host, events[:1], trace_id="t16_register")
            )
            self.client.report_event(
                _make_request(
                    instance_id, host,
                    [_ev_block_snapshot([])],
                    trace_id="t16_initial_snapshot",
                )
            )
            self.client.report_event(
                _make_request(instance_id, host, events[1:], trace_id="t16_setup_delta")
            )

        # 2. Query GetHostCacheState
        resp = self.client.get_host_cache_state({
            "trace_id": "t16_query",
            "instance_id": instance_id,
            "query_type": "QT_PREFIX_MATCH",
            "block_cache_keys": [10000, 10001, 10002, 10003, 10004],
        })

        # 3. Verify prefix_match_blocks per host
        expected = {
            "10.0.0.1:8080": 2,
            "10.0.0.2:8080": 4,
            "10.0.0.3:8080": 1,
        }
        actual = {
            h["host_ip_port"]: int(h["prefix_match_blocks"])
            for h in resp.get("hosts", [])
        }
        for host, prefix in expected.items():
            self.assertIn(host, actual, f"host {host} not found in response")
            self.assertEqual(
                actual[host], prefix,
                f"host {host}: expected prefix={prefix}, got {actual[host]}",
            )

    # 17. Snapshot is a complete reconciliation barrier and supports empty clear.
    def test_17_snapshot_reconciliation_contract(self):
        host = "192.168.1.240:8080"
        register = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_node_register(["mem", "disk"])],
                trace_id="t17_register",
            )
        )
        self.assertTrue(register.get("snapshot_required"))

        mem_uri = _build_event_report_uri(host, "mem", {"user": "mem"})
        disk_uri = _build_event_report_uri(host, "disk", {"user": "disk"})
        blocks = [
            {
                "block_key": "9170",
                "medium": "mem",
                "specs": _make_single_spec("linear_0", mem_uri),
            },
            {
                "block_key": "9171",
                "medium": "disk",
                "specs": _make_single_spec("full_3", disk_uri),
            },
        ]
        response = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [{
                    "event_type": "EVENT_BLOCK_SNAPSHOT",
                    "block_snapshot": {"blocks": blocks},
                }],
                trace_id="t17_multi_medium_snapshot",
            )
        )
        self.assertFalse(response.get("snapshot_required"))
        version = response.get("committed_snapshot_version", "")
        self.assertEqual(len(version), 32)
        self.assertTrue(all(c in "0123456789abcdef" for c in version))

        for block_key, spec_name, reported_uri in (
            ("9170", "linear_0", mem_uri),
            ("9171", "full_3", disk_uri),
        ):
            specs = _wait_for_block_spec_names(
                self.client,
                self.instance_id,
                block_key,
                {spec_name},
                "t17_query_" + block_key,
            )
            self.assertEqual(len(specs), 1)
            actual_uri = specs[0].get("uri", "")
            self.assertEqual(_snapshot_version_from_uri(self, actual_uri), version)
            _assert_reporter_scope(
                self, actual_uri, reported_uri,
                self.instance_id, host, "", version,
            )

        delta_uri = _build_event_report_uri(host, "mem", {"user": "delta"})
        self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_block_add(
                    "9172",
                    "mem",
                    _make_single_spec("linear_0", delta_uri),
                )],
                trace_id="t17_delta_after_snapshot",
            )
        )
        delta_specs = _wait_for_block_spec_names(
            self.client,
            self.instance_id,
            "9172",
            {"linear_0"},
            "t17_delta_query",
        )
        self.assertEqual(
            _snapshot_version_from_uri(self, delta_specs[0]["uri"]),
            version,
        )

    # 18. Snapshot state is isolated per reporter; medium is per-block.
    def test_18_snapshot_reporter_isolation(self):
        host_a = "192.168.1.241:8080"
        host_b = "192.168.1.242:8080"
        versions = {}
        for host in (host_a, host_b):
            self.client.report_event(
                _make_request(
                    self.instance_id,
                    host,
                    [_ev_node_register(["mem", "disk"])],
                    trace_id="t18_register_" + host,
                )
            )
            response = self.client.report_event(
                _make_request(
                    self.instance_id,
                    host,
                    [_ev_block_snapshot([])],
                    trace_id="t18_snapshot_" + host,
                )
            )
            versions[host] = response["committed_snapshot_version"]
            self.assertFalse(response.get("snapshot_required"))

        self.assertNotEqual(versions[host_a], versions[host_b])
        limited = self.client.report_event(
            _make_request(
                self.instance_id,
                host_a,
                [_ev_block_snapshot([])],
                trace_id="t18_rate_limited",
            ),
            check_ok=False,
        )
        self.assertEqual(
            limited["header"]["status"]["code"],
            "SNAPSHOT_RATE_LIMITED",
        )
        self.assertGreater(int(limited.get("retry_after_ms", 0)), 0)
        self.assertEqual(
            limited.get("committed_snapshot_version"),
            versions[host_a],
        )

        uri_b = _build_event_report_uri(host_b, "disk")
        self.client.report_event(
            _make_request(
                self.instance_id,
                host_b,
                [_ev_block_add(
                    "9180",
                    "disk",
                    _make_single_spec("full_3", uri_b),
                )],
                trace_id="t18_host_b_delta",
            )
        )
        specs = _wait_for_block_spec_names(
            self.client,
            self.instance_id,
            "9180",
            {"full_3"},
            "t18_host_b_query",
        )
        self.assertEqual(
            _snapshot_version_from_uri(self, specs[0]["uri"]),
            versions[host_b],
        )

    # 19. Invalid complete sets fail before publishing any snapshot.
    def test_19_snapshot_validation_has_no_side_effects(self):
        host = "192.168.1.243:8080"
        register = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_node_register(["mem"])],
                trace_id="t19_register",
            )
        )
        self.assertTrue(register.get("snapshot_required"))

        delta = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_block_add(
                    "9190",
                    "mem",
                    _make_single_spec(
                        "linear_0", _build_event_report_uri(host, "mem")
                    ),
                )],
                trace_id="t19_delta_before_snapshot",
            ),
            check_ok=False,
        )
        self.assertEqual(
            delta["header"]["status"]["code"],
            "SNAPSHOT_REQUIRED",
        )

        duplicate = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [{
                    "event_type": "EVENT_BLOCK_SNAPSHOT",
                    "block_snapshot": {"blocks": [
                        {"block_key": "9191", "medium": "mem", "specs": []},
                        {"block_key": "9191", "medium": "disk", "specs": []},
                    ]},
                }],
                trace_id="t19_duplicate_blocks",
            ),
            check_ok=False,
        )
        self.assertEqual(
            duplicate["header"]["status"]["code"],
            "INVALID_ARGUMENT",
        )
        self.assertEqual(duplicate.get("committed_snapshot_version", ""), "")
        self.assertTrue(duplicate.get("snapshot_required"))

        committed = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_block_snapshot([])],
                trace_id="t19_empty_snapshot",
            )
        )
        self.assertEqual(len(committed["committed_snapshot_version"]), 32)
        self.assertFalse(committed.get("snapshot_required"))

        bad_host = "192.168.1.244:8080"
        self.client.report_event(
            _make_request(
                self.instance_id,
                bad_host,
                [_ev_node_register(["mem"])],
                trace_id="t19_bad_uri_register",
            )
        )
        bad_uri = _build_event_report_uri(
            bad_host, "mem", {"s_version": "client_value"}
        )
        malformed = self.client.report_event(
            _make_request(
                self.instance_id,
                bad_host,
                [_ev_block_snapshot([{
                    "block_key": "9192",
                    "medium": "mem",
                    "specs": _make_single_spec("linear_0", bad_uri),
                }])],
                trace_id="t19_reserved_s_version",
            ),
            check_ok=False,
        )
        self.assertEqual(
            malformed["header"]["status"]["code"],
            "INVALID_ARGUMENT",
        )
        self.assertEqual(malformed.get("committed_snapshot_version", ""), "")
        self.assertTrue(malformed.get("snapshot_required"))

    def test_20_host_down_then_reregister_requires_new_snapshot(self):
        host = "192.168.1.244:8080"
        register = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_node_register(["mem", "disk"])],
                trace_id="t20_register",
            )
        )
        self.assertTrue(register.get("snapshot_required"))

        first = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_block_snapshot([])],
                trace_id="t20_first_snapshot",
            )
        )
        first_token = first["committed_snapshot_version"]
        self.assertEqual(len(first_token), 32)

        self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_host_down()],
                trace_id="t20_host_down",
            )
        )

        reregister = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_node_register(["mem", "disk"])],
                trace_id="t20_reregister",
            )
        )
        self.assertTrue(reregister.get("snapshot_required"))
        self.assertEqual(reregister.get("committed_snapshot_version", ""), "")

        rejected_delta = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_block_add(
                    "9200",
                    "mem",
                    _make_single_spec(
                        "linear_0", _build_event_report_uri(host, "mem")
                    ),
                )],
                trace_id="t20_delta_before_resnapshot",
            ),
            check_ok=False,
        )
        self.assertEqual(
            rejected_delta["header"]["status"]["code"],
            "SNAPSHOT_REQUIRED",
        )

        second = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_block_snapshot([])],
                trace_id="t20_second_snapshot",
            )
        )
        second_token = second["committed_snapshot_version"]
        self.assertEqual(len(second_token), 32)
        self.assertNotEqual(first_token, second_token)

        accepted_delta = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_block_add(
                    "9200",
                    "mem",
                    _make_single_spec(
                        "linear_0", _build_event_report_uri(host, "mem")
                    ),
                )],
                trace_id="t20_delta_after_resnapshot",
            )
        )
        self.assertEqual(
            accepted_delta.get("committed_snapshot_version"), second_token
        )

    def test_21_snapshot_request_shape_is_rejected_before_write_gate(self):
        host = "192.168.1.245:8080"
        self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_node_register(["mem"])],
                trace_id="t21_register",
            )
        )
        snapshot = _ev_block_snapshot([{
            "block_key": "9210",
            "medium": "mem",
            "specs": _make_single_spec(
                "linear_0", _build_event_report_uri(host, "mem")
            ),
        }])
        delta = _ev_block_add(
            "9211",
            "mem",
            _make_single_spec(
                "linear_0", _build_event_report_uri(host, "mem")
            ),
        )

        mixed = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [snapshot, delta],
                trace_id="t21_mixed_snapshot_delta",
            ),
            check_ok=False,
        )
        self.assertEqual(
            mixed["header"]["status"]["code"], "INVALID_ARGUMENT"
        )

        duplicate = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [_ev_block_snapshot([]), _ev_block_snapshot([])],
                trace_id="t21_two_snapshots",
            ),
            check_ok=False,
        )
        self.assertEqual(
            duplicate["header"]["status"]["code"], "INVALID_ARGUMENT"
        )

        still_requires_snapshot = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [delta],
                trace_id="t21_delta_after_invalid_requests",
            ),
            check_ok=False,
        )
        self.assertEqual(
            still_requires_snapshot["header"]["status"]["code"],
            "SNAPSHOT_REQUIRED",
        )

        committed = self.client.report_event(
            _make_request(
                self.instance_id,
                host,
                [snapshot],
                trace_id="t21_valid_snapshot",
            )
        )
        self.assertEqual(len(committed["committed_snapshot_version"]), 32)

# ---------------------------------------------------------------------------
# Bench tests
# ---------------------------------------------------------------------------


class EventReportBenchTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = KVCMClient(BASE_URL, ADMIN_URL)
        cls.instance_id = INSTANCE_ID

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    @staticmethod
    def _percentile(data, p):
        if not data:
            return 0
        k = (len(data) - 1) * (p / 100.0)
        f = int(k)
        c = f + 1
        if c >= len(data):
            return data[-1]
        return data[f] + (k - f) * (data[c] - data[f])

    @staticmethod
    def _ensure_host_registered(client, instance_id, host):
        # Register and establish the initial snapshot so deltas are accepted.
        client.report_event(
            _make_request(instance_id, host,
                          [_ev_node_register(["mem"])],
                          trace_id="bench_setup")
        )
        client.report_event(
            _make_request(instance_id, host, [_ev_block_snapshot([])],
                          trace_id="bench_initial_snapshot")
        )

    @staticmethod
    def _raise_for_report_error(resp):
        resp.raise_for_status()
        body = resp.json()
        code = body.get("header", {}).get("status", {}).get("code")
        if code not in ("OK", 1, "1", None):
            raise AssertionError(
                f"ReportEvent failed: code={code}, body={json.dumps(body)}"
            )

    # 17. BLOCK_ADD throughput (one item per request)
    def test_17_block_add_throughput(self):
        num_threads = 100
        ops_per_thread = 100
        total_ops = num_threads * ops_per_thread
        latencies = []
        errors = []
        host = "192.168.1.210:8080"
        self._ensure_host_registered(self.client, self.instance_id, host)

        def worker(thread_id):
            local_latencies = []
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
            for i in range(ops_per_thread):
                block_key = thread_id * ops_per_thread + i + 100000
                payload = _make_request(
                    self.instance_id, host, [
                        _ev_block_add(
                            block_key, "mem", _make_single_spec(
                                "spec_4096", _build_event_report_uri(
                                    host, "mem")))], trace_id=f"bench_add_{thread_id}_{i}", )
                t0 = time.monotonic()
                try:
                    resp = session.post(f"{BASE_URL}/api/reportEvent", json=payload)
                    self._raise_for_report_error(resp)
                except Exception as e:
                    errors.append(str(e))
                    continue
                t1 = time.monotonic()
                local_latencies.append((t1 - t0) * 1000)
            session.close()
            return local_latencies

        t_start = time.monotonic()
        with ThreadPoolExecutor(max_workers=num_threads) as pool:
            futures = [pool.submit(worker, tid) for tid in range(num_threads)]
            for f in as_completed(futures):
                latencies.extend(f.result())
        t_end = time.monotonic()

        elapsed = t_end - t_start
        qps = len(latencies) / elapsed if elapsed > 0 else 0
        latencies.sort()
        p50 = self._percentile(latencies, 50)
        p99 = self._percentile(latencies, 99)

        print(f"\n[BENCH] BLOCK_ADD throughput:")
        print(f"  Total ops:   {len(latencies)} / {total_ops} (errors: {len(errors)})")
        print(f"  Elapsed:     {elapsed:.2f}s")
        print(f"  QPS:         {qps:.1f}")
        print(f"  Latency p50: {p50:.2f}ms")
        print(f"  Latency p99: {p99:.2f}ms")
        if latencies:
            print(f"  Latency avg: {statistics.mean(latencies):.2f}ms")
        self.assertEqual(len(errors), 0, f"Bench had errors: {errors[:5]}")

    # 18. Mixed ADD/DELETE/HEARTBEAT batch throughput.
    def test_18_block_add_delete_mixed(self):
        num_threads = 50
        ops_per_thread = 100
        total_batches = num_threads * ops_per_thread
        latencies = []
        errors = []
        host = "192.168.1.211:8080"
        self._ensure_host_registered(self.client, self.instance_id, host)

        def worker(thread_id):
            local_latencies = []
            session = requests.Session()
            session.headers.update({"Content-Type": "application/json"})
            for i in range(ops_per_thread):
                block_key = thread_id * ops_per_thread + i + 200000
                events = [_ev_block_add(block_key, "mem", _make_single_spec("spec_4096", _build_event_report_uri(
                    host, "mem"))), _ev_block_delete(block_key, "mem", ["spec_4096"]), _ev_heartbeat({"thread": str(thread_id)}), ]
                payload = _make_request(
                    self.instance_id, host, events,
                    trace_id=f"bench_mixed_{thread_id}_{i}",
                )
                t0 = time.monotonic()
                try:
                    resp = session.post(f"{BASE_URL}/api/reportEvent", json=payload)
                    self._raise_for_report_error(resp)
                except Exception as e:
                    errors.append(str(e))
                    continue
                t1 = time.monotonic()
                local_latencies.append((t1 - t0) * 1000)
            session.close()
            return local_latencies

        t_start = time.monotonic()
        with ThreadPoolExecutor(max_workers=num_threads) as pool:
            futures = [pool.submit(worker, tid) for tid in range(num_threads)]
            for f in as_completed(futures):
                latencies.extend(f.result())
        t_end = time.monotonic()

        elapsed = t_end - t_start
        qps = len(latencies) / elapsed if elapsed > 0 else 0
        latencies.sort()
        p50 = self._percentile(latencies, 50)
        p99 = self._percentile(latencies, 99)

        print(f"\n[BENCH] Mixed ADD/DELETE/HEARTBEAT batch:")
        print(f"  Total batches: {len(latencies)} / {total_batches} (errors: {len(errors)})")
        print(f"  Elapsed:       {elapsed:.2f}s")
        print(f"  Batch QPS:     {qps:.1f}")
        print(f"  Latency p50:   {p50:.2f}ms")
        print(f"  Latency p99:   {p99:.2f}ms")
        if latencies:
            print(f"  Latency avg:   {statistics.mean(latencies):.2f}ms")
        self.assertEqual(len(errors), 0, f"Bench had errors: {errors[:5]}")


def main():
    parser = argparse.ArgumentParser(description="Event Report ReportEvent HTTP integration tests")
    parser.add_argument("--host", default="localhost", help="KVCM host")
    parser.add_argument("--http_port", type=int, default=56020, help="KVCM meta HTTP port")
    parser.add_argument("--admin_http_port", type=int, default=None,
                        help="KVCM admin HTTP port (for addStorage). Defaults to http_port.")
    parser.add_argument("--instance_id", default="event_report_cluster_0", help="event report instance_id")
    parser.add_argument("--skip-bench", action="store_true", help="Skip benchmark tests")
    parser.add_argument("--only-bench", action="store_true", help="Run only benchmark tests")
    parser.add_argument(
        "--enable-liveness-timing-tests",
        action="store_true",
        help=("Run heartbeat/cleanup timing tests. Requires the Event report storage to be opened with "
              "small heartbeat_timeout_ms / cleanup_grace_ms (defaults to 1000ms / 2000ms here)."),
    )
    parser.add_argument("--heartbeat-timeout-ms", type=int, default=1000)
    parser.add_argument("--cleanup-grace-ms", type=int, default=2000)

    args, _ = parser.parse_known_args()

    admin_port = args.admin_http_port or args.http_port

    global BASE_URL, ADMIN_URL, INSTANCE_ID, SKIP_BENCH, ONLY_BENCH
    global ENABLE_LIVENESS_TIMING_TESTS
    global HEARTBEAT_TIMEOUT_MS, CLEANUP_GRACE_MS
    BASE_URL = f"http://{args.host}:{args.http_port}"
    ADMIN_URL = f"http://{args.host}:{admin_port}"
    INSTANCE_ID = args.instance_id
    SKIP_BENCH = args.skip_bench
    ONLY_BENCH = args.only_bench
    ENABLE_LIVENESS_TIMING_TESTS = args.enable_liveness_timing_tests
    HEARTBEAT_TIMEOUT_MS = args.heartbeat_timeout_ms
    CLEANUP_GRACE_MS = args.cleanup_grace_ms

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    if not ONLY_BENCH:
        suite.addTests(loader.loadTestsFromTestCase(EventReportFunctionalTest))
    if not SKIP_BENCH:
        suite.addTests(loader.loadTestsFromTestCase(EventReportBenchTest))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
