# KVCacheManager MetaService API curl Examples

## Register Instance
```bash
curl -g -vvv -X POST http://localhost:6382/api/registerInstance \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "trace_id": "trace_id_123",
    "instance_group": "test_group",
    "instance_id": "test_instance_id",
    "model_deployment": {
        "model_name": "test",
        "dtype": "fp16",
        "use_mla": false,
        "tp_size": 1,
        "dp_size": 1,
        "lora_name": "custom_lora",
        "pp_size": 1,
        "extra": "extra",
        "user_data": "custom_user_data"
    },
    "block_size": 8,
    "query_type": "QT_PREFIX_MATCH",
    "location_spec_infos": [
        {"name": "tp0", "size": 4096000}
    ]
}'
```
`query_type` is optional. When `GetHostCacheState` does not set `query_type`, the service uses this registered value.

## Get Instance Info
```bash
curl -g -vvv -X POST http://localhost:6382/api/getInstanceInfo \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "trace_id": "trace_id_124",
    "instance_id": "test_instance"
}'
```

## Get Cache Location
```bash
curl -g -vvv -X POST http://localhost:6382/api/getCacheLocation \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "trace_id": "trace_id_125",
    "instance_id": "test_instance",
    "block_keys": [123],
    "block_mask": {
        "offset": 0
    }
}'
```

## Start Write Cache
```bash
curl -g -vvv -X POST http://localhost:6382/api/startWriteCache \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "trace_id": "trace_id_126",
    "instance_id": "test_instance_id_2",
    "block_keys": [1234, 4567, 1234],
    "token_ids": [],
    "write_timeout_seconds": 10
}'
```

## Finish Write Cache
```bash
curl -g -vvv -X POST http://localhost:6382/api/finishWriteCache \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "trace_id": "trace_id_127",
    "instance_id": "test_instance",
    "write_session_id": "session_id_from_start_write",
    "success_blocks": {
        "bool_masks": {
          "values": [true]
        }
    }
}'
```

Note: To use the Finish Write Cache API, you need to replace "session_id_from_start_write" with the actual write_session_id returned by the Start Write Cache API.

## Remove Cache
```bash
curl -g -vvv -X POST http://localhost:6382/api/removeCache \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "trace_id": "trace_id_128",
    "instance_id": "test_instance",
    "block_keys": [123],
    "block_mask": {
        "offset": 0
    }
}'
```

## Report Event

`reportEvent` is the cache-subscriber ingestion API. Subscribers use ordered
incremental events for steady-state traffic and a complete snapshot for
startup, periodic, or anomaly reconciliation:

- RTP-LLM subscriber: poll full cache status, compute local deltas, and send a
  lower-frequency `EVENT_BLOCK_SNAPSHOT` as an authoritative reconciliation.
- vLLM subscriber: map KV events to ordered `EVENT_BLOCK_ADD`,
  `EVENT_BLOCK_DELETE`, and snapshots after full-clear or event gaps.

`EVENT_BLOCK_SNAPSHOT` is authoritative for one
`instance_id + host_ip_port + medium`. It must contain the complete block set
and every block's complete spec set, including full-attention and mamba-state
components. It cannot be paginated or mixed with block add/delete events in
the same request; wait for its ACK before sending later deltas. An empty
snapshot clears the scope.
KVCM also serializes snapshot and delta mutations across concurrent requests for
the same scope. A delta request acquires a lease that pins the current committed
version until all of its metadata writes finish; snapshot version allocation is
rejected while such a lease is active. Conversely, once a snapshot version is
in flight, later add/delete events for that scope are rejected. The rejected
item is not written and the caller should retry it after the earlier request
finishes. Different hosts or media remain independent.


KVCM writes internal scope metadata into every event-report URI. Snapshot data
also carries its version: KVCM writes the new version to copy-on-write location
ids after persisting an allocated-version high-water mark, flushes the block
writes, and then persists a scope-level commit marker before acknowledging the
snapshot. Queries filter old versions through `MightExist`, while a coalesced
background scan deletes them asynchronously. A snapshot rewrites every reported
block and should therefore be used for low-frequency reconciliation, not
high-frequency status reporting. Callers must not set the internal `kvcm_*` URI
params themselves. `EVENT_HOST_DOWN` is terminal and must be sent as the only
This reservation applies to every parameter name beginning with `kvcm_`,
including blank values and names introduced by future KVCM versions.
event in its request.

```bash
curl -g -vvv -X POST http://localhost:6382/api/reportEvent \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "trace_id": "trace_id_131",
    "instance_id": "test_instance",
    "host_ip_port": "192.168.2.1:8080",
    "storage_type": "ST_EVENT_REPORT",
    "events": [
      {
        "event_type": "EVENT_NODE_REGISTER",
        "node_register": {
          "mediums": ["gpu"]
        }
      },
      {
        "event_type": "EVENT_BLOCK_SNAPSHOT",
        "block_snapshot": {
          "medium": "gpu",
          "blocks": [
            {
              "block_key": "123",
              "specs": [
                {
                  "name": "full_attention:group=0:tp=0",
                  "uri": "event_report://physical-storage:9600/gpu/123?size=4096"
                }
              ]
            }
          ]
        }
      }
    ]
}'
```

## Trim Cache
```bash
curl -g -vvv -X POST http://localhost:6382/api/trimCache \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "trace_id": "trace_id_129",
    "instance_id": "test_instance",
    "strategy": "TS_REMOVE_ALL_CACHE",
    "begin_timestamp": 0,
    "end_timestamp": 0
}'
```

## Get Cache Meta
```bash
curl -g -vvv -X POST http://localhost:6382/api/getCacheMeta \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "trace_id": "trace_id_130",
    "instance_id": "test_instance",
    "block_keys": [123],
    "block_mask": {
        "offset": 0
    },
    "detail_level": 1
}'
```
