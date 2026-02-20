# Aliyun Cribl Collector

This repository contains a custom Cribl Stream Collector that targets Alibaba Cloud OSS by consuming Aliyun Simple Message Queue (MNS) notifications produced by the bucket. Each message identifies a new object; the collector downloads it, optionally decompresses `.gz` files, and forwards each log line into a Cribl pipeline.

## Repository Layout

- `aliyun_oss/` – self-contained collector bundle (code, schemas, package.json).

```
aliyun_oss/
  test/               # unit tests for discover/collect logic
  package.json        # declares ali-oss / @alicloud/mns dependencies
  package-lock.json   # exact dependency tree for reproducible installs
  conf.schema.json    # configuration schema presented in the UI
  index.js            # discover/collect logic
  ui-schema.json      # UI layout customisation
```

## Quick Start (Online)

1. Copy `aliyun_oss` into your Cribl installation under `$CRIBL_HOME/local/cribl/collectors/aliyun_oss`.
2. From that directory run `npm install --production` to fetch `ali-oss` / `@alicloud/mns`.
3. In the Cribl UI (`Data Sources → Collectors`) create a new collector and select **Aliyun OSS Bucket**.
4. Provide the OSS endpoint, bucket, credentials, and optional filters, then populate the SMQ section with the queue name/endpoint so the collector can long-poll those notifications.
5. Run the collector manually or attach a schedule, then route its output to the desired pipelines/destinations.

## Offline / Air-Gapped Installation

Use this flow when the target Cribl node cannot access the internet. The idea is to build `node_modules` on a connected machine with the same OS/CPU and Node.js major version, then transfer the bundle.

1. On a connected build machine, create the dependency tree:
   - `cd aliyun_oss`
   - `npm install --production`
2. Package the collector (including `node_modules`) for transfer:
   - `tar -czf aliyun_oss_offline.tgz .`
3. Move `aliyun_oss_offline.tgz` to the offline Cribl node.
4. On the offline node, unpack into the Cribl collectors directory:
   - `mkdir -p $CRIBL_HOME/local/cribl/collectors/aliyun_oss`
   - `tar -xzf aliyun_oss_offline.tgz -C $CRIBL_HOME/local/cribl/collectors/aliyun_oss`
5. Restart Cribl to load the new collector code. You should see **Aliyun OSS Bucket** as an option when creating a new collector in the UI.

If you need to regenerate `node_modules`, repeat steps 1-2 on a connected machine and re-transfer the bundle.

## How It Works

1. **Discover**
   - Creates an `@alicloud/mns` client using the queue credentials.
   - Long-polls the queue for up to `queueMaxMessages` notifications.
   - Parses each message for the bucket/key metadata and ignores objects that fail the include/exclude filters.
   - Leaves the message invisible until the `collect` phase deletes it.
2. **Collect**
   - Downloads the object with `GetObject`.
   - Optionally gunzips the payload.
   - Splits the content into individual lines and emits them either as raw strings or JSON envelopes with object metadata.
   - Updates the checkpoint so the next run records the most recent object for observability.
   - Deletes the SMQ message after a successful download so the object is not replayed.

## Configuration Fields

| Field | Required | Description |
| --- | --- | --- |
| `decompressGzip` | ➖ | Gunzip objects with `.gz` extension (enabled by default). |
| `outputFormat` | ➖ | `json` (default) wraps each line with metadata, `raw` emits plain lines. |
| `checkpointKey` | ➖ | Optional suffix to isolate checkpoints when the collector is reused with different schedules. |
| `queueName` | ✔️ | SMQ/MNS queue that receives OSS object-created notifications. |
| `queueEndpoint` | ✔️ | Fully qualified MNS endpoint URL (for example `https://<accountId>.mns.cn-hangzhou.aliyuncs.com`). |
| `queueWaitSeconds` | ➖ | Long-poll wait seconds (0–30) for each receive call. |
| `queueVisibilityTimeout` | ➖ | Visibility timeout (seconds) applied after receive. Set to `0` to use the queue default. |
| `queueBatchSize` | ➖ | Number of queue messages to request per poll (1–16). |
| `queueMaxMessages` | ➖ | Maximum queue messages processed per run before the collector yields control back to Cribl. |
| `ossEndpoint` | ✔️ | Full OSS endpoint URL (example: `https://oss-cn-shanghai.aliyuncs.com`). |
| `bucket` | ✔️ | Bucket name that stores the log files. |
| `accessKeyIdSecretName` | ✔️ | Cribl secret name holding AccessKeyId. |
| `accessKeySecretSecretName` | ✔️ | Cribl secret name holding AccessKeySecret. |
| `securityTokenSecretName` | ➖ | Optional Cribl secret name holding STS token for temporary credentials. |
| `includeFilters` | ➖ | Substrings that **must** appear in the key. Leave empty to accept everything. |
| `excludeFilters` | ➖ | Substrings that cause the key to be skipped. |

The collector expects secret names (not raw AccessKey values) in config.

Important: the referenced Cribl secrets must be created with secret type `Text`.
Use `Settings -> Security`, create each secret as `Text`, then put the secret **name** in:
- `accessKeyIdSecretName`
- `accessKeySecretSecretName`
- `securityTokenSecretName` (only when using STS)

## SMQ-Driven Discovery

1. Configure your OSS bucket to send *ObjectCreated* events into an Aliyun Simple Message Queue (MNS) queue. Ensure the queue and bucket live in the same region or have cross-region permissions.
2. Copy the queue endpoint and queue name into the collector configuration. The collector derives SMQ account ID from the endpoint host and uses the same AccessKeyId/Secret for both OSS and SMQ. Adjust `queueWaitSeconds`, `queueBatchSize`, or `queueMaxMessages` to align with your throughput targets.
3. Each SMQ message is deleted only after the corresponding object is downloaded and split into events. If `queueVisibilityTimeout` is set, the collector extends visibility on receive (and uses the returned receipt handle for deletion). If the collector fails mid-run, the visibility timeout expires and the message becomes eligible for reprocessing.
4. When the queue is empty, discovery returns after the long-poll wait (`queueWaitSeconds`) with no results. The queue remains the system of record for “files landed”; checkpoints are only updated after successful collection.

## Checkpoint Storage

The collector uses Cribl's progress store to persist the most recent `key` and `lastModified` value it has processed. The progress entry is informational only; the queue remains the authoritative source of new messages. If your deployment does not provide a progress store, the collector still works but you lose the observability benefits.

## Output Payload

- `outputFormat = "json"` (default) emits newline-delimited JSON documents shaped as:

```json
{
  "message": "<original log line>",
  "oss": {
    "bucket": "example-bucket",
    "key": "logs/2025/01/01/example.log.gz",
    "size": 12345,
    "lastModified": "2025-01-01T12:34:56.000Z",
    "etag": "\"abcd1234\""
  }
}
```

- `outputFormat = "raw"` removes the JSON envelope and streams the decompressed lines exactly as they appear in the file.

## Permissions

The IAM principal (user or role) used by the collector needs access to both OSS and SMQ/MNS.

**OSS (bucket access)**
- `oss:GetObject`
- If the bucket is encrypted, include the required KMS permissions for the key.

**SMQ/MNS (queue access)**
- `mns:ReceiveMessage`
- `mns:BatchReceiveMessage`
- `mns:DeleteMessage`
- `mns:ChangeMessageVisibility`

Scope resources to the specific bucket and queue:
- Bucket: `acs:oss:*:<accountId>:<bucket>` and `acs:oss:*:<accountId>:<bucket>/*`
- Queue: `acs:mns:<region>:<accountId>:/queues/<queueName>`

If the bucket or queue is reachable only via VPC endpoints, ensure the Cribl workers have network access to those endpoints.

## Troubleshooting Tips

- Use Cribl's *Job Inspector* to review the log output. Discovery and collection steps log their progress and any checkpoints they read/write.
- If the collector repeatedly reports "no new objects" but you expect data, confirm the filters, verify that OSS is still publishing to the queue, and check the queue metrics for in-flight / delayed messages.
- When the run fails with `RequestError: connect ENETUNREACH`, verify outbound connectivity from the worker to the OSS endpoint. For private endpoints inside a VPC, the worker must live in the same network.

## TODO & Future Enhancements
Areas to iterate on next:
- Stream large objects instead of buffering the entire payload before splitting.
- Add dead-letter handling or metrics around SMQ dequeue counts to spot repeatedly failing objects.

## Publishing & Distribution

This collector is intended to be installed into each user’s own Cribl deployment.

Online distribution:
1. Publish this repository (or a release archive containing `aliyun_oss`).
2. Users copy the collector into `$CRIBL_HOME/local/cribl/collectors/aliyun_oss` and run `npm install --production`.

Offline distribution:
1. Build `node_modules` on a connected machine that matches the target OS/CPU and Node.js major version.
2. Package the collector directory into a tarball and transfer it.
3. Users unpack into `$CRIBL_HOME/local/cribl/collectors/aliyun_oss` and do not run `npm install` on the offline node. Just restart Cribl to load the new collector.

Credential rotation:
- This collector accepts temporary credentials but does not refresh them on its own.
- Users are responsible for supplying refreshed credentials (via config updates, secret stores, or external automation).
