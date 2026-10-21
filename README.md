# cometbft-archive

Experimental tooling for running an archive-capable CometBFT blocksync peer and
migrating immutable block ranges into S3-compatible object storage.

## Overview

The live peer is a blocksync-only process. It does not run consensus, state
execution, mempool, evidence, validator signing, or ABCI connections. It
embeds CometBFT's P2P transport, blocksync wire messages, block types, and
blockstore APIs as libraries, and owns the archive-specific request planner,
hot-store ingestion, serving policy, and S3 archival loop.

Responsibilities:

- Participate in P2P blocksync and follow the advertised head.
- Persist hot blocks locally with bounded retention.
- Archive immutable ranges to object storage after a safety delay.
- Serve hot blocks and manifest-indexed cold archive blocks to peers.
- Verify archived ranges before pruning the hot blockstore.

Archive segment objects are immutable. Existing objects are reused only when
size and checksum match, uploads use create-if-absent semantics, and fresh
uploads are read back and verified before the manifest advertises them.

See [docs/architecture.md](docs/architecture.md) for the storage layout and
protocol model, [docs/operations.md](docs/operations.md) for preflight,
metrics, and soak guidance.

## Validation modes

Live ingest defaults to `--validation=storage-only`. Stronger modes:

- `--validation=checkpoint --checkpoint <height>:<hexhash>` pins trusted block
  hashes at selected heights.
- `--validation=validator-set --validator-set <height>:<validators.pb>`
  verifies the next block's `LastCommit` against an operator-supplied
  `tendermint.types.ValidatorSet`.
- `--validator-set-rpc <rpc-url>` fetches missing validator sets from a
  trusted CometBFT RPC endpoint on demand. RPC-only mode is trust-on-RPC; use
  pinned files or checkpoints when independent rejection is required.

## Metrics and health

`--metrics-listen <host:port>` exposes `/healthz`, `/readyz`, and `/metrics`.
`/healthz` is process liveness; `/readyz` requires live peer connectivity and
a non-zero advertised provider height. Metrics cover archive/prune run and
error counts, last error strings, archived/pruned block totals, last
attempted/successful maintenance times, P2P peer count, peer best height,
blocksync inflight requests, buffered out-of-order responses, request
timeouts, local hot range, combined served range, hot/cold/no-block serving
counters, cold serving errors, active cold reads, cold queue depth and
queue-full events, next ingest height, and pending block height.

## Serve config

`serve --config <file.json>` loads settings from JSON; CLI flags override.
See [docs/serve.config.example.json](docs/serve.config.example.json) for a full
example. `segment_blocks` defaults to `100`, is capped at `1000`, and encoded
segment payloads are capped at 1 GiB.

`--p2p-send-rate` / `p2p_send_rate` and `--p2p-recv-rate` / `p2p_recv_rate`
set positive integer bytes/second limits on each peer connection in the
CometBFT transport, across all its channels. Both default to `5120000`.
These are transport rate limits with packet buffering and sampling, not a
node-wide bandwidth budget or a promised archive ingestion rate. Zero and
negative values are rejected by the CLI and JSON config. Programmatic
`NodeOptions` uses zero to select the same default.

Normal ingest requests prioritize the earliest missing height in the bounded
request window and choose the eligible provider with the fewest outstanding
network requests (peer ID breaks ties). Each provider is capped at 32 inflight
requests. Received blocks and delayed local validation retries do not count
as network load. Timeouts prefer another eligible provider; no-block replies
and invalid blocks exclude that provider at the affected height. Recovery
continues to fetch its bounded coherent pair from one provider at a time.

Reproduce the planner distribution measurement with:

```sh
go test ./internal/blocksyncarchive -run '^$' -bench '^BenchmarkRequestPlannerDistribution$' -benchtime=100x -count=1
```

This deterministic 32-request window reports 32 requests for one provider or
8 requests each for four providers, versus the former sorted-first policy's
32/0/0/0 for four identical providers. It measures request distribution and
planner CPU cost only; it provides no evidence of a network throughput gain.

## Other commands

- `prune-hot` enforces local retention only after a full archive
  verification succeeds, then delegates pruning to the blockstore API.
  `serve` uses the same verified path when `--prune-interval > 0`.
- `soak` polls a running `serve` diagnostics endpoint and fails on readiness
  drops, low peer count, stalled peer head, or rising archive/prune/cold
  serving errors. Use `--min-cold-responses-delta` to require evidence that
  peers received manifest-backed cold responses.

## Object store URLs

```text
file:///tmp/archive-objects
s3://bucket/root-prefix?region=us-east-1
s3://bucket/root-prefix?region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A9000&path_style=true
```

The S3 backend uses the standard AWS SDK credential chain. Do not point it at
a real bucket unless you intend to write archive objects there.

Query parameters: `region` (defaults to `us-east-1`), `endpoint` (URL-encoded,
`http`/`https` only), and `path_style` (defaults to `true` when `endpoint` is
set). The backend needs `GetObject`, `HeadObject`, `PutObject`, and
`ListObjectsV2`; it never deletes objects. Segment uploads use
`PutObject` with `If-None-Match: *`, so the provider must support S3
conditional writes. A provider that ignores the header loses the
create-if-absent guarantee; one that rejects it shows up as rising
`archive_errors`.

## Docker

| File | Purpose |
| --- | --- |
| `docker-compose.dev.yml` | Local development: archive peer, status UI, and a throwaway RustFS S3 server. The bucket comes from `BUCKET` (default `archive`), with an optional `STORE_PREFIX`. |
| `docker-compose.prod.yml` | Production: archive peer and status UI against an external object store. No bundled storage. |

Development:

```text
docker compose -f docker-compose.dev.yml up --build
```

The status UI is at <http://localhost:8080>, metrics at
<http://localhost:26660/metrics>, and the RustFS console at
<http://localhost:9001> (`rustfsadmin` / `rustfsadmin`).

Production:

```text
cp .env.example .env    # fill in STORE_URL, PERSISTENT_PEERS, credentials
docker compose -f docker-compose.prod.yml --env-file .env up -d --build
```

`STORE_URL` and `PERSISTENT_PEERS` are required. The production stack turns
on hot-store pruning (`PRUNE_INTERVAL=5m`, `RETAIN_BLOCKS=1000`). Metrics and
the UI bind to `127.0.0.1` by default; set `UI_BIND=0.0.0.0` only behind a
reverse proxy or firewall. Only the P2P port (`26656`) is public. Create the
bucket before the first start; the archiver does not create it.

## Cloud object storage

Every provider below is reached through the `s3://` backend. Set these values
in `.env` for `docker-compose.prod.yml`, or pass them as `--store` plus the
`AWS_*` environment variables when running the binary directly.

After the first `ARCHIVE_INTERVAL`, check that `archive_runs` is increasing,
`archive_errors` is still `0`, and segment objects appear under
`<prefix>/segments/` in the bucket.

### AWS S3

```dotenv
STORE_URL=s3://my-archive-bucket/mocha-5?region=eu-west-1
AWS_REGION=eu-west-1
# Leave empty to use an EC2 instance profile, ECS task role, or EKS IRSA/Pod Identity.
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

Minimal IAM policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::my-archive-bucket"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::my-archive-bucket/*"
    }
  ]
}
```

No `endpoint` is needed. S3 supports conditional writes natively. Turn on
bucket versioning or Object Lock if you want protection against accidental
overwrites from other tools.

### Google Cloud Storage

GCS is reached through its S3-compatible XML API with HMAC keys:

1. Create a service account and grant it `roles/storage.objectUser` on the
   bucket.
2. Create an HMAC key for it (Cloud Storage → Settings → Interoperability, or
   `gcloud storage hmac create <sa-email>`).

```dotenv
STORE_URL=s3://my-archive-bucket/mocha-5?region=auto&endpoint=https%3A%2F%2Fstorage.googleapis.com
AWS_ACCESS_KEY_ID=GOOG1E...        # HMAC access ID
AWS_SECRET_ACCESS_KEY=...          # HMAC secret
AWS_REGION=auto
AWS_REQUEST_CHECKSUM_CALCULATION=when_required
AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
```

The checksum settings stop the AWS SDK from sending default CRC32 checksums,
which GCS's XML API rejects. GCS documents its own preconditions
(`x-goog-if-generation-match`) but does not clearly document
`If-None-Match: *` on S3-style PUTs. Check the create-if-absent behaviour on a
scratch bucket before you rely on it. If it doesn't hold, run
[s3proxy](https://github.com/gaul/s3proxy) with the `google-cloud-storage`
backend instead; s3proxy performs the conditional PUT natively on GCS.

### Azure Blob Storage

Azure Blob has no S3 API, so the production compose ships an optional
[s3proxy](https://github.com/gaul/s3proxy) gateway (`azureblob` backend,
which performs conditional PUTs natively against Azure):

1. Create a storage account and a container, for example
   `az storage container create --account-name myacct --name cometbft-archive`.
2. Copy an account key (Storage account → Access keys).

```dotenv
# Bucket name = container name; s3proxy is reached over the compose network.
STORE_URL=s3://cometbft-archive/mocha-5?region=us-east-1&endpoint=http%3A%2F%2Fs3proxy%3A80
AZURE_STORAGE_ACCOUNT=myacct
AZURE_STORAGE_KEY=...
# Shared secret between the archiver and s3proxy (any random values).
AWS_ACCESS_KEY_ID=archive
AWS_SECRET_ACCESS_KEY=<random-32-bytes>
```

```text
docker compose -f docker-compose.prod.yml --env-file .env --profile azure up -d --build
```

s3proxy is not published on a host port. Pin `S3PROXY_TAG` to a specific
image digest for production.

### Scaleway Object Storage

1. Create a bucket in the target region (`fr-par`, `nl-ams`, or `pl-waw`).
2. Create an IAM API key for an application with `ObjectStorageFullAccess`,
   or a narrower read/write/list policy, scoped to the project.

```dotenv
STORE_URL=s3://my-archive-bucket/mocha-5?region=fr-par&endpoint=https%3A%2F%2Fs3.fr-par.scw.cloud
AWS_ACCESS_KEY_ID=SCW...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=fr-par
```

Scaleway documents support for `If-None-Match` conditional writes.

### DigitalOcean Spaces

1. Create a Space in a datacenter such as `nyc3`, `ams3`, `fra1`, or `sgp1`.
2. Create a Spaces access key (API → Spaces Keys), limited to that bucket
   with read/write access.

```dotenv
STORE_URL=s3://my-archive-space/mocha-5?region=us-east-1&endpoint=https%3A%2F%2Fnyc3.digitaloceanspaces.com
AWS_ACCESS_KEY_ID=DO...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
AWS_REQUEST_CHECKSUM_CALCULATION=when_required
AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
```

Spaces expects the SDK region `us-east-1`; the datacenter is chosen by the
endpoint. Spaces does not clearly document `If-None-Match: *` on PUT, so
check create-if-absent behaviour on a scratch Space before you rely on it.

## Development

This module currently uses the sibling CometBFT checkout declared in
`go.mod`:

```text
replace github.com/cometbft/cometbft => ../cometbft
```

Use CometBFT commit `3b0311fc6a8c6b7024e3b1e226a5f9808ba3ccf1` for local and
CI builds until this project is cut over to a tagged CometBFT release.

```text
make fmt
make fmt-check
make verify-cometbft
make e2e-real-cometbft
make test
make lint
make race
```

For an operator-approved deployment soak against a running `serve` process:

```text
make production-soak SOAK_METRICS_URL=http://127.0.0.1:26660
```

`make lint` runs `golangci-lint` with the strict rule set in `.golangci.yml`.
