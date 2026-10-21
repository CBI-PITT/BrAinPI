# Storage management

[Back to installation and startup](../README.md#installation)

## Pyramid root and quota

Generated TIFF, NIfTI, and JP2 pyramids share one soft quota. Configure one root
in `settings.ini`; the default quota is **50 GiB**:

```ini
[pyramid_store]
path = /absolute/path/to/pyramid_images_store
max_size_gb = 50
cleanup_fraction = 0.70
poll_seconds = 2
drain_timeout_seconds = 120
retry_seconds = 60
```

The application creates all format and state directories automatically:

```text
pyramid_images_store/
├── tif/
├── nifti/
├── jp2/
└── .brainpi-storage/    # SQLite ledger, maintenance marker, locks
```

`~` is expanded; relative roots are resolved against the settings file directory.
The root must be writable, persistent, and shared by workers on the same POSIX
host. `BRAINPI_PYRAMIDS_DIR` overrides the root and its automatic state path;
`BRAINPI_PYRAMID_LIMIT_GB` overrides the quota.

Compose uses `/var/lib/brainpi/pyramids` in the `brainpi-pyramids` volume. Its
environment override takes precedence over the INI path. Old per-loader paths
and `[pyramid_storage]` options remain supported when no unified root is set.
Changing the root does not move existing files.

Sizes are GiB (1024³ bytes). `max_size_gb = 0` disables eviction but retains
accounting. Per-loader `pyramids_images_allowed_generation_size_gb` limits each
source eligible for generation; it is separate from the total quota.

## Capacity ledger

SQLite stores each generated artifact's path, size, and generation time. A
first adoption pass scans existing artifacts once. Later restarts reuse the
persistent ledger; there is no periodic full-directory scan.

Each generation measures only its new TIFF file or Zarr directory. Accounting
is committed before atomic publication; concurrent updates and duplicate paths
are handled transactionally. Interrupted publication may overstate usage rather
than leave a published artifact unaccounted for. Temporary generation space is
outside the soft quota, so actual usage can exceed the threshold temporarily.

## Automatic cleanup

When recorded usage reaches the limit:

1. One worker requests maintenance; new protected requests receive HTTP 503
   with `Retry-After: 2` rather than queueing.
2. Existing requests continue. Their protection lasts until the response is
   closed, including streaming responses and synchronous generation tasks.
   NIfTI generation children inherit an additional shared lease.
3. Each worker closes its cached image objects and releases its shared lease.
   Cleanup requires the exclusive lease, proving no worker still holds readers.
4. Missing artifacts are removed from the ledger. If usage remains at the
   limit, the oldest complete generated artifacts are deleted, targeting at
   least 70% of the reconciled byte total. Source datasets are not deleted.
5. The marker is removed and requests resume. Later access reopens or regenerates
   datasets on demand. Workers stay running throughout.

The current barrier is global: it includes browser pages and reads of IMS,
already-pyramidal TIFF, and other formats. `/healthz`, `/static/`,
`/browser/storage` remain available. Browser navigation receives a styled
maintenance page that stays open after maintenance ends, without polling,
refreshing, or opening the viewer automatically. Users can click the retry
button to navigate to the same URL with GET; blocked POST requests are not
replayed. Data/image clients still receive a plain 503
and decide whether to retry; an already-open viewer may need refreshing.

| Setting | Meaning |
|---|---|
| `poll_seconds = 2` | Worker maintenance polling interval; no full-directory scan |
| `drain_timeout_seconds = 120` | Wait for the exclusive cleanup lock; does not cap deletion duration |
| `retry_seconds = 60` | Cooldown before another attempt; a new generation can trigger an earlier request |

A drain timeout cancels that attempt without deleting live data. Existing
generation tasks continue. Long tasks can cause repeated waits and temporary
503 periods. Failed deletions remain charged in the ledger and can be retried.
Zarr directories with many small files can take time to delete once draining
has completed.

## Browser storage checks

Authenticated `[all]` members see **Storage** after **Sign out**. One click
queries both cache and pyramid usage and displays their limits in a popover,
with an automatic-management notice. No measurement runs on page load or on a
browser timer. Other users neither see the control nor access its API.

`/browser/storage` returns both measurements; `?kind=cache` or `?kind=pyramid`
returns one. Queries measure logical file bytes; pyramid responses also include
ledger usage and cleanup status. Measurements do not overwrite the ledger.
Concurrent status scans are rejected to avoid overlapping expensive work.

## Disk cache

The response/chunk cache has its own size and eviction policy:

```ini
[disk_cache]
location_win
location_unix = /fast/cache/brainpi
cacheSizeGB = 100
evictionPolicy = least-recently-used
shards = 16
timeout = 0.010
```

Leave the platform's location valueless to disable this cache. Compose
overrides the Unix location with `/var/cache/brainpi`, backed by the separate
`brainpi-cache` volume. Use native absolute paths outside Docker: a Windows
path such as `Z:\cache` is relative on Linux.

## Manual maintenance and standalone loaders

For manual removal, stop the service first so workers release cached objects:

```bash
docker compose stop brainpi
# Remove only the intended generated pyramid artifacts from their volume.
docker compose start brainpi
```

Keep `.brainpi-storage` and its lock files. Remove whole TIFF files or whole
Zarr directories, not individual chunks. Automatic cleanup reconciles missing
paths before deciding whether more eviction is required. Partial Zarr deletion
cannot be detected by checking the artifact directory's existence alone.

Deleting files while workers are running does not invalidate `opendata`.
Open TIFF handles may retain disk space; missing Zarr chunks can fail or return
fill values. Refreshing a browser does not necessarily release worker objects.
There is currently no automatic request-time recovery for manual deletion.

`docker compose down` preserves named volumes; `docker compose down -v` deletes
both cache and pyramid volumes. Do not use the latter for routine cleanup.

A standalone loader's `storage_manager=None` does not disable generation;
`pyramid_generation_allowed` controls that separately. Unmanaged generation
does not register the new artifact or trigger quota maintenance. Standalone
code sharing these stores must honor both accounting and the reader barrier.

## Cache identity and invalidation

Local single-file datasets use `inode + modification time` as their open-dataset
identity. Loader slice caches use:

```text
file_ino + modification_time + str((resolution, t, c, z, y, x))
```

S3 datasets use their URL because no local inode exists. Updating data in place
at the same S3 URL, or modifying a file inside a directory-backed dataset
without changing the directory timestamp, may require explicit cache clearing
or a worker restart.
