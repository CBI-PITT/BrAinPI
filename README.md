# BrAinPI

BrAinPI (pronounced “Brain Pie”) browses and serves large microscopy datasets
through Neuroglancer, OpenSeadragon, and virtual OME-Zarr v2 endpoints. It
supports local files and public, anonymous S3 OME-Zarr stores.

## Installation

Get the code:

```bash
git clone https://github.com/CBI-PITT/BrAinPI.git
cd BrAinPI
```

Choose [Docker](#docker) for a container deployment, or
[native Python](#native-python) for a local development environment.

### Docker

Requires Git and Docker with the Compose plugin. Run the commands from the
repository root. Docker supplies Python and the native image libraries.

**1. Create deployment files.** For a new installation:

```bash
cp .env.example .env
cp docker/template_settings.ini docker/settings.ini
cp docker/template_groups.ini docker/groups.ini
```

**2. Set the data directory and secret in `.env`.** Use an existing absolute
host path and replace the example secret with a newly generated random value:

```dotenv
BRAINPI_DATA_DIR=/absolute/path/to/public-data
BRAINPI_SECRET_KEY=replace-with-a-long-random-secret
```

The host data directory is mounted read-only at `/data`. To list that directory
in the anonymous browser, add this alias to `docker/settings.ini`:

```ini
[dir_anon]
data = /data
```

The templates start with no dataset aliases and no configured login backend.
For authenticated browsing, see [authentication](docs/deployment.md#authentication).

**3. Build once, then start both services.** The application and local
Neuroglancer frontend share the same image:

```bash
docker compose build brainpi
docker compose up -d
docker compose ps
curl http://localhost:5001/healthz
```

Open **[http://localhost:5001/browser/](http://localhost:5001/browser/)**.
The local Neuroglancer frontend runs at
[http://localhost:9999/v/base/](http://localhost:9999/v/base/).

For access from another computer, set `BRAINPI_PUBLIC_URL` and
`BRAINPI_NG_PUBLIC_URL` in `.env` to that server's browser-reachable URLs.
See [ports and public URLs](docs/deployment.md#ports-and-public-urls) for port
changes, HTTPS proxies, and the optional hosted viewer.

Source data stays on the host. Disk cache and generated pyramids persist in
separate Docker volumes; the default combined pyramid limit is **50 GiB**.

### Native Python

Requires Python 3.12 on Linux or macOS; use Docker on Windows. From the cloned
repository root, create an environment and install OpenJPEG and BrAinPI:

```bash
conda create -y -n brainpi python=3.12
conda activate brainpi
conda install -c conda-forge openjpeg glymur=0.13.6
python -m pip install -e .
cp BrAinPI/template_settings.ini BrAinPI/settings.ini
cp BrAinPI/template_groups.ini BrAinPI/groups.ini
```

In `BrAinPI/settings.ini`, set `[auth] secret_key`, `[app] url`,
`[neuroglancer] url`, and a writable `[pyramid_store] path`. Native dataset
aliases use host paths:

```ini
[dir_anon]
data = /absolute/path/to/public-data
```

Start the application:

```bash
gunicorn --chdir BrAinPI --worker-class gthread \
  --workers 8 --threads 2 --timeout 1800 \
  --bind 0.0.0.0:5001 wsgi:app
```

Start the local Neuroglancer frontend in a second terminal with the same
Conda environment:

```bash
python BrAinPI/neuroglancer_server.py
```

Open [http://localhost:5001/browser/](http://localhost:5001/browser/).
For Flask development, use `python BrAinPI/brain_api_main.py` instead of
Gunicorn. See the [deployment guide](docs/deployment.md) for concurrency,
file-descriptor limits, and production setup.

## Running

Common Docker operations:

```bash
docker compose logs -f --tail=100 brainpi
docker compose restart brainpi neuroglancer
docker compose down
```

`restart` reloads edited INI settings. After changing `.env`, ports, mounts, or
resource limits, recreate the containers with `docker compose up -d --force-recreate`.
After changing application code or dependencies, rebuild with
`docker compose build brainpi`, then recreate the containers.

`docker compose down` preserves the cache and pyramid volumes. Adding `-v`
deletes them. If using the IPA certificate overlay, include the same
`-f compose.yaml -f compose.ipa.yaml` selection in subsequent commands.

## Configuration

Docker reads `docker/settings.ini` and `docker/groups.ini`; native startup
reads the corresponding files under `BrAinPI/`. These deployment files and
`.env` are ignored by Git. Keep your existing files when updating an installation.

| Setting | Purpose |
|---|---|
| `[dir_anon]`, `[dir_auth]` | Dataset aliases for anonymous and authenticated browsing. |
| `[auth]`, `groups.ini` | FreeIPA/AD-LDAP login and browser group restrictions. |
| `[pyramid_store]` | One root for `tif/`, `nifti/`, `jp2/`, and state files; 50 GiB default quota. |
| `[disk_cache]` | Independent response/chunk cache location, size, and eviction policy. |
| `[app] url`, `[neuroglancer] url` | Browser-reachable service and viewer URLs. |
| Loader sections | Per-source generation size limits and generated artifact suffixes. |

Pyramid storage is managed automatically. At its limit, BrAinPI closes cached
image objects and removes the oldest generated pyramids, targeting 70% of
recorded usage. The current cleanup barrier temporarily returns HTTP 503 for
most new requests, including IMS and browser requests. Details and exceptions
are in [storage management](docs/storage.md#automatic-cleanup).

Members of `[all]` in `groups.ini` see a **Storage** button after **Sign out**.
One click reports cache and pyramid usage; page loading does not trigger a scan.

Browser group restrictions do not by themselves protect the viewer-facing
`/ng/`, `/osd/`, and `/omezarr/` data routes. Verify access control at your proxy
or application boundary before exposing restricted dataset aliases. Use HTTPS
for authenticated browser sessions.

## Further documentation

- [Deployment and troubleshooting](docs/deployment.md): Docker mounts, ports,
  public URLs, authentication, certificate mounts, environment variables, and
  file-descriptor/memory issues.
- [Storage management](docs/storage.md): cache configuration, the pyramid
  ledger, automatic cleanup, manual maintenance, and cache invalidation.
- [Viewer and API reference](docs/viewer-reference.md): endpoint URLs, link
  discovery, OME-Zarr variants, pixel conversion, and viewer behavior.

## Supported sources

| Source | Extensions or layout | Notes |
|---|---|---|
| Imaris | `.ims` | Multiscale image data; Imaris annotations are not exposed. |
| OME-Zarr | `.ome.zarr`, `.zarr` | Local `.ome.zarr` directories and anonymous, read-only `s3://` stores; OME multiscales metadata is required. |
| Alternative Zarr stores | `.omezans`, `.omehans` | Archived and HDF5-backed nested stores supplied by `zarr_stores`. |
| TIFF / OME-TIFF | `.tif`, `.tiff`, `.ome.tif`, `.ome.tiff`, `.ome-tif`, `.ome-tiff` | Existing pyramids are reused; optional generated pyramids are stored according to `settings.ini`. |
| JPEG 2000 | `.jp2` | Requires OpenJPEG through Glymur. RGB/YCC declarations are distinguished from independent components. |
| Nikon ND2 | `.nd2` | Read through `limnd2`. |
| NIfTI | `.nii`, `.nii.gz`, `.nii.zarr` | NIfTI files may be converted to a cached multiscale Zarr representation. |
| TeraFly | directory ending in `.terafly` | The current loader supports one image channel. |
| Neuroglancer precomputed | directory ending in `.pcd` | Raw image volumes can be loaded by `/ng/` and the virtual OME-Zarr endpoint; native annotation and segmentation datasets are passed through by `/ng/`. |

All image loaders expose logical `TCZYX` data. TIFF/JP2 sample axes such as RGB
`YXS` are folded into the logical channel axis.

## Architecture

All image loaders expose a common five-dimensional `TCZYX` interface. Metadata
and chunks are produced on demand; sources that need additional pyramid levels
can use generated artifacts without changing the original dataset.

```text
configured local/S3 path
        │
        ├── file browser and link discovery
        │
        └── format-specific loader → TCZYX arrays
                                  │
                 ┌────────────────┼────────────────┐
                 │                │                │
              /ng/             /osd/          /omezarr/
         info + raw chunks   HTML + PNG     Zarr v2 metadata
                                              + compressed chunks
```

## License

BrAinPI is distributed under the [BSD 3-Clause License](LICENCE).
