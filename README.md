# BrAinPI

BrAinPI (pronounced “Brain Pie”) is a Flask service for browsing and serving
large, multiscale microscopy datasets. It presents supported source formats
through three viewer-facing interfaces:

- Neuroglancer precomputed (`/ng/`)
- OpenSeadragon pages and PNG tiles (`/osd/`)
- a virtual OME-Zarr v2 hierarchy (`/omezarr/`)

Datasets remain in their source format. BrAinPI opens them through a common
five-dimensional `TCZYX` loader interface and generates metadata or chunks on
demand. Local files and public, anonymous S3 OME-Zarr stores are supported.

## Architecture

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


## Supported sources

| Source | Extensions or layout | Notes |
|---|---|---|
| Imaris | `.ims` | Multiscale image data; Imaris annotations are not exposed. |
| OME-Zarr | `.ome.zarr`, `.zarr` | Local stores and anonymous, read-only `s3://` stores; OME multiscales metadata is required. |
| Alternative Zarr stores | `.omezans`, `.omehans` | Archived and HDF5-backed nested stores supplied by `zarr_stores`. |
| TIFF / OME-TIFF | `.tif`, `.tiff`, `.ome.tif`, `.ome.tiff`, `.ome-tif`, `.ome-tiff` | Existing pyramids are reused; optional generated pyramids are stored according to `settings.ini`. |
| JPEG 2000 | `.jp2` | Requires OpenJPEG through Glymur. RGB/YCC declarations are distinguished from independent components. |
| Nikon ND2 | `.nd2` | Read through `limnd2`. |
| NIfTI | `.nii`, `.nii.gz`, `.nii.zarr` | NIfTI files may be converted to a cached multiscale Zarr representation. |
| TeraFly | directory ending in `.terafly` | The current loader supports one image channel. |
| Neuroglancer precomputed | directory ending in `.pcd` | Raw image volumes can be loaded by `/ng/` and the virtual OME-Zarr endpoint; native annotation and segmentation datasets are passed through by `/ng/`. |

All image loaders expose logical `TCZYX` data. TIFF/JP2 sample axes such as RGB
`YXS` are folded into the logical channel axis.

## Installation

BrAinPI requires Python 3.12. The development environment and Docker images use
Python 3.12; deployments should use a clean, pinned environment and run the
test suite before release.

```bash
git clone https://github.com/CBI-PITT/BrAinPI.git
cd BrAinPI

conda create -y -n brainpi python=3.12
conda activate brainpi
pip install -e .
```

Glymur requires the native OpenJPEG library. If it is not provided by the host
system, install it from conda-forge:

```bash
conda install -c conda-forge openjpeg glymur=0.13.6
```

## Configuration

Create local configuration files before starting the service:

```bash
cp BrAinPI/template_settings.ini BrAinPI/settings.ini
cp BrAinPI/template_groups.ini BrAinPI/groups.ini
```

At minimum, review these sections in `settings.ini`:

- `[app]`: public service URL, name, logging mode, templates and static assets
- `[dir_anon]`: path aliases visible to anonymous browser users
- `[dir_auth]`: path aliases shown to authenticated users
- `[auth]`: authentication and path-restriction policy
- `[disk_cache]`: platform-specific cache directory and size
- `[neuroglancer]`: viewer URL and advertised chunk strategy
- loader sections: generated-pyramid locations and per-source generation limits

Example path configuration:

```ini
[dir_anon]
world = /h20/Public/world
public_s3 = s3://example-public-bucket/data

[dir_auth]
research = /h20/Restricted/research
```

Viewer endpoints currently construct links from the complete configured path
map so that generated links are shareable. Treat `/ng/`, `/osd/`, and
`/omezarr/` as public data-serving routes unless an upstream proxy or an
application authorization layer restricts them. Do not register sensitive
datasets until the deployment’s access policy has been verified.

### Disk cache

Leave the platform location empty to disable disk caching:

```ini
[disk_cache]
location_win
location_unix = /fast/cache/brainpi
cacheSizeGB = 100
evictionPolicy = least-recently-used
shards = 16
timeout = 0.010
```

Use a native absolute path for the host operating system. A Windows path such
as `Z:\cache` is a relative filename on Linux and will create an unintended
directory inside the working tree.

## Running

Development server:

```bash
python BrAinPI/brain_api_main.py
```

Gunicorn example:

```bash
gunicorn \
  --worker-class gthread \
  -b 0.0.0.0:5001 \
  --chdir BrAinPI \
  wsgi:app \
  -w 8 \
  --threads 2 \
  --timeout 1800
```

Worker count, cache size and file-descriptor limits must be sized for the
deployment. Each worker owns its loader objects; the configured disk cache is
shared. Run BrAinPI behind a reverse proxy in production and configure request
timeouts and maximum response sizes for large chunks.

Gunicorn workers never start a Neuroglancer frontend. The default settings point
to a local frontend, which must be run as one standalone process alongside the
plain/native BrAinPI service:

```bash
python BrAinPI/neuroglancer_server.py
```

Google's hosted viewer remains optional: set `BRAINPI_NG_PUBLIC_URL`, or the
fallback `[neuroglancer] url`, to `https://neuroglancer-demo.appspot.com/` when
a local frontend is not wanted.
For access from another machine, bind the standalone frontend to `0.0.0.0` and
set `url` to the server's browser-reachable hostname instead of `localhost`.

## Docker

The repository includes a Linux-based Docker deployment that runs on Docker
Engine and Docker Desktop for Linux, macOS, and Windows. Configuration, source
datasets, generated pyramids, and disk cache are kept outside the image.
The Docker image starts Gunicorn with 8 workers and 2 threads per worker.
Adjust concurrency to the memory available for the datasets being served.
On ARM64, the builder uses signed plain `char` so the pinned
`v3d-py-helper` C++ extension compiles. It also provides HDF5 build libraries
if `h5py` needs to be built from source; the pinned `h5py` version has a
Python 3.12 ARM64 wheel.

Create the local environment file and edit the data path and secret:

```bash
cp .env.example .env
cp docker/template_settings.ini docker/settings.ini
cp docker/template_groups.ini docker/groups.ini
```

Edit the renamed INI files for the deployment. They are ignored by Git, just
like the plain/native `BrAinPI/settings.ini` and `BrAinPI/groups.ini` files.
The Docker settings template exposes no dataset roots by default. Add only the
container paths that this deployment is intended to serve, for example:

```ini
[dir_anon]
public = /data/Public

[dir_auth]
protected = /data/Protected
```

`BRAINPI_DATA_DIR` must be an existing absolute host path. On Docker Desktop,
make sure that directory is shared with Docker. The container mounts source
datasets read-only at `/data`; generated pyramids and cache entries use Docker
named volumes.

JP2 pyramid generation stages the entire decoded image in memory. The decoded
image can be much larger than the `.jp2` file, so even the default worker count
may exceed Docker's memory limit for large sources. Repeated worker `SIGKILL`
messages during JP2 conversion are a sign to check Docker memory allocation
and conversion concurrency. The per-source generation size limit checks the
compressed file size, not decoded memory use.

Generated pyramids have no automatic store quota and are never deleted by the
application. `pyramids_images_allowed_generation_size_gb` limits only the size
of an individual source eligible for on-demand generation. Workers derive each
artifact path directly from the source identity and check that path on demand;
they do not scan the pyramid volume at startup. Administrators must monitor and
maintain the `brainpi-pyramids` volume. Stop the BrainPI service before removing
generated artifacts so every Gunicorn worker releases its open dataset objects,
then start it again after maintenance:

```bash
docker compose stop brainpi
docker volume ls
# Inspect and remove only the intended generated pyramid artifacts.
docker compose start brainpi
```

`docker compose down -v` deletes both named volumes and must not be used for
routine pyramid maintenance. The disk cache remains separately bounded by its
`[disk_cache]` LRU size setting.

The default Docker deployment starts BrAinPI and one independently supervised
local Neuroglancer frontend:

```ini
[neuroglancer]
local_ip = 0.0.0.0
local_port = 9999
url = http://localhost:9999/v/base/
```

Build and start both services:

```bash
docker compose up --build -d
docker compose ps
curl http://localhost:5001/healthz
curl http://localhost:9999/v/base/
```

Use a plain URL in the INI file; Markdown `[text](url)` syntax is invalid. If
users open BrAinPI from other computers, replace `localhost` with the server's
hostname or public HTTPS URL. Both services reuse the same `brainpi:local`
image. BrAinPI/Gunicorn never starts a frontend process automatically. To use
Google's hosted viewer instead, set this in `.env`:

```dotenv
BRAINPI_NG_PUBLIC_URL=https://neuroglancer-demo.appspot.com/
```

The local Compose service is not selected by that URL, so stop an existing one
with `docker compose stop neuroglancer`. On a fresh hosted-viewer deployment,
`docker compose up --build -d brainpi` starts only BrAinPI. A bare
`docker compose up` always starts both services and would leave an unused local
viewer running while generated links continue to use the hosted viewer.

### Docker ports and public URLs

Listening ports and browser-facing URLs serve different purposes. A listening
port controls where a process accepts connections; a public URL is written into
links returned to the browser. Docker adds a host-to-container port mapping
between them:

| Setting | Purpose | Default |
|---|---|---|
| Gunicorn bind in `Dockerfile` | BrAinPI port inside the container; normally do not change it | `0.0.0.0:5001` |
| `BRAINPI_PORT` in `.env` | Publishes a host port to container port `5001` | `5001:5001` |
| `BRAINPI_PUBLIC_URL` in `.env` | Browser-reachable BrAinPI base URL; overrides `[app] url` | `http://localhost:5001/` |
| `[neuroglancer] local_ip` | Standalone viewer bind address inside its container | `0.0.0.0` |
| `[neuroglancer] local_port` | Standalone viewer listening port inside its container; normally do not change it | `9999` |
| `BRAINPI_NG_PORT` in `.env` | Publishes a host port to container port `9999` | `9999:9999` |
| `BRAINPI_NG_PUBLIC_URL` in `.env` | Browser-reachable viewer base URL; overrides `[neuroglancer] url` | `http://localhost:9999/v/base/` |

The two `.env` port variables only change Docker's host-side published ports.
They do not rewrite either public URL. For example, publishing BrAinPI on host
port 8000 requires both values below:

```dotenv
BRAINPI_PORT=8000
BRAINPI_PUBLIC_URL=http://localhost:8000/
```

Publishing the local viewer on host port 10999 keeps its container listener on
9999 and requires the matching public URL:

```dotenv
BRAINPI_NG_PORT=10999
BRAINPI_NG_PUBLIC_URL=http://localhost:10999/v/base/
```

`localhost` means the computer running the browser, not necessarily the Docker
host. When other computers use the deployment, set both public URLs to a DNS
name or IP address they can reach. Behind a TLS reverse proxy, use the external
`https://` URLs and ports exposed by that proxy even though the containers keep
listening internally on HTTP ports 5001 and 9999.

Generated viewer links use `BRAINPI_NG_PUBLIC_URL` (or its INI fallback) for
the frontend and embed `BRAINPI_PUBLIC_URL` as the precomputed data source. The
user's browser connects to both addresses directly. Do not put Compose-only DNS
names such as `http://brainpi:5001/` or `http://neuroglancer:9999/` in these
public URL settings; those names resolve between containers but not in a normal
browser.

Docker configuration templates are in `docker/template_settings.ini` and
`docker/template_groups.ini`. Copy and rename them as shown above, then fill in
deployment-specific values. The templates intentionally contain no LDAP
server, domain, usernames, group membership, or enabled dataset roots. The
service starts with empty `[dir_anon]` and `[dir_auth]` sections, but no data is
listed until deployment-specific aliases are added. Without LDAP settings,
login and authenticated paths remain unavailable. Keep `[all]` in the groups
file even when it has no members. `BRAINPI_SETTINGS_FILE` and
`BRAINPI_GROUPS_FILE` in `.env` may point Compose at differently named host
files when needed.

Compose also supplies these application-level environment variables inside the
containers:

- `BRAINPI_SETTINGS` and `BRAINPI_GROUPS`: select configuration files inside
  the container; they do not modify INI values.
- `BRAINPI_SECRET_KEY`: overrides the Flask session and signed-URL secret.
- `BRAINPI_PUBLIC_URL`: overrides `[app] url` with the browser-reachable base
  URL.
- `BRAINPI_NG_PUBLIC_URL`: overrides `[neuroglancer] url` with the
  browser-reachable frontend base URL.
- `BRAINPI_CACHE_DIR`: overrides the Unix disk-cache path.
- `BRAINPI_PYRAMIDS_DIR`: overrides all generated-pyramid roots beneath one
  writable parent directory.
- `BRAINPI_LOG_FILE`: optional file-log override. Plain production startup
  defaults to `logfile.log`; Compose passes an empty value so Docker uses its
  collected stdout logs instead.

`BRAINPI_PORT` and `BRAINPI_NG_PORT` are interpreted by Compose itself and are
not passed to the application as configuration overrides.

For production, put the services behind a TLS reverse proxy and set both public
URLs to their browser-reachable HTTPS addresses. Do not add protected datasets
until authorization has been verified for the viewer-facing data endpoints.

## Main endpoints

| Endpoint | Purpose |
|---|---|
| `/browser/` | Auth-aware HTML browser for configured path aliases. |
| `/browser_json/` | JSON representation of browser data. |
| `/path_to_html_options/?path=<source>` | Return every viewer URL BrAinPI can construct for a source path. |
| `/ng/<alias>/<dataset>` | Redirect to Neuroglancer or serve precomputed `info` and raw chunks. |
| `/osd/<alias>/<dataset>` | Render OpenSeadragon or serve PNG tiles. `/osd/.../info` is intentionally unsupported. |
| `/omezarr/<alias>/<dataset>.<variant>.ome.zarr` | Present the source as a virtual OME-Zarr v2 store. |
| `/ng_supported_filetypes/` | Return extensions accepted by the Neuroglancer endpoint. |
| `/opsd_supported_filetypes/` | Return extensions accepted by the OpenSeadragon endpoint. |

### Link discovery

The physical path passed to `path_to_html_options` must fall under a configured
`[dir_anon]` or `[dir_auth]` root.

```text
http://localhost:5001/path_to_html_options/?path=/h20/Public/world/BrainA.ims
```

The response contains nullable entries. A null value means that the source is
missing or the corresponding viewer does not support its type.

| JSON key | Meaning |
|---|---|
| `neuroglancer` | Browser link for Neuroglancer. |
| `neuroglancer_metadata` | Neuroglancer precomputed `info` URL. |
| `openseadragon` | OpenSeadragon browser link. |
| `omezarr` | Standard virtual OME-Zarr view. |
| `omezarr_metadata` | Root `.zattrs` URL for the standard view. |
| `omezarr_validator` | OME-NGFF validator URL for the standard view. |
| `omezarr_8bit` | Virtual OME-Zarr view converted to `uint8`. |
| `omezarr_8bit_metadata` | Root `.zattrs` URL for the 8-bit view. |
| `omezarr_8bit_validator` | Validator URL for the 8-bit view. |
| `omezarr_neuroglancer_optimized` | OME-Zarr view whose chunks include all logical channels. |
| `omezarr_neuroglancer_optimized_validator` | Validator URL for the channel-combined view. |
| `omezarr_8bit_neuroglancer_optimized` | Channel-combined and `uint8` OME-Zarr view. |
| `omezarr_8bit_neuroglancer_optimized_validator` | Validator URL for the combined 8-bit view. |
| `path` | Normalized source path supplied by the caller. |

## Virtual OME-Zarr behavior

The endpoint exposes a Zarr v2 group with OME multiscales metadata:

```text
<dataset>.ome.zarr/
├── .zgroup
├── .zattrs
├── .zmetadata
├── 0/.zarray
├── 0/<t>.<c>.<z>.<y>.<x>
└── 1/...
```

A virtual chunk shape can be placed before the OME-Zarr suffix:

```text
sample.ims.32x128x128.ome.zarr
```

The token represents `ZYX`; singleton `T` and `C` dimensions are added. Custom
uncompressed chunks larger than 256 MiB are rejected.

### 8-bit conversion

The 8-bit variant converts pixels from the source dtype, independent of the
minimum or maximum values in an individual chunk:

- `uint8` is unchanged
- booleans map to 0 or 255
- wider unsigned integers are reduced by bit depth
- negative signed integers clip to zero and the positive range maps to 8-bit
- floating-point values are clipped to `[0, 1]`, with non-finite values handled

OME channel metadata uses the same conversion rule as pixel chunks. This keeps
equal source values consistent across chunk boundaries.

## Neuroglancer behavior

The `/ng/` endpoint creates precomputed `info` metadata and raw chunks on
demand. Source `float16` and `float64` arrays are advertised and encoded as
`float32`, keeping metadata and payload bytes consistent.

When complete OMERO channel windows are available, the generated shader reuses
them. Otherwise, the lowest resolution is sampled and the 1st/99th percentiles
become the initial display window. Packed RGB sources receive an RGB shader;
ordinary channels receive independent visibility, LUT, gamma and color controls.

Native `.pcd` annotation and segmentation datasets are served without image
conversion. Image-style `.pcd` datasets currently require raw encoding.

## OpenSeadragon behavior

OpenSeadragon receives PNG tiles. Non-`uint8` input is converted according to
its dtype, not per-tile extrema, so identical source values produce identical
PNG values in every tile. Channel display controls use OMERO windows when
available and sampled 1st/99th percentile defaults otherwise.

There is no public OpenSeadragon metadata endpoint. The browser page receives
the internal channel and pyramid information it needs while being rendered.

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

## License

BrAinPI is distributed under the BSD 3-Clause License.
