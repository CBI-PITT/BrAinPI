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

The current development environment uses Python 3.12 on Linux. The package
metadata declares Python 3.8 or newer, but deployments should use a clean,
pinned environment and run the test suite before release.

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
- loader sections: generated-pyramid locations and size limits

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
  -w 20 \
  --threads 2 \
  --timeout 1800
```

Worker count, cache size and file-descriptor limits must be sized for the
deployment. Each worker owns its loader objects; the configured disk cache is
shared. Run BrAinPI behind a reverse proxy in production and configure request
timeouts and maximum response sizes for large chunks.

Recommand to use one seperated Neuroglancer instance, configure it in setting.ini
```bash
python BrAinPI/neuroglancer_server.py
```

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
