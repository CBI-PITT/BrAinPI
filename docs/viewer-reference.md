# Viewer and API reference

[Back to installation and startup](../README.md#installation)

## Main endpoints

| Endpoint | Purpose |
|---|---|
| `/healthz` | Lightweight application liveness endpoint. |
| `/browser/` | Auth-aware HTML browser for configured path aliases. |
| `/browser_json/` | JSON representation of browser data. |
| `/browser/storage` | On-demand cache/pyramid usage; authenticated `[all]` members only. |
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
