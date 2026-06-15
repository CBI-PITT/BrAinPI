# OME-Zarr S3 Prototype Notes

This note records the current prototype work for a read-only S3-shaped facade over the existing virtual OME-Zarr endpoint.

## Goal

Expose the same virtual OME-Zarr content currently served from `/omezarr/` through a minimal S3-like API for clients that require S3 semantics instead of plain HTTPS object URLs.

## What Was Implemented

### 1. Shared virtual OME-Zarr object access

The existing `/omezarr/` route logic in `BrAinPI/ome_zarr_ep.py` was factored so the object generation can be reused by multiple frontends.

Added helpers:

- `get_omezarr_request_info(config, request_path)`
- `get_omezarr_object(config, request_path)`

These normalize the virtual OME-Zarr path and return either:

- chunk bytes
- `.zarray` JSON bytes
- `.zattrs` JSON bytes
- `.zgroup` JSON bytes

The existing `setup_omezarr()` route now uses `get_omezarr_object()` instead of duplicating the object-generation path.

### 2. Read-only S3 facade

Added new module: `BrAinPI/ome_zarr_s3.py`

New Flask endpoint setup:

- `setup_omezarr_s3(app, config)`

Registered routes:

- `GET /omezarrs3/`
- `GET /omezarrs3/<bucket>`
- `GET /omezarrs3/<bucket>/`
- `GET /omezarrs3/<bucket>/<path:key>`
- `HEAD /omezarrs3/<bucket>/<path:key>`

Implemented prototype behavior:

- list buckets
- `ListObjectsV2`-style listing with `?list-type=2`
- object `GET`
- object `HEAD`
- path-style bucket addressing only
- single virtual bucket only
- read-only only

### 3. App wiring

Wired the facade into startup in `BrAinPI/brain_api_main.py`.

### 4. Coordination/discovery link

Added `paths["omezarr_s3"]` in `BrAinPI/coordination_endpoints.py` so callers using `path_to_html_options()` can discover the S3-style URL.

## Bucket Name

`ome_zarr_s3.get_omezarr_s3_bucket(config)` resolves the bucket name:

- from `config.settings["s3"]["bucket"]` if present
- otherwise from a sanitized app name
- fallback default: `brainpi`

## Important Current Limits

This is a prototype, not full S3 emulation.

Not implemented yet:

- SigV4 authentication/verification
- write APIs
- multipart upload
- delete APIs
- broad AWS compatibility guarantees
- object `Range` handling
- rich S3 error XML
- accurate object size/etag metadata in listings
- pagination / continuation tokens for long listings

## Known Design Notes

- The facade reuses `/omezarr/` object generation instead of materializing real Zarr objects on disk.
- Listing behavior is synthetic.
- Before a dataset root is selected, the bucket listing walks the configured alias/filesystem structure and presents candidate dataset roots as `*.ome.zarr/` prefixes.
- Once inside a dataset root, listing is synthesized from dataset metadata and chunk grid shape.
- The current chunk object path representation is slash-separated because the generated `.zarray` advertises `dimension_separator="/"`.

## Files Touched

- `BrAinPI/ome_zarr_ep.py`
- `BrAinPI/ome_zarr_s3.py`
- `BrAinPI/brain_api_main.py`
- `BrAinPI/coordination_endpoints.py`

## Verification Performed

Ran:

```bash
python -m py_compile BrAinPI/ome_zarr_ep.py BrAinPI/ome_zarr_s3.py BrAinPI/coordination_endpoints.py BrAinPI/brain_api_main.py
```

This passed.

A direct runtime import smoke test in the current shell environment failed because `flask` was not installed in that Python environment, so no live HTTP request verification was completed from this session.

## Recommended Next Steps

1. Test the new facade in the actual BrAinPI runtime environment where Flask is installed.
2. Confirm the resolved bucket name and URL shape from `path_to_html_options()`.
3. Probe the facade with a real S3 client or HortaCloud request trace.
4. Add request logging around `omezarrs3` to capture the exact S3 operations HortaCloud performs.
5. Implement only the missing S3 behaviors required by the real client.
