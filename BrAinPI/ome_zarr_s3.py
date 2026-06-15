import math
import os
import xml.etree.ElementTree as ET

from flask import Response, abort, request
from flask_cors import cross_origin

import utils
from ome_zarr_ep import (
    exts,
    get_omezarr_object,
    get_omezarr_request_info,
    open_omezarr_dataset,
    chunks_combine_channels,
)


def get_omezarr_s3_bucket(config):
    """Return the bucket name used by the virtual S3 facade."""
    try:
        return config.settings.get("s3", "bucket")
    except Exception:
        app_name = config.settings.get("app", "name")
        bucket = app_name.strip().lower().replace(" ", "-")
        return bucket or "brainpi"


def split_virtual_dataset_key(key):
    """Split a facade key into dataset-root and object suffix."""
    for ext in sorted(exts, key=len, reverse=True):
        search_from = 0
        while True:
            index = key.find(ext, search_from)
            if index == -1:
                break
            end = index + len(ext)
            if end == len(key) or key[end] == "/":
                dataset_root = key[:end]
                suffix = ""
                if end < len(key) and key[end] == "/":
                    suffix = key[end + 1 :]
                return dataset_root, suffix
            search_from = index + 1
    return None, None


def resolve_bucket_prefix(config, prefix):
    """Map a bucket prefix to either alias browsing or a virtual dataset root."""
    prefix = prefix or ""
    dataset_root, suffix = split_virtual_dataset_key(prefix)
    if dataset_root is not None:
        return {
            "kind": "dataset",
            "dataset_root": dataset_root,
            "suffix": suffix,
        }

    stripped = prefix.rstrip("/")
    if stripped == "":
        return {"kind": "aliases"}

    path_map = utils.get_path_map(config.settings, user_authenticated=True)
    path_parts = [part for part in stripped.split("/") if part]
    alias = path_parts[0]
    if alias not in path_map:
        return {"kind": "empty"}

    filesystem_path = os.path.join(path_map[alias], *path_parts[1:])
    return {
        "kind": "browse",
        "prefix": prefix,
        "alias": alias,
        "filesystem_path": filesystem_path,
    }


def get_dataset_listing_context(config, dataset_root):
    """Load dataset metadata needed for synthetic S3 listings."""
    request_path = f"/omezarr/{dataset_root}"
    request_info = get_omezarr_request_info(config, request_path)
    datapath = open_omezarr_dataset(config, request_info["datapath"])
    dataset = config.opendata[datapath]
    return request_info, datapath, dataset


def get_resolution_chunk_counts(metadata, resolution, combine_channels):
    """Return the chunk grid shape for a resolution level."""
    if combine_channels:
        chunk_size = chunks_combine_channels(metadata, resolution)
    else:
        chunk_size = metadata[(resolution, 0, 0, "chunks")]

    dataset_shape = (
        metadata["TimePoints"],
        metadata["Channels"],
        *metadata[(resolution, 0, 0, "shape")][-3:],
    )
    counts = tuple(
        math.ceil(size / chunk) for size, chunk in zip(dataset_shape, chunk_size)
    )
    return counts


def iter_chunk_keys(counts, prefix_parts=()):
    """Yield slash-separated chunk keys for the requested suffix prefix."""
    depth = len(prefix_parts)
    if depth == len(counts):
        yield "/".join(prefix_parts)
        return

    start_index = 0
    if depth < len(prefix_parts):
        start_index = int(prefix_parts[depth])

    if depth < len(prefix_parts):
        next_parts = prefix_parts[: depth + 1]
        yield from iter_chunk_keys(counts, next_parts)
        return

    for value in range(counts[depth]):
        yield from iter_chunk_keys(counts, prefix_parts + (str(value),))


def build_chunk_entries(base_prefix, counts, suffix_parts, delimiter):
    """Build object and prefix entries beneath one resolution directory."""
    objects = []
    prefixes = []
    if len(suffix_parts) > len(counts):
        return objects, prefixes

    for index, part in enumerate(suffix_parts):
        if not part.isdigit():
            return objects, prefixes
        if int(part) >= counts[index]:
            return objects, prefixes

    if delimiter == "/":
        if len(suffix_parts) == len(counts):
            objects.append(base_prefix + "/".join(suffix_parts))
            return objects, prefixes

        for value in range(counts[len(suffix_parts)]):
            prefixes.append(base_prefix + "/".join((*suffix_parts, str(value))) + "/")
        return objects, prefixes

    if len(suffix_parts) == len(counts):
        objects.append(base_prefix + "/".join(suffix_parts))
        return objects, prefixes

    remaining_ranges = [
        range(counts[index]) for index in range(len(suffix_parts), len(counts))
    ]
    stack = [suffix_parts]
    while stack:
        current = stack.pop()
        if len(current) == len(counts):
            objects.append(base_prefix + "/".join(current))
            continue
        next_index = len(current)
        for value in reversed(range(counts[next_index])):
            stack.append(current + (str(value),))
    return objects, prefixes


def list_alias_prefixes(config):
    """List top-level aliases as CommonPrefixes."""
    path_map = utils.get_path_map(config.settings, user_authenticated=True)
    return [], [f"{alias}/" for alias in path_map]


def list_browser_prefix(config, prefix, filesystem_path):
    """List filesystem children until a virtual dataset root is chosen."""
    if not os.path.isdir(filesystem_path):
        return [], []

    prefix = prefix.rstrip("/")
    base_prefix = f"{prefix}/" if prefix else ""
    objects = []
    prefixes = []
    with os.scandir(filesystem_path) as entries:
        for entry in sorted(entries, key=lambda item: item.name):
            if entry.name.startswith("."):
                continue
            if entry.is_dir():
                prefixes.append(f"{base_prefix}{entry.name}/")
            else:
                prefixes.append(f"{base_prefix}{entry.name}.ome.zarr/")
    return objects, prefixes


def list_dataset_prefix(config, dataset_root, suffix, delimiter):
    """List synthetic Zarr keys inside a virtual dataset root."""
    request_info, _, dataset = get_dataset_listing_context(config, dataset_root)
    metadata = dataset.metadata
    combine_channels = request_info["isNeuroGlancer"]

    suffix = suffix.strip("/")
    suffix_parts = tuple(part for part in suffix.split("/") if part)
    base_prefix = f"{dataset_root}/"

    objects = []
    prefixes = []

    if not suffix_parts:
        objects.extend([f"{dataset_root}/.zattrs", f"{dataset_root}/.zgroup"])
        for resolution in range(metadata["ResolutionLevels"]):
            prefixes.append(f"{dataset_root}/{resolution}/")
        return objects, prefixes

    if len(suffix_parts) == 1 and suffix_parts[0].isdigit():
        resolution = int(suffix_parts[0])
        if resolution >= metadata["ResolutionLevels"]:
            return objects, prefixes
        resolution_prefix = f"{base_prefix}{resolution}/"
        counts = get_resolution_chunk_counts(metadata, resolution, combine_channels)
        objects.append(f"{resolution_prefix}.zarray")
        chunk_objects, chunk_prefixes = build_chunk_entries(
            resolution_prefix,
            counts,
            tuple(),
            delimiter,
        )
        objects.extend(chunk_objects)
        prefixes.extend(chunk_prefixes)
        return objects, prefixes

    if suffix_parts[0].isdigit():
        resolution = int(suffix_parts[0])
        if resolution >= metadata["ResolutionLevels"]:
            return objects, prefixes
        resolution_prefix = f"{base_prefix}{resolution}/"
        counts = get_resolution_chunk_counts(metadata, resolution, combine_channels)
        if suffix_parts[1:] == (".zarray",):
            objects.append(f"{resolution_prefix}.zarray")
            return objects, prefixes
        chunk_objects, chunk_prefixes = build_chunk_entries(
            resolution_prefix,
            counts,
            suffix_parts[1:],
            delimiter,
        )
        objects.extend(chunk_objects)
        prefixes.extend(chunk_prefixes)
        return objects, prefixes

    if suffix_parts == (".zattrs",):
        objects.append(f"{dataset_root}/.zattrs")
    elif suffix_parts == (".zgroup",):
        objects.append(f"{dataset_root}/.zgroup")
    return objects, prefixes


def get_list_entries(config, prefix, delimiter):
    """Return object keys and common prefixes for a bucket listing request."""
    resolved = resolve_bucket_prefix(config, prefix)
    if resolved["kind"] == "aliases":
        return list_alias_prefixes(config)
    if resolved["kind"] == "browse":
        return list_browser_prefix(
            config, resolved["prefix"], resolved["filesystem_path"]
        )
    if resolved["kind"] == "dataset":
        return list_dataset_prefix(
            config, resolved["dataset_root"], resolved["suffix"], delimiter
        )
    return [], []


def build_list_bucket_result(bucket, prefix, delimiter, max_keys, objects, prefixes):
    """Serialize a minimal ListObjectsV2 XML payload."""
    root = ET.Element(
        "ListBucketResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/"
    )
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "KeyCount").text = str(len(objects) + len(prefixes))
    ET.SubElement(root, "MaxKeys").text = str(max_keys)
    ET.SubElement(root, "Delimiter").text = delimiter or ""
    ET.SubElement(root, "IsTruncated").text = "false"

    for key in objects:
        contents = ET.SubElement(root, "Contents")
        ET.SubElement(contents, "Key").text = key
        ET.SubElement(contents, "LastModified").text = "1970-01-01T00:00:00.000Z"
        ET.SubElement(contents, "ETag").text = '"brainpi-virtual"'
        ET.SubElement(contents, "Size").text = "0"
        ET.SubElement(contents, "StorageClass").text = "STANDARD"

    for common_prefix in prefixes:
        prefix_element = ET.SubElement(root, "CommonPrefixes")
        ET.SubElement(prefix_element, "Prefix").text = common_prefix

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def build_bucket_list_response(config, bucket):
    """Handle ListObjectsV2 requests for the virtual bucket."""
    prefix = request.args.get("prefix", "")
    delimiter = request.args.get("delimiter", "/")
    max_keys = int(request.args.get("max-keys", "1000"))

    objects, prefixes = get_list_entries(config, prefix, delimiter)
    objects = sorted(objects)[:max_keys]
    prefixes = sorted(prefixes)
    remaining = max_keys - len(objects)
    if remaining < len(prefixes):
        prefixes = prefixes[:remaining]

    xml_bytes = build_list_bucket_result(
        bucket, prefix, delimiter, max_keys, objects, prefixes
    )
    return Response(response=xml_bytes, status=200, mimetype="application/xml")


def build_list_buckets_response(config):
    """Return a minimal bucket list response for the facade root."""
    bucket = get_omezarr_s3_bucket(config)
    root = ET.Element(
        "ListAllMyBucketsResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/"
    )
    buckets = ET.SubElement(root, "Buckets")
    bucket_element = ET.SubElement(buckets, "Bucket")
    ET.SubElement(bucket_element, "Name").text = bucket
    ET.SubElement(bucket_element, "CreationDate").text = "1970-01-01T00:00:00.000Z"
    return Response(
        response=ET.tostring(root, encoding="utf-8", xml_declaration=True),
        status=200,
        mimetype="application/xml",
    )


def setup_omezarr_s3(app, config):
    """Register a minimal read-only S3 facade for virtual OME-Zarr data."""

    bucket_name = get_omezarr_s3_bucket(config)

    def omezarr_s3_entry(bucket=None, key=""):
        if bucket is None:
            return build_list_buckets_response(config)

        if bucket != bucket_name:
            abort(404)

        if request.method == "GET" and (
            request.args.get("list-type") == "2" or key == ""
        ):
            return build_bucket_list_response(config, bucket)

        if key == "":
            abort(404)

        request_path = f"/omezarr/{key}"
        try:
            body, mimetype = get_omezarr_object(config, request_path)
        except FileNotFoundError:
            abort(404)

        content_length = len(body)
        if request.method == "HEAD":
            body = b""

        response = Response(response=body, status=200, mimetype=mimetype)
        response.headers["Content-Length"] = str(content_length)
        response.headers["x-amz-bucket-region"] = "us-east-1"
        return response

    omezarr_s3_entry = cross_origin(
        allow_headers=[
            "Content-Type",
            "Authorization",
            "x-amz-content-sha256",
            "x-amz-date",
        ]
    )(omezarr_s3_entry)
    omezarr_s3_entry = app.route(
        "/omezarrs3/", defaults={"bucket": None, "key": ""}, methods=["GET"]
    )(omezarr_s3_entry)
    omezarr_s3_entry = app.route(
        "/omezarrs3/<bucket>", defaults={"key": ""}, methods=["GET"]
    )(omezarr_s3_entry)
    omezarr_s3_entry = app.route(
        "/omezarrs3/<bucket>/", defaults={"key": ""}, methods=["GET"]
    )(omezarr_s3_entry)
    omezarr_s3_entry = app.route(
        "/omezarrs3/<bucket>/<path:key>", methods=["GET", "HEAD"]
    )(omezarr_s3_entry)
    return app
