"""Standalone NIfTI-to-Zarr worker used by the Gunicorn application."""

import argparse

from niizarr import nii2zarr


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate one NIfTI-Zarr pyramid")
    parser.add_argument("input_path")
    parser.add_argument("output_path")
    parser.add_argument("--no-time", action="store_true")
    args = parser.parse_args(argv)
    nii2zarr(args.input_path, args.output_path, no_time=args.no_time)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
