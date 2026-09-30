"""Axis planning and conversion helpers shared by image loaders."""

from dataclasses import dataclass
import numpy as np


STANDARD_AXES = "TCZYX"
SUPPORTED_SOURCE_AXES = set(STANDARD_AXES + "S")


@dataclass(frozen=True)
class NormalizedSlice:
    """Resolved form of one slice against a concrete axis length."""

    start: int
    stop: int
    step: int
    length: int

    @property
    def signature(self):
        return self.start, self.stop, self.step


@dataclass(frozen=True)
class ReadPlan:
    """One logical TCZYX request mapped to a source-array read."""

    logical_key: tuple
    logical_shape: tuple
    normalized_key: tuple
    output_shape: tuple
    source_axes: str
    source_shape: tuple
    read_key: tuple
    post_read_key: tuple
    empty: bool

    @property
    def cache_signature(self):
        return tuple(item.signature for item in self.normalized_key)


def normalize_tczyx_selection(logical_key, logical_shape):
    """Resolve a five-dimensional logical selection without dropping axes."""
    logical_key = tuple(logical_key)
    logical_shape = tuple(int(size) for size in logical_shape)
    if len(logical_key) != len(STANDARD_AXES):
        raise ValueError("A logical loader key must contain five TCZYX slices")
    if len(logical_shape) != len(STANDARD_AXES):
        raise ValueError("A logical loader shape must contain five TCZYX sizes")

    normalized = []
    for selector, size in zip(logical_key, logical_shape):
        if not isinstance(selector, slice):
            raise TypeError("Logical loader selectors must be slices")
        start, stop, step = selector.indices(size)
        normalized.append(
            NormalizedSlice(start, stop, step, len(range(start, stop, step)))
        )
    return tuple(normalized)


def selection_shape(logical_key, logical_shape):
    """Return the retained TCZYX shape for a logical selection."""
    return tuple(
        item.length
        for item in normalize_tczyx_selection(logical_key, logical_shape)
    )


def tczyx_shape_from_source(source_axes, source_shape):
    """Return the logical TCZYX shape represented by a source layout."""
    source_axes = str(source_axes).upper()
    source_shape = tuple(int(size) for size in source_shape)
    if len(source_axes) != len(source_shape):
        raise ValueError(
            f"Source shape {source_shape!r} does not match axes {source_axes!r}"
        )
    if len(set(source_axes)) != len(source_axes):
        raise ValueError(f"Axes must be unique, received {source_axes!r}")
    unsupported = set(source_axes) - SUPPORTED_SOURCE_AXES
    if unsupported:
        raise ValueError(f"Unsupported source axes: {sorted(unsupported)!r}")
    if "C" in source_axes and "S" in source_axes:
        raise ValueError(
            f"Source axes {source_axes!r} contain both C and S; this layout is unsupported"
        )
    sizes = dict(zip(source_axes, source_shape))
    return tuple(
        sizes.get("C", sizes.get("S", 1))
        if axis == "C"
        else sizes.get(axis, 1)
        for axis in STANDARD_AXES
    )


def _contiguous_read_for_slice(item):
    """Return a forward source slice and post-read step for one selection."""
    if item.length == 0:
        return slice(0, 0), slice(0, 0)
    last = item.start + (item.length - 1) * item.step
    read_start = min(item.start, last)
    read_stop = max(item.start, last) + 1
    post = slice(None) if item.step == 1 else slice(None, None, item.step)
    return slice(read_start, read_stop), post


def plan_tczyx_read(logical_key, logical_shape, source_axes, source_shape):
    """Plan one backend-safe contiguous read for arbitrary source axes.

    Every source read uses forward, unit-step slices. Positive and negative
    logical steps are applied to the smallest enclosing source region after
    I/O. Missing logical axes must have size one and are handled without I/O.
    TIFF/JP2 sample axis ``S`` is treated as logical channel ``C``.
    """
    source_axes = str(source_axes).upper()
    source_shape = tuple(int(size) for size in source_shape)
    logical_shape = tuple(int(size) for size in logical_shape)
    normalized = normalize_tczyx_selection(logical_key, logical_shape)

    if len(source_axes) != len(source_shape):
        raise ValueError(
            f"Source shape {source_shape!r} does not match axes {source_axes!r}"
        )
    if len(set(source_axes)) != len(source_axes):
        raise ValueError(f"Axes must be unique, received {source_axes!r}")
    unsupported = set(source_axes) - SUPPORTED_SOURCE_AXES
    if unsupported:
        raise ValueError(f"Unsupported source axes: {sorted(unsupported)!r}")
    if "C" in source_axes and "S" in source_axes:
        raise ValueError(
            f"Source axes {source_axes!r} contain both C and S; this layout is unsupported"
        )

    logical_positions = {axis: index for index, axis in enumerate(STANDARD_AXES)}
    present_logical_axes = {
        "C" if axis == "S" else axis for axis in source_axes
    }
    for axis, logical_position in logical_positions.items():
        if axis not in present_logical_axes and logical_shape[logical_position] != 1:
            raise ValueError(
                f"Source axes {source_axes!r} omit {axis}, but logical size is "
                f"{logical_shape[logical_position]} instead of 1"
            )

    read_key = []
    post_read_key = []
    for source_axis, source_size in zip(source_axes, source_shape):
        logical_axis = "C" if source_axis == "S" else source_axis
        item = normalized[logical_positions[logical_axis]]
        expected_size = logical_shape[logical_positions[logical_axis]]
        if source_size != expected_size:
            raise ValueError(
                f"Source axis {source_axis} has size {source_size}, but logical "
                f"axis {logical_axis} has size {expected_size}"
            )
        read_slice, post_slice = _contiguous_read_for_slice(item)
        read_key.append(read_slice)
        post_read_key.append(post_slice)

    output_shape = tuple(item.length for item in normalized)
    return ReadPlan(
        logical_key=tuple(logical_key),
        logical_shape=logical_shape,
        normalized_key=normalized,
        output_shape=output_shape,
        source_axes=source_axes,
        source_shape=source_shape,
        read_key=tuple(read_key),
        post_read_key=tuple(post_read_key),
        empty=0 in output_shape,
    )


def empty_tczyx(plan, dtype):
    """Create an empty result for a read plan without touching the backend."""
    return np.empty(plan.output_shape, dtype=np.dtype(dtype))


def finalize_tczyx(source_result, plan):
    """Apply post-read stepping and canonicalize one result to TCZYX."""
    array = np.asarray(source_result)
    if array.ndim != len(plan.source_axes):
        raise ValueError(
            f"Backend returned {array.ndim} dimensions for source axes "
            f"{plan.source_axes!r}"
        )
    array = array[plan.post_read_key]
    result = samples_as_channels(array, plan.source_axes)
    if result.shape != plan.output_shape:
        raise RuntimeError(
            f"TCZYX normalization produced {result.shape}, expected "
            f"{plan.output_shape} for source axes {plan.source_axes!r}"
        )
    return result


def execute_array_read(source_array, plan, dtype=None):
    """Execute a planned NumPy/Zarr-style read and return strict TCZYX."""
    if plan.empty:
        return empty_tczyx(plan, dtype or source_array.dtype)
    return finalize_tczyx(source_array[plan.read_key], plan)


def samples_as_channels(array, axes):
    """Return ``array`` in five-dimensional ``TCZYX`` order.

    TIFF's ``S`` (samples-per-pixel) axis is folded into logical ``C``.
    Missing standard axes are inserted as singleton dimensions. Sources that
    declare both independent channels (``C``) and samples (``S``) are rejected
    because that flattened channel selection cannot always be represented by
    one exact source read.

    Args:
        array: Source pixel array.
        axes: Source axis labels matching ``array.ndim``.

    Returns:
        numpy.ndarray: Data in five-dimensional ``TCZYX`` order.

    Raises:
        ValueError: If axes are invalid or contain both ``C`` and ``S``.
    """
    array = np.asarray(array)
    axes = str(axes).upper()

    if array.ndim != len(axes):
        raise ValueError(
            f"Array has {array.ndim} dimensions but axes {axes!r} has {len(axes)}"
        )
    if len(set(axes)) != len(axes):
        raise ValueError(f"Axes must be unique, received {axes!r}")
    unsupported = set(axes) - SUPPORTED_SOURCE_AXES
    if unsupported:
        raise ValueError(f"Unsupported source axes: {sorted(unsupported)!r}")
    if "C" in axes and "S" in axes:
        raise ValueError(
            f"Source axes {axes!r} contain both C and S; this layout is unsupported"
        )

    working_axes = list(axes)
    for axis in STANDARD_AXES:
        if axis not in working_axes:
            array = np.expand_dims(array, axis=-1)
            working_axes.append(axis)

    if "S" in working_axes:
        target_axes = "TCSZYX"
    else:
        target_axes = STANDARD_AXES
    array = array.transpose(tuple(working_axes.index(axis) for axis in target_axes))

    if "S" in target_axes:
        timepoints, channels, samples, depth, height, width = array.shape
        array = array.reshape(
            timepoints, channels * samples, depth, height, width
        )
    return array
