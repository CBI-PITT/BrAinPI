"""Axis conversion helpers shared by image loaders."""

import numpy as np


STANDARD_AXES = "TCZYX"
SUPPORTED_SOURCE_AXES = set(STANDARD_AXES + "S")


def plan_tczyx_source_read(logical_key, source_axes, source_shape):
    """Map one logical ``TCZYX`` key to one source-array read.

    The returned channel selector is only needed for the implicit singleton
    channel of sources without C/S. Sources containing both C and S are
    deliberately unsupported because their flattened channel selections cannot
    always be represented by one exact source slice.

    Args:
        logical_key: Five slices in logical ``TCZYX`` order.
        source_axes: Axis labels describing the source array.
        source_shape: Shape of the source array.

    Returns:
        tuple: ``(source_key, post_channel_key)``. ``source_key`` performs the
        only source read; ``post_channel_key`` selects an implicit singleton
        channel when the source has neither ``C`` nor ``S``.

    Raises:
        ValueError: If axes are inconsistent, duplicated, unsupported, or
        contain both ``C`` and ``S``.
    """
    source_axes = str(source_axes).upper()
    source_shape = tuple(source_shape)
    logical_key = tuple(logical_key)
    if len(logical_key) != len(STANDARD_AXES):
        raise ValueError("A logical loader key must contain five TCZYX slices")
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

    logical = dict(zip(STANDARD_AXES, logical_key))
    source_key = [slice(None)] * len(source_axes)
    for axis in "TZYX":
        if axis in source_axes:
            source_key[source_axes.index(axis)] = logical[axis]

    c_position = source_axes.find("C")
    s_position = source_axes.find("S")
    channel_key = logical["C"]
    post_channel_key = None

    if c_position >= 0:
        # Ordinary channel data: push the logical channel slice directly into C.
        source_key[c_position] = channel_key
    elif s_position >= 0:
        # Packed samples such as RGB YXS: logical C maps directly to source S.
        source_key[s_position] = channel_key
    else:
        # A source without C/S has one implicit channel. Apply the logical
        # channel key after singleton axes are inserted.
        post_channel_key = channel_key

    return tuple(source_key), post_channel_key


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
