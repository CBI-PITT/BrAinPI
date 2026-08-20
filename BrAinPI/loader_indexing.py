"""Shared index normalization for BrAinPI dataset loaders."""

from numbers import Integral


def _integer_as_slice(index):
    """Preserve a loader dimension while retaining normal negative indexing."""
    index = int(index)
    return slice(index, None if index == -1 else index + 1)


def normalize_data_key(key, ndim):
    """Return a fixed-length tuple of slices for a loader data request.

    Loader results intentionally retain dimensions, so integer indices become
    one-element slices. Slice objects otherwise retain their standard Python
    meaning; in particular, ``:stop`` remains ``slice(None, stop)``.

    Args:
        key: Integer, slice, ellipsis, or tuple of those values.
        ndim: Required number of output dimensions.

    Returns:
        tuple: Exactly ``ndim`` slice objects with missing axes filled by
        ``slice(None)``.

    Raises:
        IndexError: If the key has multiple ellipses or too many dimensions.
        TypeError: If the key contains an unsupported index type.
    """
    if isinstance(key, (Integral, slice)) or key is Ellipsis:
        values = [key]
    elif isinstance(key, tuple):
        values = list(key)
    else:
        raise TypeError(f"Unsupported key type: {type(key).__name__}")

    ellipsis_count = values.count(Ellipsis)
    if ellipsis_count > 1:
        raise IndexError("An index can only contain a single ellipsis")
    if ellipsis_count:
        ellipsis_position = values.index(Ellipsis)
        missing = ndim - (len(values) - 1)
        if missing < 0:
            raise IndexError(f"Too many indices for a {ndim}-dimensional dataset")
        values[ellipsis_position : ellipsis_position + 1] = [slice(None)] * missing

    if len(values) > ndim:
        raise IndexError(f"Too many indices for a {ndim}-dimensional dataset")

    normalized = []
    for value in values:
        if isinstance(value, Integral):
            normalized.append(_integer_as_slice(value))
        elif isinstance(value, slice):
            normalized.append(value)
        else:
            raise TypeError(
                "Loader indices must be integers, slices, or an ellipsis; "
                f"received {type(value).__name__}"
            )

    normalized.extend([slice(None)] * (ndim - len(normalized)))
    return tuple(normalized)
