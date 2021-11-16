try:
    from ._version import version as __version__
except ImportError:
    __version__ = "unknown"


# from .flaskAPI import launchAPI
# from .dataset_info import dataset_info