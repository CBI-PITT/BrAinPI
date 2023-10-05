

from diskcache import FanoutCache
import os
import config_tools

settings = config_tools.get_config()
def get_cache():
    ## Setup cache location based on OS type
    ## Optional situations like machine name can be used to customize
    if os.name == 'nt':
        cacheLocation = settings.get('disk_cache', 'location_win')
    else:
        cacheLocation = settings.get('disk_cache', 'location_unix')
        # cacheLocation = None

    if cacheLocation is not None:
        # Instantiate class that will manage all open datasets
        # This will remain in the global env and be accessed by multiple route methods
        cacheLocation=cacheLocation
        cacheSizeGB=settings.getint('disk_cache', 'cacheSizeGB')
        cacheSizeBytes = cacheSizeGB * (1024 ** 3)
        evictionPolicy=settings.get('disk_cache', 'evictionPolicy')
        shards=settings.getint('disk_cache', 'shards')
        timeout=settings.getfloat('disk_cache', 'timeout')

        return FanoutCache(cacheLocation, shards=shards, timeout=timeout,
                                     size_limit=cacheSizeBytes, eviction_policy=evictionPolicy)