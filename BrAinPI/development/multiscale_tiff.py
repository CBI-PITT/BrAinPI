
from tifffile import imread, imwrite, TiffFile, TiffWriter
import zarr
import numpy as np
from itertools import product

inFile = '/CBI_Hive/CBI/Mike/Slide Scanner/Alan/Eye fluo scan/_Image_/stack1/frame_t_0.tif'

with TiffFile(inFile) as tif_read:
    seriesNum = len(tif_read.series)
    with tif_read.aszarr() as img_store:
        img = zarr.open(img_store)
        tileshape = img.chunks


with TiffFile(inFile) as tif_read:
    seriesNum = len(tif_read.series)
    for series in range(seriesNum):
        with tif_read.series[series].aszarr() as img_store:
            img = zarr.open(img_store)
            tileshape = img.chunks
            break

            if series == 0:
                tif_write.write(
                    tiles(img, tileshape),
                    subifds=seriesNum - 1,
                    metadata={'axes': axes},
                    tile=tileshape[XY_index:XY_index + 2] if (tileshape[XY_index] % 16 == 0 and tileshape[
                        XY_index + 1] % 16 == 0) else fallback_tileshape,
                    shape=img.shape,
                    dtype=img.dtype,
                    compression=compression
                )
            else:
                tif_write.write(
                    tiles(img, tileshape),
                    # subfiletype=1,
                    metadata={'axes': axes},
                    tile=tileshape[XY_index:XY_index + 2] if (tileshape[XY_index] % 16 == 0 and tileshape[
                        XY_index + 1] % 16 == 0) else fallback_tileshape,
                    shape=img.shape,
                    dtype=img.dtype,
                    compression=compression
                )
