


## BOTO3 Way to do dir and files from s3
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from functools import lru_cache
import time
import os

from cache_tools import get_cache
# get cache one time to be reused for performance
cache = get_cache()

def get_ttl_hash(hours=24):
    """Return the same value withing `hours` time period"""
    seconds = hours*60*60
    return round(time.time() / seconds)

client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
paginator = client.get_paginator('list_objects_v2')
@lru_cache(maxsize=10000)
def s3_get_dir_contents(path, recursive=False, ttl_hash=get_ttl_hash(hours=24)):
    # Get from diskcache if exists
    if cache is not None:
        key = f's3_get_dir_contents_{path}{str(recursive)}'
        out = cache.get(key)
        if out is not None:
            print('GOT FROM CACHE')
            return out

    bucket, path_split = s3_get_bucket_and_path_parts(path)
    # print(bucket)
    if len(path_split) > 1:
        prefix = '/'.join(path_split[1:]) + '/' # Make sure you provide / in the end
        root = f'{bucket}/{prefix}'
    else:
        prefix = ''  # Root prefix
        root = f'{bucket}'
    # client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    # paginator = client.get_paginator('list_objects_v2')
    if recursive:
        pages = paginator.paginate(Bucket=bucket, MaxKeys=1000)
    else:
        pages = paginator.paginate(Bucket=bucket, MaxKeys=1000, Prefix=prefix, Delimiter='/')
    dirs = ()
    files = ()
    files_sizes = ()
    files_modified = ()
    current_page = 0
    for page in pages:
        current_page += 1
        print(f'Page {current_page}')
        if 'CommonPrefixes' in page:
            dirs += tuple((x.get('Prefix')[:-1] for x in page.get('CommonPrefixes')))
        if 'Contents' in page:
            files += tuple((x.get('Key') for x in page.get('Contents')))
            files_sizes += tuple((x.get('Size') for x in page.get('Contents')))
            files_modified += tuple((x.get('LastModified') for x in page.get('Contents'))) #datetime objects
    r = root.replace(bucket + '/','')
    dirs = tuple((x.replace(r,'') for x in dirs))
    files = tuple((x.replace(r,'') for x in files))
    print(len(dirs) + len(files))
    out = root, dirs, files, files_sizes, files_modified
    if cache is not None:
        print('SENT TO CACHE')
        cache.set(key, out, expire=3600, tag='S3')  # Expire after day 86400
    return out

def s3_get_bucket_and_path_parts(path):
    path = s3_clean_path(path)
    path_split = path.split('/')
    # print(path_split)
    if isinstance(path_split, str):
        path_split = [path_split]
    bucket = path_split[0]
    return bucket, path_split
def s3_clean_path(path):
    if 's3://' in path.lower():
        path = path[5:]
    elif path.startswith('/'):
        path = path[1:]
    if path.endswith('/'):
        path = path[:-1]
    return path

def s3_path_split(path):
    path = s3_clean_path(path)
    p, f = os.path.split(path)
    if p == '':
        return f,p
    else:
        return p,f
def s3_isfile(path):
    # print(path)
    p,f = s3_path_split(path)
    _, _, files, _, _ = s3_get_dir_contents(p)
    # print(f in files)
    # print(f'''
    # ##########################################
    # ISFILE {f in files}
    # #########################################
    # ''')
    return f in files

def s3_isdir(path):
    # print(path)
    p,f = s3_path_split(path)
    if f == '':
        return True
    _, dirs, _, _, _ = s3_get_dir_contents(p)
    # print(f in dirs)
    # print(f'''
    # ##########################################
    # ISDIR {f in dirs}
    # #########################################
    # ''')
    return f in dirs



def s3_catch_exceptions_retry(func):
    def wrapper(*args, **kwargs):
        tries = 0
        try:
            return func(*args, **kwargs)
        except botocore.exceptions.SSLError as e:
            print(e)
            tries+=1
            if tries == 2:
                raise(e)
    return wrapper


#############################################################################
## ZARR READ ONLY STORE BASED ON BOTO3
## NEEDED to get around s3fs async causing issues with gevent workers
#############################################################################

import os
import time

from zarr._storage.store import Store

import boto3
from botocore import UNSIGNED, exceptions
from botocore.client import Config
import functools

class s3_boto_store(Store):
    '''
    READ ONLY
    '''

    def __init__(self, path, normalize_keys=False, dimension_separator='/', s3_cred='anon', mode='r'):

        self.raw_path = path
        self.bucket, path_split = self.s3_get_bucket_and_path_parts(self.raw_path)
        if len(path_split) > 1:
            self.zarr_dir = '/'.join(path_split[1:])
        else:
            self.zarr_dir = ''

        # if os.path.exists(path) and not os.path.isdir(path):
        #     raise FSPathExistNotDir(path)

        self.normalize_keys = normalize_keys
        if dimension_separator is None:
            dimension_separator = "/"
        elif dimension_separator != "/":
            raise ValueError(
                "s3_boto_store only supports '/' as dimension_separator")
        self._dimension_separator = dimension_separator
        self.mode = mode
        assert self.mode == 'r', "s3_boto_store only supports read_only mode (mode='r')"

        # Form client
        assert s3_cred.lower() == 'anon', 'Currently only anonymous connections to s3 are supported'
        self.client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        self.paginator = self.client.get_paginator('list_objects_v2')

        from cache_tools import get_cache
        self.cache = get_cache()

    def __hash__(self):
        return hash(
            f'{self.raw_path}{self.bucket}{self.zarr_dir}'
        )

    def __getstate__(self):
        return (self.raw_path, self.bucket, self.zarr_dir, self.normalize_keys, self._dimension_separator,
                self.mode)

    def __setstate__(self, state):
        (self.raw_path, self.bucket, self.zarr_dir, self.normalize_keys, self._dimension_separator,
                self.mode) = state


    @s3_catch_exceptions_retry
    @functools.lru_cache(maxsize=10000)
    def s3_get_dir_contents(self, path, recursive=False):
        # Get from diskcache if exists
        if self.cache is not None:
            key = f's3_get_dir_contents_{path}{str(recursive)}'
            out = self.cache.get(key)
            if out is not None:
                print('GOT FROM CACHE')
                return out

        print('INSIDE S#_GET_DIR_COTENTS')
        bucket, path_split = self.s3_get_bucket_and_path_parts(path)
        # print(bucket)
        if len(path_split) > 1:
            prefix = '/'.join(path_split[1:]) + '/'  # Make sure you provide / in the end
            root = f'{bucket}/{prefix}'
        else:
            prefix = ''  # Root prefix
            root = f'{bucket}'
        # client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        # paginator = client.get_paginator('list_objects_v2')
        if recursive:
            pages = self.paginator.paginate(Bucket=bucket, MaxKeys=1000)
        else:
            pages = self.paginator.paginate(Bucket=bucket, MaxKeys=1000, Prefix=prefix, Delimiter='/')

        dirs = ()
        files = ()
        files_sizes = ()
        files_modified = ()
        for page in pages:
            if 'CommonPrefixes' in page:
                dirs += tuple([x.get('Prefix')[:-1] for x in page.get('CommonPrefixes')])
            if 'Contents' in page:
                files += tuple((x.get('Key') for x in page.get('Contents')))
                files_sizes += tuple((x.get('Size') for x in page.get('Contents')))
                files_modified += tuple((x.get('LastModified') for x in page.get('Contents')))  # datetime objects
        r = root.replace(bucket + '/', '')
        dirs = tuple((x.replace(r,'') for x in dirs))
        files = tuple((x.replace(r,'') for x in files))
        out = root, dirs, files, files_sizes, files_modified
        if self.cache is not None:
            print('SENT TO CACHE')
            self.cache.set(key, out, expire=3600, tag='S3')  # Expire after day 86400
        return out
        # return root, tuple(dirs), tuple(files), tuple(files_sizes), tuple(files_modified)

    def s3_get_bucket_and_path_parts(self, path):
        path = self.s3_clean_path(path)
        path_split = path.split('/')
        # print(path_split)
        if isinstance(path_split, str):
            path_split = [path_split]
        bucket = path_split[0]
        return bucket, path_split

    def s3_clean_path(self, path):
        if 's3://' in path.lower():
            path = path[5:]
        elif path.startswith('/'):
            path = path[1:]
        if path.endswith('/'):
            path = path[:-1]
        return path

    def s3_path_split(self, path):
        path = self.s3_clean_path(path)
        p, f = os.path.split(path)
        if p == '':
            return f, p
        else:
            return p, f

    def s3_isfile(self, path):
        # print(path)
        p, f = self.s3_path_split(path)
        # print(f's3_isfile {p} and {f}')
        print(f's3_isfile {path}')
        # print(f's3_isfile TYPE {type(path)}')
        _, _, files, _, _ = self.s3_get_dir_contents(p)
        # print(f in files)
        # print(f'''
        # ##########################################
        # ISFILE {f in files}
        # #########################################
        # ''')
        return f in files

    def s3_isdir(self, path):
        # print(path)
        p, f = self.s3_path_split(path)
        if f == '':
            return True
        _, dirs, _, _, _ = self.s3_get_dir_contents(p)
        # print(f in dirs)
        # print(f'''
        # ##########################################
        # ISDIR {f in dirs}
        # #########################################
        # ''')
        return f in dirs

    def get_file_size(self, path):
        if self.s3_isfile(path):
            p, f = self.s3_path_split(path)
            parent, _, files, files_sizes, _ = self.s3_get_dir_contents(p)
            idx = files.index(f)
            return files_sizes[idx]
        else:
            return 0

    def num_dirs_files(self, path, skip_s3=True):
        # skip_s3 if passed to get_dir_contents will ignore contents
        # Getting this information from large remote s3 stores can be very slow on the first try
        # however after caching, it is much faster.
        _, dirs, files = self.get_dir_contents(path, skip_s3=skip_s3)
        return len(dirs), len(files)

    def get_mod_time(self, path):
        if self.s3_isfile(path):
            p, f = self.s3_path_split(path)
            parent, _, files, _, files_modified = self.s3_get_dir_contents(p)
            idx = files.index(f)
            return files_modified[idx]
        else:
            return datetime.datetime.now()

    def __del__(self):
        pass

    def _normalize_key(self, key):
        return key.lower() if self.normalize_keys else key


    def get_full_path_from_key(self, key):
        if key[0] == '/':
            return f'{self.bucket}/{self.zarr_dir}{key}'
        return f'{self.bucket}/{self.zarr_dir}/{key}'

    def __getitem__(self, key):
        # print(f'GETTING {key}')
        # print('In Get Item')
        # key = self._normalize_key(key)
        filepath = self.get_full_path_from_key(key)

        if self.s3_isfile(filepath):
            # print(f's3_isfile {filepath}')
            try:
                return self._fromfile(filepath)
                # print(f's3_isfile RETURNED {a}')
                # return a
            except:
                raise KeyError(key)
        else:
            raise KeyError(key)

    def _fromfile(self, filepath):
        filepath = self.s3_clean_path(filepath)
        object_name = filepath.replace(self.bucket + '/','')
        # print(f'__fromFILE {object_name}')
        # file = io.BytesIO()
        from io import BytesIO
        with BytesIO() as f:
            print(f'LEN {f}')
            self.client.download_fileobj(self.bucket, object_name, f)
            return f.getvalue()
            # print(f'{f.getvalue()}')

        # print(f)
        # return f.seek(0).read()

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __contains__(self, key):
        filepath = self.get_full_path_from_key(key)
        print(f'__CONTAINS__ {filepath}')
        # print(f'__CONTAINS__ TYPE {type(filepath)}')
        if self.s3_isfile(filepath):
            return True
        return False

    def __eq__(self, other):
        return isinstance(other, s3_boto_store) and \
                self.bucket == other.bucket and \
                self.zarr_dir == other.zarr_dir

    def keys(self):
        if os.path.exists(self.path):
            yield from self._keys_fast()

    def _keys_fast(self, walker=os.walk):
        for dirpath, _, filenames in walker(self.path):
            dirpath = os.path.relpath(dirpath, self.path)
            if dirpath == os.curdir:
                for f in filenames:
                    yield f
            else:
                # dirpath = dirpath.replace("\\", "/")
                for f in filenames:
                    basefile, ext = os.path.splitext(f)
                    if ext == self.container_ext:
                        names = self._get_zip_keys(os.path.join(self.path, dirpath, f))
                        # Keys are stored in h5 with '.' separator, replace with appropriate separator
                        names = (x.replace('.', os.path.sep) for x in tuple(names)[0])
                        names = (os.path.sep.join((dirpath, basefile, x)) for x in names)
                        yield from names
                    # elif ext == '.tmp' and os.path.splitext(basefile)[-1] == self.container_ext:
                    #     basefile, ext = os.path.splitext(basefile)
                    #     names = self._get_zip_keys(f)
                    #     names = ("/".join((dirpath, basefile,x)) for x in names)
                    #     yield from names
                    else:
                        yield os.path.sep.join((dirpath, f))

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return sum(1 for _ in self.keys())
