


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
