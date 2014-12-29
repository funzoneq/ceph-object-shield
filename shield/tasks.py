#!/usr/bin/env python

import math, os, sys
import boto
import boto.s3.connection
import logging
import requests
from celery import Celery
from filechunkio import FileChunkIO
from pprint import pprint
from urlparse import urlparse
from boto.s3.key import Key

# Settings
access_key = 'AJWEW9J8XWXXDF9BSQS9'
secret_key = 'maLnZfwBZGYS4JaejiaYH5Rh4oFOqwBgbBQjGjyj'
chunk_size = 52428800

# Setup debug logger
logging.basicConfig(filename='fetcher.log',level=logging.DEBUG)

# Connect to Ceph
conn = boto.connect_s3(
			aws_access_key_id = access_key,
			aws_secret_access_key = secret_key,
			host = 'ceph-svc-003.leasewebcdn.com',
			is_secure=False,
			calling_format = boto.s3.connection.OrdinaryCallingFormat(),
		)

# Setup Celery Queue
queue = Celery('lswshield', broker='redis://localhost:6379/0')

def download (url, path):
    r = requests.get(url, stream=True)
    with open(path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()

def store (key, source_path, b):
    source_size = os.stat(source_path).st_size
    chunk_count = int(math.ceil(source_size / chunk_size))

    mp = b.initiate_multipart_upload(key)

    for i in range(chunk_count + 1):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)

    mp.complete_upload()

    k = Key(b)
    k.key = key
    k.set_acl('public-read')

@queue.task
def fetchstore(url, bucketname):
    logging.debug("connecting to bucket: %s" % bucketname)
    try:
        bucket = conn.get_bucket(bucketname)
    except:
        bucket = conn.create_bucket(bucketname)
        bucket.set_acl('public-read')

    local_filename = url.split('/')[-1]
    source_path = "/tmp/%s" % local_filename
    key = urlparse(url).path

    try:
        logging.debug("Downloading %s as to %s" % (url, source_path))
        download(url, source_path)
        store(key, source_path, bucket)
        os.remove(source_path)
    except (NameError, AttributeError) as e:
        pprint(e)
    except:
        print "Unexpected error:", sys.exc_info()[0]

