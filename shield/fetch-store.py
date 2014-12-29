#!/usr/bin/env python

import math, os
import boto
import boto.s3.connection
import logging
from celery import Celery
from filechunkio import FileChunkIO

# Settings
access_key = 'put your access key here!'
secret_key = 'put your secret key here!'
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

def store (source_path, b):
	source_size = os.stat(source_path).st_size
	chunk_count = int(math.ceil(source_size / chunk_size))

    mp = b.initiate_multipart_upload(os.path.basename(source_path))

    for i in range(chunk_count + 1):
    	offset = chunk_size * i
    	bytes = min(chunk_size, source_size - offset)
    	with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
    		mp.upload_part_from_file(fp, part_num=i + 1)

    mp.complete_upload()

@app.task
def fetchstore(url, bucket):
	b = conn.get_bucket(bucket)

	local_filename = url.split('/')[-1]
	source_path = "/tmp/%s" % local_filename

	try:
    	download(url, source_path)
    	store(source_path, b)
    except:
    	echo "fetch & store failed"

