#!/usr/bin/env python

from flask import Flask, redirect, request
import redis
import requests
from celery import Celery
from pprint import pprint
import logging

# Setup debug logger
logging.basicConfig(filename='debug.log',level=logging.DEBUG)

# We have a flask app
app = Flask(__name__)

# Setup Celery Queue
queue = Celery('lswshield', broker='redis://localhost:6379/0')

# Setup Redis Cache
rc = redis.StrictRedis(host='localhost', port=6379, db=0)

# TODO: Fetch this from CDN database
origin = { "dumpert": "http://origin.dumpert.nl" }

class ObjectShield:
	def get_bucket (self, host):
		return host.split(".")[0]

	def ceph_url (self, bucket, file):
		return "http://%s.ceph-svc-003.leasewebcdn.com/%s" % (bucket, file)

	def origin_url (self, bucket, file):
		return "%s/%s" % (origin[bucket], file)

	def cache_key (self, bucket, file):
		return "%s-%s" % (bucket, file)

	def hit_cache(self, bucket, file):
		rc.get(self.cache_key(bucket, file))

	def set_hit_cache(self, bucket, file):
		rc.set(self.cache_key(bucket, file), True)

	def get_head (self, bucket, file):
		try:
			r = requests.head(self.ceph_url(bucket, file), timeout=2)
			if r.status_code == 200:
				return True
			else:
				return False
		except:
			logging.debug("failed head")
			return False

	def add_queue(self, url, bucket):
		return True


@app.route("/")
def hello():
	return "Leaseweb Origin Shield 0.0.1"

@app.route('/<path:path>')
def shieldlogic(path):
	os = ObjectShield()
	bucket = os.get_bucket(request.host)
	
	if os.hit_cache(bucket, path):
		logging.debug("cache hit, redirect to ceph")
		return redirect(os.ceph_url(bucket, path), code=302)
	else:
		logging.debug("cache miss, get head")
		if os.get_head(bucket, path):
			logging.debug("head hit, redirect to ceph")
			os.set_hit_cache(bucket, path)
			return redirect(os.ceph_url(bucket, path), code=302)
		else:
			logging.debug("head miss, redirect to origin")
			os.add_queue(os.origin_url(bucket, path), bucket)
			return redirect(os.origin_url(bucket, path), code=302)

if __name__ == "__main__":
    app.run()

	
	
