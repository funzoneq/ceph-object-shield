# Installation

	sudo pip install virtualenv
	pip install -r requirements.txt
	brew install redis nginx
	mkdir -p /usr/local/etc/nginx/conf.d/
	cp nginx.conf /usr/local/etc/nginx/conf.d/
	nginx reload
