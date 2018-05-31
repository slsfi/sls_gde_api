#!/bin/bash
source slsapi/bin/activate
pip install --upgrade -e .
export FLASK_APP=${PWD}/sls_api/__init__.py
export FLASK_DEBUG=1
ip="$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' |head -n 1)"
flask run --host $ip
