#!/bin/bash
source slsapi/bin/activate
pip install -e .
export FLASK_APP=${PWD}sls_api/sls_api/__init__.py
export FLASK_DEBUG=1
flask run

