#!/bin/bash
source slsapi/bin/activate
pip install -e .
export FLASK_APP=/Users/toffe/dev/sls/generic-edition/sls_api/sls_api/__init__.py
export FLASK_DEBUG=1
flask run

