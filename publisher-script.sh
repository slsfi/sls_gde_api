#!/bin/bash

docker-compose exec backend python /app/sls_api/scripts/publisher.py ${@:1}
