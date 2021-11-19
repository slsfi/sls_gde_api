#!/bin/bash

docker-compose exec backend python /app/sls_api/scripts/password_reset.py ${@:1}
