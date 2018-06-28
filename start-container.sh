#!/bin/bash
if [ -f ssl/cert.pem ]
then
    /bin/bash -c "twistd -n web --notracebacks -c ssl/cert.pem -k ssl/key.pem --https=8000 --wsgi sls_api.app"
else
    /bin/bash -c "twistd -n web --notracebacks --port tcp:port=8000 --wsgi sls_api.app"
fi
