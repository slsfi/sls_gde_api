FROM tiangolo/uwsgi-nginx-flask:python2.7

# Remove demo app and add sls_api files to /app folder
WORKDIR /app
RUN rm -rf /app
ADD . /app

# Install sls_api package
RUN pip install -e .
