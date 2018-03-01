FROM tiangolo/uwsgi-nginx-flask:python3.6

# Remove demo app and add sls_api files to /app folder
RUN rm -rf /app
WORKDIR /app
COPY setup.py /app
COPY uwsgi.ini /app
RUN mkdir /app/sls_api
COPY sls_api/__init__.py /app/sls_api/__init__.py
COPY sls_api/endpoints /app/sls_api/endpoints

# Install sls_api package
RUN pip install -e .
