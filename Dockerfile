FROM python:3-slim-stretch

RUN apt update && apt install -y build-essential

RUN pip install Twisted[tls]

RUN mkdir /app
WORKDIR /app
COPY openapi.json /app
COPY setup.py /app
RUN mkdir /app/sls_api
COPY sls_api/__init__.py /app/sls_api/__init__.py
COPY sls_api/endpoints /app/sls_api/endpoints

# Install sls_api package
RUN pip install -e .

CMD ["twistd", "-n", "web", "--notracebacks", "--wsgi", "sls_api.app"]
