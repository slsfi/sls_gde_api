FROM python:3-slim-stretch

RUN apt update && apt install -y build-essential libmariadbclient-dev

RUN pip install Twisted[tls]

# Make folders and copy in all code
RUN mkdir /app && mkdir /log && mkdir /log/digital_editions
WORKDIR /app
<<<<<<< HEAD
ADD . /app
=======
COPY openapi.json /app
COPY setup.py /app
RUN mkdir /app/sls_api && mkdir /log && mkdir /log/digital_editions

COPY sls_api/__init__.py /app/sls_api/__init__.py
COPY sls_api/models.py /app/sls_api/models.py
COPY sls_api/endpoints /app/sls_api/endpoints
>>>>>>> 65a3a1376ce5407f9547bb1d1db7c29120b8dd4f

# Install sls_api package
RUN pip install -e .

CMD ["./start-container.sh"]
