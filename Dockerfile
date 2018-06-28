FROM python:3-slim-stretch

RUN apt update && apt install -y build-essential libmariadbclient-dev

RUN pip install Twisted[tls]

# Make folders and copy in all code
RUN mkdir /app && mkdir /log && mkdir /log/digital_editions
WORKDIR /app
ADD . /app

# Install sls_api package
RUN pip install -e .

CMD ["./start-container.sh"]
