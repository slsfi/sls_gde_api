FROM python:3.6-slim-stretch

# build-essential is needed to build some libraries (mainly uwsgi and the various database support ones)
# libmariadbclient-dev is needed to build mysqlclient for mysql/mariadb support
RUN apt update && apt install -y build-essential libmariadbclient-dev

# create uwsgi user for uWSGI to run as (running as root is a Bad Idea, generally)
RUN useradd -ms /bin/bash uwsgi
RUN mkdir /app
RUN chown -R uwsgi /app

# drop into uwsgi user to copy over API files, should ensure proper permissions for these files
USER uwsgi
WORKDIR /app
COPY . /app/

# drop back into root in order to install API and required libraries
USER root
RUN pip install uwsgi
RUN pip install -e .

# finally drop back into uwsgi user to copy final files and run API
USER uwsgi
# Add SSH configuration to ssh config file for openssh agent
RUN mkdir ~/.ssh
RUN cat ssh_config >> ~/.ssh/config
RUN cp /app/ssh/* ~/.ssh/

# run using 4 workers with 1 process each, gracefully reload idle workers if they've reserved >500MB RAM
CMD ["uwsgi", \
     "--socket", "0.0.0.0:3031", \
     "--uid", "uwsgi", \
     "--plugins", "python3", \
     "--protocol", "uwsgi", \
     "--wsgi", "sls_api:app", \
     "--master", \
     "--enable-threads", \
     "--processes", "1", \
     "--workers", "4", \
     "--reload-on-rss", "500" \
]
